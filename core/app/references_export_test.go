package app

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

func TestReferencesLookupFailureIsVisibleWithoutFailingRun(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	root := t.TempDir()
	run := newCitationRun(&Options{Input: "failed-catalog.csv", DOIResolverURL: srv.URL + "/v2/files/resolve-dois"}, srv.Client())
	run.add("Study", "", "", "", "Selected", "one", "")
	run.recordEvent(SeriesEvent{SeriesInstanceUID: "one", Status: seriesStatusSucceeded})
	if err := run.write(context.Background(), root, nil); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(filepath.Join(referenceDirectory(root, run.input), "references.json"))
	if err != nil {
		t.Fatal(err)
	}
	var report struct {
		RunOutcome string              `json:"run_outcome"`
		Warnings   []string            `json:"lookup_warnings"`
		Datasets   []*datasetReference `json:"datasets"`
	}
	if err := json.Unmarshal(body, &report); err != nil {
		t.Fatal(err)
	}
	if report.RunOutcome != "Completed" || len(report.Warnings) != 1 || report.Datasets[0].TransferResults[seriesStatusSucceeded] != 1 {
		t.Fatalf("missing warning or changed transfer: %s", body)
	}
	text, _ := os.ReadFile(filepath.Join(referenceDirectory(root, run.input), "references.txt"))
	if !strings.Contains(string(text), "Lookup warning:") || !strings.Contains(string(text), "https://www.cancerimagingarchive.net/") || strings.Contains(string(text), "consult the dataset page") {
		t.Fatalf("unhelpful unresolved text: %s", text)
	}
}

func TestReferenceCitationCacheDoesNotLeakSuppliedVersions(t *testing.T) {
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		requests.Add(1)
		if r.Header.Get("Accept") == "application/x-bibtex" {
			_, _ = w.Write([]byte(`@misc{remote,doi={10.1234/versions},title={Current dataset}}`))
		} else {
			_, _ = w.Write([]byte(`Current dataset (Version 3).`))
		}
	}))
	defer srv.Close()
	first := &datasetReference{DOI: "10.1234/versions", Citation: "Historical dataset (Version 1)."}
	second := &datasetReference{DOI: "10.1234/versions"}
	resolveReferenceCitations(context.Background(), srv.Client(), srv.URL+"/", []*datasetReference{first})
	resolveReferenceCitations(context.Background(), srv.Client(), srv.URL+"/", []*datasetReference{second})
	if first.CitationVersion != "1" || first.BibTeXVersion != "3" || !strings.Contains(first.BibTeX, "version = {3}") || second.Citation != "Current dataset (Version 3)." || requests.Load() != 2 {
		t.Fatalf("version/cache mixup: first=%+v second=%+v requests=%d", first, second, requests.Load())
	}
}

func TestReferencesRelatedBibTeXIsDeduplicatedAndAcknowledgementIsText(t *testing.T) {
	root := t.TempDir()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		if strings.Contains(r.URL.Path, "/wp/") {
			_, _ = w.Write([]byte(`{"total_pages":1,"results":[]}`))
			return
		}
		doi := strings.TrimPrefix(r.URL.Path, "/cite/")
		if r.Header.Get("Accept") == "application/x-bibtex" {
			_, _ = fmt.Fprintf(w, "@misc{remote,doi={%s},title={Reference}}", doi)
		} else {
			_, _ = w.Write([]byte("Reference (Version 2)."))
		}
	}))
	defer srv.Close()
	run := newCitationRun(&Options{Input: "related.csv"}, srv.Client())
	run.crossciteBase = srv.URL + "/cite/"
	run.resolverURL, run.crossciteBase = srv.URL+"/v2/files/resolve-dois", srv.URL+"/cite/"
	for _, doi := range []string{"10.1234/first", "10.1234/second"} {
		run.add(doi, doi, "https://example.org/dataset/", "Supplied dataset citation.", "Excluded", "", "")
		entry := run.entries["doi:"+doi]
		entry.addRelatedReference(&relatedReference{Type: "Publication Citation", DOI: "10.1234/shared", Citation: "Supplied related paper."})
		entry.addRelatedReference(&relatedReference{Type: "Acknowledgement", Citation: "Credit the team."})
	}
	if err := run.write(context.Background(), root, nil); err != nil {
		t.Fatal(err)
	}
	dir := referenceDirectory(root, run.input)
	bib, _ := os.ReadFile(filepath.Join(dir, "references.bib"))
	text, _ := os.ReadFile(filepath.Join(dir, "references.txt"))
	if strings.Count(string(bib), "@misc{") != 3 || !strings.Contains(string(bib), "% Credit the team.") || strings.Contains(string(bib), "doi = {}") || !strings.Contains(string(text), "Dataset page: https://example.org/dataset/") {
		t.Fatalf("bad related export:\n%s\n%s", bib, text)
	}
	for _, entry := range readReferenceReport(t, root, run.input) {
		if len(entry.TransferResults) != 0 || len(entry.RelatedReferences) != 2 {
			t.Fatalf("related reference changed selection/results: %+v", entry)
		}
	}
}

func TestReferenceDOIFallsBackToStandardNegotiation(t *testing.T) {
	var hosts []string
	client := &http.Client{Transport: referenceRoundTripper(func(req *http.Request) (*http.Response, error) {
		hosts = append(hosts, req.URL.Host)
		if req.Header.Get("Accept") != "application/x-bibtex" {
			t.Fatal("lost content negotiation")
		}
		status, body := 404, "Not found"
		if req.URL.Host == "doi.org" {
			status, body = 200, "@article{paper,doi={10.1234/paper}}"
		}
		return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
	})}
	bib, err := fetchReferenceDOIFormat(context.Background(), client, "https://data.crosscite.org/", "10.1234/paper", "application/x-bibtex")
	if err != nil || !strings.HasPrefix(bib, "@article") || strings.Join(hosts, ",") != "data.crosscite.org,doi.org" {
		t.Fatalf("fallback failed: %v %v %s", err, hosts, bib)
	}
}

type referenceRoundTripper func(*http.Request) (*http.Response, error)

func (f referenceRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func TestReferenceBibTeXVersionDoesNotOverwriteExistingVersion(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		if r.Header.Get("Accept") == "application/x-bibtex" {
			_, _ = w.Write([]byte(`@misc{v,doi={10.1234/existing},version={2},title={Dataset}}`))
		} else {
			_, _ = w.Write([]byte("Dataset (Version 3)."))
		}
	}))
	defer srv.Close()
	entry := &datasetReference{DOI: "10.1234/existing"}
	resolveReferenceCitations(context.Background(), srv.Client(), srv.URL+"/", []*datasetReference{entry})
	if entry.BibTeXVersion != "2" || entry.CitationVersion != "3" || strings.Contains(entry.BibTeX, "version = {3}") {
		t.Fatalf("overwrote version: %+v", entry)
	}
}

func TestReferenceDOIFailureIsNotCached(t *testing.T) {
	var fail atomic.Bool
	fail.Store(true)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		if fail.Load() {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		if r.Header.Get("Accept") == "application/x-bibtex" {
			_, _ = w.Write([]byte(`@misc{v,doi={10.1234/recover},title={Dataset}}`))
		} else {
			_, _ = w.Write([]byte("Recovered citation."))
		}
	}))
	defer srv.Close()
	entry := &datasetReference{DOI: "10.1234/recover", Resolution: "Unresolved"}
	resolveReferenceCitations(context.Background(), srv.Client(), srv.URL+"/", []*datasetReference{entry})
	fail.Store(false)
	resolveReferenceCitations(context.Background(), srv.Client(), srv.URL+"/", []*datasetReference{entry})
	if entry.Citation != "Recovered citation." || entry.BibTeX == "" {
		t.Fatalf("retry cached failure: %+v", entry)
	}
}

func TestReferencesResolveMissingDOIThenUseDOICitation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v2/datasets/Study" {
			_, _ = w.Write([]byte(`{"datasets":[{"short_title":"Study","doi":"10.1234/catalog-version"}]}`))
			return
		}
		if r.Header.Get("Accept") == "application/x-bibtex" {
			_, _ = w.Write([]byte(`@misc{v,doi={10.1234/catalog-version},title={Study}}`))
		} else {
			_, _ = w.Write([]byte("Current DOI citation (Version 2)."))
		}
	}))
	defer srv.Close()
	run := newCitationRun(&Options{Input: "catalog-version.csv", DOIResolverURL: srv.URL + "/v2/files/resolve-dois"}, srv.Client())
	run.crossciteBase = srv.URL + "/cite/"
	run.observeFiles([]*FileInfo{{Collection: "Study", SeriesInstanceUID: "1.2.3"}})
	root := t.TempDir()
	if err := run.write(context.Background(), root, nil); err != nil {
		t.Fatal(err)
	}
	entry := readReferenceReport(t, root, run.input)[0]
	if entry.Citation != "Current DOI citation (Version 2)." || entry.CitationVersion != "2" || entry.BibTeXVersion != "2" {
		t.Fatalf("bad formatting: %+v", entry)
	}
}
