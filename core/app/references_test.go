package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

func readReferenceReport(t *testing.T, output, input string) []*datasetReference {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(referenceDirectory(output, input), "references.json"))
	if err != nil {
		t.Fatal(err)
	}
	var report struct {
		Datasets []*datasetReference `json:"datasets"`
	}
	if err = json.Unmarshal(body, &report); err != nil {
		t.Fatal(err)
	}
	return report.Datasets
}

func TestReferencesIncludeExcludedDatasetsAndSeparateTransferResults(t *testing.T) {
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		requests.Add(1)
		doi := strings.TrimPrefix(req.URL.Path, "/cite/")
		if req.Header.Get("Accept") != "application/x-bibtex" {
			t.Error("supplied citation text should be retained")
			http.Error(w, "unexpected", 500)
			return
		}
		_, _ = fmt.Fprintf(w, "@misc{remote_key, doi = {%s}, title = {Dataset}}", doi)
	}))
	defer srv.Close()
	root := t.TempDir()
	input := filepath.Join(root, "manifest.jsonld")
	manifest := `{"@type":"Dataset","@id":"https://doi.org/10.1234/DERIVED","name":"Derived dataset","citeAs":"Exact supplied citation, Version 2.","distribution":[{"@id":"derived","name":"labels.csv","contentUrl":"https://example.org/labels.csv"}],"isBasedOn":[{"@type":"Dataset","@id":"doi:10.1234/SOURCE","name":"Source dataset","citeAs":"Exact source citation."},{"@type":"DataDownload","@id":"source","name":"source.zip","contentUrl":"https://example.org/source.zip","isPartOf":{"@type":"Dataset","@id":"https://doi.org/10.1234/source","name":"Source dataset","citeAs":"Exact source citation."}}]}`
	if err := os.WriteFile(input, []byte(manifest), 0600); err != nil {
		t.Fatal(err)
	}
	r := newCitationRun(&Options{Input: input}, srv.Client())
	r.crossciteBase = srv.URL + "/cite/"
	r.observeFiles([]*FileInfo{{Collection: "Derived", DatasetDOI: "10.1234/derived", SeriesInstanceUID: "one"}, {Collection: "Derived", DatasetDOI: "https://doi.org/10.1234/DERIVED", SeriesInstanceUID: "two"}})
	r.recordEvent(SeriesEvent{SeriesInstanceUID: "one", Status: seriesStatusSucceeded})
	r.recordEvent(SeriesEvent{SeriesInstanceUID: "two", Status: seriesStatusFailed})
	if err := r.write(context.Background(), root, errors.New("partial transfer failure")); err != nil {
		t.Fatal(err)
	}
	entries := readReferenceReport(t, root, input)
	if len(entries) != 2 {
		t.Fatalf("wanted two DOI-deduplicated datasets, got %+v", entries)
	}
	for _, entry := range entries {
		if entry.DOI == "10.1234/derived" && (entry.TransferResults[seriesStatusSucceeded] != 1 || entry.TransferResults[seriesStatusFailed] != 1 || !containsReferenceString(entry.Selection, "Selected")) {
			t.Fatalf("wrong transfer/selection: %+v", entry)
		}
		if entry.DOI == "10.1234/source" && (!containsReferenceString(entry.Selection, "Excluded") || len(entry.TransferResults) != 0) {
			t.Fatalf("excluded source marked downloaded: %+v", entry)
		}
	}
	text, _ := os.ReadFile(filepath.Join(referenceDirectory(root, input), "references.txt"))
	bib, _ := os.ReadFile(filepath.Join(referenceDirectory(root, input), "references.bib"))
	if !strings.Contains(string(text), "Exact supplied citation, Version 2.") || !strings.Contains(string(text), "Exact source citation.") || strings.Count(string(bib), "@misc{") != 2 || requests.Load() != 2 {
		t.Fatalf("bad export or redundant lookup: %s\n%s; requests %d", text, bib, requests.Load())
	}
	// A retry replaces this manifest's report rather than appending duplicates.
	if err := r.write(context.Background(), root, nil); err != nil {
		t.Fatal(err)
	}
	if len(readReferenceReport(t, root, input)) != 2 {
		t.Fatal("retry appended duplicate references")
	}
}

func TestReferencesResolveOnlyUniqueVisibleWordPressIdentity(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/wp/collections/":
			_, _ = w.Write([]byte(`{"total_pages":1,"results":[{"collection_short_title":"Study","collection_doi":"10.1234/study","collection_downloads":[{"id":42}],"citations":[{"type":"Data Citation","doi":"10.1234/study","html":"An &amp; B. <strong>Study</strong> (Version 3)."}]},{"collection_short_title":"Duplicate","collection_doi":"10.1234/one"},{"collection_short_title":"Duplicate","collection_doi":"10.1234/two"},{"collection_short_title":"Hidden","collection_doi":"10.1234/hidden","hide_from_browse_table":true}]}`))
		case "/wp/analysis-results/":
			_, _ = w.Write([]byte(`{"total_pages":1,"results":[]}`))
		default:
			http.Error(w, "DOI service unavailable", http.StatusServiceUnavailable)
		}
	}))
	defer srv.Close()
	root := t.TempDir()
	input := filepath.Join(root, "input.csv")
	r := newCitationRun(&Options{Input: input}, srv.Client())
	r.wordpressBase = srv.URL + "/wp"
	r.crossciteBase = srv.URL + "/cite/"
	r.add("Study", "", "", "", "Selected", "", "42")
	r.add("Duplicate", "", "", "", "Selected", "", "")
	r.add("Hidden", "", "", "", "Referenced", "", "")
	if err := r.write(context.Background(), root, nil); err != nil {
		t.Fatal(err)
	}
	entries := readReferenceReport(t, root, input)
	for _, entry := range entries {
		if entry.Name == "Study" && (entry.DOI != "10.1234/study" || entry.Citation != "An & B. Study (Version 3)." || entry.BibTeX != "") {
			t.Fatalf("bad WordPress citation or failure handling: %+v", entry)
		}
		if entry.Name != "Study" && entry.DOI != "" {
			t.Fatalf("ambiguous/hidden identity accepted: %+v", entry)
		}
	}
}

func TestReferencesCancellationAndManifestIsolation(t *testing.T) {
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		requests.Add(1)
		http.Error(w, "unexpected request", 500)
	}))
	defer srv.Close()
	out := t.TempDir()
	a := filepath.Join(out, "a", "input.csv")
	b := filepath.Join(out, "b", "input.csv")
	if referenceDirectory(out, a) == referenceDirectory(out, b) {
		t.Fatal("same-basename manifests collide")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, input := range []string{a, b} {
		r := newCitationRun(&Options{Input: input}, srv.Client())
		r.wordpressBase = srv.URL
		r.crossciteBase = srv.URL + "/"
		r.add("Dataset", "10.1234/test", "", "Supplied citation v1", "Selected", "", "")
		r.add("Dataset", "https://doi.org/10.1234/TEST", "", "Supplied citation v2", "Excluded", "", "")
		if err := r.write(ctx, out, ctx.Err()); err != nil {
			t.Fatal(err)
		}
		text, err := os.ReadFile(filepath.Join(referenceDirectory(out, input), "references.txt"))
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(text), "Cancelled") || !strings.Contains(string(text), "Supplied citation v1") || !strings.Contains(string(text), "Supplied citation v2") {
			t.Fatal(string(text))
		}
	}
	if requests.Load() != 0 {
		t.Fatal("cancelled run attempted remote citation lookup")
	}
}

func TestReferencesRejectMismatchedBibTeXDOI(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		_, _ = w.Write([]byte(`@misc{wrong,doi={10.1234/ab},title={Wrong dataset}}`))
	}))
	defer srv.Close()
	entries := []*datasetReference{{DOI: "10.1234/a", Citation: "Exact supplied citation", Resolution: "Unresolved"}}
	resolveReferenceCitations(context.Background(), srv.Client(), srv.URL+"/", entries)
	if entries[0].BibTeX != "" || entries[0].Citation != "Exact supplied citation" {
		t.Fatal("mismatched DOI replaced a supplied citation")
	}
}

func TestReferencesCitationFallbackIsPlainText(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Accept") == "application/x-bibtex" {
			_, _ = w.Write([]byte(`@misc{test,doi={10.1234/study},title={Study}}`))
			return
		}
		_, _ = w.Write([]byte(`Author. <i>Study &amp; annotations</i> (Version 2) [Dataset]. https://doi.org/10.1234/study`))
	}))
	defer srv.Close()
	entries := []*datasetReference{{DOI: "10.1234/study", Resolution: "Unresolved"}}
	resolveReferenceCitations(context.Background(), srv.Client(), srv.URL+"/", entries)
	if entries[0].Citation != "Author. Study & annotations (Version 2) [Dataset]. https://doi.org/10.1234/study" || entries[0].BibTeX == "" {
		t.Fatalf("bad citation formats: %+v", entries[0])
	}
}

func TestReferencesFindNestedDatasetIdentifiers(t *testing.T) {
	r := newCitationRun(&Options{Input: "unused.csv"}, nil)
	r.addCroissant(map[string]interface{}{"@type": "Dataset", "name": "Primary", "identifier": "https://doi.org/10.1234/primary", "isBasedOn": []interface{}{map[string]interface{}{"@type": "Dataset", "name": "Source", "identifier": "doi:10.1234/source", "isBasedOn": map[string]interface{}{"@type": "Dataset", "@id": "https://doi.org/10.1234/nested", "name": "Nested"}}}}, &Options{})
	if _, ok := r.entries["doi:10.1234/primary"]; !ok {
		t.Fatal("root identifier not retained")
	}
	if _, ok := r.entries["doi:10.1234/nested"]; !ok {
		t.Fatal("nested dataset omitted")
	}
}

func TestReferencesKeepAnalysisResultAndSourceDOIsDistinct(t *testing.T) {
	r := newCitationRun(&Options{Input: "unused.csv"}, nil)
	r.observeFiles([]*FileInfo{{Collection: "Source collection", SourceDOI: "10.1234/source", DatasetDOI: "10.1234/derived", AnalysisResultID: "Derived analysis", SeriesInstanceUID: "one"}})
	if len(r.entries) != 2 || r.entries["doi:10.1234/derived"].Name != "Derived analysis" || r.entries["doi:10.1234/source"].Name != "Source collection" {
		t.Fatalf("source and derived citation identities mixed: %+v", r.entries)
	}
}
