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
	"time"
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
		if strings.HasPrefix(req.URL.Path, "/wp/") {
			_, _ = w.Write([]byte(`{"total_pages":1,"results":[]}`))
			return
		}
		requests.Add(1)
		doi := strings.TrimPrefix(req.URL.Path, "/cite/")
		if req.Header.Get("Accept") != "application/x-bibtex" {
			_, _ = fmt.Fprintf(w, "Current DOI citation (Version 3). https://doi.org/%s", doi)
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
	r.resolverURL = srv.URL + "/v2/files/resolve-dois"
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
	if !strings.Contains(string(text), "Exact supplied citation, Version 2.") || !strings.Contains(string(text), "Exact source citation.") || strings.Count(string(bib), "@misc{") != 2 || requests.Load() != 4 {
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

func TestReferencesResolveOnlyUniqueSnapshotIdentity(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/v2/datasets/Study":
			_, _ = w.Write([]byte(`{"datasets":[{"short_title":"Study","doi":"10.1234/study","tcia_page":"https://example.org/study"}]}`))
		case "/v2/datasets/Duplicate":
			_, _ = w.Write([]byte(`{"datasets":[{"doi":"10.1234/one"},{"doi":"10.1234/two"}]}`))
		case "/v2/datasets/Hidden":
			http.NotFound(w, req)
		default:
			http.Error(w, "DOI service unavailable", 503)
		}
	}))
	defer srv.Close()
	root := t.TempDir()
	input := filepath.Join(root, "input.csv")
	r := newCitationRun(&Options{Input: input, DOIResolverURL: srv.URL + "/v2/files/resolve-dois"}, srv.Client())
	r.crossciteBase = srv.URL + "/cite/"
	for _, name := range []string{"Study", "Duplicate", "Hidden"} {
		r.add(name, "", "", "", "Selected", "", "")
	}
	if err := r.write(context.Background(), root, nil); err != nil {
		t.Fatal(err)
	}
	for _, entry := range readReferenceReport(t, root, input) {
		if entry.Name == "Study" && (entry.DOI != "10.1234/study" || entry.URL != "https://example.org/study" || entry.BibTeX != "") {
			t.Fatalf("bad snapshot identity: %+v", entry)
		}
		if entry.Name != "Study" && entry.DOI != "" {
			t.Fatalf("ambiguous/hidden identity accepted: %+v", entry)
		}
	}
}

func TestReferencesCitationIsCachedAcrossRuns(t *testing.T) {
	var wpRequests, citeRequests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/v2/datasets/Study":
			wpRequests.Add(1)
			_, _ = w.Write([]byte(`{"datasets":[{"short_title":"Study","doi":"10.1234/cached"}]}`))
		case "/cite/10.1234/cached":
			citeRequests.Add(1)
			if req.Header.Get("Accept") == "application/x-bibtex" {
				_, _ = w.Write([]byte(`@misc{cached,doi={10.1234/cached},title={Cached dataset}}`))
				return
			}
			_, _ = w.Write([]byte(`Author. Cached dataset.`))
		default:
			http.Error(w, "unexpected request", 500)
		}
	}))
	defer srv.Close()
	root := t.TempDir()

	// Formatting results are reused across manifests; identity uses one compact GET.
	for i, name := range []string{"first.csv", "second.csv"} {
		input := filepath.Join(root, name)
		r := newCitationRun(&Options{Input: input}, srv.Client())
		r.resolverURL = srv.URL + "/v2/files/resolve-dois"
		r.crossciteBase = srv.URL + "/cite/"
		r.add("Study", "", "", "", "Selected", "", "")
		if err := r.write(context.Background(), root, nil); err != nil {
			t.Fatal(err)
		}
		entries := readReferenceReport(t, root, input)
		if len(entries) != 1 || entries[0].DOI != "10.1234/cached" || entries[0].BibTeX == "" {
			t.Fatalf("run %d: expected resolved DOI and BibTeX, got %+v", i, entries)
		}
	}
	if wpRequests.Load() != 2 {
		t.Fatalf("expected one compact dataset lookup per manifest, got %d requests", wpRequests.Load())
	}
	if citeRequests.Load() != 2 {
		t.Fatalf("expected one crosscite citation request and one BibTeX request on the first run only, got %d requests", citeRequests.Load())
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

func TestReferenceCitationTimeoutScalesWithUniqueDOIs(t *testing.T) {
	cases := []struct {
		dois int
		want time.Duration
	}{
		{0, 30 * time.Second},
		{3, 30 * time.Second},
		{4, 30 * time.Second},
		{5, 50 * time.Second},
		{8, 50 * time.Second},
		{100, 3 * time.Minute}, // capped
	}
	for _, c := range cases {
		entries := make([]*datasetReference, 0, c.dois)
		for i := 0; i < c.dois; i++ {
			entries = append(entries, &datasetReference{DOI: fmt.Sprintf("10.1234/d%d", i)})
		}
		if got := referenceCitationTimeout(entries); got != c.want {
			t.Errorf("dois=%d: got %s, want %s", c.dois, got, c.want)
		}
	}
}
