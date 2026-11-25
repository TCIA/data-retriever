package app

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestReferenceKnownDOISkipsAllIdentityAndMetadataAPIs(t *testing.T) {
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if !strings.HasPrefix(r.URL.Path, "/cite/") {
			t.Errorf("known DOI requested identity metadata: %s", r.URL.Path)
			http.Error(w, "forbidden", 500)
			return
		}
		if r.Header.Get("Accept") == "application/x-bibtex" {
			_, _ = w.Write([]byte(`@misc{v,doi={10.1234/known},title={Known}}`))
		} else {
			_, _ = w.Write([]byte("Current citation (Version 2)."))
		}
	}))
	defer srv.Close()
	run := newCitationRun(&Options{Input: "known.csv", DOIResolverURL: srv.URL + "/v2/files/resolve-dois"}, srv.Client())
	run.crossciteBase = srv.URL + "/cite/"
	run.observeFiles([]*FileInfo{{DatasetDOI: "10.1234/known", DRSURI: "drs://example.org/file", Collection: "Known", DatasetCitation: "Supplied historical citation (Version 1)."}})
	root := t.TempDir()
	for range 2 {
		if err := run.write(context.Background(), root, nil); err != nil {
			t.Fatal(err)
		}
	}
	if requests.Load() != 2 {
		t.Fatalf("unexpected lookup count: %d", requests.Load())
	}
	entry := readReferenceReport(t, root, run.input)[0]
	if entry.CitationVersion != "1" || entry.BibTeXVersion != "2" {
		t.Fatalf("lost supplied version: %+v", entry)
	}
}

func TestReferenceBareLocatorsResolveMultipleDatasetsAndCorrelateByID(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			var request struct {
				Locators []referenceLocator `json:"locators"`
			}
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Error(err)
				return
			}
			if len(request.Locators) != 2 {
				t.Errorf("expected two unique locators: %+v", request)
			}
			results := []referenceResolverResult{}
			for i := len(request.Locators) - 1; i >= 0; i-- {
				locator := request.Locators[i]
				doi := "10.1234/" + strings.TrimPrefix(locator.Value, "drs://example.org/")
				results = append(results, referenceResolverResult{referenceLocator: locator, Status: "matched", Matches: []referenceResolverMatch{{Name: doi, DOI: doi, Page: "https://example.org/dataset"}}})
			}
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"results": results, "release_fingerprint": "test-release"})
			return
		}
		doi := strings.TrimPrefix(r.URL.Path, "/cite/")
		if r.Header.Get("Accept") == "application/x-bibtex" {
			_, _ = fmt.Fprintf(w, "@misc{v,doi={%s},title={Dataset}}", doi)
		} else {
			_, _ = fmt.Fprintf(w, "Citation for %s.", doi)
		}
	}))
	defer srv.Close()
	run := newCitationRun(&Options{Input: "bare.csv", DOIResolverURL: srv.URL + "/v2/files/resolve-dois"}, srv.Client())
	run.crossciteBase = srv.URL + "/cite/"
	for _, id := range []string{"a", "b"} {
		run.observeFiles([]*FileInfo{{DRSURI: "drs://example.org/" + id, SeriesInstanceUID: id}})
		run.recordEvent(SeriesEvent{SeriesInstanceUID: id, Status: seriesStatusSucceeded})
	}
	root := t.TempDir()
	if err := run.write(context.Background(), root, nil); err != nil {
		t.Fatal(err)
	}
	entries := readReferenceReport(t, root, run.input)
	if len(entries) != 2 {
		t.Fatalf("different datasets collapsed: %+v", entries)
	}
	for _, entry := range entries {
		if entry.DOI == "" || entry.ResolverFingerprint != "test-release" || entry.TransferResults[seriesStatusSucceeded] != 1 {
			t.Fatalf("wrong file mapping/results: %+v", entry)
		}
	}
}

func TestReferenceResolverBatchesAndDeduplicatesLocators(t *testing.T) {
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		var input struct {
			Locators []referenceLocator `json:"locators"`
		}
		_ = json.NewDecoder(r.Body).Decode(&input)
		if len(input.Locators) > 200 {
			t.Error("oversized batch")
		}
		results := []referenceResolverResult{}
		for _, locator := range input.Locators {
			results = append(results, referenceResolverResult{referenceLocator: locator, Status: "matched", Matches: []referenceResolverMatch{{Name: "Dataset", DOI: "10.1234/a"}}})
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"results": results})
	}))
	defer srv.Close()
	run := newCitationRun(&Options{Input: "large.csv", DOIResolverURL: srv.URL}, srv.Client())
	for i := 0; i < 201; i++ {
		file := &FileInfo{DRSURI: fmt.Sprintf("drs://example.org/%d", i), SeriesInstanceUID: fmt.Sprint(i)}
		run.observeFiles([]*FileInfo{file, file})
	}
	if err := run.resolvePendingFiles(context.Background()); err != nil {
		t.Fatal(err)
	}
	if requests.Load() != 2 || len(run.entries) != 1 || len(run.pending) != 0 || len(run.entries["doi:10.1234/a"].series) != 201 {
		t.Fatalf("bad batching/deduplication: requests=%d entries=%d", requests.Load(), len(run.entries))
	}
}

func TestReferenceUnmatchedOrAmbiguousNeverFallsBackToNameGuess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("unexpected name/DOI lookup: %s", r.URL.Path)
			http.Error(w, "forbidden", 500)
			return
		}
		var input struct {
			Locators []referenceLocator `json:"locators"`
		}
		_ = json.NewDecoder(r.Body).Decode(&input)
		results := []referenceResolverResult{}
		for _, locator := range input.Locators {
			status := "unmatched"
			if strings.HasSuffix(locator.Value, "b") {
				status = "ambiguous"
			}
			results = append(results, referenceResolverResult{referenceLocator: locator, Status: status})
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"results": results})
	}))
	defer srv.Close()
	run := newCitationRun(&Options{Input: "unmatched.csv", DOIResolverURL: srv.URL + "/v2/files/resolve-dois"}, srv.Client())
	for _, id := range []string{"a", "b"} {
		run.observeFiles([]*FileInfo{{DRSURI: "drs://example.org/" + id, Collection: "Untrusted name", SeriesInstanceUID: id}})
	}
	root := t.TempDir()
	if err := run.write(context.Background(), root, nil); err != nil {
		t.Fatal(err)
	}
	entries := readReferenceReport(t, root, run.input)
	if len(entries) != 2 {
		t.Fatalf("unresolved file identities collapsed: %+v", entries)
	}
	for _, entry := range entries {
		if entry.DOI != "" || !strings.HasPrefix(entry.IdentityResolution, "File locator ") {
			t.Fatalf("guessed DOI: %+v", entry)
		}
	}
}

func TestReferenceResolverFailurePreservesSuccessfulTransfer(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "unavailable secret", 404) }))
	defer srv.Close()
	run := newCitationRun(&Options{Input: "failure.csv", DOIResolverURL: srv.URL}, srv.Client())
	run.observeFiles([]*FileInfo{{DRSURI: "drs://example.org/file", SeriesInstanceUID: "one"}})
	run.recordEvent(SeriesEvent{SeriesInstanceUID: "one", Status: seriesStatusSucceeded})
	root := t.TempDir()
	if err := run.write(context.Background(), root, nil); err != nil {
		t.Fatal(err)
	}
	entry := readReferenceReport(t, root, run.input)[0]
	if entry.DOI != "" || entry.TransferResults[seriesStatusSucceeded] != 1 || len(run.warnings) == 0 || strings.Contains(strings.Join(run.warnings, " "), "secret") {
		t.Fatalf("bad failure handling: %+v warnings=%v", entry, run.warnings)
	}
}

func TestReferenceSignedURLIsNotForwarded(t *testing.T) {
	file := &FileInfo{DownloadURL: "https://example.org/file.tif?token=secret", SeriesInstanceUID: "image-1"}
	if locator, ok := fileReferenceLocator(file); ok {
		t.Fatalf("signed URL would be sent: %+v", locator)
	}
}

func TestReferenceResolverRejectsIncorrectCorrelationAndRecoversOnRetry(t *testing.T) {
	var corrupt atomic.Bool
	corrupt.Store(true)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var input struct {
			Locators []referenceLocator `json:"locators"`
		}
		_ = json.NewDecoder(r.Body).Decode(&input)
		locator := input.Locators[0]
		if corrupt.Load() {
			locator.Value = "drs://example.org/wrong"
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"results": []referenceResolverResult{{referenceLocator: locator, Status: "matched", Matches: []referenceResolverMatch{{Name: "Correct dataset", DOI: "10.1234/correct"}}}}})
	}))
	defer srv.Close()
	run := newCitationRun(&Options{Input: "retry.csv", DOIResolverURL: srv.URL}, srv.Client())
	run.observeFiles([]*FileInfo{{DRSURI: "drs://example.org/correct", SeriesInstanceUID: "one"}})
	if err := run.resolvePendingFiles(context.Background()); err == nil {
		t.Fatal("incorrect locator response was accepted")
	}
	for _, entry := range run.entries {
		if entry.DOI != "" {
			t.Fatal("assigned an uncorrelated DOI")
		}
	}
	corrupt.Store(false)
	if err := run.resolvePendingFiles(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(run.entries) != 1 || run.entries["doi:10.1234/correct"] == nil || len(run.pending) != 0 {
		t.Fatalf("retry failed: %+v", run.entries)
	}
}
