package app

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestSpreadsheetPreservesDownloadTerms(t *testing.T) {
	for _, ext := range []string{".csv", ".tsv"} {
		t.Run(ext, func(t *testing.T) {
			content := "imageUrl,LicenseName,LicenseURI,source_DOI,DatasetDOI,download_id,manifest_url\nhttps://example.org/file.zip,CC BY 4.0,https://creativecommons.org/licenses/by/4.0/,10.1234/source,10.1234/derived,42,https://example.org/manifest.csv\nhttps://example.org/other.zip\n"
			if ext == ".tsv" {
				content = strings.ReplaceAll(content, ",", "\t")
			}
			path := filepath.Join(t.TempDir(), "manifest"+ext)
			if err := os.WriteFile(path, []byte(content), 0600); err != nil {
				t.Fatal(err)
			}
			files, err := decodeSpreadsheet(path)
			if err != nil {
				t.Fatal(err)
			}
			if len(files) != 2 || files[0].LicenseName != "CC BY 4.0" || files[0].LicenseURI == "" || files[0].DatasetDOI != "10.1234/derived" || files[0].SourceDOI != "10.1234/source" || files[0].WordPressDownloadID != "42" || files[1].LicenseURI != "" {
				t.Fatalf("terms lost or copied to wrong row: %+v", files)
			}
		})
	}
}

func TestCroissantNestedLicenseAndDOIPreserved(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("imageUrl\nhttps://example.org/image.tif\n"))
	}))
	defer srv.Close()
	out := t.TempDir()
	if err := os.Mkdir(filepath.Join(out, "metadata"), 0700); err != nil {
		t.Fatal(err)
	}
	manifest := map[string]interface{}{
		"@type": "Dataset", "@id": "https://doi.org/10.1234/DERIVED", "citeAs": "Exact dataset citation", "license": []string{"WRONG-OPEN", "WRONG-CONTROLLED"},
		"distribution": []interface{}{map[string]interface{}{"@id": "file", "name": "input.csv", "contentUrl": srv.URL + "/input.csv", "license": "https://creativecommons.org/licenses/by/4.0/", "additionalProperty": []interface{}{map[string]interface{}{"name": "TCIA download artifact role", "value": "manifest"}, map[string]interface{}{"name": "TCIA access mechanism", "value": "TCIA Data Retriever"}}}},
	}
	body, _ := json.Marshal(manifest)
	path := filepath.Join(out, "manifest.jsonld")
	if err := os.WriteFile(path, body, 0600); err != nil {
		t.Fatal(err)
	}
	files, err := decodeCroissant(context.Background(), path, srv.Client(), &Options{Output: out}, Callbacks{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || files[0].LicenseURI != "https://creativecommons.org/licenses/by/4.0/" || files[0].DatasetDOI != "10.1234/derived" || files[0].DatasetCitation != "Exact dataset citation" {
		t.Fatalf("nested metadata lost: %+v", files)
	}
	csv, err := os.ReadFile(filepath.Join(out, "metadata", "metadata.csv"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(csv), "10.1234/derived") || !strings.Contains(string(csv), "LicenseSource") {
		t.Fatal(string(csv))
	}
	file := &FileInfo{LicenseURI: "source-license", SourceDOI: "10.1234/source"}
	applyCroissantFileMetadata(file, croissantDatasetInfo{DOI: "10.1234/derived"}, croissantDownloadRow{License: "component-license", Group: CroissantGroupIsBasedOn})
	if file.LicenseURI != "source-license" || file.SourceDOI != "10.1234/source" || file.DatasetDOI != "" || file.LicenseConflict == "" {
		t.Fatalf("source metadata overwritten: %+v", file)
	}
}

func TestWordPressLicenseMatching(t *testing.T) {
	requests := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		_, _ = w.Write([]byte(`{"total_pages":1,"results":[{"id":42,"download_url":"https://example.org/unique.csv","data_license":{"label":"Controlled","url":"https://example.org/policy"}},{"id":43,"download_url":"https://a.example/duplicate.csv"},{"id":44,"download_url":"https://b.example/duplicate.csv"}]}`))
	}))
	defer srv.Close()
	resolver := &downloadMetadataResolver{client: srv.Client(), baseURL: srv.URL}
	file := &FileInfo{DRSURI: "drs://example.org/file", WordPressDownloadID: "42"}
	enrichDownloadMetadata(context.Background(), []*FileInfo{file}, "renamed.csv", resolver)
	if file.LicenseName != "Controlled" || file.LicenseURI != "https://example.org/policy" {
		t.Fatalf("license missing: %+v", file)
	}
	if record, _ := resolver.find(context.Background(), "", "", "duplicate.csv"); record != nil {
		t.Fatal("ambiguous filename accepted")
	}
	if record, _ := resolver.find(context.Background(), "999", "https://example.org/unique.csv", "unique.csv"); record != nil {
		t.Fatal("unmatched explicit ID fell back to filename")
	}
	if record, method := resolver.find(context.Background(), "", "", "unique.csv"); record == nil || method != "WordPress manifest filename" {
		t.Fatal("unique filename not resolved")
	}
	if requests != 1 {
		t.Fatalf("catalog fetched %d times", requests)
	}
}

func TestIncompleteWordPressCatalogDoesNotMatch(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("page") == "2" {
			http.Error(w, "unavailable", 503)
			return
		}
		_, _ = w.Write([]byte(`{"total_pages":2,"results":[{"id":42,"download_url":"https://example.org/manifest.csv"}]}`))
	}))
	defer srv.Close()
	r := &downloadMetadataResolver{client: srv.Client(), baseURL: srv.URL}
	if record, _ := r.find(context.Background(), "", "", "manifest.csv"); record != nil {
		t.Fatal("partial catalog established uniqueness")
	}
}

func TestSeriesManifestTermsKeepProviderLicense(t *testing.T) {
	path := filepath.Join(t.TempDir(), "input.csv")
	if err := os.WriteFile(path, []byte("SeriesInstanceUID,LicenseURI,DatasetDOI\n1.2.3,manifest-license,10.1234/derived\n"), 0600); err != nil {
		t.Fatal(err)
	}
	file := &FileInfo{SeriesInstanceUID: "1.2.3", LicenseURI: "provider-license"}
	applySeriesManifestMetadata(file, seriesManifestMetadata(path))
	if file.LicenseURI != "provider-license" || file.LicenseConflict == "" || file.DatasetDOI != "10.1234/derived" {
		t.Fatalf("wrong source precedence: %+v", file)
	}
}

func TestGenericLicenseColumnAndCompatibleURIFill(t *testing.T) {
	for _, license := range []string{"https://creativecommons.org/licenses/by/4.0/", "CC BY 4.0"} {
		path := filepath.Join(t.TempDir(), "manifest.csv")
		if err := os.WriteFile(path, []byte("imageUrl,license\nhttps://example.org/file,"+license+"\n"), 0600); err != nil {
			t.Fatal(err)
		}
		files, err := decodeSpreadsheet(path)
		if err != nil {
			t.Fatal(err)
		}
		if files[0].LicenseURI != license && files[0].LicenseName != license {
			t.Fatal("generic license discarded")
		}
	}
	file := &FileInfo{LicenseName: "CC BY 4.0", LicenseSource: "Manifest row"}
	applyLicenseMetadata(file, "NIH Controlled Data Access Policy", "https://example.org/controlled", "WordPress")
	if file.LicenseURI != "" {
		t.Fatal("unrelated license URI attached to existing label")
	}
	applyLicenseMetadata(file, "CC BY 4.0", "https://creativecommons.org/licenses/by/4.0/", "WordPress")
	if file.LicenseName != "CC BY 4.0" || file.LicenseURI == "" {
		t.Fatal("compatible missing URI was not filled")
	}
}

func TestWordPressPaginationIsConcurrentBoundedAndOrdered(t *testing.T) {
	var active, maxActive atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		page, _ := strconv.Atoi(r.URL.Query().Get("page"))
		if page > 1 {
			n := active.Add(1)
			defer active.Add(-1)
			for previous := maxActive.Load(); n > previous; previous = maxActive.Load() {
				if maxActive.CompareAndSwap(previous, n) {
					break
				}
			}
			time.Sleep(20 * time.Millisecond)
		}
		fmt.Fprintf(w, `{"total_pages":9,"results":[{"id":%d}]}`, page)
	}))
	defer srv.Close()
	rows, err := fetchWordPressRecords(context.Background(), srv.Client(), srv.URL, "downloads")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 9 || maxActive.Load() < 2 || maxActive.Load() > 4 {
		t.Fatalf("bad pagination: rows=%d max_concurrent=%d", len(rows), maxActive.Load())
	}
	for i, row := range rows {
		if metadataText(row, "id") != strconv.Itoa(i+1) {
			t.Fatal("page order changed")
		}
	}
}
