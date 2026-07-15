package app

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDecodeInputFileCroissantJSONLDDataFile(t *testing.T) {
	outputDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(outputDir, "metadata"), 0755); err != nil {
		t.Fatalf("failed to create metadata dir: %v", err)
	}

	manifestPath := filepath.Join(outputDir, "sample.jsonld")
	manifest := `{
		"@type": "sc:Dataset",
		"name": "Sample Dataset",
		"alternateName": "SAMPLE-DATASET",
		"distribution": [
			{
				"@id": "file-1",
				"name": "labels.csv",
				"contentUrl": "https://example.org/data/labels.csv",
				"additionalProperty": [
					{
						"name": "TCIA download artifact role",
						"value": "data file"
					}
				]
			}
		]
	}`

	if err := os.WriteFile(manifestPath, []byte(manifest), 0644); err != nil {
		t.Fatalf("failed to write manifest: %v", err)
	}

	options := &Options{Output: outputDir}
	files, newJobs, err := decodeInputFile(context.Background(), manifestPath, http.DefaultClient, options, Callbacks{}, map[string]string{})
	if err != nil {
		t.Fatalf("decodeInputFile returned error: %v", err)
	}
	if newJobs != 0 {
		t.Fatalf("expected 0 new jobs for Croissant input, got %d", newJobs)
	}
	if len(files) != 1 {
		t.Fatalf("expected 1 file from Croissant data row, got %d", len(files))
	}

	if files[0].DownloadURL != "https://example.org/data/labels.csv" {
		t.Fatalf("unexpected DownloadURL: %s", files[0].DownloadURL)
	}
	if !strings.HasPrefix(files[0].SeriesInstanceUID, "croissant-") {
		t.Fatalf("expected croissant surrogate SeriesInstanceUID, got %s", files[0].SeriesInstanceUID)
	}
}

func TestDecodeInputFileCroissantManifestRowExpandsNestedSpreadsheet(t *testing.T) {
	outputDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(outputDir, "metadata"), 0755); err != nil {
		t.Fatalf("failed to create metadata dir: %v", err)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/manifest.csv" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/csv")
		_, _ = w.Write([]byte("drs_uri,name,collection\n12345,file-one.dcm,NESTED\n"))
	}))
	defer srv.Close()

	manifestPath := filepath.Join(outputDir, "nested.json")
	manifest := `{
		"@type": "sc:Dataset",
		"name": "Nested Dataset",
		"alternateName": "NESTED",
		"recordSet": [
			{
				"@id": "download-1",
				"name": "Radiology Images",
				"data": [
					{
						"download-1/download_url": "` + srv.URL + `/manifest.csv",
						"download-1/download_artifact_role": "manifest",
						"download-1/access_mechanism": "TCIA Data Retriever"
					}
				]
			}
		]
	}`

	if err := os.WriteFile(manifestPath, []byte(manifest), 0644); err != nil {
		t.Fatalf("failed to write manifest: %v", err)
	}

	options := &Options{Output: outputDir}
	files, _, err := decodeInputFile(context.Background(), manifestPath, srv.Client(), options, Callbacks{}, map[string]string{})
	if err != nil {
		t.Fatalf("decodeInputFile returned error: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("expected nested manifest to expand to 1 file, got %d", len(files))
	}

	if files[0].DRSURI != "drs://nci-crdc.datacommons.io/12345" {
		t.Fatalf("unexpected DRSURI from nested manifest expansion: %s", files[0].DRSURI)
	}
}

func TestDecodeInputFileCroissantSkipsTransferPackage(t *testing.T) {
	outputDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(outputDir, "metadata"), 0755); err != nil {
		t.Fatalf("failed to create metadata dir: %v", err)
	}

	manifestPath := filepath.Join(outputDir, "transfer.json")
	manifest := `{
		"@type": "sc:Dataset",
		"name": "Transfer Dataset",
		"distribution": [
			{
				"@id": "file-1",
				"name": "faspex",
				"contentUrl": "https://faspex.example.org/package",
				"additionalProperty": [
					{
						"name": "TCIA download artifact role",
						"value": "transfer package"
					},
					{
						"name": "TCIA access mechanism",
						"value": "Aspera"
					}
				]
			}
		]
	}`

	if err := os.WriteFile(manifestPath, []byte(manifest), 0644); err != nil {
		t.Fatalf("failed to write manifest: %v", err)
	}

	options := &Options{Output: outputDir}
	_, _, err := decodeInputFile(context.Background(), manifestPath, http.DefaultClient, options, Callbacks{}, map[string]string{})
	if err == nil {
		t.Fatalf("expected error when all Croissant rows are unsupported")
	}
	if !strings.Contains(err.Error(), "no actionable rows found") {
		t.Fatalf("expected actionable-rows error, got: %v", err)
	}
}
