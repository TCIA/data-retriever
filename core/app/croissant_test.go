package app

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type croissantRoundTripFunc func(*http.Request) (*http.Response, error)

func (fn croissantRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}

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

func TestCroissantExamplesDiscoverExternalRecordSetManifests(t *testing.T) {
	tests := []struct {
		name      string
		fixture   string
		fileNames []string
	}{
		{
			name:      "4D-Lung public DICOM",
			fixture:   "4d-lung.croissant.jsonld",
			fileNames: []string{"42107-public-dicom-series.csv"},
		},
		{
			name:    "CMB-AML mixed routes",
			fixture: "cmb-aml.croissant.jsonld",
			fileNames: []string{
				"41647-aspera-pathology-files.csv",
				"48105-public-dicom-series.csv",
				"48107-controlled-drs-files.csv",
			},
		},
		{
			name:      "HNSCC PathDB",
			fixture:   "hnscc-mif-mihc-comparison.croissant.jsonld",
			fileNames: []string{"pathdb-images.csv"},
		},
		{
			name:    "SAROS derived files",
			fixture: "saros.croissant.jsonld",
			fileNames: []string{
				"46289-segmentation-files.csv",
				"46291-data-records.csv",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			content, err := os.ReadFile(filepath.Join("testdata", "croissant-examples", tt.fixture))
			if err != nil {
				t.Fatalf("failed to read fixture: %v", err)
			}
			var doc map[string]interface{}
			if err := json.Unmarshal(content, &doc); err != nil {
				t.Fatalf("failed to parse fixture: %v", err)
			}

			rows := extractCroissantRows(doc)
			manifestRows := make([]croissantDownloadRow, 0, len(rows))
			for _, row := range rows {
				if resolveCroissantArtifactRole(row) == croissantRoleManifest {
					manifestRows = append(manifestRows, row)
				}
			}
			if len(manifestRows) != len(tt.fileNames) {
				t.Fatalf("expected %d external record-set manifests, got %d", len(tt.fileNames), len(manifestRows))
			}
			for i, expectedName := range tt.fileNames {
				if manifestRows[i].FileName != expectedName {
					t.Fatalf("row %d: expected %q, got %q", i, expectedName, manifestRows[i].FileName)
				}
				if resolveCroissantArtifactRole(manifestRows[i]) != croissantRoleManifest {
					t.Fatalf("row %d: expected nested manifest role", i)
				}
				if !isCroissantDataRetrieverRow(manifestRows[i]) {
					t.Fatalf("row %d: expected Data Retriever access mechanism", i)
				}
			}
		})
	}
}

func TestDecodeCroissantExampleExpandsPathDBRecordSet(t *testing.T) {
	outputDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(outputDir, "metadata"), 0755); err != nil {
		t.Fatalf("failed to create metadata dir: %v", err)
	}

	client := &http.Client{Transport: croissantRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		if !strings.HasSuffix(req.URL.Path, "/pathdb-images.csv") {
			return &http.Response{
				StatusCode: http.StatusNotFound,
				Status:     "404 Not Found",
				Body:       io.NopCloser(strings.NewReader("not found")),
				Header:     make(http.Header),
				Request:    req,
			}, nil
		}
		csv := "Collection,PatientID,SlideID,imageUrl,FileFormat,accessLevel,retrievalRoute\n" +
			"HNSCC-mIF-mIHC-Comparison,HNSCC-01,slide-1,https://pathdb.example.org/slide-1.ome.tif,OME-TIFF,public,pathdb\n" +
			"HNSCC-mIF-mIHC-Comparison,HNSCC-02,slide-2,https://pathdb.example.org/slide-2.svs,SVS,public,pathdb\n"
		return &http.Response{
			StatusCode: http.StatusOK,
			Status:     "200 OK",
			Body:       io.NopCloser(strings.NewReader(csv)),
			Header:     http.Header{"Content-Type": []string{"text/csv"}},
			Request:    req,
		}, nil
	})}

	manifestPath := filepath.Join("testdata", "croissant-examples", "hnscc-mif-mihc-comparison.croissant.jsonld")
	files, err := decodeCroissant(context.Background(), manifestPath, client, &Options{Output: outputDir}, Callbacks{}, map[string]string{})
	if err != nil {
		t.Fatalf("decodeCroissant returned error: %v", err)
	}
	if len(files) != 2 {
		t.Fatalf("expected two PathDB file jobs, got %d", len(files))
	}
	if files[0].DownloadURL != "https://pathdb.example.org/slide-1.ome.tif" {
		t.Fatalf("unexpected first PathDB URL: %s", files[0].DownloadURL)
	}
	if files[1].DownloadURL != "https://pathdb.example.org/slide-2.svs" {
		t.Fatalf("unexpected second PathDB URL: %s", files[1].DownloadURL)
	}
}
