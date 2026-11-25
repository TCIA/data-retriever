package app

import (
	"context"
	"crypto/sha256"
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBoundedCroissantSmokeManifests(t *testing.T) {
	root := filepath.Join(repoRootFromManifestTest(t), "test-manifests", "croissant")
	cases := map[string]int{
		"4d-lung.open-manifest.croissant.jsonld":                         1,
		"breast-cancer-screening-dbt.noncommercial.croissant.jsonld":     1,
		"cmb-aml.radiology-pathology-external-clinical.croissant.jsonld": 2,
		"hnscc.mixed-access.croissant.jsonld":                            1,
		"hnscc-mif-mihc-comparison.croissant.jsonld":                     2,
		"saros.analysis-result-nifti-derived.croissant.jsonld":           1,
	}
	paths, err := filepath.Glob(filepath.Join(root, "*.jsonld"))
	if err != nil || len(paths) != len(cases) {
		t.Fatalf("all runnable Croissant documents must have a bounded expansion test: %v (%d files)", err, len(paths))
	}
	for name, count := range cases {
		t.Run(name, func(t *testing.T) {
			doc, err := readCroissantDocument(filepath.Join(root, name))
			if err != nil {
				t.Fatal(err)
			}
			for _, object := range croissantFileObjects(doc) {
				body, err := os.ReadFile(filepath.Join(root, "records", croissantString(object["name"])))
				if err != nil {
					t.Fatal(err)
				}
				rows, err := csv.NewReader(strings.NewReader(string(body))).ReadAll()
				if err != nil || len(rows) < 2 || len(rows) > 3 {
					t.Fatalf("inventory must have 1–2 data rows: %v", err)
				}
				if croissantString(object["sha256"]) != fmt.Sprintf("%x", sha256.Sum256(body)) || croissantString(object["contentSize"]) != fmt.Sprintf("%d B", len(body)) {
					t.Fatal("bounded inventory hash or byte size is stale")
				}
			}
			client := &http.Client{Transport: croissantRoundTripFunc(func(req *http.Request) (*http.Response, error) {
				const prefix = "https://raw.githubusercontent.com/TCIA/data-retriever/09828f916cf868c357968aa2bbf4cf36727178ce/test-manifests/croissant/records/"
				if !strings.HasPrefix(req.URL.String(), prefix) {
					return nil, fmt.Errorf("unexpected live or unbounded request: %s", req.URL)
				}
				body, err := os.ReadFile(filepath.Join(root, "records", filepath.Base(req.URL.Path)))
				if err != nil {
					return nil, err
				}
				return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(string(body))), Header: http.Header{"Content-Type": []string{"text/csv"}}, Request: req}, nil
			})}
			output := t.TempDir()
			if err := os.MkdirAll(filepath.Join(output, "metadata"), 0755); err != nil {
				t.Fatal(err)
			}
			files, err := decodeCroissant(context.Background(), filepath.Join(root, name), client, &Options{Output: output}, Callbacks{}, map[string]string{})
			if err != nil {
				t.Fatal(err)
			}
			for _, file := range files {
				if file.DRSURI != "" && file.LicenseURI != "https://www.cancerimagingarchive.net/nih-controlled-data-access-policy/" {
					t.Fatalf("controlled component policy lost: %q", file.LicenseURI)
				}
			}
			if len(files) != count {
				t.Fatalf("expected %d bounded payloads, got %d", count, len(files))
			}
		})
	}
}
