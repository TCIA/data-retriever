package app

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCroissantPublishedSummariesKeepManifestSelections(t *testing.T) {
	paths, err := filepath.Glob("testdata/croissant-summaries/*.jsonld")
	if err != nil || len(paths) != 5 {
		t.Fatalf("expected five published summaries: %v (%d files)", err, len(paths))
	}
	manifests := 0
	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			plan, err := InspectCroissantManifest(path)
			if err != nil {
				t.Fatal(err)
			}
			for _, item := range plan.Items {
				if item.Role == croissantRoleManifest && item.AccessMechanism == "TCIA Data Retriever" {
					manifests++
					if !item.Supported {
						t.Errorf("manifest %s disabled: %s", item.FileName, item.UnsupportedReason)
					}
					if item.SelectedByDefault != (item.Group != CroissantGroupIsBasedOn) {
						t.Errorf("unexpected default selection for %s", item.FileName)
					}
				}
				if item.Role == croissantRoleTransferPackage && item.Supported {
					t.Errorf("transfer package %s must remain unsupported", item.FileName)
				}
			}
		})
	}
	if manifests != 13 {
		t.Fatalf("expected 13 nested manifests including optional source images, got %d", manifests)
	}
}

func decodeCroissantCSVForTest(t *testing.T, document, csvName string, csvBody []byte) []*FileInfo {
	t.Helper()
	output := t.TempDir()
	if err := os.MkdirAll(filepath.Join(output, "metadata"), 0755); err != nil {
		t.Fatal(err)
	}
	requests := 0
	client := &http.Client{Transport: croissantRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		requests++
		if !strings.HasSuffix(req.URL.Path, "/"+csvName) {
			return nil, fmt.Errorf("unexpected request: %s", req.URL.Path)
		}
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(string(csvBody))),
			Header: http.Header{"Content-Type": []string{"text/csv"}}, Request: req}, nil
	})}
	files, err := decodeCroissant(context.Background(), document, client, &Options{Output: output}, Callbacks{}, map[string]string{})
	if err != nil {
		t.Fatal(err)
	}
	if requests != 1 {
		t.Fatalf("expected one local inventory read and no payload requests, got %d", requests)
	}
	return files
}

func TestCroissantPublishedPathDBInventoryPreservesAllFiles(t *testing.T) {
	const root = "testdata/croissant-examples"
	body, err := os.ReadFile(filepath.Join(root, "pathdb-images.csv"))
	if err != nil {
		t.Fatal(err)
	}
	files := decodeCroissantCSVForTest(t, filepath.Join(root, "hnscc-mif-mihc-comparison.croissant.jsonld"), "pathdb-images.csv", body)
	if len(files) != 3212 {
		t.Fatalf("expected all 3212 distinct PathDB images, got %d", len(files))
	}
	ids, urls := map[string]bool{}, map[string]bool{}
	for _, file := range files {
		if ids[file.SeriesInstanceUID] || urls[file.DownloadURL] {
			t.Fatalf("duplicate job identity or URL: %s", file.SeriesInstanceUID)
		}
		ids[file.SeriesInstanceUID], urls[file.DownloadURL] = true, true
	}
	// The real mIF/mIHC pairs share names as well as filename stems. Preserve
	// distinct destinations in both layouts, without touching the filesystem.
	for _, mode := range []string{"classic", "descriptive"} {
		paths := map[string]bool{}
		for _, file := range files {
			parts := []string{file.Collection, file.PatientID, file.StudyInstanceUID, file.SeriesInstanceUID}
			if mode == "descriptive" {
				collection, patient, study, series := descriptiveOutputSegments(file.Collection, file.PatientID,
					file.StudyDate, file.StudyDesc, file.StudyInstanceUID, file.SeriesNumber, file.SeriesDescription, file.SeriesInstanceUID)
				parts = []string{collection, patient, study, series}
			}
			path := filepath.Join(append(parts, file.FileName)...)
			if paths[path] {
				t.Fatalf("duplicate %s destination: %s", mode, path)
			}
			paths[path] = true
		}
	}
}

func TestCroissantNestedFilesUseFullLocatorIdentity(t *testing.T) {
	for _, route := range []struct{ header, first, second string }{
		{"imageUrl", "https://example.org/mIF/image.png", "https://example.org/mIHC/image.png"},
		{"drs_uri", "drs://nci-crdc.datacommons.io/first/image.dcm", "drs://nci-crdc.datacommons.io/second/image.dcm"},
	} {
		t.Run(route.header, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "manifest.jsonld")
			document := `{"@type":"sc:Dataset","recordSet":[{"@id":"downloads","data":[{"download_url":"https://example.org/inventory.csv","download_artifact_role":"manifest","access_mechanism":"TCIA Data Retriever"}]}]}`
			if err := os.WriteFile(path, []byte(document), 0644); err != nil {
				t.Fatal(err)
			}
			csv := fmt.Sprintf("%s,PatientID\n%s,SAME\n%s,SAME\n%s,SAME\n", route.header, route.first, route.second, route.first)
			files := decodeCroissantCSVForTest(t, path, "inventory.csv", []byte(csv))
			if len(files) != 2 || files[0].SeriesInstanceUID == files[1].SeriesInstanceUID {
				t.Fatalf("distinct locators must survive and repeated locators deduplicate: %+v", files)
			}
			outputs := map[string]bool{}
			for _, mode := range []string{"classic", "descriptive"} {
				for _, file := range files {
					output := file.getOutput(t.TempDir(), &Options{DirectoryMode: mode})
					key := mode + ":" + filepath.Base(output)
					if outputs[key] {
						t.Fatalf("colliding output directories in %s mode", mode)
					}
					outputs[key] = true
				}
			}
			// Row order and which other files are selected must not change identity.
			again := decodeCroissantCSVForTest(t, path, "inventory.csv", []byte(fmt.Sprintf("%s\n%s\n", route.header, route.second)))
			if len(again) != 1 || again[0].SeriesInstanceUID != files[1].SeriesInstanceUID {
				t.Fatal("file identity changed with the selected inventory subset")
			}
		})
	}
}

func TestCroissantNestedFileIdentityPreservesDICOMSeries(t *testing.T) {
	for _, file := range []*FileInfo{
		{SeriesInstanceUID: "1.2.3"},
		{SeriesInstanceUID: "1.2.3", DownloadURL: "s3://bucket/series", S5cmdManifestPath: "/local/series"},
		{SeriesInstanceUID: "1.2.3", DownloadURL: "s3://bucket/series"},
	} {
		setCroissantNestedFileIdentity(file)
		if file.SeriesInstanceUID != "1.2.3" {
			t.Fatalf("changed DICOM series identity: %+v", file)
		}
	}
}
