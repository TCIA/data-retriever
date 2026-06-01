package app

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestParseLegacyTCIASeriesIDs(t *testing.T) {
	content := strings.Join([]string{
		"downloadServerUrl=https://download.example.org/",
		"",
		"1.2.3.4",
		"# comment",
		"5.6.7.8",
	}, "\n")

	ids, err := parseLegacyTCIASeriesIDs(strings.NewReader(content))
	if err != nil {
		t.Fatalf("parseLegacyTCIASeriesIDs returned error: %v", err)
	}

	expected := []string{"1.2.3.4", "5.6.7.8"}
	if !reflect.DeepEqual(ids, expected) {
		t.Fatalf("unexpected IDs: got=%v want=%v", ids, expected)
	}
}

func TestReadTCIASeriesIDs_XMLManifestDownloadsLegacyTCIA(t *testing.T) {
	legacyManifest := strings.Join([]string{
		"downloadServerUrl=https://download.example.org/",
		"1.2.3.4",
		"5.6.7.8",
	}, "\n")

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/manifest.tcia" {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(legacyManifest))
	}))
	defer ts.Close()

	xmlManifest := strings.Join([]string{
		"<?xml version=\"1.0\" encoding=\"UTF-8\"?>",
		"<TCIACollection apiVersion=\"v2\">",
		"  <Downloads>",
		"    <URL>" + ts.URL + "/manifest.tcia</URL>",
		"  </Downloads>",
		"</TCIACollection>",
	}, "\n")

	tmpDir := t.TempDir()
	manifestPath := filepath.Join(tmpDir, "sample.tcia")
	if err := os.WriteFile(manifestPath, []byte(xmlManifest), 0644); err != nil {
		t.Fatalf("failed to write XML manifest: %v", err)
	}

	ids, err := readTCIASeriesIDs(context.Background(), manifestPath, ts.Client())
	if err != nil {
		t.Fatalf("readTCIASeriesIDs returned error: %v", err)
	}

	expected := []string{"1.2.3.4", "5.6.7.8"}
	if !reflect.DeepEqual(ids, expected) {
		t.Fatalf("unexpected IDs: got=%v want=%v", ids, expected)
	}
}

func TestReadTCIASeriesIDs_XMLManifestMissingDownloadURL(t *testing.T) {
	xmlManifest := strings.Join([]string{
		"<?xml version=\"1.0\" encoding=\"UTF-8\"?>",
		"<TCIACollection apiVersion=\"v2\">",
		"  <Downloads>",
		"  </Downloads>",
		"</TCIACollection>",
	}, "\n")

	tmpDir := t.TempDir()
	manifestPath := filepath.Join(tmpDir, "sample.tcia")
	if err := os.WriteFile(manifestPath, []byte(xmlManifest), 0644); err != nil {
		t.Fatalf("failed to write XML manifest: %v", err)
	}

	_, err := readTCIASeriesIDs(context.Background(), manifestPath, nil)
	if err == nil {
		t.Fatalf("expected error when XML manifest has no download URL")
	}
	if !strings.Contains(err.Error(), "does not contain any download URL") {
		t.Fatalf("unexpected error: %v", err)
	}
}
