package app

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func repoRootFromManifestTest(t *testing.T) string {
	t.Helper()

	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("failed to locate test file path")
	}

	return filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))
}

func TestReadTCIASeriesIDs_LegacyManifestFixture(t *testing.T) {
	repoRoot := repoRootFromManifestTest(t)
	manifestPath := filepath.Join(repoRoot, "test-manifests", "manifest-1790098311121.tcia")

	ids, err := readTCIASeriesIDs(manifestPath, nil)
	if err != nil {
		t.Fatalf("readTCIASeriesIDs returned error: %v", err)
	}

	want := []string{
		"1.3.6.1.4.1.14519.5.2.1.200342509692530171737702339931600829926",
		"1.3.6.1.4.1.14519.5.2.1.308437036965019940878147441014703289154",
	}

	if len(ids) != len(want) {
		t.Fatalf("expected %d series IDs, got %d (%v)", len(want), len(ids), ids)
	}
	for i := range want {
		if ids[i] != want[i] {
			t.Fatalf("series ID %d mismatch: got %q want %q", i, ids[i], want[i])
		}
	}
}

func TestReadTCIASeriesIDs_XMLManifestFixture(t *testing.T) {
	repoRoot := repoRootFromManifestTest(t)
	templatePath := filepath.Join(repoRoot, "test-manifests", "manifest-v2-template.tcia")
	nestedPath := filepath.Join(repoRoot, "test-manifests", "manifest-v2-nested-legacy.tcia")

	templateContent, err := os.ReadFile(templatePath)
	if err != nil {
		t.Fatalf("failed to read XML template fixture: %v", err)
	}
	nestedContent, err := os.ReadFile(nestedPath)
	if err != nil {
		t.Fatalf("failed to read nested legacy fixture: %v", err)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/nested.tcia" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write(nestedContent)
	}))
	defer srv.Close()

	xmlContent := strings.ReplaceAll(string(templateContent), "__NESTED_MANIFEST_URL__", srv.URL+"/nested.tcia")
	xmlPath := filepath.Join(t.TempDir(), "manifest-v2.tcia")
	if err := os.WriteFile(xmlPath, []byte(xmlContent), 0644); err != nil {
		t.Fatalf("failed to write temp XML manifest: %v", err)
	}

	ids, err := readTCIASeriesIDs(xmlPath, srv.Client())
	if err != nil {
		t.Fatalf("readTCIASeriesIDs returned error for XML manifest: %v", err)
	}

	want, err := parseLegacyTCIASeriesIDs(bytes.NewReader(nestedContent))
	if err != nil {
		t.Fatalf("failed to parse nested fixture into expected IDs: %v", err)
	}

	if len(ids) != len(want) {
		t.Fatalf("expected %d series IDs, got %d (%v)", len(want), len(ids), ids)
	}
	for i := range want {
		if ids[i] != want[i] {
			t.Fatalf("series ID %d mismatch: got %q want %q", i, ids[i], want[i])
		}
	}
}
