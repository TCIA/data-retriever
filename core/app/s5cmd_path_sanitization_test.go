package app

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBuildS5cmdFileInfo_DescriptivePathIsSanitized(t *testing.T) {
	outputDir := t.TempDir()
	originalURI := "s3://idc-open-data/some-prefix/data.zip"
	seriesMeta := map[string]*SeriesMetadata{
		originalURI: {
			SeriesInstanceUID:  "1.3.6.1.4.1.14519.5.2.1.7085.2036.150542968706930261866940519950",
			series_size_MB:     1.25,
			collection_id:      "Pelvic:Reference/Data",
			PatientID:          "Pelvic-Ref*009",
			StudyInstanceUID:   "1.2.3.4.56789",
			StudyDate:          "07-09-2013",
			StudyDescription:   "Infinity-1 Treat:Tx Plan",
			SeriesNumber:       "56406.000000",
			SeriesDescription:  "NewCBCT:Acq",
			analysis_result_id: "analysis|id?123",
		},
	}

	fi, isNewJob, found := buildS5cmdFileInfo(
		originalURI,
		seriesMeta,
		outputDir,
		map[string]string{},
		&Options{DirectoryMode: "descriptive"},
	)

	if !found {
		t.Fatal("expected series metadata to be found for URI")
	}
	if !isNewJob {
		t.Fatal("expected URI not present in processed map to be a new job")
	}
	if fi == nil {
		t.Fatal("expected non-nil FileInfo")
	}

	if fi.S5cmdManifestPath == "" {
		t.Fatal("expected S5cmdManifestPath to be set")
	}
	if _, err := os.Stat(fi.S5cmdManifestPath); err != nil {
		t.Fatalf("expected output directory to exist, got: %v", err)
	}

	rel, err := filepath.Rel(outputDir, fi.S5cmdManifestPath)
	if err != nil {
		t.Fatalf("failed to compute relative path: %v", err)
	}

	for _, part := range strings.Split(rel, string(os.PathSeparator)) {
		if strings.ContainsAny(part, `<>:"/\|?*`) {
			t.Fatalf("path segment %q still contains invalid characters (full path: %s)", part, fi.S5cmdManifestPath)
		}
	}

	if !strings.Contains(fi.S5cmdManifestPath, "Treat_Tx Plan") {
		t.Fatalf("expected study description to be sanitized in path, got: %s", fi.S5cmdManifestPath)
	}
	if !strings.Contains(fi.S5cmdManifestPath, "NewCBCT_Acq") {
		t.Fatalf("expected series description to be sanitized in path, got: %s", fi.S5cmdManifestPath)
	}
}
