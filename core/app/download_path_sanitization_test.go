package app

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSanitizePathSegment_ReplacesInvalidCharacters(t *testing.T) {
	input := `  Infinity-1 Treat:Tx Plan<>:"/\|?* ` + "\x00"
	got := sanitizePathSegment(input)

	if strings.ContainsAny(got, `<>:"/\|?*`) {
		t.Fatalf("sanitizePathSegment left invalid characters in %q", got)
	}
	if got != strings.TrimSpace(got) {
		t.Fatalf("sanitizePathSegment should trim surrounding whitespace, got %q", got)
	}
	if !strings.Contains(got, "Infinity-1 Treat_Tx Plan") {
		t.Fatalf("unexpected sanitized value %q", got)
	}
}

func TestDescriptiveOutputSegments_SanitizesAllSegments(t *testing.T) {
	collection, patientID, studySegment, seriesSegment := descriptiveOutputSegments(
		"Pelvic:Reference/Data",
		"Pelvic-Ref*009",
		"07-09-2013",
		"Infinity-1 Treat:Tx Plan",
		"1.2.3.4.56789",
		"56406.000000",
		"NewCBCT:Acq",
		"1.3.6.1.4.1.14519.5.2.1.7085.2036.150542968706930261866940519950",
	)

	values := []string{collection, patientID, studySegment, seriesSegment}
	for _, v := range values {
		if strings.ContainsAny(v, `<>:"/\|?*`) {
			t.Fatalf("segment %q contains invalid path characters", v)
		}
	}

	if !strings.Contains(studySegment, "Treat_Tx Plan") {
		t.Fatalf("study segment %q did not preserve expected sanitized description", studySegment)
	}
	if !strings.Contains(seriesSegment, "NewCBCT_Acq") {
		t.Fatalf("series segment %q did not preserve expected sanitized description", seriesSegment)
	}
}

func TestGetOutput_DescriptiveModeProducesSanitizedPath(t *testing.T) {
	baseDir := t.TempDir()
	info := &FileInfo{
		Collection:        "Pelvic:Reference/Data",
		PatientID:         "Pelvic-Ref*009",
		StudyDate:         "07-09-2013",
		StudyDesc:         "Infinity-1 Treat:Tx Plan",
		StudyInstanceUID:  "1.2.3.4.56789",
		SeriesNumber:      "56406.000000",
		SeriesDescription: "NewCBCT:Acq",
		SeriesInstanceUID: "1.3.6.1.4.1.14519.5.2.1.7085.2036.150542968706930261866940519950",
	}
	options := &Options{DirectoryMode: "descriptive"}

	outputDir := info.getOutput(baseDir, options)
	if _, err := os.Stat(outputDir); err != nil {
		t.Fatalf("expected output directory to exist, got error: %v", err)
	}

	rel, err := filepath.Rel(baseDir, outputDir)
	if err != nil {
		t.Fatalf("failed to compute relative output path: %v", err)
	}
	for _, part := range strings.Split(rel, string(os.PathSeparator)) {
		if strings.ContainsAny(part, `<>:"/\|?*`) {
			t.Fatalf("path part %q contains invalid path characters (full path: %s)", part, outputDir)
		}
	}
}
