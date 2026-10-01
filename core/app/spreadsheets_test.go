package app

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeTempCSV(t *testing.T, content string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "manifest.csv")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write csv: %v", err)
	}

	return path
}

func TestDecodeSpreadsheetAcceptsGUIDAliases(t *testing.T) {
	tests := []struct {
		name   string
		header string
	}{
		{name: "uppercase guid", header: "GUID"},
		{name: "lowercase guid", header: "guid"},
		{name: "file guid alias", header: "file_guid"},
		{name: "drs guid alias", header: "drs guid"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			csvBody := fmt.Sprintf("%s,participant id,name\ndg.4DFC/e0ae9672-7d35-4f85-a05e-68735c6bad95,PT-1,example.dcm\n", tc.header)
			path := writeTempCSV(t, csvBody)

			files, err := decodeSpreadsheet(path)
			if err != nil {
				t.Fatalf("decodeSpreadsheet returned error: %v", err)
			}
			if len(files) != 1 {
				t.Fatalf("expected 1 decoded row, got %d", len(files))
			}

			if files[0].DRSURI != "drs://nci-crdc.datacommons.io/dg.4DFC/e0ae9672-7d35-4f85-a05e-68735c6bad95" {
				t.Fatalf("unexpected DRS URI: %q", files[0].DRSURI)
			}
			if files[0].PatientID != "PT-1" {
				t.Fatalf("expected participant id alias to populate PatientID, got %q", files[0].PatientID)
			}
		})
	}
}

func TestDecodeSpreadsheetSupportsLegacyDRSAliases(t *testing.T) {
	tests := []struct {
		name   string
		header string
	}{
		{name: "drs uri", header: "drs_uri"},
		{name: "file id", header: "file_id"},
		{name: "access", header: "access"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			csvBody := fmt.Sprintf("%s,subject,studyid\ndg.4DFC/series-1,SUBJ-1,STUDY-1\n", tc.header)
			path := writeTempCSV(t, csvBody)

			files, err := decodeSpreadsheet(path)
			if err != nil {
				t.Fatalf("decodeSpreadsheet returned error: %v", err)
			}
			if len(files) != 1 {
				t.Fatalf("expected 1 decoded row, got %d", len(files))
			}

			if files[0].PatientID != "SUBJ-1" {
				t.Fatalf("expected subject alias to map to PatientID, got %q", files[0].PatientID)
			}
			if files[0].StudyID != "STUDY-1" {
				t.Fatalf("expected studyid to map to StudyID, got %q", files[0].StudyID)
			}
		})
	}
}

func TestDecodeSpreadsheetToleratesTrailingDelimiterOnDataRow(t *testing.T) {
	csvBody := strings.Join([]string{
		"GUID,name,collection",
		"dg.4DFC/series-one,one.dcm,COLL,",
		"dg.4DFC/series-two,two.dcm,COLL",
		"",
	}, "\n")
	path := writeTempCSV(t, csvBody)

	files, err := decodeSpreadsheet(path)
	if err != nil {
		t.Fatalf("decodeSpreadsheet returned error: %v", err)
	}
	if len(files) != 2 {
		t.Fatalf("expected 2 decoded rows, got %d", len(files))
	}

	if files[0].SeriesInstanceUID != "series-one" || files[1].SeriesInstanceUID != "series-two" {
		t.Fatalf("unexpected series IDs: %q, %q", files[0].SeriesInstanceUID, files[1].SeriesInstanceUID)
	}
}

func TestDecodeSpreadsheetMissingRequiredColumns(t *testing.T) {
	path := writeTempCSV(t, "foo,bar\n1,2\n")

	_, err := decodeSpreadsheet(path)
	if err == nil {
		t.Fatalf("expected an error for missing required columns")
	}
	if !strings.Contains(err.Error(), "no 'drs_uri', 'imageUrl', 'SeriesInstanceUID', or 'Series UID' column found") {
		t.Fatalf("unexpected error: %v", err)
	}
}
