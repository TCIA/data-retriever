package app

import (
	"path/filepath"
	"testing"
)

func TestReferenceSmokeManifestsKeepOneDOIFreeEntryPerSource(t *testing.T) {
	root := filepath.Join(repoRootFromManifestTest(t), "test-manifests", "reference-smoke")
	for _, name := range []string{"idc", "nbia", "ctdc", "general-commons", "pathdb"} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(root, name+".csv")
			if name == "idc" || name == "nbia" {
				ids, err := getSeriesInstanceUIDsFromSpreadsheet(path)
				if err != nil || len(ids) != 1 || ids[0] == "" {
					t.Fatalf("expected one UID: %v %v", ids, err)
				}
				return
			}
			files, err := decodeSpreadsheet(path)
			if err != nil || len(files) != 1 {
				t.Fatalf("expected one file: %v %v", files, err)
			}
			file := files[0]
			if file.DatasetDOI != "" || file.SourceDOI != "" || file.DatasetCitation != "" || file.Collection != "" {
				t.Fatal("smoke input must require dataset identity lookup")
			}
			locator, ok := fileReferenceLocator(file)
			if !ok || locator.Value == "" {
				t.Fatal("missing resolver locator")
			}
			if name == "pathdb" && locator.Kind != "image_url" || name != "pathdb" && locator.Kind != "drs_uri" {
				t.Fatalf("wrong locator kind: %s", locator.Kind)
			}
		})
	}
}
