package app

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
)

func TestCroissantReviewDoesNotUseManifestSize(t *testing.T) {
	row := croissantDownloadRow{DownloadURL: "https://example.org/manifest.csv", DownloadArtifactRole: croissantRoleManifest, AccessMechanism: "TCIA Data Retriever", ContentSize: "28033 B"}
	if _, _, known := croissantPayloadSize(row); known {
		t.Fatal("manifest bytes presented as payload size")
	}
	row.PayloadSize, row.PayloadSizeUnit = "1.59", "gb"
	if size, bytes, known := croissantPayloadSize(row); !known || size != "1.59 GB" || bytes != 1.59e9 {
		t.Fatalf("wrong payload size %s %f %t", size, bytes, known)
	}
	row.PayloadSize = "NaN"
	if _, _, known := croissantPayloadSize(row); known {
		t.Fatal("non-finite size accepted")
	}
}

func TestCroissantReviewReadsLongListsAndDisablesInventory(t *testing.T) {
	plan, err := InspectCroissantManifest(filepath.Join("testdata", "croissant-examples", "cmb-aml.croissant.jsonld"))
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range plan.Items {
		if item.Name == "Original pathology package inventory" && (item.Supported || item.UnsupportedReason == "") {
			t.Fatal("inventory presented as downloadable")
		}
		if item.DataSizeKnown {
			t.Fatal("manifest size used without payload metadata")
		}
	}
	row := croissantDownloadRow{}
	applyCroissantObjectMetadata(&row, map[string]interface{}{"additionalProperty": []interface{}{
		map[string]interface{}{"name": "TCIA WordPress download id", "value": 51606},
		map[string]interface{}{"name": "data_categories", "value": []interface{}{"Clinical Data", "Image Annotations", "Other"}},
		map[string]interface{}{"name": "data_types", "value": []interface{}{"Demographic", "Measurement", "Diagnosis", "Follow-Up", "Other", "Molecular Test"}},
	}})
	if row.DownloadID != "51606" || row.DataCategories != "Clinical Data, Image Annotations, Other" || row.DataTypes != "Demographic, Measurement, Diagnosis, Follow-Up, Other, Molecular Test" {
		t.Fatalf("long lists lost: %+v", row)
	}
	if safeMetadataURL("javascript:alert(1)") != "" || safeMetadataURL("https://user:password@example.org") != "" {
		t.Fatal("unsafe metadata link accepted")
	}
}

func TestCroissantReviewWordPressSizesRespectSubsets(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"total_pages":1,"results":[{"id":48105,"download_title":"Images","download_type":["Radiology Images","Image Annotations"],"data_type":["CT","RTSTRUCT"],"file_type":["DICOM"],"download_url":"https://example.org/original.tcia","series_count":"52","download_size":"1.59","download_size_unit":"gb","data_license":{"label":"CC BY 4.0","url":"https://creativecommons.org/licenses/by/4.0/"}}]}`))
	}))
	defer srv.Close()
	resolver := &downloadMetadataResolver{client: srv.Client(), baseURL: srv.URL}
	rows := []croissantDownloadRow{
		{DownloadID: "48105", DownloadURL: "https://example.org/subset.csv", RetrievalKey: "SeriesInstanceUID", RecordCount: "10"},
		{DownloadID: "48105", DownloadURL: "https://example.org/complete.csv", RetrievalKey: "SeriesInstanceUID", RecordCount: "52"},
	}
	enrichCroissantReview(context.Background(), rows, resolver)
	if rows[0].PayloadSize != "" || rows[1].PayloadSize != "1.59" || rows[0].DataCategories != "Radiology Images, Image Annotations" || rows[0].Name != "Images" {
		t.Fatalf("incorrect enrichment: %+v", rows)
	}
}
