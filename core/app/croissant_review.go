package app

import (
	"context"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

var croissantReviewCache struct {
	sync.Mutex
	resolver *downloadMetadataResolver
	created  time.Time
}

// InspectCroissantManifestWithMetadata may read public WordPress metadata but
// never fetches nested manifests or payloads. Local metadata works offline.
func InspectCroissantManifestWithMetadata(ctx context.Context, path string) (CroissantDownloadPlan, error) {
	doc, err := readCroissantDocument(path)
	if err != nil {
		return CroissantDownloadPlan{}, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	croissantReviewCache.Lock()
	if croissantReviewCache.resolver == nil || time.Since(croissantReviewCache.created) > 5*time.Minute {
		croissantReviewCache.resolver = &downloadMetadataResolver{client: &http.Client{Timeout: 8 * time.Second}}
		croissantReviewCache.created = time.Now()
	}
	resolver := croissantReviewCache.resolver
	croissantReviewCache.Unlock()
	rows := extractCroissantRows(doc)
	enrichCroissantReview(ctx, rows, resolver)
	return croissantPlan(croissantDatasetFromDoc(doc), rows), nil
}

func safeMetadataURL(raw string) string {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Host == "" || u.User != nil || (u.Scheme != "https" && u.Scheme != "http") {
		return ""
	}
	return u.String()
}

func applyCroissantObjectMetadata(row *croissantDownloadRow, object map[string]interface{}) {
	applyCroissantRecordData(row, object)
	for _, raw := range croissantSlice(object["additionalProperty"]) {
		property := croissantMap(raw)
		if property == nil {
			continue
		}
		key := strings.TrimPrefix(compactCroissantKey(metadataText(property, "name")), "tcia")
		applyCroissantRecordData(row, map[string]interface{}{key: property["value"]})
	}
	if key := croissantMap(object["key"]); row.RetrievalKey == "" && key != nil {
		id := metadataText(key, "@id")
		if slash := strings.LastIndex(id, "/"); slash >= 0 {
			row.RetrievalKey = id[slash+1:]
		}
	}
}

func croissantUnsupportedReason(row croissantDownloadRow) string {
	if strings.TrimSpace(row.DownloadURL) == "" {
		return "No download URL supplied"
	}
	role := resolveCroissantArtifactRole(row)
	if role == croissantRoleTransferPackage {
		return "Separate download from the dataset page"
	}
	if role == croissantRoleManifest {
		if !isCroissantDataRetrieverRow(row) {
			return "Requires a separate download tool"
		}
		if row.RetrievalKey != "" {
			switch normalize(row.RetrievalKey) {
			case "seriesinstanceuid", "seriesuid", "imageurl", "wsiimageurl", "drsuri":
			default:
				return "Inventory only; download data from the dataset page"
			}
		}
	}
	return ""
}

func enrichCroissantReview(ctx context.Context, rows []croissantDownloadRow, resolver *downloadMetadataResolver) {
	for i := range rows {
		row := &rows[i]
		record, _ := resolver.find(ctx, row.DownloadID, row.DownloadURL, "")
		if record == nil {
			continue
		}
		if title := metadataText(record, "download_title"); title != "" {
			row.Name = title
		}
		setCroissantStringIfEmpty(&row.DataCategories, metadataText(record, "download_type"))
		setCroissantStringIfEmpty(&row.DataTypes, metadataText(record, "data_type"))
		setCroissantStringIfEmpty(&row.FileTypes, metadataText(record, "file_type"))
		terms := croissantMap(record["data_license"])
		// Do not attach a different license's label to an embedded URI.
		if row.License == "" || row.License == metadataText(terms, "url") {
			setCroissantStringIfEmpty(&row.License, metadataText(terms, "url"))
			setCroissantStringIfEmpty(&row.LicenseName, metadataText(terms, "label"))
		}
		// A WordPress download's total cannot describe an arbitrary subset.
		fullComponent := canonicalCroissantURL(row.DownloadURL) == canonicalCroissantURL(metadataText(record, "download_url", "download_file"))
		if !fullComponent && row.RecordCount != "" {
			countField := ""
			switch normalize(row.RetrievalKey) {
			case "seriesinstanceuid", "seriesuid", "drsuri":
				countField = "series_count"
			case "imageurl", "wsiimageurl":
				countField = "image_count"
			}
			fullComponent = countField != "" && row.RecordCount == metadataText(record, countField)
		}
		if fullComponent && row.PayloadSize == "" {
			row.PayloadSize = metadataText(record, "download_size")
			row.PayloadSizeUnit = metadataText(record, "download_size_unit")
		}
	}
}

func croissantPayloadSize(row croissantDownloadRow) (string, float64, bool) {
	text := strings.TrimSpace(row.PayloadSize + " " + row.PayloadSizeUnit)
	if text == "" && resolveCroissantArtifactRole(row) == croissantRoleDataFile {
		text = row.ContentSize
	}
	parts := strings.Fields(text)
	if len(parts) != 2 {
		return "", 0, false
	}
	n, err := strconv.ParseFloat(parts[0], 64)
	if err != nil || n < 0 || math.IsNaN(n) || math.IsInf(n, 0) {
		return "", 0, false
	}
	factors := map[string]float64{"b": 1, "bytes": 1, "kb": 1e3, "mb": 1e6, "gb": 1e9, "tb": 1e12, "kib": 1024, "mib": 1048576, "gib": 1073741824, "tib": 1099511627776}
	factor, ok := factors[strings.ToLower(parts[1])]
	if !ok {
		return "", 0, false
	}
	if math.IsInf(n*factor, 0) {
		return "", 0, false
	}
	return parts[0] + " " + strings.ToUpper(parts[1]), n * factor, true
}
