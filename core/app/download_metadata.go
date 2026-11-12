package app

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const wordPressAPI = "https://cancerimagingarchive.net/api/v2"

// Metadata lookup is best effort, bounded, and separate from payload retrieval.
// A failed/partial catalog must never establish a unique filename match.
type downloadMetadataResolver struct {
	once    sync.Once
	client  *http.Client
	baseURL string
	records []map[string]interface{}
	byID    map[string][]map[string]interface{}
	byURL   map[string][]map[string]interface{}
	byName  map[string][]map[string]interface{}
	err     error
}

func (r *downloadMetadataResolver) load(ctx context.Context) {
	r.once.Do(func() {
		ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
		defer cancel()
		r.records, r.err = fetchWordPressRecords(ctx, r.client, r.baseURL, "downloads")
		r.byID, r.byURL, r.byName = make(map[string][]map[string]interface{}), make(map[string][]map[string]interface{}), make(map[string][]map[string]interface{})
		for _, record := range r.records {
			id := metadataText(record, "id")
			r.byID[id] = append(r.byID[id], record)
			link := metadataText(record, "download_url", "download_file")
			r.byURL[canonicalCroissantURL(link)] = append(r.byURL[canonicalCroissantURL(link)], record)
			if parsed, err := url.Parse(link); err == nil && parsed.Path != "" {
				name := filepath.Base(parsed.Path)
				r.byName[name] = append(r.byName[name], record)
			}
		}
	})
}

func fetchWordPressRecords(ctx context.Context, client *http.Client, base, endpoint string) ([]map[string]interface{}, error) {
	if client == nil {
		client = http.DefaultClient
	}
	if base == "" {
		base = wordPressAPI
	}
	var records []map[string]interface{}
	for page := 1; page <= 100; page++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/%s/?per_page=100&page=%d&v=1", base, endpoint, page), nil)
		if err != nil {
			return nil, err
		}
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		var result struct {
			TotalPages int                      `json:"total_pages"`
			Results    []map[string]interface{} `json:"results"`
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, (8<<20)+1))
		_ = resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("WordPress metadata HTTP %d", resp.StatusCode)
		}
		if len(body) > 8<<20 {
			return nil, fmt.Errorf("WordPress metadata response exceeds 8 MiB")
		}
		if err := json.Unmarshal(body, &result); err != nil {
			return nil, err
		}
		if result.TotalPages < 1 || result.TotalPages > 100 {
			return nil, fmt.Errorf("invalid WordPress pagination")
		}
		records = append(records, result.Results...)
		if page >= result.TotalPages {
			return records, nil
		}
	}
	return nil, fmt.Errorf("WordPress pagination limit exceeded")
}

func metadataText(record map[string]interface{}, keys ...string) string {
	for _, key := range keys {
		// WordPress uses false for missing text. Do not turn it into "false".
		value := record[key]
		if _, isBool := value.(bool); isBool {
			continue
		}
		if text := strings.TrimSpace(croissantString(value)); text != "" {
			return text
		}
	}
	return ""
}

func (r *downloadMetadataResolver) find(ctx context.Context, id, originalURL, localPath string) (map[string]interface{}, string) {
	if r == nil {
		return nil, ""
	}
	r.load(ctx)
	if r.err != nil {
		return nil, ""
	}
	var matches []map[string]interface{}
	method := "WordPress download ID"
	switch {
	case id != "":
		matches = r.byID[id]
	case originalURL != "":
		method, matches = "WordPress download URL", r.byURL[canonicalCroissantURL(originalURL)]
	case localPath != "":
		method, matches = "WordPress manifest filename", r.byName[filepath.Base(localPath)]
	}
	if len(matches) != 1 {
		return nil, ""
	}
	return matches[0], method
}

func applyLicenseMetadata(file *FileInfo, name, uri, source string) {
	name, uri = strings.TrimSpace(name), strings.TrimSpace(uri)
	// Keep source-system terms intact, including when only its short name is known.
	if file.LicenseURI != "" || file.LicenseName != "" || file.LicenseShortName != "" {
		if uri != "" && file.LicenseURI != "" && canonicalCroissantURL(uri) != canonicalCroissantURL(file.LicenseURI) {
			file.LicenseConflict = source + ": " + uri
		}
		return
	}
	file.LicenseName, file.LicenseURI = name, uri
	if name != "" || uri != "" {
		file.LicenseSource = source
	}
}

func enrichDownloadMetadata(ctx context.Context, files []*FileInfo, manifestPath string, resolver *downloadMetadataResolver) {
	if resolver == nil {
		return
	}
	for _, file := range files {
		if file == nil || (file.LicenseName != "" || file.LicenseURI != "" || file.LicenseShortName != "") {
			continue
		}
		// IDC/NBIA own their per-series licenses; do not replace them with a
		// collection-wide license when their metadata is missing.
		if file.downloadSource() == "IDC" || file.downloadSource() == "nbia" {
			continue
		}
		record, method := resolver.find(ctx, file.WordPressDownloadID, file.ManifestURL, manifestPath)
		if record == nil {
			continue
		}
		license := croissantMap(record["data_license"])
		if license == nil {
			license = croissantMap(record["license"])
		}
		applyLicenseMetadata(file, metadataText(license, "label"), metadataText(license, "url"), method)
		file.WordPressDownloadID = metadataText(record, "id")
	}
}
