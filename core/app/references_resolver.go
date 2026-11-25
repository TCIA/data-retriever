package app

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

const DefaultDOIResolverURL = "https://tcia.duckdns.org/v2/files/resolve-dois"
const referenceIdentityTimeout = 60 * time.Second
const referenceResolverBatchSize = 200

type referenceLocator struct {
	ID    string `json:"id,omitempty"`
	Kind  string `json:"kind"`
	Value string `json:"value"`
}

type pendingReference struct {
	Locator              referenceLocator
	Name, Citation, Page string
	UIDs                 map[string]bool
}

type referenceResolverMatch struct {
	Name  string `json:"short_title"`
	Title string `json:"title"`
	DOI   string `json:"doi"`
	Page  string `json:"tcia_page"`
}

type referenceResolverResult struct {
	referenceLocator
	Status    string                   `json:"status"`
	Matches   []referenceResolverMatch `json:"matches"`
	Truncated bool                     `json:"truncated"`
}

func fileReferenceLocator(file *FileInfo) (referenceLocator, bool) {
	for _, candidate := range []referenceLocator{{Kind: "drs_uri", Value: file.DRSURI}, {Kind: "image_url", Value: file.DownloadURL}} {
		if candidate.Value == "" {
			continue
		}
		parsed, err := url.Parse(candidate.Value)
		// Never forward signed URLs, credentials or query strings to a metadata service.
		if err != nil || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
			continue
		}
		if candidate.Kind == "drs_uri" && parsed.Scheme == "drs" && parsed.Host != "" {
			return candidate, true
		}
		if candidate.Kind == "image_url" && (parsed.Scheme == "https" || parsed.Scheme == "http") && parsed.Host != "" {
			return candidate, true
		}
	}
	if file.Collection != "" && (file.downloadSource() == "IDC" || file.downloadSource() == "nbia") {
		return referenceLocator{}, false // Provider identified the dataset; use its compact dataset record.
	}
	uid := file.SeriesInstanceUID
	if uid != "" && len(uid) <= 64 && strings.Contains(uid, ".") && strings.Trim(uid, "0123456789.") == "" {
		return referenceLocator{Kind: "series_instance_uid", Value: uid}, true
	}
	return referenceLocator{}, false
}

func (r *citationRun) queueFileReference(file *FileInfo, name string) bool {
	locator, ok := fileReferenceLocator(file)
	if !ok {
		return false
	}
	key := locator.Kind + ":" + locator.Value
	r.Lock()
	defer r.Unlock()
	if r.pending == nil {
		r.pending = make(map[string]*pendingReference)
	}
	item := r.pending[key]
	if item == nil {
		item = &pendingReference{Locator: locator, Name: name, Citation: file.DatasetCitation, Page: safeMetadataURL(file.DataDescriptionURI), UIDs: make(map[string]bool)}
		r.pending[key] = item
	}
	if file.SeriesInstanceUID != "" {
		item.UIDs[file.SeriesInstanceUID] = true
	}
	return true
}

func resolverJSON(ctx context.Context, client *http.Client, method, endpoint string, input interface{}, output interface{}) (int, error) {
	var body io.Reader
	if input != nil {
		payload, err := json.Marshal(input)
		if err != nil {
			return 0, err
		}
		body = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, body)
	if err != nil {
		return 0, fmt.Errorf("invalid DOI resolver endpoint")
	}
	req.Header.Set("Accept", "application/json")
	if input != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if client == nil {
		client = http.DefaultClient
	}
	response, err := client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		return 0, fmt.Errorf("DOI resolver request failed")
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return response.StatusCode, fmt.Errorf("DOI resolver HTTP %d", response.StatusCode)
	}
	payload, err := io.ReadAll(io.LimitReader(response.Body, (4<<20)+1))
	if err != nil || len(payload) > 4<<20 {
		return response.StatusCode, fmt.Errorf("invalid DOI resolver response")
	}
	if err = json.Unmarshal(payload, output); err != nil {
		return response.StatusCode, fmt.Errorf("invalid DOI resolver JSON")
	}
	return response.StatusCode, nil
}

// Called under citationRun's lock. Each unresolved file retains its own identity
// until a unique match is known; a bare multi-dataset manifest must not collapse
// into one guessed dataset.
func (r *citationRun) resolvePendingFiles(ctx context.Context) error {
	keys := make([]string, 0, len(r.pending))
	for key := range r.pending {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var lookupErr error
	for start := 0; start < len(keys); start += referenceResolverBatchSize {
		end := start + referenceResolverBatchSize
		if end > len(keys) {
			end = len(keys)
		}
		locators := make([]referenceLocator, 0, end-start)
		for i, key := range keys[start:end] {
			locator := r.pending[key].Locator
			locator.ID = fmt.Sprintf("file-%d", start+i)
			locators = append(locators, locator)
		}
		var response struct {
			Results     []referenceResolverResult `json:"results"`
			Fingerprint string                    `json:"release_fingerprint"`
		}
		_, err := resolverJSON(ctx, r.client, http.MethodPost, r.resolverURL, map[string]interface{}{"locators": locators}, &response)
		if err != nil {
			lookupErr = err
			break
		}
		byID := make(map[string]referenceResolverResult)
		for _, result := range response.Results {
			if _, duplicate := byID[result.ID]; duplicate {
				lookupErr = fmt.Errorf("duplicate DOI resolver result")
				break
			}
			byID[result.ID] = result
		}
		if lookupErr != nil {
			break
		}
		if len(byID) != len(locators) {
			lookupErr = fmt.Errorf("incomplete DOI resolver response")
			break
		}
		for _, locator := range locators {
			result, ok := byID[locator.ID]
			if !ok || result.Kind != locator.Kind || result.Value != locator.Value || (result.Status != "matched" && result.Status != "unmatched" && result.Status != "ambiguous" && result.Status != "unavailable") || (result.Status == "matched" && (len(result.Matches) != 1 || result.Truncated || normalizeDatasetDOI(result.Matches[0].DOI) == "")) {
				lookupErr = fmt.Errorf("inconsistent DOI resolver response")
				break
			}
		}
		if lookupErr != nil {
			break
		}
		for i, key := range keys[start:end] {
			item := r.pending[key]
			result := byID[locators[i].ID]
			if result.Status == "matched" {
				match := result.Matches[0]
				name := match.Name
				if name == "" {
					name = match.Title
				}
				if name == "" {
					name = item.Name
				}
				entry := r.addReference(name, match.DOI, match.Page, item.Citation, "Selected", "", "")
				entry.Name = name
				entry.IdentityResolution = "Exact file-locator match"
				entry.ResolverFingerprint = response.Fingerprint
				for uid := range item.UIDs {
					entry.series[uid] = true
				}
				delete(r.entries, "locator:"+key)
				delete(r.pending, key)
			} else {
				r.unresolvedFileEntry(key, item, "File locator "+result.Status)
				if result.Status == "unavailable" {
					lookupErr = fmt.Errorf("file DOI metadata or index unavailable")
				}
			}
		}
		if lookupErr != nil {
			break
		}
	}
	if lookupErr != nil {
		for key, item := range r.pending {
			if r.entries["locator:"+key] == nil {
				r.unresolvedFileEntry(key, item, "File DOI lookup unavailable")
			}
		}
	}
	return lookupErr
}

func (r *citationRun) unresolvedFileEntry(key string, item *pendingReference, status string) {
	name := item.Name
	if name == "" {
		name = "Unidentified dataset(s)"
	}
	locator := item.Locator
	r.entries["locator:"+key] = &datasetReference{UnresolvedLocator: &locator, Name: name, URL: item.Page, Citation: item.Citation, Selection: []string{"Selected"}, Resolution: "Unresolved", IdentityResolution: status, series: item.UIDs, fromFileLocator: true, locatorKey: key}
}

// Names from a manifest can use the existing compact dataset endpoint. A failed
// exact file match never falls back to a name guess.
func resolveReferenceIdentities(ctx context.Context, client *http.Client, endpoint string, entries []*datasetReference) error {
	base := strings.TrimSuffix(strings.TrimRight(endpoint, "/"), "/files/resolve-dois")
	for _, entry := range entries {
		if entry.DOI != "" || entry.fromFileLocator || entry.Name == "Unidentified dataset(s)" {
			continue
		}
		var response struct {
			Datasets []referenceResolverMatch `json:"datasets"`
		}
		status, err := resolverJSON(ctx, client, http.MethodGet, base+"/datasets/"+url.PathEscape(entry.Name), nil, &response)
		if status == http.StatusNotFound {
			entry.IdentityResolution = "No unique snapshot dataset match"
			continue
		}
		if err != nil {
			return err
		}
		if len(response.Datasets) != 1 || normalizeDatasetDOI(response.Datasets[0].DOI) == "" {
			entry.IdentityResolution = "No unique snapshot dataset match"
			continue
		}
		match := response.Datasets[0]
		entry.DOI = normalizeDatasetDOI(match.DOI)
		if match.Name != "" {
			entry.Name = match.Name
		}
		if page := safeMetadataURL(match.Page); page != "" {
			entry.URL = page
		}
		entry.IdentityResolution = "Matched snapshot dataset identity"
	}
	return nil
}
