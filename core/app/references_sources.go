package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/html"
)

// referenceCatalogCache holds the merged "collections" + "analysis-results"
// WordPress catalog used to identify datasets by name/URL/download ID. These
// endpoints return the whole catalog (no per-item filter), so without this
// cache every run — even a second download of the same dataset in the same
// session — repeats the full paginated fetch. Mirrors croissantReviewCache's
// 5-minute window.
var referenceCatalogCache struct {
	sync.Mutex
	base    string
	catalog []map[string]interface{}
	created time.Time
}

func fetchReferenceCatalog(ctx context.Context, client *http.Client, base string) ([]map[string]interface{}, error) {
	referenceCatalogCache.Lock()
	if referenceCatalogCache.catalog != nil && referenceCatalogCache.base == base && time.Since(referenceCatalogCache.created) < 5*time.Minute {
		catalog := referenceCatalogCache.catalog
		referenceCatalogCache.Unlock()
		return catalog, nil
	}
	referenceCatalogCache.Unlock()

	var catalog []map[string]interface{}
	for _, endpoint := range []string{"collections", "analysis-results"} {
		records, err := fetchWordPressRecords(ctx, client, base, endpoint)
		if err != nil {
			return nil, err // A partial catalog cannot establish unique identity.
		}
		catalog = append(catalog, records...)
	}

	referenceCatalogCache.Lock()
	referenceCatalogCache.base = base
	referenceCatalogCache.catalog = catalog
	referenceCatalogCache.created = time.Now()
	referenceCatalogCache.Unlock()
	return catalog, nil
}

func resolveReferenceIdentities(ctx context.Context, client *http.Client, base string, entries []*datasetReference) {
	needed := false
	for _, entry := range entries {
		if entry.DOI == "" && (entry.Name != "Unidentified dataset(s)" || entry.downloadID != "") {
			needed = true
		}
	}
	if !needed {
		return
	}
	catalog, err := fetchReferenceCatalog(ctx, client, base)
	if err != nil {
		return // A partial catalog cannot establish unique identity.
	}
	for _, entry := range entries {
		if entry.DOI != "" {
			continue
		}
		var matches []map[string]interface{}
		for _, record := range catalog {
			hidden := metadataText(record, "hide_from_browse_table")
			if record["hide_from_browse_table"] == true || hidden == "1" || hidden == "true" {
				continue
			}
			match := false
			if entry.downloadID != "" {
				for _, key := range []string{"collection_downloads", "downloads", "result_downloads"} {
					for _, raw := range croissantSlice(record[key]) {
						if metadataText(croissantMap(raw), "id") == entry.downloadID {
							match = true
						}
					}
				}
			} else if entry.URL != "" && canonicalCroissantURL(entry.URL) == canonicalCroissantURL(metadataText(record, "url")) {
				match = true
			} else {
				for _, key := range []string{"collection_short_title", "result_short_title", "collection_title", "result_title"} {
					if name := metadataText(record, key); name != "" && normalize(name) == normalize(entry.Name) {
						match = true
					}
				}
			}
			if match {
				matches = append(matches, record)
			}
		}
		if len(matches) != 1 {
			continue
		}
		record := matches[0]
		entry.DOI = normalizeDatasetDOI(metadataText(record, "collection_doi", "result_doi"))
		setCroissantStringIfEmpty(&entry.URL, safeMetadataURL(metadataText(record, "url")))
		for _, raw := range croissantSlice(record["citations"]) {
			citation := croissantMap(raw)
			if metadataText(citation, "type") == "Data Citation" && normalizeDatasetDOI(metadataText(citation, "doi")) == entry.DOI {
				setCroissantStringIfEmpty(&entry.Citation, referenceHTMLText(metadataText(citation, "html")))
			}
		}
	}
}

func referenceHTMLText(value string) string {
	tokens := html.NewTokenizer(strings.NewReader(value))
	var out strings.Builder
	for {
		switch tokens.Next() {
		case html.ErrorToken:
			return strings.Join(strings.Fields(out.String()), " ")
		case html.TextToken:
			out.Write(tokens.Text())
		case html.StartTagToken, html.EndTagToken:
			name, _ := tokens.TagName()
			if string(name) == "br" || string(name) == "p" {
				out.WriteByte(' ')
			}
		}
	}
}

// referenceCitationCache holds resolved citation text + BibTeX per (base,
// DOI), so re-downloading the same dataset within the window doesn't repeat
// the crosscite.org round trip. Same 5-minute window as the catalog cache.
var referenceCitationCache struct {
	sync.Mutex
	entries map[string]referenceCitationCacheEntry
}

type referenceCitationCacheEntry struct {
	citation string
	bib      string
	created  time.Time
}

func cachedReferenceCitation(base, doi string) (citation, bib string, ok bool) {
	referenceCitationCache.Lock()
	defer referenceCitationCache.Unlock()
	entry, found := referenceCitationCache.entries[base+"|"+doi]
	if !found || time.Since(entry.created) > 5*time.Minute {
		return "", "", false
	}
	return entry.citation, entry.bib, true
}

func storeReferenceCitation(base, doi, citation, bib string) {
	referenceCitationCache.Lock()
	defer referenceCitationCache.Unlock()
	if referenceCitationCache.entries == nil {
		referenceCitationCache.entries = make(map[string]referenceCitationCacheEntry)
	}
	referenceCitationCache.entries[base+"|"+doi] = referenceCitationCacheEntry{citation: citation, bib: bib, created: time.Now()}
}

// referenceCitationConcurrency bounds how many unique DOIs resolveReferenceCitations
// looks up in crosscite.org at once; referenceCitationTimeout uses the same
// figure so the budget it hands out actually covers that many lookup rounds.
const referenceCitationConcurrency = 4

// referenceCitationTimeout sizes the lookup budget to the actual amount of
// work: one crosscite.org round trip (citation + BibTeX) per unique DOI,
// referenceCitationConcurrency at a time. A manifest with many isBasedOn /
// source datasets can have far more unique DOIs than a flat timeout
// accounts for, which is what left most of them stuck "Unresolved" even
// under the old 25s budget. Capped so a pathological manifest can't block
// run completion indefinitely.
func referenceCitationTimeout(entries []*datasetReference) time.Duration {
	const (
		base       = 10 * time.Second
		perRound   = 20 * time.Second
		maxTimeout = 3 * time.Minute
	)
	unique := make(map[string]struct{})
	for _, entry := range entries {
		if entry != nil && entry.DOI != "" {
			unique[entry.DOI] = struct{}{}
		}
	}
	rounds := (len(unique) + referenceCitationConcurrency - 1) / referenceCitationConcurrency
	if rounds < 1 {
		rounds = 1
	}
	timeout := base + time.Duration(rounds)*perRound
	if timeout > maxTimeout {
		timeout = maxTimeout
	}
	return timeout
}

func resolveReferenceCitations(ctx context.Context, client *http.Client, base string, entries []*datasetReference) {
	if client == nil {
		client = http.DefaultClient
	}
	if base == "" {
		base = "https://data.crosscite.org/"
	}
	// Resolve each DOI once, even when multiple components identify it.
	groups := make(map[string][]*datasetReference)
	for _, entry := range entries {
		if entry.Citation != "" {
			entry.Resolution = "Supplied dataset citation"
		}
		if entry.DOI != "" {
			groups[entry.DOI] = append(groups[entry.DOI], entry)
		}
	}
	dois := make([]string, 0, len(groups))
	for doi := range groups {
		dois = append(dois, doi)
	}
	sort.Strings(dois)
	sem := make(chan struct{}, referenceCitationConcurrency)
	var wg sync.WaitGroup
	for _, doi := range dois {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			wg.Wait()
			return
		}
		wg.Add(1)
		go func(doi string) {
			defer wg.Done()
			defer func() { <-sem }()
			refs := groups[doi]
			citation := ""
			for _, ref := range refs {
				if ref.Citation != "" {
					citation = ref.Citation
					break
				}
			}
			cachedCitation, cachedBib, hasCache := cachedReferenceCitation(base, doi)
			if citation == "" {
				if hasCache {
					citation = cachedCitation
				} else {
					citation, _ = fetchReferenceFormat(ctx, client, base+doi, "text/x-bibliography; style=apa")
					// The DOI service can return inline HTML even for this text
					// format. Keep the plain-text export free of formatting tags.
					citation = referenceHTMLText(citation)
				}
			}
			var bib string
			if hasCache {
				bib = cachedBib
			} else {
				var err error
				bib, err = fetchReferenceFormat(ctx, client, base+doi, "application/x-bibtex")
				match := referenceBibDOI.FindStringSubmatch(bib)
				if err != nil || !strings.HasPrefix(strings.TrimSpace(bib), "@") || len(match) != 2 || normalizeDatasetDOI(match[1]) != doi {
					bib = ""
				}
				if bib != "" {
					comma := strings.Index(bib, ",")
					brace := strings.Index(bib, "{")
					if comma > brace && brace >= 0 {
						hash := sha256.Sum256([]byte(doi))
						bib = bib[:brace+1] + "tcia_" + hex.EncodeToString(hash[:6]) + bib[comma:]
					} else {
						bib = ""
					}
				}
				storeReferenceCitation(base, doi, citation, bib)
			}
			for _, ref := range refs {
				setCroissantStringIfEmpty(&ref.Citation, citation)
				ref.BibTeX = bib
				if ref.Citation != "" {
					ref.Resolution = "Dataset citation available"
				}
				if bib == "" {
					ref.Resolution += "; DOI BibTeX unavailable"
				}
			}
		}(doi)
	}
	wg.Wait()
}

var referenceBibDOI = regexp.MustCompile(`(?i)\bdoi\s*=\s*[{"]([^}"]+)[}"]`)

func fetchReferenceFormat(ctx context.Context, client *http.Client, url, accept string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Accept", accept)
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("DOI citation HTTP %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, (1<<20)+1))
	if err != nil {
		return "", err
	}
	text := strings.TrimSpace(string(body))
	if len(body) > 1<<20 || text == "" || strings.HasPrefix(text, "<") {
		return "", fmt.Errorf("invalid DOI citation response")
	}
	return text, nil
}
