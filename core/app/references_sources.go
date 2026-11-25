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
			citation, bib, hasCache := cachedReferenceCitation(base, doi)
			if !hasCache {
				// Cache only DOI-service metadata. A manifest's supplied/versioned
				// citation must never leak into another manifest through this cache.
				citation, _ = fetchReferenceDOIFormat(ctx, client, base, doi, "text/x-bibliography; style=apa")
				citation = referenceHTMLText(citation)
				var err error
				bib, err = fetchReferenceDOIFormat(ctx, client, base, doi, "application/x-bibtex")
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
						bib = referenceBibWithVersion(bib, referenceVersion(citation))
					} else {
						bib = ""
					}
				}
				// Do not cache failures: a retry should be able to recover.
				if citation != "" && bib != "" {
					storeReferenceCitation(base, doi, citation, bib)
				}
			}
			for _, ref := range refs {
				if ref.Citation == "" {
					ref.Citation = citation
				}
				ref.BibTeX = bib
				ref.CitationVersion = referenceVersion(ref.Citation)
				ref.BibTeXVersion = ""
				if match := referenceBibVersion.FindStringSubmatch(bib); len(match) == 2 {
					ref.BibTeXVersion = match[1]
				}
				if ref.Citation != "" {
					ref.Resolution = "Dataset citation available"
				}
				if citation == "" && ref.Citation == "" {
					ref.Resolution = "Unresolved; DOI citation unavailable"
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

// DataCite's formatter does not cover every publisher. Standard DOI content
// negotiation also handles Crossref papers linked from TCIA dataset pages.
func fetchReferenceDOIFormat(ctx context.Context, client *http.Client, base, doi, accept string) (string, error) {
	value, err := fetchReferenceFormat(ctx, client, base+doi, accept)
	if err != nil && base == "https://data.crosscite.org/" && ctx.Err() == nil {
		return fetchReferenceFormat(ctx, client, "https://doi.org/"+doi, accept)
	}
	return value, err
}

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

// Versions refer to citation metadata, never independently verified asset versions.
var referenceVersionPattern = regexp.MustCompile(`(?i)\(version ([a-z0-9][a-z0-9._-]*)\)`)
var referenceBibVersion = regexp.MustCompile(`(?im)(?:^|,)\s*version\s*=\s*[{"]([^}"]+)[}"]`)

func referenceVersion(citation string) string {
	match := referenceVersionPattern.FindStringSubmatch(citation)
	if len(match) == 2 {
		return match[1]
	}
	return ""
}

func referenceBibWithVersion(bib, version string) string {
	if version == "" || referenceBibVersion.MatchString(bib) {
		return bib
	}
	end := strings.LastIndex(bib, "}")
	if end < 0 {
		return bib
	}
	prefix := strings.TrimSpace(bib[:end])
	if !strings.HasSuffix(prefix, ",") {
		prefix += ","
	}
	return prefix + "\n  version = {" + version + "}\n" + bib[end:]
}
