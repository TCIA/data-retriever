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

	"golang.org/x/net/html"
)

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
	var catalog []map[string]interface{}
	for _, endpoint := range []string{"collections", "analysis-results"} {
		records, err := fetchWordPressRecords(ctx, client, base, endpoint)
		if err != nil {
			return
		} // A partial catalog cannot establish unique identity.
		catalog = append(catalog, records...)
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
	sem := make(chan struct{}, 4)
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
			if citation == "" {
				citation, _ = fetchReferenceFormat(ctx, client, base+doi, "text/x-bibliography; style=apa")
				// The DOI service can return inline HTML even for this text
				// format. Keep the plain-text export free of formatting tags.
				citation = referenceHTMLText(citation)
			}
			bib, err := fetchReferenceFormat(ctx, client, base+doi, "application/x-bibtex")
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
