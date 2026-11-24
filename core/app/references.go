package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

type relatedReference struct {
	Type       string `json:"type"`
	DOI        string `json:"doi,omitempty"`
	Citation   string `json:"citation,omitempty"`
	BibTeX     string `json:"bibtex,omitempty"`
	Resolution string `json:"resolution,omitempty"`
}

type datasetReference struct {
	UnresolvedLocator   *referenceLocator `json:"unresolved_locator,omitempty"`
	ResolverFingerprint string            `json:"resolver_release_fingerprint,omitempty"`
	fromFileLocator     bool
	locatorKey          string
	IdentityResolution  string              `json:"identity_resolution,omitempty"`
	CitationVersion     string              `json:"citation_version,omitempty"`
	BibTeXVersion       string              `json:"bibtex_version,omitempty"`
	RelatedReferences   []*relatedReference `json:"related_references,omitempty"`
	Name                string              `json:"name"`
	DOI                 string              `json:"doi,omitempty"`
	URL                 string              `json:"url,omitempty"`
	Citation            string              `json:"citation,omitempty"`
	CitationVariants    []string            `json:"additional_supplied_citations,omitempty"`
	BibTeX              string              `json:"bibtex,omitempty"`
	Resolution          string              `json:"resolution"`
	Selection           []string            `json:"selection"`
	TransferResults     map[string]int      `json:"transfer_results,omitempty"`
	downloadID          string
	series              map[string]bool
}

type citationRun struct {
	sync.Mutex
	input         string
	entries       map[string]*datasetReference
	terminal      map[string]string
	client        *http.Client
	resolverURL   string
	pending       map[string]*pendingReference
	crossciteBase string
	warnings      []string
}

func newCitationRun(options *Options, client *http.Client) *citationRun {
	r := &citationRun{input: options.Input, entries: make(map[string]*datasetReference), terminal: make(map[string]string), client: client, resolverURL: options.DOIResolverURL, crossciteBase: "https://data.crosscite.org/"}
	if r.resolverURL == "" {
		r.resolverURL = DefaultDOIResolverURL
	}

	ext := strings.ToLower(filepath.Ext(options.Input))
	if ext == ".json" || ext == ".jsonld" {
		if doc, err := readCroissantDocument(options.Input); err == nil {
			r.addCroissant(doc, options)
		}
	}
	return r
}

func (r *citationRun) add(name, doi, url, citation, selection, uid, downloadID string) {
	if r == nil {
		return
	}
	r.Lock()
	defer r.Unlock()
	r.addReference(name, doi, url, citation, selection, uid, downloadID)
}

// addReference is used under the run lock, including while applying resolver results.
func (r *citationRun) addReference(name, doi, url, citation, selection, uid, downloadID string) *datasetReference {
	doi = normalizeDatasetDOI(doi)
	key := "doi:" + doi
	if doi == "" {
		key = "unresolved:" + normalize(name) + ":" + downloadID + ":" + url
	}
	entry := r.entries[key]
	if entry == nil {
		if name == "" {
			name = doi
			if name == "" {
				name = "Unidentified dataset(s)"
			}
		}
		entry = &datasetReference{Name: name, DOI: doi, URL: safeMetadataURL(url), Citation: citation, Resolution: "Unresolved", downloadID: downloadID, series: make(map[string]bool)}
		r.entries[key] = entry
	} else {
		if citation != "" && entry.Citation != "" && citation != entry.Citation && !containsReferenceString(entry.CitationVariants, citation) {
			entry.CitationVariants = append(entry.CitationVariants, citation)
		}
		setCroissantStringIfEmpty(&entry.Citation, citation)
		setCroissantStringIfEmpty(&entry.URL, safeMetadataURL(url))
	}
	if !containsReferenceString(entry.Selection, selection) {
		entry.Selection = append(entry.Selection, selection)
	}
	if uid != "" {
		entry.series[uid] = true
	}
	return entry
}

func containsReferenceString(values []string, value string) bool {
	for _, v := range values {
		if v == value {
			return true
		}
	}
	return false
}

func (r *citationRun) addCroissant(doc map[string]interface{}, options *Options) {
	dataset := croissantDatasetFromDoc(doc)
	if dataset.DOI == "" {
		dataset.DOI = referenceObjectDOI(doc)
	}
	rows := extractCroissantRows(doc)
	selections := make(map[string]bool)
	sourceSelected, sourceExcluded := false, false
	for _, row := range rows {
		selection := "Excluded"
		if croissantUnsupportedReason(row) != "" {
			selection = "Unsupported"
		} else if croissantRowSelected(row, options) {
			selection = "Selected"
		}
		if croissantRowGroup(row) == CroissantGroupPrimary {
			selections[selection] = true
		} else {
			if selection == "Selected" {
				sourceSelected = true
			} else {
				sourceExcluded = true
			}
		}
	}
	if len(selections) == 0 {
		selections["Referenced in manifest"] = true
	}
	for selection := range selections {
		r.add(dataset.DatasetName, dataset.DOI, dataset.URL, dataset.Citation, selection, "", "")
	}
	for _, raw := range referenceSlice(doc["isBasedOn"]) {
		object := croissantMap(raw)
		if object == nil {
			continue
		}
		if croissantTypeIncludesDataset(object["@type"]) {
			info := croissantDatasetFromDoc(object)
			selection := "Referenced; source-image selection not specified"
			if !sourceSelected && sourceExcluded {
				selection = "Referenced; source-image downloads excluded"
			}
			if sourceSelected {
				selection = "Referenced; source-image selection cannot be assigned to this dataset"
			}
			r.add(info.DatasetName, info.DOI, info.URL, info.Citation, selection, "", "")
		}
		// Some manifests explicitly assign a source download to a dataset.
		if croissantTypeIncludesDataDownload(object["@type"]) {
			row := croissantDownloadRowFromObject(object, CroissantGroupIsBasedOn)
			for _, rawParent := range referenceSlice(object["isPartOf"]) {
				parent := croissantMap(rawParent)
				if parent == nil || !croissantTypeIncludesDataset(parent["@type"]) {
					continue
				}
				info := croissantDatasetFromDoc(parent)
				selection := "Excluded"
				if croissantRowSelected(row, options) {
					selection = "Selected"
				}
				r.add(info.DatasetName, info.DOI, info.URL, info.Citation, selection, "", row.DownloadID)
			}
		}
	}
	// Preserve named datasets nested elsewhere too, without inventing a
	// relationship between those datasets and selected download components.
	for _, value := range doc {
		r.addNestedDatasetReferences(value, 0)
	}
}

func referenceObjectDOI(object map[string]interface{}) string {
	for _, key := range []string{"@id", "identifier", "url"} {
		if doi := normalizeDatasetDOI(metadataText(object, key)); doi != "" {
			return doi
		}
	}
	return ""
}

func (r *citationRun) addNestedDatasetReferences(value interface{}, depth int) {
	if depth > 64 {
		return
	}
	switch object := value.(type) {
	case map[string]interface{}:
		if croissantTypeIncludesDataset(object["@type"]) {
			info := croissantDatasetFromDoc(object)
			info.DOI = referenceObjectDOI(object)
			key := "doi:" + info.DOI
			if info.DOI == "" {
				key = "unresolved:" + normalize(info.DatasetName) + "::" + info.URL
			}
			r.Lock()
			_, exists := r.entries[key]
			r.Unlock()
			if !exists {
				r.add(info.DatasetName, info.DOI, info.URL, info.Citation, "Referenced; no download component identified", "", "")
			}
		}
		for _, child := range object {
			r.addNestedDatasetReferences(child, depth+1)
		}
	case []interface{}:
		for _, child := range object {
			r.addNestedDatasetReferences(child, depth+1)
		}
	}
}

func referenceSlice(value interface{}) []interface{} {
	if value == nil {
		return nil
	}
	if values, ok := value.([]interface{}); ok {
		return values
	}
	return []interface{}{value}
}

func (r *citationRun) observeFiles(files []*FileInfo) {
	if r == nil {
		return
	}
	for _, file := range files {
		if file == nil {
			continue
		}
		doi := file.DatasetDOI
		if doi == "" {
			doi = file.SourceDOI
		}
		if doi == "" {
			doi = normalizeDatasetDOI(file.DataDescriptionURI)
		}
		datasetName := file.Collection
		if file.DatasetDOI != "" && file.AnalysisResultID != "" {
			datasetName = file.AnalysisResultID
		} else if file.DatasetDOI != "" && file.SourceDOI != "" && normalizeDatasetDOI(file.DatasetDOI) != normalizeDatasetDOI(file.SourceDOI) {
			datasetName = normalizeDatasetDOI(file.DatasetDOI)
		}
		if doi == "" && r.queueFileReference(file, datasetName) {
			continue
		}
		r.add(datasetName, doi, file.DataDescriptionURI, file.DatasetCitation, "Selected", file.SeriesInstanceUID, file.WordPressDownloadID)
		if source := normalizeDatasetDOI(file.SourceDOI); source != "" && source != normalizeDatasetDOI(doi) {
			r.add(file.Collection, source, "", "", "Selected", file.SeriesInstanceUID, "")
		}
		if file.DatasetDOI == "" && file.AnalysisResultID != "" && normalize(file.AnalysisResultID) != normalize(file.Collection) {
			r.add(file.AnalysisResultID, "", "", "", "Selected", file.SeriesInstanceUID, "")
		}
	}
}

func (r *citationRun) recordEvent(event SeriesEvent) {
	if r == nil {
		return
	}
	switch event.Status {
	case seriesStatusSucceeded, seriesStatusFailed, seriesStatusCancelled:
		r.Lock()
		r.terminal[event.SeriesInstanceUID] = event.Status
		r.Unlock()
	}
}

// Each input path gets its own directory so concurrent manifests and retries
// cannot overwrite another manifest's references. DOI duplicates within a run
// are consolidated after identities are resolved.
func referenceDirectory(output, input string) string {
	abs, err := filepath.Abs(input)
	if err != nil {
		abs = input
	}
	hash := sha256.Sum256([]byte(abs))
	name := sanitizePathSegment(strings.TrimSuffix(filepath.Base(input), filepath.Ext(input)))
	if runes := []rune(name); len(runes) > 70 {
		name = string(runes[:70])
	}
	if name == "" {
		name = "manifest"
	}
	return filepath.Join(output, "metadata", "references", name+"-"+hex.EncodeToString(hash[:6]))
}

var referenceWriteMu sync.Mutex

func (r *citationRun) write(ctx context.Context, output string, runErr error) error {
	if r == nil {
		return nil
	}
	r.Lock()
	defer r.Unlock()
	r.warnings = nil
	if ctx.Err() == nil {
		pendingCtx, cancel := context.WithTimeout(ctx, referenceIdentityTimeout)
		err := r.resolvePendingFiles(pendingCtx)
		cancel()
		if err != nil {
			r.warnings = append(r.warnings, "File DOI lookup unavailable; references may be incomplete. The snapshot resolver service and resolver-enabled artifacts are required.")
		}
	} else {
		for key, item := range r.pending {
			r.unresolvedFileEntry(key, item, "File DOI lookup cancelled")
		}
	}
	entries := make([]*datasetReference, 0, len(r.entries))
	for _, entry := range r.entries {
		if entry.Citation != "" {
			entry.Resolution = "Supplied dataset citation"
		}
		entries = append(entries, entry)
	}
	if len(entries) == 0 {
		entries = append(entries, &datasetReference{Name: "Unidentified dataset(s)", Selection: []string{"Selection unavailable"}, Resolution: "Unresolved"})
	}
	// Cancellation writes the local report immediately. Remote lookup failures
	// remain visible in reports and logs without failing successful downloads.
	if ctx.Err() == nil {
		idCtx, idCancel := context.WithTimeout(ctx, referenceIdentityTimeout)
		err := resolveReferenceIdentities(idCtx, r.client, r.resolverURL, entries)
		idCancel()
		if err != nil {
			warning := "Snapshot dataset identity lookup unavailable; references may be incomplete."
			if errors.Is(err, context.DeadlineExceeded) {
				warning = "Snapshot dataset identity lookup timed out; references may be incomplete."
			}
			r.warnings = append(r.warnings, warning)
		}

		targets, related := referenceCitationTargets(entries)
		citeCtx, citeCancel := context.WithTimeout(ctx, referenceCitationTimeout(targets))
		resolveReferenceCitations(citeCtx, r.client, r.crossciteBase, targets)
		citeCancel()
		for target, notice := range related {
			notice.Citation, notice.BibTeX = target.Citation, target.BibTeX
			notice.Resolution = strings.ReplaceAll(target.Resolution, "Dataset citation", "Citation")
		}
		for _, target := range targets {
			if target.DOI != "" && (target.Citation == "" || target.BibTeX == "") {
				r.warnings = append(r.warnings, "Some DOI citations or BibTeX formats could not be retrieved; see per-reference details.")
				break
			}
		}
	}

	merged := make(map[string]*datasetReference)
	for _, entry := range entries {
		key := "doi:" + entry.DOI
		if entry.DOI == "" {
			key = "unresolved:" + normalize(entry.Name) + ":" + entry.URL
		}
		if entry.fromFileLocator {
			key = "unresolved-file:" + entry.locatorKey
		}
		if prior := merged[key]; prior != nil {
			for _, selection := range entry.Selection {
				if !containsReferenceString(prior.Selection, selection) {
					prior.Selection = append(prior.Selection, selection)
				}
			}
			if prior.series == nil {
				prior.series = make(map[string]bool)
			}
			for uid := range entry.series {
				prior.series[uid] = true
			}
			setCroissantStringIfEmpty(&prior.Citation, entry.Citation)
			for _, citation := range append(entry.CitationVariants, entry.Citation) {
				if citation != "" && citation != prior.Citation && !containsReferenceString(prior.CitationVariants, citation) {
					prior.CitationVariants = append(prior.CitationVariants, citation)
				}
			}
			setCroissantStringIfEmpty(&prior.BibTeX, entry.BibTeX)
			setCroissantStringIfEmpty(&prior.URL, entry.URL)
			setCroissantStringIfEmpty(&prior.BibTeXVersion, entry.BibTeXVersion)
			setCroissantStringIfEmpty(&prior.ResolverFingerprint, entry.ResolverFingerprint)
			for _, notice := range entry.RelatedReferences {
				prior.addRelatedReference(notice)
			}
		} else {
			merged[key] = entry
		}
	}
	entries = entries[:0]
	for _, entry := range merged {
		entry.CitationVersion = referenceVersion(entry.Citation)
		sort.Slice(entry.RelatedReferences, func(i, j int) bool {
			a, b := entry.RelatedReferences[i], entry.RelatedReferences[j]
			return a.Type+"|"+a.DOI+"|"+a.Citation < b.Type+"|"+b.DOI+"|"+b.Citation
		})
		entry.TransferResults = make(map[string]int)
		for uid := range entry.series {
			status := r.terminal[uid]
			if status == "" {
				status = "Not completed"
			}
			entry.TransferResults[status]++
		}
		sort.Strings(entry.Selection)
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Name == entries[j].Name {
			return entries[i].DOI < entries[j].DOI
		}
		return entries[i].Name < entries[j].Name
	})
	outcome := "Completed"
	if runErr != nil {
		outcome = "Failed"
	}
	if ctx.Err() != nil {
		outcome = "Cancelled"
	}
	report := struct {
		Manifest       string              `json:"manifest"`
		GeneratedAt    string              `json:"generated_at"`
		RunOutcome     string              `json:"run_outcome"`
		Datasets       []*datasetReference `json:"datasets"`
		LookupWarnings []string            `json:"lookup_warnings,omitempty"`
	}{filepath.Base(r.input), time.Now().UTC().Format(time.RFC3339), outcome, entries, r.warnings}
	jsonBytes, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	text, bib := formatReferences(report.Manifest, outcome, entries, r.warnings)

	dir := referenceDirectory(output, r.input)
	referenceWriteMu.Lock()
	defer referenceWriteMu.Unlock()
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	for _, file := range []struct {
		name string
		body []byte
	}{{"references.txt", []byte(text)}, {"references.bib", []byte(bib)}, {"references.json", jsonBytes}} {
		if err := writeReferenceFile(dir, file.name, file.body); err != nil {
			return err
		}
	}
	return nil
}

func referenceComment(value string) string { return strings.Join(strings.Fields(value), " ") }

func writeReferenceFile(dir, name string, body []byte) error {
	f, err := os.CreateTemp(dir, ".references-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(f.Name()) }()
	if _, err = f.Write(body); err != nil {
		_ = f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), filepath.Join(dir, name))
}
