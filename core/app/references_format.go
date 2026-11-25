package app

import (
	"fmt"
	"sort"
	"strings"
)

func (entry *datasetReference) addRelatedReference(notice *relatedReference) {
	for _, prior := range entry.RelatedReferences {
		if prior.Type == notice.Type && prior.DOI == notice.DOI && (prior.Citation == notice.Citation || prior.Citation == "" || notice.Citation == "") {
			setCroissantStringIfEmpty(&prior.Citation, notice.Citation)
			return
		}
	}
	entry.RelatedReferences = append(entry.RelatedReferences, notice)
}

// Related papers and acknowledgements are kept separate from dataset selection
// and transfer counts. Listing a paper does not establish analytical use.
func referenceCitationTargets(entries []*datasetReference) ([]*datasetReference, map[*datasetReference]*relatedReference) {
	targets := append([]*datasetReference(nil), entries...)
	related := make(map[*datasetReference]*relatedReference)
	for _, entry := range entries {
		for _, notice := range entry.RelatedReferences {
			if notice.DOI == "" {
				continue // An acknowledgement is text, not a fabricated bibliographic item.
			}
			target := &datasetReference{DOI: notice.DOI, Citation: notice.Citation, Resolution: "Unresolved"}
			targets = append(targets, target)
			related[target] = notice
		}
	}
	return targets, related
}

func formatReferences(manifest, outcome string, entries []*datasetReference, warnings []string) (string, string) {
	var text, bib strings.Builder
	fmt.Fprintf(&text, "Dataset references\nManifest: %s\nRun: %s\n\nAll datasets identified in the manifest are included. Selection and transfer results do not establish research use. Cite the datasets you actually used.\nCitation versions describe citation metadata; downloaded file versions have not been independently verified.\n\n", manifest, outcome)
	bib.WriteString("% Dataset references. Includes excluded/referenced datasets; cite those actually used.\n% Versions describe citation metadata, not independently verified downloaded file versions.\n")
	for _, warning := range warnings {
		fmt.Fprintf(&text, "Lookup warning: %s\n\n", warning)
		fmt.Fprintf(&bib, "%% Lookup warning: %s\n", referenceComment(warning))
	}
	written := make(map[string]bool)
	writeBib := func(doi, label, value string) {
		if doi != "" && written[doi] {
			return
		}
		fmt.Fprintf(&bib, "%% %s\n", referenceComment(label))
		if value == "" {
			bib.WriteString("% BibTeX unresolved; see references.txt.\n\n")
			return
		}
		bib.WriteString(value + "\n\n")
		written[doi] = true
	}
	for i, entry := range entries {
		fmt.Fprintf(&text, "%d. %s\n", i+1, entry.Name)
		if entry.UnresolvedLocator != nil {
			fmt.Fprintf(&text, "Unresolved file: %s\n", entry.UnresolvedLocator.Value)
		}
		if entry.DOI != "" {
			fmt.Fprintf(&text, "DOI: https://doi.org/%s\n", entry.DOI)
		}
		if entry.URL != "" {
			fmt.Fprintf(&text, "Dataset page: %s\n", entry.URL)
		}
		if entry.Citation != "" {
			fmt.Fprintf(&text, "\n%s\n\n", entry.Citation)
		} else if entry.URL != "" {
			text.WriteString("\nCitation unresolved; consult the dataset page above.\n\n")
		} else {
			text.WriteString("\nCitation unresolved; find the dataset at https://www.cancerimagingarchive.net/.\n\n")
		}
		for _, citation := range entry.CitationVariants {
			fmt.Fprintf(&text, "Additional supplied citation: %s\n", citation)
		}
		fmt.Fprintf(&text, "Selection: %s\n", strings.Join(entry.Selection, "; "))
		if entry.IdentityResolution != "" && entry.IdentityResolution != "Matched visible TCIA dataset" {
			fmt.Fprintf(&text, "Dataset details: %s\n", entry.IdentityResolution)
		}
		if entry.Resolution != "" {
			fmt.Fprintf(&text, "Citation metadata: %s\n", entry.Resolution)
		}
		statuses := make([]string, 0, len(entry.TransferResults))
		for status := range entry.TransferResults {
			statuses = append(statuses, status)
		}
		sort.Strings(statuses)
		for _, status := range statuses {
			fmt.Fprintf(&text, "Transfer %s: %d download items\n", status, entry.TransferResults[status])
		}
		if len(entry.RelatedReferences) > 0 {
			text.WriteString("\nRelated references and acknowledgements from supplied or DOI metadata\nReview these instructions for the data you used. Listing a related paper does not establish analytical use.\n")
			for _, notice := range entry.RelatedReferences {
				fmt.Fprintf(&text, "\n%s:\n", notice.Type)
				if notice.Citation != "" {
					fmt.Fprintln(&text, notice.Citation)
				}
				if notice.DOI != "" {
					fmt.Fprintf(&text, "DOI: https://doi.org/%s\n", notice.DOI)
					if notice.Resolution != "" {
						fmt.Fprintf(&text, "Citation metadata: %s\n", notice.Resolution)
					}
				}
			}
		}
		text.WriteByte('\n')
		writeBib(entry.DOI, entry.Name+" | "+strings.Join(entry.Selection, "; "), entry.BibTeX)
	}
	// Dataset entries come first, followed by DOI-deduplicated related papers.
	for _, entry := range entries {
		for _, notice := range entry.RelatedReferences {
			if notice.DOI != "" {
				writeBib(notice.DOI, notice.Type+" | Related to "+entry.Name, notice.BibTeX)
			} else {
				fmt.Fprintf(&bib, "%% %s | %s\n%% %s\n\n", referenceComment(notice.Type), referenceComment(entry.Name), referenceComment(notice.Citation))
			}
		}
	}
	return text.String(), bib.String()
}
