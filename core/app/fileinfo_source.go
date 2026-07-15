package app

import "strings"

// downloadSource reports how this FileInfo resolves to a payload download.
func (info *FileInfo) downloadSource() string {
	if info == nil {
		return ""
	}

	if info.S5cmdManifestPath != "" || strings.HasPrefix(strings.ToLower(strings.TrimSpace(info.DownloadURL)), "s3://") {
		return "IDC"
	}
	if strings.TrimSpace(info.DRSURI) != "" {
		return "drs"
	}
	if strings.TrimSpace(info.DownloadURL) != "" {
		return "spreadsheet"
	}

	return "nbia"
}

// sourceURL reports the concrete URL/URI this FileInfo uses when available.
func (info *FileInfo) sourceURL() string {
	if info == nil {
		return ""
	}

	if url := strings.TrimSpace(info.DownloadURL); url != "" {
		return url
	}
	if uri := strings.TrimSpace(info.DRSURI); uri != "" {
		return uri
	}

	return ""
}
