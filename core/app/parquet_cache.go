package app

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

const (
	parquetAppName = "net.cancerimagingarchive.tciadataretriever"

	// Both files are published as assets on each idc-index-data GitHub
	// Release; freshness is checked via the latest release tag.
	githubLatestReleaseAPI = "https://api.github.com/repos/ImagingDataCommons/idc-index-data/releases/latest"

	idcParquetFileName = "idc_index.parquet"
	idcMetaFileName    = "idc_index.parquet.meta.json"
	idcDownloadURLFmt  = "https://github.com/ImagingDataCommons/idc-index-data/releases/download/%s/idc_index.parquet"

	priorParquetFileName = "prior_versions_index.parquet"
	priorMetaFileName    = "prior_versions_index.parquet.meta.json"
	priorDownloadURLFmt  = "https://github.com/ImagingDataCommons/idc-index-data/releases/download/%s/prior_versions_index.parquet"
)

// parquetMeta is persisted alongside each parquet file so we can do
// cheap freshness checks on subsequent launches without re-downloading.
type parquetMeta struct {
	GitHubTag    string    `json:"github_tag,omitempty"`
	DownloadedAt time.Time `json:"downloaded_at"`
}

// ParquetPaths holds the local paths to both cached parquet files.
// An empty string means no cache is available — the caller should
// fall back to the embedded file.
type ParquetPaths struct {
	IDCIndex      string // empty → use embedded idc_index.parquet
	PriorVersions string // empty → use embedded prior_versions_index.parquet
}

// parquetCacheDir returns ~/Library/Caches/TCIA Data Retriever.
// Under Mac App Store sandboxing this is automatically redirected to
// ~/Library/Containers/<bundle-id>/Data/Library/Caches/TCIA Data Retriever.
func parquetCacheDir() (string, error) {
	base, err := os.UserCacheDir() // ~/Library/Caches on macOS
	if err != nil {
		return "", fmt.Errorf("could not locate user cache dir: %w", err)
	}
	dir := filepath.Join(base, parquetAppName)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return "", fmt.Errorf("could not create parquet cache dir: %w", err)
	}
	return dir, nil
}

// EnsureParquetsUpToDate checks and updates both parquet files.
// Priority for each file:
//  1. Download fresh file if cache is missing or outdated
//  2. Use cache if still current
//  3. Return empty string if no cache and server unreachable (caller uses embedded fallback)
//
// Never returns an error for network failures — those degrade silently to
// the embedded fallback. Only returns an error for local filesystem problems.
func EnsureParquetsUpToDate() (ParquetPaths, error) {
	cacheDir, err := parquetCacheDir()
	if err != nil {
		// Can't even determine cache dir — return empty paths so both
		// files fall back to embedded.
		log.Printf("[parquet] WARN: could not determine cache dir (%v); using embedded files", err)
		return ParquetPaths{}, nil
	}

	// Both files ship as assets on the same idc-index-data release, so
	// resolve the latest tag once and reuse it for both downloads.
	latestTag, tagErr := fetchLatestGitHubTag(githubLatestReleaseAPI)

	idcPath := ensureGitHubParquet(cacheDir, idcParquetFileName, idcMetaFileName, idcDownloadURLFmt, latestTag, tagErr)
	priorPath := ensureGitHubParquet(cacheDir, priorParquetFileName, priorMetaFileName, priorDownloadURLFmt, latestTag, tagErr)

	return ParquetPaths{
		IDCIndex:      idcPath,
		PriorVersions: priorPath,
	}, nil
}

// ── GitHub release parquet (idc_index / prior_versions_index) ───────────────

// ensureGitHubParquet returns the local cached path for a parquet asset
// published on the latest idc-index-data GitHub release, or "" to signal
// embedded fallback. latestTag/tagErr come from a single shared call to
// fetchLatestGitHubTag so both assets agree on the same release.
func ensureGitHubParquet(cacheDir, fileName, metaFileName, downloadURLFmt, latestTag string, tagErr error) string {
	parquetPath := filepath.Join(cacheDir, fileName)
	metaPath := filepath.Join(cacheDir, metaFileName)

	savedMeta := loadMeta(metaPath)

	if tagErr != nil {
		// Network unavailable.
		if fileExists(parquetPath) {
			log.Printf("[parquet] could not reach GitHub; using cached %s", fileName)
			return parquetPath
		}
		// No cache either — signal fallback to embedded.
		return ""
	}

	// Cache is current — no download needed.
	if savedMeta != nil && fileExists(parquetPath) && savedMeta.GitHubTag == latestTag {
		log.Printf("[parquet] %s up to date (tag %s)", fileName, latestTag)
		return parquetPath
	}

	// Cache is stale or absent — download.
	downloadURL := fmt.Sprintf(downloadURLFmt, latestTag)
	log.Printf("[parquet] downloading %s (tag %s)", fileName, latestTag)
	if err := downloadFile(downloadURL, parquetPath); err != nil {
		if fileExists(parquetPath) {
			log.Printf("[parquet] WARN: download failed (%v); using stale cached %s", err, fileName)
			return parquetPath
		}
		// Download failed, no cache — signal fallback to embedded.
		log.Printf("[parquet] WARN: download failed and no cache for %s; using embedded file", fileName)
		return ""
	}

	saveMeta(metaPath, &parquetMeta{
		GitHubTag:    latestTag,
		DownloadedAt: time.Now(),
	})
	log.Printf("[parquet] %s cached at %s", fileName, parquetPath)
	return parquetPath
}

// ── GitHub API ────────────────────────────────────────────────────────────────

func fetchLatestGitHubTag(apiURL string) (string, error) {
	client := &http.Client{Timeout: 15 * time.Second}
	req, err := http.NewRequest(http.MethodGet, apiURL, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", "TCIA-Data-Retriever")
	req.Header.Set("Accept", "application/vnd.github+json")

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("GitHub API returned %s", resp.Status)
	}

	var release struct {
		TagName string `json:"tag_name"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&release); err != nil {
		return "", fmt.Errorf("could not decode GitHub release response: %w", err)
	}
	if release.TagName == "" {
		return "", fmt.Errorf("GitHub release response had empty tag_name")
	}
	return release.TagName, nil
}

// ── Shared helpers ────────────────────────────────────────────────────────────

func downloadFile(url, destPath string) error {
	client := &http.Client{Timeout: 5 * time.Minute}
	resp, err := client.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s returned %s", url, resp.Status)
	}

	tmpPath := destPath + ".tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("could not create temp file: %w", err)
	}

	if _, err := io.Copy(f, resp.Body); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("error writing parquet data: %w", err)
	}
	f.Close()

	if err := os.Rename(tmpPath, destPath); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("could not move parquet file into place: %w", err)
	}
	return nil
}

func loadMeta(path string) *parquetMeta {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var m parquetMeta
	if err := json.Unmarshal(data, &m); err != nil {
		return nil
	}
	return &m
}

func saveMeta(path string, m *parquetMeta) {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		log.Printf("[parquet] WARN: could not marshal metadata: %v", err)
		return
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		log.Printf("[parquet] WARN: could not save metadata to %s: %v", path, err)
	}
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
