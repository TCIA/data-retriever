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

	// tcia_idc_subset — freshness checked via GCS ETag
	idcParquetFileName = "tcia_idc_subset.parquet"
	idcParquetURL      = "https://storage.googleapis.com/idc-index-data-artifacts/current/release_artifacts/tcia_idc_subset.parquet"
	idcMetaFileName    = "tcia_idc_subset.parquet.meta.json"

	// prior_versions_index — freshness checked via GitHub Releases latest tag
	priorParquetFileName = "prior_versions_index.parquet"
	priorMetaFileName    = "prior_versions_index.parquet.meta.json"
	priorGitHubLatestAPI = "https://api.github.com/repos/ImagingDataCommons/idc-index-data/releases/latest"
	priorDownloadURLFmt  = "https://github.com/ImagingDataCommons/idc-index-data/releases/download/%s/prior_versions_index.parquet"
)

// parquetMeta is persisted alongside each parquet file so we can do
// cheap freshness checks on subsequent launches without re-downloading.
type parquetMeta struct {
	// Used for GCS files (tcia_idc_subset)
	ETag         string `json:"etag,omitempty"`
	LastModified string `json:"last_modified,omitempty"`
	// Used for GitHub release files (prior_versions_index)
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

	idcPath := ensureGCSParquet(cacheDir)
	priorPath := ensureGitHubParquet(cacheDir)

	return ParquetPaths{
		IDCIndex:      idcPath,
		PriorVersions: priorPath,
	}, nil
}

// ── GCS parquet (tcia_idc_subset) ────────────────────────────────────────────

// ensureGCSParquet returns the local cached path, or "" to signal embedded fallback.
func ensureGCSParquet(cacheDir string) string {
	parquetPath := filepath.Join(cacheDir, idcParquetFileName)
	metaPath := filepath.Join(cacheDir, idcMetaFileName)

	savedMeta := loadMeta(metaPath)

	remoteETag, remoteLastModified, err := fetchRemoteHeaders(idcParquetURL)
	if err != nil {
		// Network unavailable.
		if fileExists(parquetPath) {
			log.Printf("[parquet] could not reach GCS; using cached idc file")
			return parquetPath
		}
		// No cache either — signal fallback to embedded.
		return ""
	}

	// Cache is current — no download needed.
	if savedMeta != nil && fileExists(parquetPath) {
		if remoteETag != "" && savedMeta.ETag == remoteETag {
			log.Printf("[parquet] idc index up to date (ETag match)")
			return parquetPath
		}
		if remoteETag == "" && remoteLastModified != "" && savedMeta.LastModified == remoteLastModified {
			log.Printf("[parquet] idc index up to date (Last-Modified match)")
			return parquetPath
		}
	}

	// Cache is stale or absent — download.
	log.Printf("[parquet] downloading updated idc index from GCS")
	if err := downloadFile(idcParquetURL, parquetPath); err != nil {
		if fileExists(parquetPath) {
			log.Printf("[parquet] WARN: download failed (%v); using stale idc cached file", err)
			return parquetPath
		}
		// Download failed, no cache — signal fallback to embedded.
		log.Printf("[parquet] WARN: download failed and no idc cache; using embedded file")
		return ""
	}

	saveMeta(metaPath, &parquetMeta{
		ETag:         remoteETag,
		LastModified: remoteLastModified,
		DownloadedAt: time.Now(),
	})
	log.Printf("[parquet] idc index cached at %s", parquetPath)
	return parquetPath
}

// ── GitHub release parquet (prior_versions_index) ────────────────────────────

// ensureGitHubParquet returns the local cached path, or "" to signal embedded fallback.
func ensureGitHubParquet(cacheDir string) string {
	parquetPath := filepath.Join(cacheDir, priorParquetFileName)
	metaPath := filepath.Join(cacheDir, priorMetaFileName)

	savedMeta := loadMeta(metaPath)

	latestTag, err := fetchLatestGitHubTag(priorGitHubLatestAPI)
	if err != nil {
		// Network unavailable.
		if fileExists(parquetPath) {
			log.Printf("[parquet] could not reach GitHub; using cached prior_versions file")
			return parquetPath
		}
		// No cache either — signal fallback to embedded.
		return ""
	}

	// Cache is current — no download needed.
	if savedMeta != nil && fileExists(parquetPath) && savedMeta.GitHubTag == latestTag {
		log.Printf("[parquet] prior_versions index up to date (tag %s)", latestTag)
		return parquetPath
	}

	// Cache is stale or absent — download.
	downloadURL := fmt.Sprintf(priorDownloadURLFmt, latestTag)
	log.Printf("[parquet] downloading prior_versions index (tag %s)", latestTag)
	if err := downloadFile(downloadURL, parquetPath); err != nil {
		if fileExists(parquetPath) {
			log.Printf("[parquet] WARN: download failed (%v); using stale prior_versions cached file", err)
			return parquetPath
		}
		// Download failed, no cache — signal fallback to embedded.
		log.Printf("[parquet] WARN: download failed and no prior_versions cache; using embedded file")
		return ""
	}

	saveMeta(metaPath, &parquetMeta{
		GitHubTag:    latestTag,
		DownloadedAt: time.Now(),
	})
	log.Printf("[parquet] prior_versions index cached at %s", parquetPath)
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

func fetchRemoteHeaders(url string) (etag, lastModified string, err error) {
	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Head(url)
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", "", fmt.Errorf("HEAD %s returned %s", url, resp.Status)
	}
	return resp.Header.Get("ETag"), resp.Header.Get("Last-Modified"), nil
}

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
