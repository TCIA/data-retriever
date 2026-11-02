package app

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

// directIntegrityCacheEntry records the outcome of a prior successful
// checksum verification so a resumed run against a large, already-downloaded
// manifest doesn't re-hash every unchanged file. It is only ever written
// after a hash match, and is only trusted back if the file's size and mtime
// are unchanged AND the manifest's currently-expected checksums still match
// what was verified — so a corrected manifest or a replaced file can't be
// skipped on a stale entry.
type directIntegrityCacheEntry struct {
	Size      int64  `json:"size"`
	ModUnixNs int64  `json:"modUnixNs"`
	MD5       string `json:"md5,omitempty"`
	SHA256    string `json:"sha256,omitempty"`
}

var integrityCacheMu sync.Mutex

func directIntegrityCachePath(output, seriesUID string) string {
	return filepath.Join(output, "metadata", fmt.Sprintf("%s.integrity.json", seriesUID))
}

func loadDirectIntegrityCache(cachePath string) (*directIntegrityCacheEntry, error) {
	data, err := os.ReadFile(cachePath)
	if err != nil {
		return nil, err
	}
	var entry directIntegrityCacheEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		return nil, err
	}
	return &entry, nil
}

func saveDirectIntegrityCache(cachePath string, entry directIntegrityCacheEntry) error {
	integrityCacheMu.Lock()
	defer integrityCacheMu.Unlock()

	if err := os.MkdirAll(filepath.Dir(cachePath), 0755); err != nil {
		return err
	}
	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	tmp := cachePath + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, cachePath)
}

func directFileVerified(path string, info *FileInfo, output string) bool {
	size, md5sum, sha256sum, err := directIntegrity(info)
	if err != nil {
		return false
	}
	stat, err := os.Lstat(path)
	if err != nil || !stat.Mode().IsRegular() || size >= 0 && stat.Size() != size {
		return false
	}
	if md5sum == "" && sha256sum == "" {
		return true
	}

	modNs := stat.ModTime().UnixNano()
	cachePath := directIntegrityCachePath(output, info.SeriesInstanceUID)
	if cached, err := loadDirectIntegrityCache(cachePath); err == nil &&
		cached.Size == stat.Size() && cached.ModUnixNs == modNs &&
		(md5sum == "" || cached.MD5 == md5sum) &&
		(sha256sum == "" || cached.SHA256 == sha256sum) {
		// Size, mtime, and the manifest's currently-expected checksums all
		// match what was verified last time: the bytes haven't changed, so
		// skip re-reading and re-hashing potentially gigabytes of data.
		return true
	}

	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()
	md5hash, sha256hash := md5.New(), sha256.New()
	var writers []io.Writer
	if md5sum != "" {
		writers = append(writers, md5hash)
	}
	if sha256sum != "" {
		writers = append(writers, sha256hash)
	}
	if _, err := io.Copy(io.MultiWriter(writers...), f); err != nil {
		return false
	}
	verified := (md5sum == "" || hex.EncodeToString(md5hash.Sum(nil)) == md5sum) && (sha256sum == "" || hex.EncodeToString(sha256hash.Sum(nil)) == sha256sum)
	if verified {
		if err := saveDirectIntegrityCache(cachePath, directIntegrityCacheEntry{
			Size:      stat.Size(),
			ModUnixNs: modNs,
			MD5:       md5sum,
			SHA256:    sha256sum,
		}); err != nil {
			logger.Debugf("failed to save direct-integrity cache for %s: %v", info.SeriesInstanceUID, err)
		}
	}
	return verified
}

// FileSize describes payload bytes for direct/DRS files; NBIA series archives
// use a different (uncompressed) meaning and never call this helper.
func directIntegrity(info *FileInfo) (int64, string, string, error) {
	size := int64(-1)
	if raw := strings.TrimSpace(info.FileSize); raw != "" {
		parsed, err := strconv.ParseInt(strings.ReplaceAll(raw, ",", ""), 10, 64)
		if err != nil || parsed < 0 {
			return -1, "", "", fmt.Errorf("invalid direct-file byte count")
		}
		size = parsed
	}
	md5sum, sha256sum := strings.ToLower(strings.TrimSpace(info.MD5)), strings.ToLower(strings.TrimSpace(info.SHA256))
	for _, sum := range []struct {
		text  string
		bytes int
	}{{md5sum, 16}, {sha256sum, 32}} {
		if sum.text == "" {
			continue
		}
		decoded, err := hex.DecodeString(sum.text)
		if err != nil || len(decoded) != sum.bytes {
			return -1, "", "", fmt.Errorf("invalid supplied direct-file checksum")
		}
	}
	return size, md5sum, sha256sum, nil
}
