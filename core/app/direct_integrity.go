package app

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

func directFileVerified(path string, info *FileInfo) bool {
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
	return (md5sum == "" || hex.EncodeToString(md5hash.Sum(nil)) == md5sum) && (sha256sum == "" || hex.EncodeToString(sha256hash.Sum(nil)) == sha256sum)
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
