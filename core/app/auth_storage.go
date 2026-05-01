package app

import (
	"io"
	"os"
	"path/filepath"
)

// SaveAuthFile copies srcPath to the platform-standard saved auth location with mode 0600.
// If srcPath is already the saved location, this is a no-op.
func SaveAuthFile(srcPath string) error {
	destPath := DefaultAuthFilePath()
	if filepath.Clean(srcPath) == filepath.Clean(destPath) {
		return nil
	}

	src, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer src.Close()

	if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
		return err
	}

	dest, err := os.OpenFile(destPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	defer dest.Close()

	_, err = io.Copy(dest, src)
	return err
}

// LoadSavedAuthFilePath returns the path to the saved auth file if it exists, or "".
func LoadSavedAuthFilePath() string {
	path := DefaultAuthFilePath()
	if _, err := os.Stat(path); err == nil {
		return path
	}
	return ""
}
