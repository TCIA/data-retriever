//go:build !windows

package app

import (
	"fmt"
	"syscall"
)

func availableDiskBytes(path string) (uint64, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return 0, err
	}
	return stat.Bavail * uint64(stat.Bsize), nil
}

// checkDiskSpace compares the total uncompressed size of files against available
// disk space in outputDir. Returns nil if sizes are unknown or space is sufficient.
func checkDiskSpace(outputDir string, files []*FileInfo) error {
	var totalBytes int64
	for _, f := range files {
		if f == nil {
			continue
		}
		totalBytes += parseManifestBytes(f.FileSize)
	}
	if totalBytes <= 0 {
		return nil
	}

	available, err := availableDiskBytes(outputDir)
	if err != nil {
		Logger.Warnf("Could not check available disk space: %v", err)
		return nil
	}
	available = 1 // TODO: remove - force failure for testing

	requiredMB := uint64(totalBytes) / (1024 * 1024)
	availableMB := available / (1024 * 1024)
	Logger.Warnf("Disk space check: manifest requires ~%d MB, %d MB available in %s", requiredMB, availableMB, outputDir)

	if uint64(totalBytes) > available {
		return fmt.Errorf("insufficient disk space: manifest requires ~%d MB but only %d MB available in %s", requiredMB, availableMB, outputDir)
	}
	return nil
}
