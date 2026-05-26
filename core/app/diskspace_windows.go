//go:build windows

package app

import (
	"fmt"
	"syscall"
	"unsafe"
)

var getDiskFreeSpaceEx = syscall.NewLazyDLL("kernel32.dll").NewProc("GetDiskFreeSpaceExW")

func availableDiskBytes(path string) (uint64, error) {
	pathPtr, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return 0, err
	}
	var freeBytesAvailable uint64
	r1, _, callErr := getDiskFreeSpaceEx.Call(
		uintptr(unsafe.Pointer(pathPtr)),
		uintptr(unsafe.Pointer(&freeBytesAvailable)),
		0,
		0,
	)
	if r1 == 0 {
		return 0, callErr
	}
	return freeBytesAvailable, nil
}

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

	requiredMB := uint64(totalBytes) / (1024 * 1024)
	availableMB := available / (1024 * 1024)
	Logger.Infof("Disk space check: manifest requires ~%d MB, %d MB available in %s", requiredMB, availableMB, outputDir)

	if uint64(totalBytes) > available {
		return fmt.Errorf("insufficient disk space: manifest requires ~%d MB but only %d MB available in %s", requiredMB, availableMB, outputDir)
	}
	return nil
}
