package app

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

const (
	// LogDirectoryAppName is used for platform log directory naming.
	LogDirectoryAppName = "TCIA Data Retriever"

	// Keep Linux/XDG path segment lowercase and filesystem-friendly.
	logDirectoryLinuxAppName = "tcia-data-retriever"
)

// DefaultLogDir resolves the platform-standard log directory.
//
// macOS:  ~/Library/Logs/TCIA Data Retriever
// Windows: %LOCALAPPDATA%/TCIA Data Retriever/Logs
// Linux:  $XDG_STATE_HOME/tcia-data-retriever/logs (fallback ~/.local/state/...)
func DefaultLogDir() string {
	home, _ := os.UserHomeDir()

	switch runtime.GOOS {
	case "darwin":
		base, _ := os.UserCacheDir() 
		if base != ""{
			return filepath.Join(base, "net.cancerimagingarchive.tciadataretriever", "logs")
		}
	case "windows":
		if localAppData := strings.TrimSpace(os.Getenv("LOCALAPPDATA")); localAppData != "" {
			return filepath.Join(localAppData, LogDirectoryAppName, "Logs")
		}
		if appData := strings.TrimSpace(os.Getenv("APPDATA")); appData != "" {
			return filepath.Join(appData, LogDirectoryAppName, "Logs")
		}
		if home != "" {
			return filepath.Join(home, "AppData", "Local", LogDirectoryAppName, "Logs")
		}
	default:
		if xdgStateHome := strings.TrimSpace(os.Getenv("XDG_STATE_HOME")); xdgStateHome != "" {
			return filepath.Join(xdgStateHome, logDirectoryLinuxAppName, "logs")
		}
		if home != "" {
			return filepath.Join(home, ".local", "state", logDirectoryLinuxAppName, "logs")
		}
	}

	return filepath.Join(".", "logs")
}

// DefaultLogFilePath returns a file path under the platform-standard log directory.
func DefaultLogFilePath(fileName string) string {
	name := strings.TrimSpace(fileName)
	if name == "" {
		name = "app.log"
	}
	return filepath.Join(DefaultLogDir(), filepath.Base(name))
}
