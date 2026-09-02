package app

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
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
		if base != "" {
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

// ExpectedNBIALogPathPattern returns the default log path pattern shown to users
// when a timestamped log has not been created yet.
func ExpectedNBIALogPathPattern() string {
	return DefaultLogFilePath("nbia-output-YYYYMMDD-HHMMSS.log")
}

// LatestNBIALogFilePath returns the newest NBIA timestamped log file path from
// the default log directory, selected by modification time.
func LatestNBIALogFilePath() (string, bool) {
	entries, err := os.ReadDir(DefaultLogDir())
	if err != nil {
		return "", false
	}

	latestPath := ""
	var latestModTime time.Time

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		if !strings.HasPrefix(name, "nbia-output-") || !strings.HasSuffix(strings.ToLower(name), ".log") {
			continue
		}

		info, infoErr := entry.Info()
		if infoErr != nil {
			continue
		}

		if latestPath == "" || info.ModTime().After(latestModTime) {
			latestPath = filepath.Join(DefaultLogDir(), name)
			latestModTime = info.ModTime()
		}
	}

	if latestPath == "" {
		return "", false
	}

	return latestPath, true
}

// DefaultAuthFilePath returns the platform-standard path for the saved authentication file.
//
// macOS:   ~/Library/Caches/net.cancerimagingarchive.tciadataretriever/saved_auth.json
// Windows: %LOCALAPPDATA%/TCIA Data Retriever/saved_auth.json
// Linux:   $XDG_STATE_HOME/tcia-data-retriever/saved_auth.json
func DefaultAuthFilePath() string {
	home, _ := os.UserHomeDir()

	switch runtime.GOOS {
	case "darwin":
		base, _ := os.UserCacheDir()
		if base != "" {
			return filepath.Join(base, "net.cancerimagingarchive.tciadataretriever", "saved_auth.json")
		}
	case "windows":
		if localAppData := strings.TrimSpace(os.Getenv("LOCALAPPDATA")); localAppData != "" {
			return filepath.Join(localAppData, LogDirectoryAppName, "saved_auth.json")
		}
		if appData := strings.TrimSpace(os.Getenv("APPDATA")); appData != "" {
			return filepath.Join(appData, LogDirectoryAppName, "saved_auth.json")
		}
		if home != "" {
			return filepath.Join(home, "AppData", "Local", LogDirectoryAppName, "saved_auth.json")
		}
	default:
		if xdgStateHome := strings.TrimSpace(os.Getenv("XDG_STATE_HOME")); xdgStateHome != "" {
			return filepath.Join(xdgStateHome, logDirectoryLinuxAppName, "saved_auth.json")
		}
		if home != "" {
			return filepath.Join(home, ".local", "state", logDirectoryLinuxAppName, "saved_auth.json")
		}
	}

	return filepath.Join(".", "saved_auth.json")
}
