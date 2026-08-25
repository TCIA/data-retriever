package app

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func prepareDefaultLogDir(t *testing.T) string {
	t.Helper()

	root := t.TempDir()

	switch runtime.GOOS {
	case "darwin":
		t.Setenv("HOME", root)
	case "windows":
		t.Setenv("LOCALAPPDATA", root)
		t.Setenv("APPDATA", "")
	default:
		t.Setenv("XDG_STATE_HOME", root)
	}

	logDir := DefaultLogDir()
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		t.Fatalf("failed to create log dir %q: %v", logDir, err)
	}

	return logDir
}

func TestLatestNBIALogFilePathReturnsNewestByModTime(t *testing.T) {
	logDir := prepareDefaultLogDir(t)

	older := filepath.Join(logDir, "nbia-output-20260101-010101.log")
	newer := filepath.Join(logDir, "nbia-output-20260101-020202.log")
	ignored := filepath.Join(logDir, "app.log")

	if err := os.WriteFile(older, []byte("old"), 0o644); err != nil {
		t.Fatalf("failed to create older log: %v", err)
	}
	if err := os.WriteFile(newer, []byte("new"), 0o644); err != nil {
		t.Fatalf("failed to create newer log: %v", err)
	}
	if err := os.WriteFile(ignored, []byte("ignore"), 0o644); err != nil {
		t.Fatalf("failed to create ignored log: %v", err)
	}

	base := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(older, base, base); err != nil {
		t.Fatalf("failed to set older mod time: %v", err)
	}
	if err := os.Chtimes(newer, base.Add(1*time.Hour), base.Add(1*time.Hour)); err != nil {
		t.Fatalf("failed to set newer mod time: %v", err)
	}
	if err := os.Chtimes(ignored, base.Add(2*time.Hour), base.Add(2*time.Hour)); err != nil {
		t.Fatalf("failed to set ignored mod time: %v", err)
	}

	got, ok := LatestNBIALogFilePath()
	if !ok {
		t.Fatalf("expected a latest log path, got none")
	}
	if got != newer {
		t.Fatalf("LatestNBIALogFilePath() = %q, want %q", got, newer)
	}
}

func TestLatestNBIALogFilePathNoLogs(t *testing.T) {
	logDir := prepareDefaultLogDir(t)

	if err := os.WriteFile(filepath.Join(logDir, "random.txt"), []byte("x"), 0o644); err != nil {
		t.Fatalf("failed to create non-log file: %v", err)
	}

	got, ok := LatestNBIALogFilePath()
	if ok {
		t.Fatalf("expected no latest log path, got %q", got)
	}
	if got != "" {
		t.Fatalf("expected empty path, got %q", got)
	}
}

func TestExpectedNBIALogPathPattern(t *testing.T) {
	logDir := prepareDefaultLogDir(t)
	want := filepath.Join(logDir, "nbia-output-YYYYMMDD-HHMMSS.log")

	got := ExpectedNBIALogPathPattern()
	if got != want {
		t.Fatalf("ExpectedNBIALogPathPattern() = %q, want %q", got, want)
	}
}
