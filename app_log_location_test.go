package main

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestResolveLogLocationTarget(t *testing.T) {
	tempDir := t.TempDir()
	fallbackDir := filepath.Join(tempDir, "fallback")

	realDir := filepath.Join(tempDir, "real-dir")
	if err := os.MkdirAll(realDir, 0o755); err != nil {
		t.Fatalf("failed to create real directory: %v", err)
	}

	realFile := filepath.Join(tempDir, "nbia-output-20260101-010101.log")
	if err := os.WriteFile(realFile, []byte("log"), 0o644); err != nil {
		t.Fatalf("failed to create real file: %v", err)
	}

	testCases := []struct {
		name       string
		inputPath  string
		wantPath   string
		wantReveal bool
	}{
		{
			name:       "existing file reveals file",
			inputPath:  realFile,
			wantPath:   realFile,
			wantReveal: true,
		},
		{
			name:       "existing directory opens directory",
			inputPath:  realDir,
			wantPath:   realDir,
			wantReveal: false,
		},
		{
			name:       "missing path falls back",
			inputPath:  filepath.Join(tempDir, "does-not-exist.log"),
			wantPath:   fallbackDir,
			wantReveal: false,
		},
		{
			name:       "empty path falls back",
			inputPath:  "  ",
			wantPath:   fallbackDir,
			wantReveal: false,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			gotPath, gotReveal := resolveLogLocationTarget(tc.inputPath, fallbackDir)
			if gotPath != tc.wantPath || gotReveal != tc.wantReveal {
				t.Fatalf("resolveLogLocationTarget(%q, %q) = (%q, %t), want (%q, %t)", tc.inputPath, fallbackDir, gotPath, gotReveal, tc.wantPath, tc.wantReveal)
			}
		})
	}
}

func TestOpenLogLocationCommandForOS(t *testing.T) {
	targetFile := filepath.Join("tmp", "logs", "nbia-output.log")
	targetDir := filepath.Join("tmp", "logs")

	testCases := []struct {
		name      string
		goos      string
		target    string
		reveal    bool
		wantName  string
		wantArgs  []string
		wantError bool
	}{
		{
			name:     "windows reveal file",
			goos:     "windows",
			target:   targetFile,
			reveal:   true,
			wantName: "explorer",
			wantArgs: []string{"/select," + targetFile},
		},
		{
			name:     "windows open directory",
			goos:     "windows",
			target:   targetDir,
			reveal:   false,
			wantName: "explorer",
			wantArgs: []string{targetDir},
		},
		{
			name:     "darwin reveal file",
			goos:     "darwin",
			target:   targetFile,
			reveal:   true,
			wantName: "open",
			wantArgs: []string{"-R", targetFile},
		},
		{
			name:     "darwin open directory",
			goos:     "darwin",
			target:   targetDir,
			reveal:   false,
			wantName: "open",
			wantArgs: []string{targetDir},
		},
		{
			name:     "linux file opens parent directory",
			goos:     "linux",
			target:   targetFile,
			reveal:   true,
			wantName: "xdg-open",
			wantArgs: []string{filepath.Dir(targetFile)},
		},
		{
			name:     "linux open directory",
			goos:     "linux",
			target:   targetDir,
			reveal:   false,
			wantName: "xdg-open",
			wantArgs: []string{targetDir},
		},
		{
			name:      "unsupported os returns error",
			goos:      "plan9",
			target:    targetDir,
			reveal:    false,
			wantError: true,
		},
		{
			name:      "empty target returns error",
			goos:      "linux",
			target:    " ",
			reveal:    false,
			wantError: true,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			gotName, gotArgs, err := openLogLocationCommandForOS(tc.goos, tc.target, tc.reveal)
			if tc.wantError {
				if err == nil {
					t.Fatalf("openLogLocationCommandForOS(%q, %q, %t) expected error, got nil", tc.goos, tc.target, tc.reveal)
				}
				return
			}

			if err != nil {
				t.Fatalf("openLogLocationCommandForOS(%q, %q, %t) returned error: %v", tc.goos, tc.target, tc.reveal, err)
			}
			if gotName != tc.wantName || !reflect.DeepEqual(gotArgs, tc.wantArgs) {
				t.Fatalf("openLogLocationCommandForOS(%q, %q, %t) = (%q, %v), want (%q, %v)", tc.goos, tc.target, tc.reveal, gotName, gotArgs, tc.wantName, tc.wantArgs)
			}
		})
	}
}
