package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func syntheticCredentialFile(t *testing.T, dir, name, key string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	data, err := json.Marshal(map[string]string{"api_key": key})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func credentialResponse(req *http.Request, status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Status: fmt.Sprintf("%d %s", status, http.StatusText(status)), Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), ContentLength: int64(len(body)), Request: req}
}

// The real constructor policy is exercised with an isolated saver. A regression
// must fail the test rather than create/replace a developer's remembered GUI key.
func TestCLICredentialIsTransientAcrossTransferOutcomes(t *testing.T) {
	for _, outcome := range []string{"success", "authorization failure", "retry", "token refresh", "cancellation"} {
		t.Run(outcome, func(t *testing.T) {
			root := t.TempDir()
			key := syntheticCredentialFile(t, root, "invocation-key.json", "synthetic-invocation-key")
			saved := syntheticCredentialFile(t, root, "gui-saved-key.json", "synthetic-gui-key")
			originalKey, _ := os.ReadFile(key)
			originalSaved, _ := os.ReadFile(saved)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var authCalls, resolveCalls, storageCalls, saveCalls int
			client := &http.Client{Transport: controlledRoundTripFunc(func(req *http.Request) (*http.Response, error) {
				switch {
				case strings.HasSuffix(req.URL.Path, "/access_token"):
					authCalls++
					var payload map[string]string
					if err := json.NewDecoder(req.Body).Decode(&payload); err != nil {
						t.Fatal(err)
					}
					if req.URL.Host != controlledDRSHost || payload["api_key"] != "synthetic-invocation-key" {
						t.Fatal("wrong key or credential destination")
					}
					return credentialResponse(req, 200, `{"access_token":"synthetic-token"}`), nil
				case req.URL.Host == controlledDRSHost:
					resolveCalls++
					if req.Header.Get("Authorization") != "Bearer synthetic-token" {
						t.Fatal("missing invocation token")
					}
					if outcome == "authorization failure" {
						return credentialResponse(req, 403, ""), nil
					}
					if outcome == "retry" && resolveCalls == 1 {
						return credentialResponse(req, 503, ""), nil
					}
					if outcome == "token refresh" && resolveCalls == 1 {
						return credentialResponse(req, 401, ""), nil
					}
					return credentialResponse(req, 200, `{"url":"https://storage.example/payload?signature=synthetic-signature"}`), nil
				case req.URL.Host == "storage.example":
					storageCalls++
					if req.Header.Get("Authorization") != "" {
						t.Fatal("credential reached storage")
					}
					if outcome == "cancellation" {
						cancel()
						return nil, context.Canceled
					}
					return credentialResponse(req, 200, "payload"), nil
				default:
					t.Fatal("unexpected network destination")
					return nil, errors.New("unexpected network destination")
				}
			})}
			options := &Options{CLI: true, Auth: key, Output: filepath.Join(root, "output"), MaxRetries: 1, RetryDelay: time.Millisecond}
			auth, err := newRunGen3AuthManagerWithSaver(client, key, options, func(path string) error {
				saveCalls++
				return saveAuthFileToPath(path, saved)
			})
			if err != nil {
				t.Fatal(err)
			}
			info := &FileInfo{DRSURI: "drs://" + controlledDRSHost + "/dg.4DFC/object", SeriesInstanceUID: "synthetic-object", FileName: "payload.bin", FileSize: "7"}
			if err := os.MkdirAll(filepath.Join(options.Output, "metadata"), 0755); err != nil {
				t.Fatal(err)
			}
			if err := InitCompletionStatus(options.Output, []*FileInfo{info}); err != nil {
				t.Fatal(err)
			}
			err = info.Download(ctx, options.Output, client, options, nil, nil, auth)
			success := outcome == "success" || outcome == "retry" || outcome == "token refresh"
			if (err == nil) != success {
				t.Fatalf("outcome %s: %v", outcome, err)
			}
			if outcome == "cancellation" && !errors.Is(err, context.Canceled) {
				t.Fatalf("lost cancellation: %v", err)
			}
			wantAuthCalls := 1
			if outcome == "token refresh" {
				wantAuthCalls = 2
			}
			if authCalls != wantAuthCalls {
				t.Fatalf("auth exchanges = %d, want %d", authCalls, wantAuthCalls)
			}
			if (outcome == "retry" || outcome == "token refresh") && resolveCalls != 2 {
				t.Fatalf("resolver attempts = %d, want 2", resolveCalls)
			}
			if saveCalls != 0 {
				t.Fatal("CLI attempted to save a credential")
			}
			actualKey, _ := os.ReadFile(key)
			actualSaved, _ := os.ReadFile(saved)
			if string(actualKey) != string(originalKey) || string(actualSaved) != string(originalSaved) {
				t.Fatal("invocation or remembered credential changed")
			}
			if err := filepath.WalkDir(options.Output, func(path string, entry os.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if entry.IsDir() {
					return nil
				}
				data, err := os.ReadFile(path)
				if err != nil {
					return err
				}
				for _, secret := range []string{"synthetic-invocation-key", "synthetic-gui-key", "synthetic-token", "synthetic-signature"} {
					if strings.Contains(string(data), secret) {
						t.Errorf("credential or temporary URL persisted in output %s", filepath.Base(path))
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if success && storageCalls != 1 {
				t.Fatalf("storage requests = %d", storageCalls)
			}
		})
	}
}

func TestCLIWithoutAuthDoesNotRequestTokenOrEnterGUIAuthGate(t *testing.T) {
	root := t.TempDir()
	var requests, saves, guiPrompts int
	client := &http.Client{Transport: controlledRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		requests++
		if strings.HasSuffix(req.URL.Path, "/access_token") || req.Header.Get("Authorization") != "" {
			t.Fatal("CLI without --auth used credentials")
		}
		return credentialResponse(req, 401, ""), nil
	})}
	options := &Options{CLI: true, Output: root, AuthGate: &AuthGate{}, Concurrent: 1}
	// Even an explicitly configured GUI gate must be ignored by CLI. Mark it
	// cancelled so an accidental entry cannot hang the suite indefinitely.
	options.AuthGate.Resolve("")
	auth, err := newRunGen3AuthManagerWithSaver(client, "", options, func(string) error { saves++; return nil })
	if err != nil {
		t.Fatal(err)
	}
	info := &FileInfo{DRSURI: "drs://" + controlledDRSHost + "/object", SeriesInstanceUID: "synthetic-object", FileName: "file.bin"}
	if err := os.MkdirAll(filepath.Join(root, "metadata"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := InitCompletionStatus(root, []*FileInfo{info}); err != nil {
		t.Fatal(err)
	}
	stats := &DownloadStats{Total: 1, StartTime: time.Now()}
	wc := &WorkerContext{Context: context.Background(), HTTPClient: client, Gen3Auth: auth, Options: options, Stats: stats,
		Callbacks: Callbacks{Stderr: func(string) {}, EmitEvent: func(name string, _ ...interface{}) {
			if name == "open:auth-modal" || name == "auth-error" {
				guiPrompts++
			}
		}},
	}
	wc.handleFile(info)
	if requests != 1 || saves != 0 || guiPrompts != 0 || options.Auth != "" || stats.Failed != 1 {
		t.Fatalf("requests=%d saves=%d prompts=%d auth=%q failed=%d", requests, saves, guiPrompts, options.Auth, stats.Failed)
	}
}

func TestGUIAuthPolicyStillRemembersSelectedCredential(t *testing.T) {
	root := t.TempDir()
	key := syntheticCredentialFile(t, root, "selected.json", "synthetic-gui-selected-key")
	saved := filepath.Join(root, "isolated-gui-cache", "saved_auth.json")
	var calls int
	manager, err := newRunGen3AuthManagerWithSaver(nil, key, &Options{CLI: false}, func(path string) error {
		calls++
		return saveAuthFileToPath(path, saved)
	})
	if err != nil {
		t.Fatal(err)
	}
	actual, err := os.ReadFile(saved)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 || manager.apiKey != "synthetic-gui-selected-key" || !strings.Contains(string(actual), "synthetic-gui-selected-key") {
		t.Fatal("GUI remember-key behavior changed")
	}
	if runtime.GOOS != "windows" {
		stat, err := os.Stat(saved)
		if err != nil || stat.Mode().Perm() != 0600 {
			t.Fatalf("saved credential mode: %v %v", stat, err)
		}
	}
	if err := saveAuthFileToPath(saved, saved); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(saved)
	if err != nil || string(after) != string(actual) {
		t.Fatal("reselecting remembered credential truncated it")
	}
}

func TestTransientCredentialRejectsInvalidSuppliedFile(t *testing.T) {
	root := t.TempDir()
	for _, content := range []string{`not json`, `{"api_key":"  "}`, `{"api_key":42}`} {
		path := filepath.Join(root, "invalid.json")
		if err := os.WriteFile(path, []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := NewTransientGen3AuthManager(nil, path); err == nil {
			t.Fatalf("accepted malformed credential: %s", content)
		}
	}
	if _, err := NewTransientGen3AuthManager(nil, filepath.Join(root, "missing.json")); err == nil {
		t.Fatal("accepted missing supplied key")
	}
}
