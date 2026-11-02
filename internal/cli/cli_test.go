package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"TCIA_Data_Retriever/core/app"
)

func TestHelpVersionAndUsageNeverRunTransfers(t *testing.T) {
	for _, tc := range []struct {
		args []string
		code int
		want string
	}{
		{nil, 0, "--input"},
		{[]string{"--help"}, 0, "--report-json"},
		{[]string{"--version"}, 0, "1.2.3"},
		{[]string{"--prompt"}, 2, "--prompt is unavailable"},
		{[]string{"--auth", "key.json"}, 2, "--input"},
	} {
		t.Run(strings.Join(tc.args, " "), func(t *testing.T) {
			var out bytes.Buffer
			code := execute(context.Background(), tc.args, &out, &out, "1.2.3", func(context.Context, *app.Options, app.Callbacks) (*app.Summary, error) {
				t.Fatal("transfer ran for help, version or invalid usage")
				return nil, nil
			})
			if code != tc.code || !strings.Contains(out.String(), tc.want) {
				t.Fatalf("code=%d output=%q", code, out.String())
			}
		})
	}
}

func manifestArgs(t *testing.T) ([]string, string) {
	t.Helper()
	dir := t.TempDir()
	manifest := filepath.Join(dir, "manifest.csv")
	if err := os.WriteFile(manifest, []byte("imageUrl,name\nhttps://example.test/file,file.bin\n"), 0600); err != nil {
		t.Fatal(err)
	}
	reportPath := filepath.Join(dir, "report.json")
	return []string{"--input", manifest, "--output", filepath.Join(dir, "downloads"), "--report-json", reportPath, "--accept-data-policy"}, reportPath
}

func readReport(t *testing.T, path string) (Report, []byte) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var report Report
	if err := json.Unmarshal(b, &report); err != nil {
		t.Fatal(err)
	}
	return report, b
}

func TestReportOutcomes(t *testing.T) {
	for _, tc := range []struct {
		name              string
		summary           *app.Summary
		err               error
		manifest          bool
		code              int
		status, errorCode string
	}{
		{"success", &app.Summary{Total: 3, Downloaded: 1, Skipped: 1, Synced: 1}, nil, true, 0, "success", ""},
		{"partial failure", &app.Summary{Total: 2, Downloaded: 1, Failed: 1}, nil, true, 1, "failed", "items_failed"},
		{"error", nil, errors.New("secret credential https://storage.test/?signature=secret"), false, 1, "failed", "transfer_failed"},
		{"empty", &app.Summary{}, nil, true, 1, "failed", "empty_manifest"},
		{"no manifest", &app.Summary{Total: 1}, nil, false, 1, "failed", "empty_manifest"},
		{"cancelled", &app.Summary{Total: 2, Downloaded: 1}, context.Canceled, true, 130, "cancelled", "cancelled"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args, path := manifestArgs(t)
			var out bytes.Buffer
			code := execute(context.Background(), args, &out, &out, "1.2.3", func(_ context.Context, opts *app.Options, cb app.Callbacks) (*app.Summary, error) {
				initial, _ := readReport(t, path)
				if initial.Status != "failed" || initial.ErrorCode != "incomplete_run" {
					t.Fatalf("unsafe initial report: %+v", initial)
				}
				if !opts.CLI {
					t.Fatal("headless run did not set CLI mode")
				}
				if tc.manifest {
					cb.Manifest(app.ManifestPayload{})
				}
				return tc.summary, tc.err
			})
			report, raw := readReport(t, path)
			if code != tc.code || report.Status != tc.status || report.ErrorCode != tc.errorCode || report.SchemaVersion != 1 || report.Version != "1.2.3" {
				t.Fatalf("code=%d report=%+v", code, report)
			}
			if !filepath.IsAbs(report.OutputDirectory) {
				t.Fatal("output directory is not absolute")
			}
			if strings.Contains(string(raw)+out.String(), "secret") {
				t.Fatal("raw error leaked into diagnostics or report")
			}
			if tc.summary != nil && (report.Total != tc.summary.Total || report.Downloaded != tc.summary.Downloaded || report.Failed != tc.summary.Failed || report.Skipped != tc.summary.Skipped || report.Synced != tc.summary.Synced) {
				t.Fatalf("counts changed: %+v", report)
			}
		})
	}
}

func TestPolicyAndReportDestinationFailBeforeTransfer(t *testing.T) {
	for _, kind := range []string{"policy", "report_directory", "manifest_alias", "credential_alias", "zero_workers"} {
		t.Run(kind, func(t *testing.T) {
			args, path := manifestArgs(t)
			switch kind {
			case "policy":
				args = args[:len(args)-1]
			case "report_directory":
				args[5] = filepath.Join(filepath.Dir(path), "missing", "report.json")
			case "manifest_alias":
				args[5] = args[1]
			case "credential_alias":
				args = append(args, "--auth", path)
			case "zero_workers":
				args = append(args, "--processes", "0")
			}
			code := execute(context.Background(), args, io.Discard, io.Discard, "test", func(context.Context, *app.Options, app.Callbacks) (*app.Summary, error) {
				t.Fatal("transfer started before validation")
				return nil, nil
			})
			if code == 0 {
				t.Fatal("invalid run reported success")
			}
		})
	}
}

func TestRealDirectDownloadReportsSuccessAndHTTPFailure(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusForbidden} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(status)
				_, _ = io.WriteString(w, "test payload")
			}))
			defer srv.Close()
			args, path := manifestArgs(t)
			if err := os.WriteFile(args[1], []byte("imageUrl,name\n"+srv.URL+"/file,file.bin\n"), 0600); err != nil {
				t.Fatal(err)
			}
			args = append(args, "--max-retries", "0", "--processes", "1")
			code := Main(context.Background(), args, io.Discard, io.Discard, "test")
			report, _ := readReport(t, path)
			if status == http.StatusOK {
				if code != 0 || report.Status != "success" || report.Downloaded != 1 {
					t.Fatalf("code=%d report=%+v", code, report)
				}
				var got []byte
				err := filepath.WalkDir(args[3], func(p string, d os.DirEntry, e error) error {
					if e == nil && d.Name() == "file.bin" {
						got, e = os.ReadFile(p)
					}
					return e
				})
				if err != nil || string(got) != "test payload" {
					t.Fatalf("download=%q err=%v", got, err)
				}
			} else if code != 1 || report.Status != "failed" || report.Failed != 1 {
				t.Fatalf("HTTP failure reported success: code=%d report=%+v", code, report)
			}
		})
	}
}
