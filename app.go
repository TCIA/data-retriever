package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	stdRuntime "runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"TCIA_Data_Retriever/core/app"
	wailsRuntime "github.com/wailsapp/wails/v2/pkg/runtime"
)

type UpdateInfo struct {
	Available     bool   `json:"available"`
	LatestVersion string `json:"latestVersion"`
	URL           string `json:"url"`
}

type SupportInfo struct {
	AppVersion string `json:"appVersion"`
	OSPlatform string `json:"osPlatform"`
	OSVersion  string `json:"osVersion"`
}

func (a *App) GetVersion() string {
	return version
}

func buildSupportInfo(appVersion string, osPlatform string, osVersion string) SupportInfo {
	appVersion = strings.TrimSpace(appVersion)
	if appVersion == "" {
		appVersion = "dev"
	}

	osPlatform = strings.TrimSpace(osPlatform)
	if osPlatform == "" {
		osPlatform = "unknown"
	}

	osVersion = strings.TrimSpace(osVersion)
	if osVersion == "" {
		osVersion = "Unknown"
	}

	return SupportInfo{
		AppVersion: appVersion,
		OSPlatform: osPlatform,
		OSVersion:  osVersion,
	}
}

func parseLinuxOSRelease(content string) string {
	var prettyName string
	var name string
	var versionID string

	for _, rawLine := range strings.Split(content, "\n") {
		line := strings.TrimSpace(rawLine)
		if line == "" || strings.HasPrefix(line, "#") || !strings.Contains(line, "=") {
			continue
		}

		parts := strings.SplitN(line, "=", 2)
		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		value = strings.Trim(value, `"'`)

		switch key {
		case "PRETTY_NAME":
			prettyName = value
		case "NAME":
			name = value
		case "VERSION_ID":
			versionID = value
		}
	}

	if prettyName != "" {
		return prettyName
	}
	if name != "" && versionID != "" {
		return name + " " + versionID
	}
	if name != "" {
		return name
	}

	return ""
}

func detectLinuxVersion() string {
	if osRelease, err := os.ReadFile("/etc/os-release"); err == nil {
		if parsed := parseLinuxOSRelease(string(osRelease)); parsed != "" {
			return parsed
		}
	}

	output, err := exec.Command("uname", "-r").Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(output))
}

func detectWindowsVersion() string {
	output, err := exec.Command("cmd", "/C", "ver").Output()
	if err != nil {
		return ""
	}

	cleaned := strings.ReplaceAll(string(output), "\r", "")
	return strings.TrimSpace(cleaned)
}

func detectDarwinVersion() string {
	output, err := exec.Command("sw_vers", "-productVersion").Output()
	if err != nil {
		return ""
	}

	versionText := strings.TrimSpace(string(output))
	if versionText == "" {
		return ""
	}

	return "macOS " + versionText
}

func detectOSVersion(goos string) string {
	switch goos {
	case "linux":
		return detectLinuxVersion()
	case "windows":
		return detectWindowsVersion()
	case "darwin":
		return detectDarwinVersion()
	default:
		return ""
	}
}

func (a *App) GetSupportInfo() SupportInfo {
	return buildSupportInfo(version, stdRuntime.GOOS, detectOSVersion(stdRuntime.GOOS))
}

func (a *App) CheckForUpdate() (UpdateInfo, error) {
	if version == "" || version == "dev" {
		return UpdateInfo{}, nil
	}
	// Store builds (Mac App Store, Microsoft Store) update through their
	// respective storefronts, so skip the GitHub release check entirely.
	if distChannel != "" && distChannel != "github" {
		return UpdateInfo{}, nil
	}

	const apiURL = "https://api.github.com/repos/TCIA/data-retriever/releases/latest"
	client := &http.Client{Timeout: 10 * time.Second}
	req, err := http.NewRequest("GET", apiURL, nil)
	if err != nil {
		return UpdateInfo{}, err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("User-Agent", "TCIA/data-retriever/"+version)

	resp, err := client.Do(req)
	if err != nil {
		return UpdateInfo{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return UpdateInfo{}, nil
	}

	var release struct {
		TagName string `json:"tag_name"`
		HTMLURL string `json:"html_url"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&release); err != nil {
		return UpdateInfo{}, err
	}

	current := strings.TrimPrefix(strings.TrimSpace(version), "v")
	latest := strings.TrimPrefix(strings.TrimSpace(release.TagName), "v")

	return UpdateInfo{
		Available:     latest != "" && latest != current,
		LatestVersion: release.TagName,
		URL:           release.HTMLURL,
	}, nil
}

func (b *App) GetPendingFileOpen() string {
	path := b.pendingFileOpen
	b.pendingFileOpen = ""
	return path
}

func (b *App) HandleFileOpen(filePath string) {
	// The app may not be fully started yet when this fires on cold launch,
	// so guard against a nil context.

	if b.ctx == nil {
		b.pendingFileOpen = filePath
		return
	}
	go func() {
		select {
		case <-b.frontendReady:
			wailsRuntime.EventsEmit(b.ctx, "file-opened", filePath)
		}
	}()

}

func isWindowsSystemPath(p string) bool {
	lower := strings.ToLower(filepath.ToSlash(p))
	return strings.Contains(lower, "/windows/system32") ||
		strings.Contains(lower, "/windows/syswow64")
}

func dirExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}

func linuxDownloadsDir(home string) string {
	config := filepath.Join(home, ".config", "user-dirs.dirs")
	file, err := os.Open(config)
	if err != nil {
		return ""
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "XDG_DOWNLOAD_DIR=") {
			value := strings.TrimPrefix(line, "XDG_DOWNLOAD_DIR=")
			value = strings.Trim(value, `"`)
			value = strings.Replace(value, "$HOME", home, 1)

			if dirExists(value) {
				return value
			}
		}
	}
	return ""
}

func (b *App) GetDefaultOutputDirectory() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}

	switch stdRuntime.GOOS {
	case "windows":
		// Prefer USERPROFILE over os.UserHomeDir, which can resolve to a
		// system profile (e.g. C:\Windows\system32\config\systemprofile)
		// when the process is spawned by an installer or service account.
		if up := os.Getenv("USERPROFILE"); up != "" && !isWindowsSystemPath(up) {
			home = up
		} else if isWindowsSystemPath(home) {
			// Last resort: walk up to a drive root rather than leaving
			// the user stuck in a Windows system directory.
			return ""
		}
		downloads := filepath.Join(home, "Downloads")
		if dirExists(downloads) {
			return downloads
		}
		return home

	case "darwin":
		// macOS: ~/Downloads
		u, err := user.Lookup(os.Getenv("LOGNAME"))
		if err == nil {
			return filepath.Join(u.HomeDir, "Downloads")
		}
		downloads := filepath.Join(home, "Downloads")
		if dirExists(downloads) {
			return downloads
		}
		return home

	case "linux":
		// Linux: try XDG user-dirs first
		if xdg := linuxDownloadsDir(home); xdg != "" {
			return xdg
		}

		// Fallback: ~/Downloads
		downloads := filepath.Join(home, "Downloads")
		if dirExists(downloads) {
			return downloads
		}
		return home

	default:
		return home
	}
}

// OpenAuthFileDialog opens a system file dialog and returns the selected file path
func (b *App) OpenAuthFileDialog() (string, error) {
	result, err := wailsRuntime.OpenFileDialog(b.ctx, wailsRuntime.OpenDialogOptions{
		Title: "Select .json Auth Token",
	})
	if err != nil {
		return "", err
	}
	if result == "" {
		return "", nil // User cancelled
	}
	return result, nil
}

// OpenInputFileDialog opens a system file dialog and returns the selected file path
func (b *App) OpenInputFileDialog() (string, error) {
	result, err := wailsRuntime.OpenFileDialog(b.ctx, wailsRuntime.OpenDialogOptions{
		Title: "Select Manifest File (.tcia/.s5cmd/.csv/.tsv/.xlsx/.json/.jsonld)",
	})
	if err != nil {
		return "", err
	}
	if result == "" {
		return "", nil // User cancelled
	}
	return result, nil
}

// OpenOutputDirectoryDialog opens a system directory dialog and returns the selected directory path
func (b *App) OpenOutputDirectoryDialog(inputFile string) (string, error) {
	var result string
	var err error
	defaultDir := b.GetDefaultOutputDirectory()
	result, err = wailsRuntime.OpenDirectoryDialog(b.ctx, wailsRuntime.OpenDialogOptions{
		Title:            "Download Directory",
		DefaultDirectory: defaultDir,
	})
	if stdRuntime.GOOS == "darwin" {
		result = filepath.Join(result, inputFile)
	}
	if err != nil {
		return "", err
	}
	if result == "" {
		return "", nil // User cancelled
	}
	return result, nil
}

// RunCLIFetch runs the CLI tool asynchronously with the given manifest and output directory and advanced options.
func (b *App) RunCLIFetch(
	manifestPath string,
	outputDir string,
	maxConnections int,
	maxRetries int,
	simultaneousDownloads int,
	skipExisting bool,
	downloadInParallel bool,
	authPath string,
	directoryMode string,
	verbose bool,
	runId uint64,
) (string, error) {

	if b.ctx == nil {
		return "", fmt.Errorf("application context not initialised")
	}

	// Create a new batch
	//b.mu.Lock()
	//b.runID++
	id := runId

	ctx, cancel := context.WithCancel(b.ctx)

	batch := &DownloadBatch{
		ID:            id,
		Ctx:           ctx,
		Cancel:        cancel,
		Manifest:      manifestPath,
		OutputDir:     outputDir,
		MaxConn:       maxConnections,
		MaxRetries:    maxRetries,
		Parallel:      simultaneousDownloads,
		SkipExist:     skipExisting,
		AuthPath:      authPath,
		DirectoryMode: directoryMode,
		Verbose:       verbose,
	}

	if b.batches == nil {
		b.batches = make(map[uint64]*DownloadBatch)
	}
	b.batches[id] = batch
	//b.mu.Unlock()

	// Run the batch in its own goroutine
	go b.runBatch(batch)

	// Return immediately so frontend is free to repaint
	return fmt.Sprintf("started batch %d", id), nil
}

func (b *App) runBatch(batch *DownloadBatch) {
	defer func() {
		// Remove batch from map when done
		b.mu.Lock()
		delete(b.batches, batch.ID)
		b.mu.Unlock()
	}()

	user := os.Getenv("NBIA_USER")
	if user == "" {
		user = "nbia_guest"
	}
	pass := os.Getenv("NBIA_PASS")

	gate := &app.AuthGate{}
	b.authGates.Store(uint64(batch.ID), gate)
	defer b.authGates.Delete(uint64(batch.ID))

	// "Simultaneous Downloads" and "Max Connections" are each one shared
	// setting across the whole app (see downloadSemaphore's and
	// httpClient's doc comments), so resize them to whatever this launch
	// requested rather than giving this batch its own pool/client.
	b.downloadSemaphore.SetLimit(batch.Parallel)
	app.SetMaxConnsPerHost(b.httpClient, batch.MaxConn)

	options := &app.Options{
		Input:                 batch.Manifest,
		Output:                batch.OutputDir,
		Proxy:                 "",
		Concurrent:            batch.Parallel,
		Meta:                  false,
		Username:              user,
		Password:              pass,
		Version:               false,
		Debug:                 false,
		Verbose:               batch.Verbose,
		Help:                  false,
		MetaUrl:               app.MetaUrl,
		TokenUrl:              app.TokenUrl,
		ImageUrl:              app.ImageUrl,
		SaveLog:               false,
		Prompt:                false,
		Force:                 false,
		SkipExisting:          batch.SkipExist,
		MaxRetries:            batch.MaxRetries,
		RetryDelay:            10 * time.Second,
		InterimUpdateInterval: 500 * time.Millisecond,
		MaxConnsPerHost:       batch.MaxConn,
		ServerFriendly:        false,
		RequestDelay:          500 * time.Millisecond,
		NoMD5:                 false,
		NoDecompress:          false,
		RefreshMetadata:       false,
		MetadataWorkers:       20,
		Auth: func() string {
			if batch.AuthPath != "" {
				return batch.AuthPath
			}
			return app.LoadSavedAuthFilePath()
		}(),
		DirectoryMode:    batch.DirectoryMode,
		IDCParquetPath:   b.parquetPaths.IDCIndex,
		PriorParquetPath: b.parquetPaths.PriorVersions,
		AuthGate:         gate,
		LogSink:          newGuiLogSink(b.ctx, batch.ID),
		Semaphore:        b.downloadSemaphore,
		SharedHTTPClient: b.httpClient,
	}

	logTimestamp := time.Now().Format("20060102-150405")
	logPath := app.DefaultLogFilePath(fmt.Sprintf("nbia-output-%s.log", logTimestamp))
	runStart := time.Now()

	var eventLog *app.TextEventLogger
	if l, logErr := app.NewTextEventLogger(logPath, runStart, options.InterimUpdateInterval); logErr == nil {
		eventLog = l
	}

	defer func() {
		if eventLog != nil {
			eventLog.Close()
		}
	}()

	// Stringified so large uint64 values survive the round trip through JS
	// Number without losing precision (the frontend parses it back via
	// BigInt()). Every event below must carry this so the frontend can route
	// it to the right run instead of guessing "most recently started".
	runIDStr := fmt.Sprintf("%d", batch.ID)

	manifestReceived := false
	callbacks := app.Callbacks{
		Stdout: func(line string) {
			if eventLog != nil {
				eventLog.HandleStdout(line)
			}
		},
		Stderr: func(line string) {
			if eventLog != nil {
				eventLog.HandleStderr(line)
			}
		},
		Series: func(evt app.SeriesEvent) {
			if eventLog != nil {
				eventLog.HandleSeries(evt)
			}
			wailsRuntime.EventsEmit(b.ctx, "download-series-event", struct {
				app.SeriesEvent
				RunID string `json:"runId"`
			}{evt, runIDStr})
		},
		Manifest: func(p app.ManifestPayload) {
			manifestReceived = true
			if eventLog != nil {
				eventLog.HandleManifest(p)
			}
			wailsRuntime.EventsEmit(b.ctx, "manifest-series-metadata", struct {
				app.ManifestPayload
				RunID string `json:"runId"`
			}{p, runIDStr})
		},
		EmitEvent: func(name string, data ...interface{}) {
			// Prepend runId so the frontend knows which run needs auth
			args := append([]interface{}{runIDStr}, data...)
			wailsRuntime.EventsEmit(b.ctx, name, args...)
		},
	}

	// Run the CLI download (blocking inside goroutine)
	summary, err := app.Run(batch.Ctx, options, callbacks)
	if eventLog != nil {
		eventLog.LogRunFinished(summary, err)
	}

	if err != nil {
		if errors.Is(err, context.Canceled) {
			wailsRuntime.EventsEmit(b.ctx, "cli-finished", map[string]interface{}{"runId": runIDStr, "summary": ""})
			return
		}
		wailsRuntime.EventsEmit(b.ctx, "cli-error", map[string]interface{}{"runId": runIDStr, "error": fmt.Sprintf("download failed: %v", err)})
		return
	}

	if !manifestReceived {
		wailsRuntime.EventsEmit(b.ctx, "cli-error", map[string]interface{}{"runId": runIDStr, "error": fmt.Sprintf("no metadata can be found for this manifest: %s", batch.Manifest)})
		return
	}

	summaryText := ""
	if summary != nil {
		summaryText = fmt.Sprintf(
			"Download Summary: total %d, downloaded %d, synced %d, skipped %d, failed %d, elapsed %s",
			summary.Total,
			summary.Downloaded,
			summary.Synced,
			summary.Skipped,
			summary.Failed,
			summary.Elapsed.String(),
		)
	}
	wailsRuntime.EventsEmit(b.ctx, "cli-finished", map[string]interface{}{"runId": runIDStr, "summary": summaryText})
}

func (a *App) ResolveAuth(runIdStr string, authFilePath string) error {
	runId, err := strconv.ParseUint(runIdStr, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid runId: %w", err)
	}
	if authFilePath != "" {
		if saveErr := app.SaveAuthFile(authFilePath); saveErr != nil {
			fmt.Fprintf(os.Stderr, "failed to save auth file: %v\n", saveErr)
		}
	}
	if v, ok := a.authGates.Load(runId); ok {
		v.(*app.AuthGate).Resolve(authFilePath)
	}
	return nil
}

func (a *App) CancelAuth(runIdStr string) error {
	runId, err := strconv.ParseUint(runIdStr, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid runId: %w", err)
	}
	if v, ok := a.authGates.Load(runId); ok {
		v.(*app.AuthGate).Resolve("")
	}
	return nil
}

func (b *App) CancelDownload() {
	b.mu.Lock()
	defer b.mu.Unlock()

	for id, batch := range b.batches {
		batch.Cancel()        // cancel the batch
		delete(b.batches, id) // remove it from the map
	}
}

func resolveLogLocationTarget(inputPath string, fallbackDir string) (string, bool) {
	trimmed := strings.TrimSpace(inputPath)
	if trimmed == "" {
		return fallbackDir, false
	}

	cleaned := filepath.Clean(trimmed)
	info, err := os.Stat(cleaned)
	if err != nil {
		return fallbackDir, false
	}

	if info.IsDir() {
		return cleaned, false
	}

	return cleaned, true
}

func openLogLocationCommandForOS(goos string, targetPath string, revealFile bool) (string, []string, error) {
	target := strings.TrimSpace(targetPath)
	if target == "" {
		return "", nil, fmt.Errorf("target path is empty")
	}

	switch goos {
	case "windows":
		if revealFile {
			return "explorer", []string{"/select," + target}, nil
		}
		return "explorer", []string{target}, nil
	case "darwin":
		if revealFile {
			return "open", []string{"-R", target}, nil
		}
		return "open", []string{target}, nil
	case "linux":
		if revealFile {
			target = filepath.Dir(target)
		}
		return "xdg-open", []string{target}, nil
	default:
		return "", nil, fmt.Errorf("unsupported operating system: %s", goos)
	}
}

// OpenLogLocation opens log location in the default file manager.
// If the input is an existing file path, macOS/Windows reveal the file while
// Linux opens its containing directory. If the path does not resolve to an
// existing file/directory (for example a placeholder pattern), the default log
// directory is opened instead.
func (b *App) OpenLogLocation(path string) error {
	fallbackDir := app.DefaultLogDir()
	targetPath, revealFile := resolveLogLocationTarget(path, fallbackDir)

	if !revealFile {
		if err := os.MkdirAll(targetPath, 0o755); err != nil {
			return fmt.Errorf("failed to ensure log directory exists: %w", err)
		}
	}

	cmdName, cmdArgs, err := openLogLocationCommandForOS(stdRuntime.GOOS, targetPath, revealFile)
	if err != nil {
		return err
	}

	cmd := exec.Command(cmdName, cmdArgs...)
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to open log location in file manager: %w", err)
	}

	return nil
}

func (b *App) OpenDirectory(path string) error {
	trimmed := strings.TrimSpace(path)
	if trimmed == "" {
		return fmt.Errorf("directory path is empty")
	}

	cleaned := filepath.Clean(trimmed)
	info, err := os.Stat(cleaned)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("directory does not exist: %s", cleaned)
		}
		return fmt.Errorf("failed to access directory: %w", err)
	}

	if !info.IsDir() {
		return fmt.Errorf("path is not a directory: %s", cleaned)
	}

	var cmd *exec.Cmd
	switch stdRuntime.GOOS {
	case "windows":
		cmd = exec.Command("explorer", cleaned)
	case "darwin":
		cmd = exec.Command("open", cleaned)
	case "linux":
		cmd = exec.Command("xdg-open", cleaned)
	default:
		return fmt.Errorf("unsupported operating system: %s", stdRuntime.GOOS)
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to open directory in file manager: %w", err)
	}

	return nil
}

func (a *App) IsMac() bool {
	return stdRuntime.GOOS == "darwin"
}

// GetLatestSupportLogPath returns the newest NBIA run log by modified time.
// If no log file exists yet, it returns the expected timestamped log pattern.
func (a *App) GetLatestSupportLogPath() string {
	if latestPath, ok := app.LatestNBIALogFilePath(); ok {
		return latestPath
	}
	return app.ExpectedNBIALogPathPattern()
}

type App struct {
	ctx             context.Context
	mu              sync.Mutex
	runID           uint64
	batches         map[uint64]*DownloadBatch
	pausedBatches   map[string]*DownloadBatch // keyed by manifest path for resume
	parquetPaths    app.ParquetPaths
	pendingFileOpen string
	frontendReady   chan struct{}
	authGates       sync.Map
	// downloadSemaphore is shared by every batch so "simultaneous downloads"
	// is a total across all concurrently running manifests, not an
	// allowance handed out per manifest. Its limit is resized to match
	// whatever the UI last requested each time a batch starts.
	downloadSemaphore *app.WorkerSemaphore
	// httpClient is shared by every batch so "max connections" is a
	// process-wide per-host cap across all concurrently running manifests,
	// not an allowance handed out per manifest. Its limit is resized to
	// match whatever the UI last requested each time a batch starts.
	httpClient *http.Client
}

func NewApp() *App {
	return &App{
		batches:           make(map[uint64]*DownloadBatch),
		pausedBatches:     make(map[string]*DownloadBatch),
		frontendReady:     make(chan struct{}),
		downloadSemaphore: app.NewWorkerSemaphore(8),
		httpClient:        app.NewSharedHTTPClient(8),
	}
}

func (b *App) FrontendReady() {
	select {
	case <-b.frontendReady:
		// already closed, no-op
	default:
		close(b.frontendReady)
	}
}

// PauseManifest pauses a download by canceling its context and storing the batch for resume
func (b *App) PauseManifest(manifestPath string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	for id, batch := range b.batches {
		if batch.Manifest == manifestPath {
			batch.Cancel() // Cancel the context to stop downloads
			b.pausedBatches[manifestPath] = batch
			delete(b.batches, id)
			wailsRuntime.EventsEmit(b.ctx, "manifest-paused", manifestPath)
			return nil
		}
	}
	return fmt.Errorf("no active download found for manifest: %s", manifestPath)
}

// ResumeManifest resumes a paused download by restarting with skip-existing enabled
func (b *App) ResumeManifest(manifestPath string) error {
	b.mu.Lock()
	pausedBatch, exists := b.pausedBatches[manifestPath]
	if exists {
		delete(b.pausedBatches, manifestPath)
	}
	b.mu.Unlock()

	if !exists {
		return fmt.Errorf("no paused download found for manifest: %s", manifestPath)
	}

	// Resume by re-running with skip-existing enabled
	_, err := b.RunCLIFetch(
		pausedBatch.Manifest,
		pausedBatch.OutputDir,
		pausedBatch.MaxConn,
		pausedBatch.MaxRetries,
		pausedBatch.Parallel,
		true, // skipExisting = true to resume from where we left off
		true, // downloadInParallel
		pausedBatch.AuthPath,
		pausedBatch.DirectoryMode,
		pausedBatch.Verbose,
		pausedBatch.ID,
	)
	if err != nil {
		return err
	}

	wailsRuntime.EventsEmit(b.ctx, "manifest-resumed", manifestPath)
	return nil
}

// guiLogSink forwards zap log lines to the GUI's per-run log panel via a
// Wails event. Each Write splits the buffer on newlines so the frontend
// gets one event per log entry. The runId is sent as a string (mirroring
// EmitEvent) so the frontend can route it with BigInt() without losing
// precision, letting concurrent runs keep separate log panels.
type guiLogSink struct {
	ctx      context.Context
	runIDStr string
}

func newGuiLogSink(ctx context.Context, batchID uint64) *guiLogSink {
	return &guiLogSink{ctx: ctx, runIDStr: fmt.Sprintf("%d", batchID)}
}

func (s *guiLogSink) Write(p []byte) (int, error) {
	if s == nil || s.ctx == nil {
		return len(p), nil
	}
	for _, line := range strings.Split(strings.TrimRight(string(p), "\n"), "\n") {
		if line == "" {
			continue
		}
		wailsRuntime.EventsEmit(s.ctx, "manifest-log", map[string]interface{}{"runId": s.runIDStr, "line": line})
	}
	return len(p), nil
}

type DownloadBatch struct {
	ID     uint64
	Ctx    context.Context
	Cancel context.CancelFunc

	Manifest  string
	OutputDir string

	MaxConn       int
	MaxRetries    int
	Parallel      int
	SkipExist     bool
	AuthPath      string
	DirectoryMode string
	Verbose       bool
}

func (a *App) FetchFiles() string {
	return "Done!"
}

func (b *App) startup(ctx context.Context) {
	b.ctx = ctx

	paths, err := app.EnsureParquetsUpToDate()
	if err != nil {
		// Log but don't crash — app can still run, decodeS5cmd will error
		// gracefully if the path is empty.
		fmt.Fprintf(os.Stderr, "parquet init failed: %v\n", err)
	}
	b.parquetPaths = paths
}

func (b *App) shutdown(ctx context.Context) {
	// Perform teardown here
}

func (b *App) Greet(name string) string {
	return fmt.Sprintf("Hello %s, It's show time!", name)
}

func (b *App) ShowDialog() {
	_, err := wailsRuntime.MessageDialog(b.ctx, wailsRuntime.MessageDialogOptions{
		Type:    wailsRuntime.InfoDialog,
		Title:   "Native Dialog from Go",
		Message: "This is a Native Dialog send from Go.",
	})

	if err != nil {
		panic(err)
	}
}
