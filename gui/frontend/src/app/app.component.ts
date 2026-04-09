import { Component, OnInit, OnDestroy, NgZone, HostListener } from '@angular/core';
import { Subscription } from 'rxjs';
import {
  CancelDownload,
  OpenAuthFileDialog,
  OpenInputFileDialog,
  OpenOutputDirectoryDialog,
  GetDefaultOutputDirectory,
  RunCLIFetch,
  IsMac,
  GetPendingFileOpen,
  FrontendReady,
} from '../../wailsjs/go/main/App';
import { DownloadStatusService } from './services/download-status.service';
import { RunState } from './models/run-state.model';
import { EventsOn } from '../../wailsjs/runtime/runtime';

@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.scss'],
})
export class AppComponent implements OnInit, OnDestroy {
  // ── Form state (shared across all runs) ──────────────────────────────────
  inputFilePath = '';
  outputDirPath = '';
  authFilePath = '';
  defaultDownloadDir = '';
  directoryMode: 'classic' | 'descriptive' = 'classic';
  isMac = false;

  // Track the last path that was auto-set so we can avoid overwriting manual edits
  private lastAutoSetOutputPath = '';

  // ── Advanced options ──────────────────────────────────────────────────────
  showAdvancedModal = false;
  showManifestModal = false;
  maxConnections = 8;
  maxRetries = 3;
  simultaneousDownloads = 8;
  skipExisting = true;
  downloadInParallel = true;

  // ── Dark mode ─────────────────────────────────────────────────────────────
  isDarkMode = false;

  // ── Runs (one entry per manifest) ─────────────────────────────────────────
  runs: RunState[] = [];
  private runsSubscription?: Subscription;

  constructor(
    private readonly downloadStatus: DownloadStatusService,
    private readonly ngZone: NgZone,
  ) {}

  async ngOnInit() {
    // Detect system theme
    if (window.matchMedia?.('(prefers-color-scheme: dark)').matches) {
      this.isDarkMode = true;
    }
    window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', e => {
      this.isDarkMode = e.matches;
    });

    // Detect platform
    try {
      this.isMac = await IsMac();
    } catch (err) {
      console.error('Error detecting macOS:', err);
    }

    // Set default output directory
    this.defaultDownloadDir = await GetDefaultOutputDirectory();
    if (this.isMac) this.defaultDownloadDir = '';
    this.outputDirPath = this.defaultDownloadDir;

    // Subscribe to all runs
    this.runsSubscription = this.downloadStatus.runs$.subscribe(runs => {
      this.runs = runs;
    });

    EventsOn('file-opened', (filePath: string) => {
      this.ngZone.run(() => {
        this.inputFilePath = filePath;
        const baseName = this.baseNameOf(filePath);
        if (this.outputDirPath) {
          const parts = this.outputDirPath.split('/');
          parts[parts.length - 1] = baseName;
          this.outputDirPath = parts.join('/');
          this.lastAutoSetOutputPath = this.outputDirPath;
        }
        this.openManifestModal();
      });
    });

    // Check for a file that was opened before the frontend was ready (cold launch)
    try {
      const pendingPath = await GetPendingFileOpen();
      if (pendingPath) {
        this.ngZone.run(() => {
          this.inputFilePath = pendingPath;
          const baseName = this.baseNameOf(pendingPath);
          if (this.outputDirPath) {
            const parts = this.outputDirPath.split('/');
            parts[parts.length - 1] = baseName;
            this.outputDirPath = parts.join('/');
            this.lastAutoSetOutputPath = this.outputDirPath;
          }
          this.openManifestModal();
        });
      }
    } catch (err) {
      console.error('Error checking pending file open:', err);
    }

    FrontendReady();


  }

  ngOnDestroy() {
    this.runsSubscription?.unsubscribe();
  }

  // ── Aggregates across all runs ────────────────────────────────────────────

  get globalOverview() {
    let total = 0, queued = 0, active = 0, completed = 0, failed = 0, skipped = 0, cancelled = 0;
    for (const run of this.runs) {
      total     += run.overview.total;
      queued    += run.overview.queued;
      active    += run.overview.active;
      completed += run.overview.completed;
      failed    += run.overview.failed;
      skipped   += run.overview.skipped;
      cancelled += run.overview.cancelled;
    }
    return { total, queued, active, completed, failed, skipped, cancelled };
  }


  // ── UI helpers ─────────────────────────────────────────────────────────────

  trackByRunId(_: number, run: RunState): bigint {
    return run.runId;
  }

  toggleDarkMode() {
    this.isDarkMode = !this.isDarkMode;
  }

  openAdvancedModal() {
    this.showAdvancedModal = true;
  }

  closeAdvancedModal() {
    this.showAdvancedModal = false;
  }

  openManifestModal() { this.showManifestModal = true; }
  closeManifestModal() { this.showManifestModal = false; }

  @HostListener('document:keydown.escape')
  handleEscapeKey() {
    if (this.showAdvancedModal) this.closeAdvancedModal();
    if (this.showManifestModal) this.closeManifestModal();
  }

  // ── File / directory pickers ───────────────────────────────────────────────

  onSelectInputFile() {
    OpenInputFileDialog()
      .then((filePath: string) => {
        if (!filePath) return;
        this.inputFilePath = filePath;

        const baseName = this.baseNameOf(filePath);

        if (!this.isMac) {
          // Only auto-set if user hasn't diverged from the last auto-set value
          if (!this.outputDirPath || this.outputDirPath === this.lastAutoSetOutputPath) {
            this.outputDirPath = `${this.defaultDownloadDir}/${baseName}`;
            this.lastAutoSetOutputPath = this.outputDirPath;
          }
        } else {
          if (this.outputDirPath ) {
            const parts = this.outputDirPath.split('/');
            parts[parts.length - 1] = baseName;
            this.outputDirPath = parts.join('/');
            this.lastAutoSetOutputPath = this.outputDirPath;
          }
          // If outputDirPath is empty or was set manually, leave it alone
        }
      })
      .catch(err => {
        // No active runId yet — log to console; file picker errors are non-critical
        console.error('Error selecting input file:', err);
      });
  }

  onSelectOutputDirectory() {
    const baseName = this.baseNameOf(this.inputFilePath);
    OpenOutputDirectoryDialog(baseName)
      .then((dirPath: string) => {
        if (dirPath) {
          this.outputDirPath = dirPath;
          this.lastAutoSetOutputPath = dirPath;
        }
      })
      .catch(err => console.error('Error selecting output directory:', err));
  }

  onSelectAuthFile() {
    OpenAuthFileDialog()
      .then((filePath: string) => {
        if (filePath) this.authFilePath = filePath;
      })
      .catch(err => console.error('Error selecting auth file:', err));
  }

  // ── Start a new manifest download ─────────────────────────────────────────

  onFetchFiles() {
    if (!this.inputFilePath || !this.outputDirPath) {
      // No active runId yet; create a temporary log entry via the first run or a toast
      console.warn('Please select both an input file and an output directory.');
      return;
    }

    // Generate a random uint64 ID (matches Go backend's uint64 type).
    // Combine two random uint32s into one uint64 via BigInt.
    const buf = new Uint32Array(2);
    crypto.getRandomValues(buf);
    const runId: bigint = (BigInt(buf[0]) << 32n) | BigInt(buf[1]);

    // Register the run immediately so the card appears
    this.downloadStatus.beginRun(runId, this.inputFilePath, this.outputDirPath);

    // Close the modal so the user can see the new card appear
    this.closeManifestModal();

    // Build CLI command string for the log
    const parts: string[] = [
      '../nbia-data-retriever-cli',
      '-i', `"${this.inputFilePath}"`,
      '--output', `"${this.outputDirPath}"`,
      '--max-connections', String(this.maxConnections),
      '--max-retries', String(this.maxRetries),
      '--processes', String(this.simultaneousDownloads),
    ];
    if (this.skipExisting) parts.push('--skip-existing');
    this.downloadStatus.appendManifestLog(runId, 'Running: ' + parts.join(' '));
    this.downloadStatus.appendManifestLog(runId, 'Started');

    // Kick off the download — pass runId so Go can tag events back to us
    RunCLIFetch(
      this.inputFilePath,
      this.outputDirPath,
      this.maxConnections,
      this.maxRetries,
      this.simultaneousDownloads,
      this.skipExisting,
      this.downloadInParallel,
      this.authFilePath,
      this.directoryMode,
      Number(runId),
    ).catch(err => {
      this.ngZone.run(() => {
        this.downloadStatus.setRunError(runId, err);
      });
    });

    // Reset the form fields so the user can immediately add another manifest
    this.inputFilePath = '';
    this.lastAutoSetOutputPath = '';
    // Leave outputDirPath as-is as a convenience starting point for the next run
  }

  // ── Per-run controls (called from download-card via events) ───────────────

  onCancelDownload(runId: bigint) {
    this.downloadStatus.cancelRun(runId);
    CancelDownload()
      .catch(err => this.downloadStatus.appendManifestLog(runId, 'ERROR: ' + err));
  }

  onRemoveRun(runId: bigint) {
    this.downloadStatus.removeRun(runId);
  }

  onPauseToggled(runId: bigint) {
    this.downloadStatus.togglePause(runId);
  }

  onCollapseToggled(runId: bigint, collapsed: boolean) {
    this.downloadStatus.setCollapsed(runId, collapsed);
  }

  // ── Convenience ───────────────────────────────────────────────────────────

  private baseNameOf(filePath: string): string {
    const fileName = filePath.split(/[\\/]/).pop() ?? '';
    return fileName.replace(/\.[^/.]+$/, '');
  }
}
