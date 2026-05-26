import { Component, OnInit, OnDestroy, NgZone, HostListener } from '@angular/core';
import { Subscription } from 'rxjs';
import {
  CancelDownload,
  CheckForUpdate,
  GetVersion,
  OpenAuthFileDialog,
  OpenInputFileDialog,
  OpenOutputDirectoryDialog,
  GetDefaultOutputDirectory,
  RunCLIFetch,
  IsMac,
  GetPendingFileOpen,
  FrontendReady,
  ResolveAuth,
  CancelAuth,
  DeclineLicense,
} from '../../wailsjs/go/main/App';
import { DownloadStatusService } from './services/download-status.service';
import { RunState, RunOptions } from './models/run-state.model';
import { EventsOn, BrowserOpenURL } from '../../wailsjs/runtime/runtime';

@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.scss'],
})
export class AppComponent implements OnInit, OnDestroy {
  // ── License ───────────────────────────────────────────────────────────────
  licenseAgreed = false;


  // ── Form state (shared across all runs) ──────────────────────────────────
  inputFilePath = '';
  outputDirPath = '';
  authFilePath = '';
  defaultDownloadDir = '';
  directoryMode: 'classic' | 'descriptive' = 'descriptive';
  isMac = false;
  pendingAuthRunId: string | null = null;
  authErrorMessage: string = '';

  private lastAutoSetOutputPath = '';

  // ── Advanced options ──────────────────────────────────────────────────────
  showAdvancedModal = false;
  showManifestModal = false;
  showAuthModal = false;

  authRequired = false;

  maxConnections = 8;
  maxRetries = 3;
  simultaneousDownloads = 8;
  skipExisting = true;
  downloadInParallel = true;
  verbose = false;

  // ── Version / update ─────────────────────────────────────────────────────
  appVersion = '';
  updateAvailable = false;
  updateVersion = '';
  updateUrl = '';
  showUpdateBanner = false;

  // ── Dark mode ─────────────────────────────────────────────────────────────
  isDarkMode = false;

  // ── Runs ──────────────────────────────────────────────────────────────────
  runs: RunState[] = [];
  private runsSubscription?: Subscription;

  constructor(
    private readonly downloadStatus: DownloadStatusService,
    private readonly ngZone: NgZone,
  ) {}

  async ngOnInit() {
    if (window.matchMedia?.('(prefers-color-scheme: dark)').matches) {
      this.isDarkMode = true;
    }
    window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', e => {
      this.isDarkMode = e.matches;
    });

    try {
      this.isMac = await IsMac();
    } catch (err) {
      console.error('Error detecting macOS:', err);
    }

    this.defaultDownloadDir = await GetDefaultOutputDirectory();
    if (this.isMac) this.defaultDownloadDir = '';
    this.outputDirPath = this.defaultDownloadDir;

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

    EventsOn('open:auth-modal', (runId: string) => {
      this.ngZone.run(() => {
        this.pendingAuthRunId = runId;
        this.authRequired = true;
        this.showAuthModal = true;
      });
    });

    EventsOn('auth-error', (runId: string, message: string) => {
      console.log('auth-error raw:', JSON.stringify(message));
      this.ngZone.run(() => {
        this.authErrorMessage = message;
      });
    });

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

    GetVersion().then(v => { this.appVersion = v; }).catch(() => {});

    CheckForUpdate().then(info => {
      if (info?.available) {
        this.ngZone.run(() => {
          this.updateAvailable = true;
          this.updateVersion = info.latestVersion;
          this.updateUrl = info.url;
          this.showUpdateBanner = true;
        });
      }
    }).catch(() => {});
  }

  ngOnDestroy() {
    this.runsSubscription?.unsubscribe();
  }

  // ── License ───────────────────────────────────────────────────────────────


  onLicenseDeclined() {
    DeclineLicense();
  }

  onLicenseAgreed() {
    this.licenseAgreed = true;
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

  confirmAuth() {
    if (!this.authFilePath || this.pendingAuthRunId === null) return;
    const runId = this.pendingAuthRunId;
    const path = this.authFilePath;
    this.authErrorMessage = '';
    this.pendingAuthRunId = null;
    this.authRequired = false;
    this.showAuthModal = false;
    ResolveAuth(runId, path).catch(err => console.error('ResolveAuth error:', err));
  }

  closeAdvancedModal() {
    if (this.authRequired && this.pendingAuthRunId !== null) {
      const runId = this.pendingAuthRunId;
      this.authRequired = false;
      this.pendingAuthRunId = null;
      CancelAuth(runId).catch(err => console.error('CancelAuth error:', err));
    }
    this.showAdvancedModal = false;
  }

  closeAuthModal() {
    if (this.authRequired && this.pendingAuthRunId !== null) {
      const runId = this.pendingAuthRunId;
      this.authRequired = false;
      this.pendingAuthRunId = null;
      CancelAuth(runId).catch(err => console.error('CancelAuth error:', err));
    }
    this.showAuthModal = false;
  }

  @HostListener('document:keydown.escape')
  handleEscapeKey() {
    if (this.showAuthModal) this.closeAuthModal();
    if (this.showAdvancedModal) this.closeAdvancedModal();
    if (this.showManifestModal) this.closeManifestModal();
  }

  openManifestModal() { this.showManifestModal = true; }
  closeManifestModal() { this.showManifestModal = false; }

  // ── File / directory pickers ───────────────────────────────────────────────

  onSelectInputFile() {
    OpenInputFileDialog()
      .then((filePath: string) => {
        if (!filePath) return;
        this.inputFilePath = filePath;
        const baseName = this.baseNameOf(filePath);
        if (!this.isMac) {
          if (!this.outputDirPath || this.outputDirPath === this.lastAutoSetOutputPath) {
            this.outputDirPath = `${this.defaultDownloadDir}/${baseName}`;
            this.lastAutoSetOutputPath = this.outputDirPath;
          }
        } else {
          if (this.outputDirPath) {
            const parts = this.outputDirPath.split('/');
            parts[parts.length - 1] = baseName;
            this.outputDirPath = parts.join('/');
            this.lastAutoSetOutputPath = this.outputDirPath;
          }
        }
      })
      .catch(err => console.error('Error selecting input file:', err));
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
      console.warn('Please select both an input file and an output directory.');
      return;
    }

    const buf = new Uint32Array(2);
    crypto.getRandomValues(buf);
    const runId: bigint = (BigInt(buf[0]) << 32n) | BigInt(buf[1]);

    const runOptions: RunOptions = {
      maxConnections: this.maxConnections,
      maxRetries: this.maxRetries,
      simultaneousDownloads: this.simultaneousDownloads,
      skipExisting: this.skipExisting,
      downloadInParallel: this.downloadInParallel,
      authFilePath: this.authFilePath,
      directoryMode: this.directoryMode,
      verbose: this.verbose,
    };
    this.downloadStatus.beginRun(runId, this.inputFilePath, this.outputDirPath, runOptions);
    this.closeManifestModal();

    const parts: string[] = [
      '../nbia-data-retriever-cli',
      '-i', `"${this.inputFilePath}"`,
      '--output', `"${this.outputDirPath}"`,
      '--max-connections', String(this.maxConnections),
      '--max-retries', String(this.maxRetries),
      '--processes', String(this.simultaneousDownloads),
    ];
    if (this.skipExisting) parts.push('--skip-existing');
    if (this.verbose) parts.push('--verbose');
    this.downloadStatus.appendManifestLog(runId, 'Running: ' + parts.join(' '));
    this.downloadStatus.appendManifestLog(runId, 'Started');

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
      this.verbose,
      Number(runId),
    ).catch(err => {
      this.ngZone.run(() => {
        this.downloadStatus.setRunError(runId, err);
      });
    });

    this.inputFilePath = '';
    this.lastAutoSetOutputPath = '';
  }

  dismissUpdateBanner() {
    this.showUpdateBanner = false;
  }

  openUpdateUrl() {
    BrowserOpenURL(this.updateUrl);
  }

  openNIHLink() {
    BrowserOpenURL('https://www.cancerimagingarchive.net/nih-controlled-data-access-policy/');
  }

  openDUALink() {
    BrowserOpenURL('https://www.cancerimagingarchive.net/data-usage-policies-and-restrictions/');
  }

  // ── Per-run controls ───────────────────────────────────────────────────────

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

  onRetryFailed(run: RunState) {
    const buf = new Uint32Array(2);
    crypto.getRandomValues(buf);
    const newRunId: bigint = (BigInt(buf[0]) << 32n) | BigInt(buf[1]);

    const opts = run.runOptions;
    const retryOptions: RunOptions = { ...opts, skipExisting: true };

    this.downloadStatus.beginRun(newRunId, run.inputFilePath, run.outputDirPath, retryOptions);
    this.downloadStatus.appendManifestLog(newRunId, 'Retrying failed downloads (skip-existing enabled)');

    this.downloadStatus.removeRun(run.runId);

    RunCLIFetch(
      run.inputFilePath,
      run.outputDirPath,
      opts.maxConnections,
      opts.maxRetries,
      opts.simultaneousDownloads,
      true,
      opts.downloadInParallel,
      opts.authFilePath,
      opts.directoryMode,
      opts.verbose,
      Number(newRunId),
    ).catch(err => {
      this.ngZone.run(() => {
        this.downloadStatus.setRunError(newRunId, err);
      });
    });
  }

  // ── Convenience ───────────────────────────────────────────────────────────

  private baseNameOf(filePath: string): string {
    const fileName = filePath.split(/[\\/]/).pop() ?? '';
    return fileName.replace(/\.[^/.]+$/, '');
  }
}
