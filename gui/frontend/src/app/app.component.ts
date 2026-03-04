import { Component, OnInit, OnDestroy, NgZone, HostListener } from '@angular/core';
import { Subscription } from 'rxjs';
import { EventsOn } from '../../wailsjs/runtime/runtime';
import { CancelDownload, OpenAuthFileDialog, OpenInputFileDialog, OpenOutputDirectoryDialog, GetDefaultOutputDirectory, RunCLIFetch , IsMac } from '../../wailsjs/go/main/App';
import { DownloadStatusService } from './services/download-status.service';
import { DownloadOverviewSnapshot } from './models/download-series.model';


@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.scss']
})
export class AppComponent implements OnInit, OnDestroy {
  inputFilePath = '';
  outputDirPath = '';
  authFilePath = '';
  defaultDownloadDir = '';
  directoryMode: 'classic' | 'descriptive' = 'classic';

  isMac = false;

  private unsubscribeCliError?: () => void;
  private unsubscribeCliFinished?: () => void;

  private overviewSubscription?: Subscription;
  private seriesSubscription?: Subscription;

  // Advanced options / UI state
  showAdvancedModal = false;
  showManifestSection = true;
  maxConnections = 8;
  maxRetries = 3;
  simultaneousDownloads = 8;
  skipExisting = true;
  downloadInParallel = true;

  // Collapse state
  downloadsCollapsed = true;  // Collapsed until downloads start
  hasAutoExpanded = false;

  // Dark mode
  isDarkMode = false;

  showInitializing = false;

  series$ = this.downloadStatus.series$;
  overview$ = this.downloadStatus.overview$;
  manifest$ = this.downloadStatus.manifest$;

  constructor(
    private readonly downloadStatus: DownloadStatusService,
    private ngZone: NgZone
  ) {}

  async ngOnInit() {
    // Detect system theme preference
    if (window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches) {
      this.isDarkMode = true;
    }

    try {
      this.isMac = await IsMac();
      console.log("isMac =", this.isMac);
    } catch (err) {
      console.error("Error detecting macOS:", err);
    }

    this.defaultDownloadDir = await GetDefaultOutputDirectory();
    if (this.isMac) {this.defaultDownloadDir = ""}
    this.outputDirPath = this.defaultDownloadDir;

    // Listen for system theme changes
    window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', (e) => {
      this.isDarkMode = e.matches;
    });

    this.overviewSubscription = this.overview$.subscribe((snapshot: DownloadOverviewSnapshot) => {
      // Reset auto-expand guard when no downloads are present
      if (snapshot.total === 0) {
        this.hasAutoExpanded = false;
      }
      // Auto-expand downloads section only once per run
      if (snapshot.total > 0 && this.downloadsCollapsed && !this.hasAutoExpanded) {
        this.downloadsCollapsed = false;
        this.hasAutoExpanded = true;
      }
      if (this.showInitializing && snapshot.total > 0) {
        this.showInitializing = false;
      }
    });

    this.seriesSubscription = this.series$.subscribe(series => {
      if (this.showInitializing && series.length > 0) {
        this.showInitializing = false;
      }
    });

    // Subscribe to CLI error events
    this.unsubscribeCliError = EventsOn('cli-error', (err: string) => {
      this.ngZone.run(() => {
        this.showInitializing = false;
        this.downloadStatus.appendManifestLog(`ERROR: ${err}`);
      });
    });

    // Subscribe to CLI finished event
    this.unsubscribeCliFinished = EventsOn('cli-finished', (summary: string) => {
      this.ngZone.run(() => {
        this.showInitializing = false;
        if (summary) {
          this.downloadStatus.appendManifestLog(summary);
        }
      });
    });
  }

  ngOnDestroy() {
    this.unsubscribeCliError?.();
    this.unsubscribeCliFinished?.();
    this.overviewSubscription?.unsubscribe();
    this.seriesSubscription?.unsubscribe();
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

  @HostListener('document:keydown.escape', ['$event'])
  handleEscapeKey(event: KeyboardEvent) {
    if (this.showAdvancedModal) {
      event.preventDefault();
      this.closeAdvancedModal();
    }
  }

  get isManifestPaused(): boolean {
    return this.downloadStatus.getIsPaused();
  }

  onPauseToggled() {
    this.downloadStatus.togglePause();
  }

  onSelectOutputDirectory() {
    const fileName = this.inputFilePath.split(/[\\/]/).pop() || '';
    const baseName = fileName.replace(/\.[^/.]+$/, '');
    OpenOutputDirectoryDialog(baseName).then((dirPath: string) => {
      if (dirPath) {
        this.outputDirPath = dirPath;
      }
    }).catch(err => {
      this.downloadStatus.appendManifestLog("ERROR: " + err);
    });
  }

  onSelectAuthFile() {
    OpenAuthFileDialog().then((dirPath: string) => {
      if (dirPath) {
        this.authFilePath = dirPath;
      }
    }).catch(err => {
      //this.status = "Error: " + err;
    });
  }

  onFetchFiles() {
    if (!this.inputFilePath || !this.outputDirPath) {
      this.downloadStatus.appendManifestLog("ERROR: Please select both an input TCIA file and an output directory.");
      return;
    }

    this.showManifestSection = false;
    this.showInitializing = true;
    this.downloadStatus.beginRun(this.inputFilePath, this.outputDirPath);

    // Reconstruct the exact CLI command for display (quote paths to handle spaces)
    const cliPath = '../nbia-data-retriever-cli';
    const parts: string[] = [];
    parts.push(cliPath);
    parts.push('-i');
    parts.push(`"${this.inputFilePath}"`);
    parts.push('--output');
    parts.push(`"${this.outputDirPath}"`);
    parts.push('--max-connections');
    parts.push(String(this.maxConnections));
    parts.push('--max-retries');
    parts.push(String(this.maxRetries));
    parts.push('--processes');
    parts.push(String(this.simultaneousDownloads));
    if (this.downloadInParallel) {
      // The CLI does not have a --download-in-parallel flag.
      // We keep the frontend checkbox for UI/intent, but do not forward an unsupported flag.
    }
    if (this.skipExisting) {
      parts.push('--skip-existing');
    }
    const cmdStr = parts.join(' ');

    this.downloadStatus.appendManifestLog('Running: ' + cmdStr);

    // Call backend to run the CLI
    RunCLIFetch(
      this.inputFilePath,
      this.outputDirPath,
      this.maxConnections,
      this.maxRetries,
      this.simultaneousDownloads,
      this.skipExisting,
      this.downloadInParallel,
      this.authFilePath,
      this.directoryMode
    ).catch(err => {
      this.ngZone.run(() => {
        this.downloadStatus.appendManifestLog('ERROR: ' + err);
        this.showInitializing = false;
      });
    });
    this.downloadStatus.appendManifestLog('Started');
  }

  onCancelDownload() {
    this.showInitializing = false;
    CancelDownload()
      .then(() => {
        this.downloadStatus.appendManifestLog("Cancellation requested");
      })
      .catch(err => {
        this.downloadStatus.appendManifestLog("ERROR: " + err);
        this.showInitializing = false;
      });
  }

onSelectInputFile() {
  OpenInputFileDialog()
    .then((filePath: string) => {
      if (!filePath) return;

      this.inputFilePath = filePath;

      const fileName = filePath.split(/[\\/]/).pop() || '';
      const baseName = fileName.replace(/\.[^/.]+$/, '');

      // Only auto-set if user hasn't manually changed it
      if ( !this.isMac && 
        (!this.outputDirPath ||
        this.outputDirPath === this.defaultDownloadDir)
      ) {
        this.outputDirPath = `${this.defaultDownloadDir}/${baseName}`;
      }
      if (this.isMac) {

          if (this.outputDirPath) {
            const parts = this.outputDirPath.split('/');
            parts[parts.length - 1] = baseName;
            this.outputDirPath = parts.join('/');
          } else {
            // fallback if outputDirPath is empty
            this.outputDirPath = ""
          }


      }
    })
    .catch(err => {
      this.downloadStatus.appendManifestLog('ERROR: ' + err);
    });
}
}
