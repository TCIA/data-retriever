import { Component, OnInit, OnDestroy, ElementRef, ViewChild, NgZone } from '@angular/core';
import { Subscription } from 'rxjs';
import { EventsOn, EventsOff } from '../../wailsjs/runtime/runtime';
import { CancelDownload, OpenInputFileDialog, OpenOutputDirectoryDialog, GetDefaultOutputDirectory, RunCLIFetch } from '../../wailsjs/go/main/App';
import { DownloadStatusService } from './services/download-status.service';
import { DownloadOverviewSnapshot } from './models/download-series.model';


@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.scss']
})
export class AppComponent implements OnInit, OnDestroy {
  status = 'Ready';
  inputFilePath = '';
  outputDirPath = '';
  defaultDownloadDir = '';

  // Global output logs that appear in the Output panel
  outputLogs: string[] = [];
  @ViewChild('outputContainer') outputContainer!: ElementRef;

  private unsubscribeRuntime?: () => void;
  private unsubscribeCliError?: () => void;
  private unsubscribeCliFinished?: () => void;

  private overviewSubscription?: Subscription;
  private seriesSubscription?: Subscription;

  // Advanced options / UI state
  showAdvanced = false;
  maxConnections = 8;
  maxRetries = 3;
  simultaneousDownloads = 2;
  skipExisting = true;
  downloadInParallel = true;

  // Collapse state
  settingsCollapsed = true;
  outputCollapsed = true;  // Output collapsed on startup
  downloadsCollapsed = true;  // Collapsed until downloads start

  // Dark mode
  isDarkMode = false;

  // Overall download progress
  overallProgress = 0;
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

    this.defaultDownloadDir = await GetDefaultOutputDirectory();
    this.outputDirPath = this.defaultDownloadDir;

    // Listen for system theme changes
    window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', (e) => {
      this.isDarkMode = e.matches;
    });

    this.overviewSubscription = this.overview$.subscribe((snapshot: DownloadOverviewSnapshot) => {
      this.overallProgress = snapshot.progressPercent;
      // Auto-expand downloads section when downloads start
      if (snapshot.total > 0 && this.downloadsCollapsed) {
        this.downloadsCollapsed = false;
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

    // Subscribe to streaming CLI output from backend
    this.unsubscribeRuntime = EventsOn('cli-output-line', (line: string) => {
      this.ngZone.run(() => {
        this.outputLogs.push(line);
        // Also append to manifest-level logs
        this.downloadStatus.appendManifestLog(line);

        const el = this.outputContainer?.nativeElement as HTMLElement;
        if (el) {
          el.scrollTop = el.scrollHeight;
        }
      });
    });

    // Subscribe to CLI error events
    this.unsubscribeCliError = EventsOn('cli-error', (err: string) => {
      this.ngZone.run(() => {
        this.status = 'Error';
        this.outputLogs.push(`ERROR: ${err}`);
        this.showInitializing = false;
      });
    });

    // Subscribe to CLI finished event
    this.unsubscribeCliFinished = EventsOn('cli-finished', (summary: string) => {
      this.ngZone.run(() => {
        this.status = 'Finished';
        if (summary) {
          this.outputLogs.push(summary);
        }
        this.showInitializing = false;
      });
    });
  }

  ngOnDestroy() {
    this.unsubscribeRuntime?.();
    this.unsubscribeCliError?.();
    this.unsubscribeCliFinished?.();
    this.overviewSubscription?.unsubscribe();
    this.seriesSubscription?.unsubscribe();
  }

  toggleDarkMode() {
    this.isDarkMode = !this.isDarkMode;
  }

  onSelectOutputDirectory() {
    OpenOutputDirectoryDialog().then((dirPath: string) => {
      if (dirPath) {
        this.outputDirPath = dirPath;
      }
    }).catch(err => {
      this.status = "Error: " + err;
    });
  }

  onFetchFiles() {
    if (!this.inputFilePath || !this.outputDirPath) {
      this.status = "Please select both an input TCIA file and an output directory.";
      return;
    }

    this.showInitializing = true;
    this.downloadStatus.beginRun(this.inputFilePath);
    this.outputLogs = [];
    this.overallProgress = 0;

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

    // Show command immediately in the status window
    this.status = 'Running: ' + cmdStr;
    this.appendLog(this.status);

    // Call backend to run the CLI
    RunCLIFetch(
      this.inputFilePath,
      this.outputDirPath,
      this.maxConnections,
      this.maxRetries,
      this.simultaneousDownloads,
      this.skipExisting,
      this.downloadInParallel
    ).catch(err => {
      this.ngZone.run(() => {
        this.status = 'Error: ' + err;
        this.appendLog(this.status);
        this.showInitializing = false;
      });
    });
    this.status = "Started";
  }

  onCancelDownload() {
    this.showInitializing = false;
    CancelDownload()
      .then(() => {
        this.status = "Cancellation requested";
        this.appendLog(this.status);
      })
      .catch(err => {
        this.status = "Error: " + err;
        this.appendLog(this.status);
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
      if (
        !this.outputDirPath ||
        this.outputDirPath === this.defaultDownloadDir
      ) {
        this.outputDirPath = `${this.defaultDownloadDir}/${baseName}`;
      }
    })
    .catch(err => {
      this.status = 'Error: ' + err;
    });
}


  // Append to the global output panel
  appendLog(line: string) {
    this.outputLogs.push(line);
  }
}
