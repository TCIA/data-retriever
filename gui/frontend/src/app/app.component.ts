import { Component, OnInit, OnDestroy, ElementRef, ViewChild } from '@angular/core';
import { Subscription } from 'rxjs';
import { EventsOn, EventsOff } from '../../wailsjs/runtime/runtime';
import { CancelDownload, OpenInputFileDialog, OpenOutputDirectoryDialog, RunCLIFetch } from '../../wailsjs/go/main/App';
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

  // Global output logs that appear in the Output panel
  outputLogs: string[] = [];
  @ViewChild('outputContainer') outputContainer!: ElementRef;

  private unsubscribeRuntime?: () => void;
  private overviewSubscription?: Subscription;

  // Advanced options / UI state
  showAdvanced = false;
  maxConnections = 8;
  maxRetries = 3;
  simultaneousDownloads = 2;
  skipExisting = true;
  downloadInParallel = true;

  // Collapse state
  settingsCollapsed = true;
  outputCollapsed = true;

  // Dark mode
  isDarkMode = false;

  // Overall download progress
  overallProgress = 0;

  series$ = this.downloadStatus.series$;
  overview$ = this.downloadStatus.overview$;

  constructor(private readonly downloadStatus: DownloadStatusService) {}

  ngOnInit() {
    // Detect system theme preference
    if (window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches) {
      this.isDarkMode = true;
    }

    // Listen for system theme changes
    window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', (e) => {
      this.isDarkMode = e.matches;
    });

    this.overviewSubscription = this.overview$.subscribe((snapshot: DownloadOverviewSnapshot) => {
      this.overallProgress = snapshot.progressPercent;
    });

    // Subscribe to streaming CLI output from backend
    this.unsubscribeRuntime = EventsOn('cli-output-line', (line: string) => {
      // Append line to global output logs
      this.outputLogs.push(line);
      // Auto-scroll
      setTimeout(() => {
        try {
          const el = this.outputContainer?.nativeElement as HTMLElement;
          if (el) el.scrollTop = el.scrollHeight;
        } catch (e) {
          // ignore
        }
      }, 10);
    });
  }

  ngOnDestroy() {
    if (this.unsubscribeRuntime) this.unsubscribeRuntime();
    try { EventsOff('cli-output-line'); } catch (e) { /* ignore */ }
    this.overviewSubscription?.unsubscribe();
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

    this.downloadStatus.beginRun();
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
    RunCLIFetch(this.inputFilePath, this.outputDirPath, this.maxConnections, this.maxRetries, this.simultaneousDownloads, this.skipExisting, this.downloadInParallel)
      .then((result: string) => {
        this.status = result;
        this.appendLog(result);
      })
      .catch(err => {
        this.status = "Error: " + err;
        this.appendLog(this.status);
      });
  }

  onCancelDownload() {
    CancelDownload()
      .then(() => {
        this.status = "Cancellation requested";
        this.appendLog(this.status);
      })
      .catch(err => {
        this.status = "Error: " + err;
        this.appendLog(this.status);
      });
  }

  onSelectInputFile() {
    OpenInputFileDialog().then((filePath: string) => {
      if (filePath) {
        this.inputFilePath = filePath;
      }
    }).catch(err => {
      this.status = "Error: " + err;
    });
  }

  // Append to the global output panel
  appendLog(line: string) {
    this.outputLogs.push(line);
  }
}
