import {
  ChangeDetectionStrategy,
  Component,
  EventEmitter,
  Input,
  Output,
} from '@angular/core';
import { OpenDirectory } from '../../../../wailsjs/go/main/App';
import { SeriesDownloadSnapshot, SeriesDownloadStatus } from '../../models/download-series.model';
import { RunState } from '../../models/run-state.model';

const ACTIVE_SERIES_STATUSES = new Set<SeriesDownloadStatus>([
  'worker-initiated',
  'pre-check',
  'metadata',
  'download-initiated',
  'downloading',
  'decompressing',
]);

@Component({
  selector: 'app-manifest-download-card',
  templateUrl: './manifest-download-card.component.html',
  styleUrls: ['./manifest-download-card.component.scss'],
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class ManifestDownloadCardComponent {
  @Input() run!: RunState;

  @Output() pauseToggled = new EventEmitter<void>();
  @Output() cancelRequested = new EventEmitter<void>();
  @Output() removeRequested = new EventEmitter<void>();
  @Output() retryRequested = new EventEmitter<void>();
  /** Emits the new collapsed state so the parent can persist it. */
  @Output() collapseToggled = new EventEmitter<boolean>();

  showOutput = false;

  get title(): string {
    const path = this.run?.inputFilePath ?? '';
    const parts = path.split(/[\\\/]/);
    return parts[parts.length - 1] || 'Manifest';
  }

  get subtitle(): string {
    const m = this.run?.overview;
    if (!m) return '';
    const total = this.run.overview.total ?? 0;
    const queued = m.queued ?? 0;
    const active = m.active ?? 0;
    const completed = m.completed ?? 0;
    const failed = m.failed ?? 0;
    const skipped = m.skipped ?? 0;
    const cancelled = m.cancelled ?? 0;
    const segments: string[] = [];
    segments.push(`${completed} completed`);
    if (failed) segments.push(`${failed} failed`);
    if (skipped) segments.push(`${skipped} skipped`);
    if (cancelled) segments.push(`${cancelled} cancelled`);
    if (queued) segments.push(`${queued} queued`);
    segments.push(`${active} in progress`);
    return `${total} series • ${segments.join(' · ')}`;
  }

  /**
   * Series-count-based progress for the ring fill.
   */
  get progressValue(): number {
    const percent = Math.round(this.run?.overview?.progressPercent ?? 0);
    return Math.max(0, Math.min(100, percent));
  }

  /**
   * Value shown inside the ring.
   * When paused, shows the "safe-to-skip" completed-series percentage.
   */
  get displayProgressValue(): number {
    if (this.isPaused) {
      const total = this.run?.overview?.total ?? 0;
      const done = (this.run?.overview?.completed ?? 0) + (this.run?.overview?.skipped ?? 0);
      return total > 0 ? Math.round((done / total) * 100) : 0;
    }
    return this.progressValue;
  }

  get progressLabel(): string {
    return `${this.progressValue}%`;
  }

  get isCompleted(): boolean {
    if (this.isPaused) return false;
    const o = this.run?.overview;
    const total = o?.total ?? 0;
    const done = (o?.completed ?? 0) + (o?.failed ?? 0) + (o?.skipped ?? 0) + (o?.cancelled ?? 0);
    return total > 0 && done >= total;
  }

  get isTerminal(): boolean {
    return ['done', 'error', 'cancelled'].includes(this.run?.status ?? '');
  }

  get hasLogs(): boolean {
    return (this.run?.logs?.length ?? 0) > 0;
  }

  get logLines(): string[] {
    return this.run?.logs ?? [];
  }

  get isPaused(): boolean {
    return this.run?.isPaused ?? false;
  }

  get isCollapsed(): boolean {
    return this.run?.collapsed ?? false;
  }

  get showTransferSpinner(): boolean {
    if (this.isPaused || this.isTerminal) return false;
    return (this.run?.overview?.active ?? 0) > 0;
  }

  get completedSeriesFraction(): string {
    const downloaded = (this.run?.overview?.completed ?? 0) + (this.run?.overview?.skipped ?? 0);
    const total = this.run?.overview?.total ?? 0;
    return `${downloaded} / ${total} downloaded`;
  }

  get currentActiveSeries(): SeriesDownloadSnapshot | null {
    const series = this.run?.series ?? [];
    let latest: SeriesDownloadSnapshot | null = null;

    for (const snapshot of series) {
      if (!ACTIVE_SERIES_STATUSES.has(snapshot.status)) {
        continue;
      }

      if (!latest) {
        latest = snapshot;
        continue;
      }

      const snapshotTime = snapshot.lastUpdatedAt ? Date.parse(snapshot.lastUpdatedAt) : 0;
      const latestTime = latest.lastUpdatedAt ? Date.parse(latest.lastUpdatedAt) : 0;
      if (snapshotTime >= latestTime) {
        latest = snapshot;
      }
    }

    return latest;
  }

  get transferStatusText(): string {
    const active = this.currentActiveSeries;
    if (!active) {
      return '';
    }

    const title = active.seriesDescription?.trim() || active.seriesUID || 'Series';

    switch (active.status) {
      case 'worker-initiated':
        return `Worker Initiated: ${title}`;
      case 'pre-check':
        return `Running Pre-checks: ${title}`;
      case 'metadata':
        return `Fetching Metadata: ${title}`;
      case 'download-initiated':
        return `Download Initiated: ${title}`;
      case 'downloading':
        return `Downloading: ${title}`;
      case 'decompressing':
        return `Decompressing: ${title}`;
      default:
        return '';
    }
  }

  get hasFailedSeries(): boolean {
    return (this.run?.overview?.failed ?? 0) > 0;
  }

  get canOpenOutputDirectory(): boolean {
    const o = this.run?.overview;
    const total = o?.total ?? 0;
    const completed = o?.completed ?? 0;
    const failed = o?.failed ?? 0;
    const skipped = o?.skipped ?? 0;
    const cancelled = o?.cancelled ?? 0;
    const successfulTerminal = completed + skipped;
    const outputDirPath = this.run?.outputDirPath ?? '';
    return total > 0 && successfulTerminal === total && failed === 0 && cancelled === 0 && outputDirPath.length > 0;
  }

  get statusLabel(): string {
    const status = this.run?.status;
    if (status === 'initializing') return 'Initializing';
    if (status === 'cancelled') return 'Cancelled';
    if (status === 'error') return 'Error';

    // Pause-induced cancellation can briefly set status to done before
    // paused state is fully reflected in the UI.
    if (this.isPaused) return 'Paused';

    const active = this.run?.overview?.active ?? 0;
    if (active > 0) return 'Downloading';

    if (this.isCompleted) return 'Completed';
    return 'Queued';
  }

  onTogglePause(event: MouseEvent): void {
    event.stopPropagation();
    this.pauseToggled.emit();
  }

  onCancel(event: MouseEvent): void {
    event.stopPropagation();
    this.cancelRequested.emit();
  }

  onRemove(event: MouseEvent): void {
    event.stopPropagation();
    this.removeRequested.emit();
  }

  onRetry(event: MouseEvent): void {
    event.stopPropagation();
    this.retryRequested.emit();
  }

  onToggleCollapse(): void {
    this.collapseToggled.emit(!this.isCollapsed);
  }

  toggleOutput(): void {
    this.showOutput = !this.showOutput;
  }

  openOutputDirectory(event: MouseEvent): void {
    event.stopPropagation();
    const outputDirPath = this.run?.outputDirPath;
    if (!outputDirPath) {
      console.warn('No output directory is available for this run.');
      return;
    }
    OpenDirectory(outputDirPath).catch((error) => {
      const message = typeof error === 'string' ? error : String(error);
      console.error(`Failed to open output directory: ${message}`);
    });
  }
}
