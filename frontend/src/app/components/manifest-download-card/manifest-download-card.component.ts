import {
  ChangeDetectionStrategy,
  ChangeDetectorRef,
  Component,
  EventEmitter,
  Input,
  OnDestroy,
  OnInit,
  Output,
} from '@angular/core';
import { OpenDirectory } from '../../../../wailsjs/go/main/App';
import { RunState } from '../../models/run-state.model';

@Component({
  selector: 'app-manifest-download-card',
  templateUrl: './manifest-download-card.component.html',
  styleUrls: ['./manifest-download-card.component.scss'],
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class ManifestDownloadCardComponent implements OnInit, OnDestroy {
  @Input() run!: RunState;

  private elapsedTickId?: ReturnType<typeof setInterval>;

  constructor(private readonly cdr: ChangeDetectorRef) {}

  ngOnInit(): void {
    // 1Hz tick so the elapsed-time indicator advances between progress events
    // (and during quiet periods like TCIA metadata fetches).
    this.elapsedTickId = setInterval(() => this.cdr.markForCheck(), 1000);
  }

  ngOnDestroy(): void {
    if (this.elapsedTickId !== undefined) {
      clearInterval(this.elapsedTickId);
      this.elapsedTickId = undefined;
    }
    if (this.copyFeedbackTimer !== undefined) {
      clearTimeout(this.copyFeedbackTimer);
      this.copyFeedbackTimer = undefined;
    }
  }

  @Output() pauseToggled = new EventEmitter<void>();
  @Output() cancelRequested = new EventEmitter<void>();
  @Output() removeRequested = new EventEmitter<void>();
  @Output() retryRequested = new EventEmitter<void>();
  /** Emits the new collapsed state so the parent can persist it. */
  @Output() collapseToggled = new EventEmitter<boolean>();

  showOutput = false;

  private copyFeedback: 'idle' | 'copied' | 'error' = 'idle';
  private copyFeedbackTimer?: ReturnType<typeof setTimeout>;

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

  get transferStatusText(): string {
    if (this.isPaused || this.isTerminal) {
      return '';
    }
    if ((this.run?.overview?.active ?? 0) <= 0) {
      return '';
    }

    const bps = this.run?.bytesPerSecond;
    if (typeof bps !== 'number' || !isFinite(bps) || bps <= 0) {
      return 'Download Speed: measuring…';
    }

    return `Download Speed: ${this.formatBytesPerSecond(bps)}`;
  }

  private formatBytesPerSecond(bytesPerSecond: number): string {
    return `${this.formatBytes(bytesPerSecond)}/s`;
  }

  private formatBytes(bytes: number): string {
    const units = ['B', 'KB', 'MB', 'GB', 'TB'];
    let value = bytes;
    let unitIndex = 0;
    while (value >= 1024 && unitIndex < units.length - 1) {
      value /= 1024;
      unitIndex++;
    }
    const decimals = value >= 100 || unitIndex === 0 ? 0 : value >= 10 ? 1 : 2;
    return `${value.toFixed(decimals)} ${units[unitIndex]}`;
  }

  get totalDownloadedText(): string {
    if (!this.isTerminal) return '';
    const bytes = this.run?.bytesDownloaded;
    if (typeof bytes !== 'number' || !isFinite(bytes) || bytes <= 0) return '';
    return `Total downloaded: ${this.formatBytes(bytes)}`;
  }

  /**
   * Whole seconds from run start to completion (or to now while still running).
   * Returns null when start/end timestamps are missing or inconsistent.
   */
  private elapsedSeconds(startISO?: string): number | null {
    startISO = startISO ?? this.run?.startedAt;
    if (!startISO) return null;
    const start = Date.parse(startISO);
    if (isNaN(start)) return null;

    const completedISO = this.run?.completedAt;
    const completed = completedISO ? Date.parse(completedISO) : NaN;
    const endMs = !isNaN(completed) ? completed : Date.now();
    if (endMs < start) return null;

    return Math.floor((endMs - start) / 1000);
  }

  get elapsedText(): string {
    const elapsedSec = this.elapsedSeconds();
    if (elapsedSec === null) return '';

    const completedISO = this.run?.completedAt;
    const hasCompleted = !!completedISO && !isNaN(Date.parse(completedISO));
    const label = hasCompleted || this.isTerminal ? 'Total time' : 'Elapsed';
    return `${label}: ${this.formatDuration(elapsedSec)}`;
  }

  /**
   * Average throughput: total bytes downloaded divided by time spent in the
   * download phase (from the first series transfer, excluding the initial
   * metadata preparation and any time spent paused). Shown only once the run
   * is terminal.
   */
  get averageSpeedText(): string {
    if (!this.isTerminal) return '';
    const bytes = this.run?.bytesDownloaded;
    if (typeof bytes !== 'number' || !isFinite(bytes) || bytes <= 0) return '';

    let elapsedSec = this.elapsedSeconds(this.run?.downloadStartedAt);
    if (elapsedSec === null) return '';

    const pausedMs = this.run?.downloadPausedMs;
    if (typeof pausedMs === 'number' && isFinite(pausedMs) && pausedMs > 0) {
      elapsedSec -= Math.floor(pausedMs / 1000);
    }
    if (elapsedSec <= 0) return '';

    return `Average speed: ${this.formatBytesPerSecond(bytes / elapsedSec)}`;
  }

  private formatDuration(totalSec: number): string {
    const h = Math.floor(totalSec / 3600);
    const m = Math.floor((totalSec % 3600) / 60);
    const s = totalSec % 60;
    const pad = (n: number) => n.toString().padStart(2, '0');
    return h > 0 ? `${h}:${pad(m)}:${pad(s)}` : `${pad(m)}:${pad(s)}`;
  }

  get hasFailedSeries(): boolean {
    return (this.run?.overview?.failed ?? 0) > 0;
  }

  get showCopyLog(): boolean {
    return this.isTerminal && (this.hasFailedSeries || this.run?.status === 'error');
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

  get copyLogLabel(): string {
    switch (this.copyFeedback) {
      case 'copied': return 'Copied!';
      case 'error': return 'Copy failed';
      default: return 'Copy Log';
    }
  }

  get copyFeedbackState(): 'idle' | 'copied' | 'error' {
    return this.copyFeedback;
  }

  get copyLogFeedbackText(): string {
    switch (this.copyFeedback) {
      case 'copied': return 'Copied!';
      case 'error': return 'Copy failed';
      default: return '';
    }
  }

  get copyLogButtonTitle(): string {
    switch (this.copyFeedback) {
      case 'copied': return 'Copied to clipboard';
      case 'error': return 'Copy failed';
      default: return 'Copy error message to share with support';
    }
  }

  onCopyLog(event: MouseEvent): void {
    event.stopPropagation();
    const lines = this.run?.logs ?? [];
    const text = lines.join('\n');
    const finish = (state: 'copied' | 'error') => {
      this.copyFeedback = state;
      if (this.copyFeedbackTimer) clearTimeout(this.copyFeedbackTimer);
      this.copyFeedbackTimer = setTimeout(() => {
        this.copyFeedback = 'idle';
        this.cdr.markForCheck();
      }, 1500);
      this.cdr.markForCheck();
    };

    if (navigator?.clipboard?.writeText) {
      navigator.clipboard.writeText(text)
        .then(() => finish('copied'))
        .catch(() => finish('error'));
    } else {
      finish('error');
    }
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
