import {
  ChangeDetectionStrategy,
  Component,
  EventEmitter,
  Input,
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
export class ManifestDownloadCardComponent {
  @Input() run!: RunState;

  @Output() pauseToggled = new EventEmitter<void>();
  @Output() cancelRequested = new EventEmitter<void>();
  @Output() removeRequested = new EventEmitter<void>();
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
    segments.push(`${active} in progress`);
    return `${total} series • ${segments.join(' · ')}`;
  }

  /**
   * Byte-accurate progress for the ring fill.
   * Falls back to series-count-based percent from overview.
   */
  get progressValue(): number {
    const downloaded = this.run?.bytesDownloaded ?? null;
    const total = this.run?.bytesTotal ?? null;
    let percent: number;
    if (typeof downloaded === 'number' && typeof total === 'number' && total > 0) {
      percent = Math.round((downloaded / total) * 100);
    } else {
      percent = Math.round(this.run?.overview?.progressPercent ?? 0);
    }
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
    if (this.run?.status === 'done') return true;
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

  get canOpenOutputDirectory(): boolean {
    const o = this.run?.overview;
    const total = o?.total ?? 0;
    const completed = o?.completed ?? 0;
    const failed = o?.failed ?? 0;
    const skipped = o?.skipped ?? 0;
    const cancelled = o?.cancelled ?? 0;
    const outputDirPath = this.run?.outputDirPath ?? '';
    return total > 0 && completed === total && failed === 0 && skipped === 0 && cancelled === 0 && outputDirPath.length > 0;
  }

  get statusLabel(): string {
    switch (this.run?.status) {
      case 'initializing':
        return 'Initializing';
      case 'cancelled':
        return 'Cancelled';
      case 'error':
        return 'Error';
      case 'done':
        return 'Completed';
    }
    if (this.isPaused) return 'Paused';
    const active = this.run?.overview?.active ?? 0;
    if (active > 0) return 'Downloading';
    const total = this.run?.overview?.total ?? 0;
    const done =
      (this.run?.overview?.completed ?? 0) +
      (this.run?.overview?.failed ?? 0) +
      (this.run?.overview?.skipped ?? 0) +
      (this.run?.overview?.cancelled ?? 0);
    if (total > 0 && done >= total) return 'Completed';
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
