import { ChangeDetectionStrategy, Component, EventEmitter, Input, Output } from '@angular/core';
import { ManifestDownloadSnapshot } from '../../models/download-series.model';

@Component({
  selector: 'app-manifest-download-card',
  templateUrl: './manifest-download-card.component.html',
  styleUrls: ['./manifest-download-card.component.scss'],
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class ManifestDownloadCardComponent {
  @Input() manifest!: ManifestDownloadSnapshot;
  @Output() pauseToggled = new EventEmitter<void>();
  
  showOutput = false;

  get title(): string {
    const path = this.manifest?.manifestPath || '';
    const parts = path.split(/[\\\/]/);
    return parts[parts.length - 1] || 'Manifest';
  }

  get subtitle(): string {
    const total = this.manifest?.total ?? 0;
    const active = this.manifest?.active ?? 0;
    const completed = this.manifest?.completed ?? 0;
    const failed = this.manifest?.failed ?? 0;
    const skipped = this.manifest?.skipped ?? 0;
    const cancelled = this.manifest?.cancelled ?? 0;
    const segments: string[] = [];
    segments.push(`${completed} completed`);
    if (failed) segments.push(`${failed} failed`);
    if (skipped) segments.push(`${skipped} skipped`);
    if (cancelled) segments.push(`${cancelled} cancelled`);
    segments.push(`${active} in progress`);
    return `${total} series • ${segments.join(' · ')}`;
  }

  /**
   * Progress value for ring fill - keeps current value when paused (ring becomes grey via CSS)
   */
  get progressValue(): number {
    const downloaded = this.manifest?.bytesDownloaded ?? null;
    const total = this.manifest?.bytesTotal ?? null;
    let percent: number;
    if (typeof downloaded === 'number' && typeof total === 'number' && total > 0) {
      percent = Math.round((downloaded / total) * 100);
    } else {
      percent = Math.round(this.manifest?.progressPercent ?? 0);
    }
    return Math.max(0, Math.min(100, percent));
  }

  /**
   * Display value shown inside the ring - shows completed percentage when paused
   * This reflects what percentage will be skipped on resume
   */
  get displayProgressValue(): number {
    if (this.isPaused) {
      // When paused, show the "completed" progress based on series counts
      const total = this.manifest?.total ?? 0;
      const done = (this.manifest?.completed ?? 0) + (this.manifest?.skipped ?? 0);
      if (total > 0) {
        return Math.round((done / total) * 100);
      }
      return 0;
    }
    return this.progressValue;
  }

  get progressLabel(): string {
    return `${this.progressValue}%`;
  }

  get isCompleted(): boolean {
    // Never show completed state when paused - keep the ring visible
    if (this.isPaused) {
      return false;
    }
    const total = this.manifest?.total ?? 0;
    const done = (this.manifest?.completed ?? 0) + (this.manifest?.failed ?? 0) + (this.manifest?.skipped ?? 0) + (this.manifest?.cancelled ?? 0);
    return total > 0 && done >= total;
  }

  get hasLogs(): boolean {
    return (this.manifest?.logs?.length ?? 0) > 0;
  }

  get logLines(): string[] {
    return this.manifest?.logs ?? [];
  }

  get isPaused(): boolean {
    return this.manifest?.isPaused ?? false;
  }

  get statusLabel(): string {
    if (this.isPaused) {
      return 'Paused';
    }
    const active = this.manifest?.active ?? 0;
    if (active > 0) {
      return 'Downloading';
    }
    const total = this.manifest?.total ?? 0;
    const done = (this.manifest?.completed ?? 0) + (this.manifest?.failed ?? 0) + (this.manifest?.skipped ?? 0) + (this.manifest?.cancelled ?? 0);
    if (total > 0 && done >= total) {
      return 'Completed';
    }
    return 'Queued';
  }

  onTogglePause(event: MouseEvent): void {
    event.stopPropagation();
    this.pauseToggled.emit();
  }

  toggleOutput(): void {
    this.showOutput = !this.showOutput;
  }
}
