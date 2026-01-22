import { ChangeDetectionStrategy, Component, Input } from '@angular/core';
import { SeriesDownloadSnapshot } from '../../models/download-series.model';

@Component({
  selector: 'app-download-card',
  templateUrl: './download-card.component.html',
  styleUrls: ['./download-card.component.scss'],
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class DownloadCardComponent {
  @Input() series!: SeriesDownloadSnapshot;

  get title(): string {
    const fallback = this.series?.seriesUID ?? 'Series';
    const description = this.series?.seriesDescription?.trim();
    return description && description.length > 0 ? description : fallback;
  }

  get subtitle(): string | null {
    if (!this.series) {
      return null;
    }
    const pieces = [this.series.subjectID, this.series.studyUID]
      .filter((value, index, self): value is string => !!value && self.indexOf(value) === index);
    if (pieces.length > 0) {
      return pieces.join(' • ');
    }
    const uid = this.series.seriesUID ?? null;
    return uid && uid !== this.title ? uid : null;
  }

  get statusLabel(): string {
    const status = this.series?.status ?? 'queued';
    switch (status) {
      case 'queued':
        return 'Queued';
      case 'metadata':
        return 'Fetching Metadata';
      case 'downloading':
        return 'Downloading';
      case 'skipped':
        return 'Skipped';
      case 'succeeded':
        return 'Completed';
      case 'failed':
        return 'Failed';
      case 'cancelled':
        return 'Cancelled';
      default:
        return status;
    }
  }

  get progressValue(): number {
    const value = this.series?.progress ?? 0;
    return Math.max(0, Math.min(100, Math.round(value)));
  }

  get progressLabel(): string {
    return `${this.bytesProgressValue}%`;
  }

  get accentColor(): string {
    const status = this.series?.status;
    switch (status) {
      case 'succeeded':
        return '#4caf50';
      case 'failed':
        return '#f44336';
      case 'skipped':
      case 'cancelled':
        return '#9e9e9e';
      default:
        return '#2196f3';
    }
  }

  // Bytes-based progress: downloaded / total, fallback to status progress
  get bytesProgressValue(): number {
    const downloaded = this.series?.bytesDownloaded ?? null;
    const total = this.series?.bytesTotal ?? null;
    let percent: number;
    if (typeof downloaded === 'number' && typeof total === 'number' && total > 0) {
      percent = Math.round((downloaded / total) * 100);
    } else {
      percent = this.series?.progress ?? 0;
    }
    return Math.max(0, Math.min(100, percent));
  }

  get showPauseIcon(): boolean {
    return this.series?.status === 'downloading';
  }

  get hasLogs(): boolean {
    return (this.series?.logs?.length ?? 0) > 0 || !!this.series?.errorMessage;
  }

  get logLines(): string[] {
    return this.series?.logs ?? [];
  }
}
