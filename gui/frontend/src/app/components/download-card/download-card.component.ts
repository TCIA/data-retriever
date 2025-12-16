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
    return this.series?.progress ?? 0;
  }

  get progressLabel(): string {
    return `${this.progressValue}%`;
  }

  get hasLogs(): boolean {
    return (this.series?.logs?.length ?? 0) > 0 || !!this.series?.errorMessage;
  }

  get logLines(): string[] {
    return this.series?.logs ?? [];
  }
}
