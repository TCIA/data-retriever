import { ChangeDetectionStrategy, Component, Input } from '@angular/core';
import { ManifestDownloadSnapshot } from '../../models/download-series.model';

@Component({
  selector: 'app-manifest-download-card',
  templateUrl: './manifest-download-card.component.html',
  styleUrls: ['./manifest-download-card.component.scss'],
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class ManifestDownloadCardComponent {
  @Input() manifest!: ManifestDownloadSnapshot;

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

  get progressValue(): number {
    return Math.max(0, Math.min(100, Math.round(this.manifest?.progressPercent ?? 0)));
  }

  get progressLabel(): string {
    return `${this.progressValue}%`;
  }

  get statusLabel(): string {
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

  get hasLogs(): boolean {
    return (this.manifest?.logs?.length ?? 0) > 0;
  }

  get logLines(): string[] {
    return this.manifest?.logs ?? [];
  }
}
