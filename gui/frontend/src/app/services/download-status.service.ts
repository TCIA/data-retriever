import { Injectable, NgZone, OnDestroy } from '@angular/core';
import { BehaviorSubject } from 'rxjs';
import { EventsOn, EventsOff } from '../../../wailsjs/runtime/runtime';
import {
  DownloadOverviewSnapshot,
  ManifestDownloadSnapshot,
  SeriesDownloadEventPayload,
  SeriesDownloadSnapshot,
} from '../models/download-series.model';

const TERMINAL_STATUSES = new Set<SeriesDownloadSnapshot['status']>([
  'succeeded',
  'failed',
  'skipped',
  'cancelled',
]);

const ACTIVE_STATUSES = new Set<SeriesDownloadSnapshot['status']>([
  'queued',
  'metadata',
  'downloading',
]);

@Injectable({ providedIn: 'root' })
export class DownloadStatusService implements OnDestroy {
  private readonly seriesMap = new Map<string, SeriesDownloadSnapshot>();
  private readonly seriesSubject = new BehaviorSubject<SeriesDownloadSnapshot[]>([]);
  private readonly overviewSubject = new BehaviorSubject<DownloadOverviewSnapshot>({
    total: 0,
    active: 0,
    completed: 0,
    failed: 0,
    skipped: 0,
    cancelled: 0,
    progressPercent: 0,
  });
  private readonly manifestSubject = new BehaviorSubject<ManifestDownloadSnapshot>({
    manifestPath: '',
    total: 0,
    active: 0,
    completed: 0,
    failed: 0,
    skipped: 0,
    cancelled: 0,
    progressPercent: 0,
    logs: [],
  });

  private currentManifestPath: string = '';

  private unsubscribeRuntime?: () => void;
  private unsubscribeManifestMetadata?: () => void;

  readonly series$ = this.seriesSubject.asObservable();
  readonly overview$ = this.overviewSubject.asObservable();
  readonly manifest$ = this.manifestSubject.asObservable();

  constructor(private ngZone: NgZone) {
    if (typeof window !== 'undefined') {
      this.unsubscribeRuntime = EventsOn('download-series-event', (payload: SeriesDownloadEventPayload) => {
        this.ngZone.run(() => {
          try {
            this.applyEvent(payload);
          } catch (error) {
            console.error('Failed to process download-series-event', error);
          }
        });
      });

      // Optional manifest metadata event to preload bytesTotal for all series
      type ManifestSeriesInfoPayload = {
        manifestPath?: string;
        timestamp?: string;
        series: Array<{
          seriesUID: string;
          bytesTotal: number;
          seriesDescription?: string;
          studyUID?: string;
          subjectID?: string;
          modality?: string;
        }>;
      };

      this.unsubscribeManifestMetadata = EventsOn('manifest-series-metadata', (payload: ManifestSeriesInfoPayload) => {
        this.ngZone.run(() => {
          try {
            if (payload?.manifestPath) {
              this.currentManifestPath = payload.manifestPath;
            }
            if (Array.isArray(payload?.series) && payload.series.length > 0) {
              this.ingestManifestSeriesMetadata(payload.series);
              this.appendManifestLog('Manifest metadata received');
            }
          } catch (error) {
            console.error('Failed to process manifest-series-metadata', error);
          }
        });
      });
    }
  }

  ngOnDestroy(): void {
    this.disposeRuntimeSubscription();
    this.seriesSubject.complete();
    this.overviewSubject.complete();
  }

  beginRun(manifestPath: string): void {
    this.currentManifestPath = manifestPath;
    this.seriesMap.clear();
    this.manifestSubject.next({
      manifestPath,
      total: 0,
      active: 0,
      completed: 0,
      failed: 0,
      skipped: 0,
      cancelled: 0,
      progressPercent: 0,
      logs: [],
      startedAt: new Date().toISOString(),
    });
    this.publish();
  }

  applyEvent(payload: SeriesDownloadEventPayload): void {
    if (!payload || !payload.seriesUID) {
      return;
    }

    const existing = this.seriesMap.get(payload.seriesUID);
    // Clone the existing snapshot or create a new one to ensure reference changes for OnPush detection
    const snapshot: SeriesDownloadSnapshot = existing
      ? { ...existing, logs: [...existing.logs] }
      : this.createInitialSnapshot(payload);

    snapshot.status = payload.status;
    snapshot.progress = this.resolveProgress(snapshot.progress, payload.progress, payload.status);
    // Ensure terminal states always show 100% progress
    if (TERMINAL_STATUSES.has(payload.status)) {
      snapshot.progress = 100;
    }
    snapshot.seriesDescription = payload.seriesDescription ?? snapshot.seriesDescription;
    snapshot.subjectID = payload.subjectID ?? snapshot.subjectID;
    snapshot.studyUID = payload.studyUID ?? snapshot.studyUID;
    snapshot.modality = payload.modality ?? snapshot.modality;
    snapshot.bytesDownloaded = payload.bytesDownloaded ?? snapshot.bytesDownloaded;
    snapshot.bytesTotal = payload.bytesTotal ?? snapshot.bytesTotal;
    snapshot.attempts = payload.attempt ?? snapshot.attempts;

    const timestamp = payload.timestamp ?? new Date().toISOString();
    snapshot.lastUpdatedAt = timestamp;

    if (!snapshot.startedAt && ACTIVE_STATUSES.has(payload.status)) {
      snapshot.startedAt = timestamp;
    }

    if (TERMINAL_STATUSES.has(payload.status)) {
      snapshot.completedAt = timestamp;
      if (payload.status === 'failed') {
        snapshot.errorMessage = payload.message ?? snapshot.errorMessage;
      }
    }

    if (payload.message) {
      this.appendLog(snapshot, payload.message, timestamp);
    }

    this.seriesMap.set(payload.seriesUID, snapshot);
    this.publish();
  }

  /**
   * Pre-populate seriesMap with bytesTotal for all series in the manifest.
   * Ensures total bytes reflect the entire manifest even before workers start.
   */
  private ingestManifestSeriesMetadata(list: Array<{
    seriesUID: string;
    bytesTotal: number;
    seriesDescription?: string;
    studyUID?: string;
    subjectID?: string;
    modality?: string;
  }>): void {
    for (const item of list) {
      if (!item || !item.seriesUID) continue;
      const existing = this.seriesMap.get(item.seriesUID);
      const snapshot: SeriesDownloadSnapshot = existing
        ? { ...existing, logs: [...existing.logs] }
        : {
            seriesUID: item.seriesUID,
            studyUID: item.studyUID,
            subjectID: item.subjectID,
            seriesDescription: item.seriesDescription,
            modality: item.modality,
            status: 'queued',
            progress: 0,
            logs: [],
            bytesDownloaded: 0,
          };
      // Seed total bytes; keep any existing downloaded bytes
      if (typeof item.bytesTotal === 'number' && item.bytesTotal > 0) {
        snapshot.bytesTotal = item.bytesTotal;
      }
      this.seriesMap.set(item.seriesUID, snapshot);
    }
    this.publish();
  }

  private createInitialSnapshot(payload: SeriesDownloadEventPayload): SeriesDownloadSnapshot {
    return {
      seriesUID: payload.seriesUID,
      studyUID: payload.studyUID,
      subjectID: payload.subjectID,
      seriesDescription: payload.seriesDescription,
      modality: payload.modality,
      status: payload.status,
      progress: this.resolveProgress(0, payload.progress, payload.status),
      logs: [],
      lastUpdatedAt: payload.timestamp ?? new Date().toISOString(),
    };
  }

  private resolveProgress(current: number, proposed: number | undefined, status: SeriesDownloadSnapshot['status']): number {
    if (typeof proposed === 'number' && !Number.isNaN(proposed)) {
      return this.clampProgress(proposed);
    }

    const statusDefaults: Record<SeriesDownloadSnapshot['status'], number> = {
      queued: 0,
      metadata: 10,
      downloading: 0,
      skipped: 100,
      succeeded: 100,
      failed: 100,
      cancelled: 100,
    };

    return this.clampProgress(statusDefaults[status] ?? current ?? 0);
  }

  private appendLog(snapshot: SeriesDownloadSnapshot, message: string, timestamp: string): void {
    const formatted = `[${new Date(timestamp).toLocaleTimeString()}] ${message}`;
    snapshot.logs.push(formatted);
    if (snapshot.logs.length > 100) {
      snapshot.logs.splice(0, snapshot.logs.length - 100);
    }
  }

  private clampProgress(value: number): number {
    if (Number.isNaN(value)) {
      return 0;
    }
    return Math.max(0, Math.min(100, Math.round(value)));
  }

  private publish(): void {
    this.seriesSubject.next(Array.from(this.seriesMap.values()));
    const overview = this.calculateOverview();
    // Aggregate bytes for manifest: sum downloaded and totals across series
    let bytesDownloaded = 0;
    let bytesTotal = 0;
    for (const s of this.seriesMap.values()) {
      if (typeof s.bytesDownloaded === 'number') {
        bytesDownloaded += s.bytesDownloaded;
      }
      if (typeof s.bytesTotal === 'number' && s.bytesTotal > 0) {
        bytesTotal += s.bytesTotal;
      }
    }
    this.overviewSubject.next(overview);
    // Update manifest snapshot from overview aggregation
    const current = this.manifestSubject.value;
    const updated: ManifestDownloadSnapshot = {
      ...current,
      manifestPath: this.currentManifestPath,
      total: overview.total,
      active: overview.active,
      completed: overview.completed,
      failed: overview.failed,
      skipped: overview.skipped,
      cancelled: overview.cancelled,
      progressPercent: overview.progressPercent,
      bytesDownloaded,
      bytesTotal: bytesTotal > 0 ? bytesTotal : undefined,
      completedAt:
        overview.total > 0 && overview.active === 0 && (overview.completed + overview.failed + overview.skipped + overview.cancelled) === overview.total
          ? new Date().toISOString()
          : current.completedAt,
    };
    this.manifestSubject.next(updated);
  }

  appendManifestLog(message: string): void {
    const current = this.manifestSubject.value;
    const formatted = message;
    const logs = [...current.logs, formatted];
    if (logs.length > 200) {
      logs.splice(0, logs.length - 200);
    }
    this.manifestSubject.next({ ...current, logs });
  }

  private calculateOverview(): DownloadOverviewSnapshot {
    const snapshots = Array.from(this.seriesMap.values());
    const total = snapshots.length;

    let completed = 0;
    let failed = 0;
    let skipped = 0;
    let cancelled = 0;

    for (const item of snapshots) {
      if (item.status === 'succeeded') completed++;
      else if (item.status === 'failed') failed++;
      else if (item.status === 'skipped') skipped++;
      else if (item.status === 'cancelled') cancelled++;
    }

    const done = completed + failed + skipped + cancelled;
    const progressPercent = total > 0 ? Math.round((done / total) * 100) : 0;
    const active = total - done;

    return {
      total,
      active,
      completed,
      failed,
      skipped,
      cancelled,
      progressPercent,
    };
  }

  private disposeRuntimeSubscription(): void {
    if (this.unsubscribeRuntime) {
      try {
        this.unsubscribeRuntime();
      } catch (error) {
        console.warn('Failed to unsubscribe runtime event', error);
      }
    }
    if (this.unsubscribeManifestMetadata) {
      try {
        this.unsubscribeManifestMetadata();
      } catch (error) {
        console.warn('Failed to unsubscribe manifest metadata event', error);
      }
    }
    try {
      EventsOff('download-series-event');
    } catch (error) {
      // Ignore double-off errors
    }
    try {
      EventsOff('manifest-series-metadata');
    } catch (error) {
      // Ignore double-off errors
    }
  }
}
