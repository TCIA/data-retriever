import { Injectable, NgZone, OnDestroy } from '@angular/core';
import { BehaviorSubject, Observable, map } from 'rxjs';
import { EventsOn, EventsOff } from '../../../wailsjs/runtime/runtime';
import { PauseManifest, ResumeManifest } from '../../../wailsjs/go/main/App';
import {
  DownloadOverviewSnapshot,
  SeriesDownloadEventPayload,
  SeriesDownloadSnapshot,
  SeriesDownloadPhase,
} from '../models/download-series.model';
import { RunState } from '../models/run-state.model';

const TERMINAL_STATUSES = new Set<SeriesDownloadSnapshot['status']>([
  'succeeded',
  'failed',
  'skipped',
  'cancelled',
]);

const ACTIVE_STATUSES = new Set<SeriesDownloadSnapshot['status']>([
  'worker-initiated',
  'pre-check',
  'metadata',
  'download-initiated',
  'downloading',
  'decompressing',
]);

const EMPTY_OVERVIEW: DownloadOverviewSnapshot = {
  total: 0,
  queued: 0,
  active: 0,
  completed: 0,
  failed: 0,
  skipped: 0,
  cancelled: 0,
  progressPercent: 0,
};

// ---------------------------------------------------------------------------
// Per-run internal state (richer than what we expose publicly)
// ---------------------------------------------------------------------------
interface RunInternal {
  /** uint64 as bigint */
  runId: bigint;
  inputFilePath: string;
  outputDirPath: string;
  seriesMap: Map<string, SeriesDownloadSnapshot>;
  manifestInitialBytesTotal: number;
  isPaused: boolean;
  collapsed: boolean;
  hasAutoExpanded: boolean;
  startedAt: string;
  completedAt?: string;
  logs: string[];
  status: RunState['status'];
  lastByteSampleAt?: number;
  lastByteSampleValue?: number;
  bytesPerSecond?: number;
  errorMessage?: string;
}

@Injectable({ providedIn: 'root' })
export class DownloadStatusService implements OnDestroy {
  // Map from runId → internal mutable state
  private readonly runsMap = new Map<bigint, RunInternal>();

  // Single source of truth for the UI
  private readonly runsSubject = new BehaviorSubject<RunState[]>([]);
  readonly runs$: Observable<RunState[]> = this.runsSubject.asObservable();

  // Convenience: expose the old singular streams for any components not yet migrated
  // They mirror the FIRST active run (or the most recent one).
  readonly series$: Observable<SeriesDownloadSnapshot[]> = this.runs$.pipe(
    map(runs => runs[0]?.series ?? [])
  );
  readonly overview$: Observable<DownloadOverviewSnapshot> = this.runs$.pipe(
    map(runs => runs[0]?.overview ?? EMPTY_OVERVIEW)
  );

  private unsubscribeSeriesEvent?: () => void;
  private unsubscribeManifestMetadata?: () => void;
  private unsubscribePaused?: () => void;
  private unsubscribeResumed?: () => void;
  private unsubscribeCliError?: () => void;
  private unsubscribeCliFinished?: () => void;

  constructor(private ngZone: NgZone) {
    if (typeof window === 'undefined') return;

    // -----------------------------------------------------------------------
    // download-series-event  { runId, ...SeriesDownloadEventPayload }
    // -----------------------------------------------------------------------
    this.unsubscribeSeriesEvent = EventsOn(
      'download-series-event',
      (payload: SeriesDownloadEventPayload & { runId?: number | string }) => {
        this.ngZone.run(() => {
          try {
            const run = this.resolveRun(payload.runId);
            if (!run) return;
            this.applySeriesEvent(run, payload);
            this.publishRun(run);
          } catch (err) {
            console.error('Failed to process download-series-event', err);
          }
        });
      }
    );

    // -----------------------------------------------------------------------
    // manifest-series-metadata  { runId?, manifestPath?, series[] }
    // -----------------------------------------------------------------------
    type ManifestMetaPayload = {
      runId?: number | string;
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

    this.unsubscribeManifestMetadata = EventsOn(
      'manifest-series-metadata',
      (payload: ManifestMetaPayload) => {
        this.ngZone.run(() => {
          try {
            const run = this.resolveRun(payload.runId);
            if (!run) return;
            if (Array.isArray(payload?.series) && payload.series.length > 0) {
              this.ingestManifestSeriesMetadata(run, payload.series);
              this.appendLog(run, 'Manifest metadata received');
              this.publishRun(run);
            }
          } catch (err) {
            console.error('Failed to process manifest-series-metadata', err);
          }
        });
      }
    );

    // -----------------------------------------------------------------------
    // manifest-paused / manifest-resumed  { runId?, manifestPath }
    // -----------------------------------------------------------------------
    this.unsubscribePaused = EventsOn(
      'manifest-paused',
      (payload: string | { runId?: number | string; manifestPath?: string }) => {
        this.ngZone.run(() => {
          const run = this.resolveRunFromPausePayload(payload);
          if (!run) return;
          run.isPaused = true;
          this.appendLog(run, 'Download paused');
          this.publishRun(run);
        });
      }
    );

    this.unsubscribeResumed = EventsOn(
      'manifest-resumed',
      (payload: string | { runId?: number | string; manifestPath?: string }) => {
        this.ngZone.run(() => {
          const run = this.resolveRunFromPausePayload(payload);
          if (!run) return;
          run.isPaused = false;
          this.appendLog(run, 'Download resumed');
          this.publishRun(run);
        });
      }
    );

    // -----------------------------------------------------------------------
    // cli-error / cli-finished  { runId, error/summary }
    // -----------------------------------------------------------------------
    this.unsubscribeCliError = EventsOn(
      'cli-error',
      (payload: { runId?: number | string; error?: string } | string) => {
        this.ngZone.run(() => {
          const { run, message } = this.resolveRunAndMessage(payload, 'error');
          if (!run) return;
          run.status = 'error';
          if (message) {
            run.errorMessage = message;
            this.appendLog(run, `ERROR: ${message}`);
          }
          this.publishRun(run);
        });
      }
    );

    this.unsubscribeCliFinished = EventsOn(
      'cli-finished',
      (payload: { runId?: number | string; summary?: string } | string) => {
        this.ngZone.run(() => {
          const { run, message } = this.resolveRunAndMessage(payload, 'summary');
          if (!run) return;
          run.status = 'done';
          run.completedAt = new Date().toISOString();
          if (message) this.appendLog(run, message);
          this.publishRun(run);
        });
      }
    );
  }

  ngOnDestroy(): void {
    this.disposeEvents();
    this.runsSubject.complete();
  }

  // ---------------------------------------------------------------------------
  // Public API
  // ---------------------------------------------------------------------------

  /**
   * Start tracking a new manifest run. Returns the runId.
   */
  beginRun(runId: bigint, inputFilePath: string, outputDirPath: string): bigint {
    const run: RunInternal = {
      runId,
      inputFilePath,
      outputDirPath,
      seriesMap: new Map(),
      manifestInitialBytesTotal: 0,
      isPaused: false,
      collapsed: false,
      hasAutoExpanded: false,
      startedAt: new Date().toISOString(),
      logs: [],
      status: 'initializing',
    };
    this.runsMap.set(runId, run);
    this.publishRun(run);
    return runId;
  }

  appendManifestLog(runId: bigint, message: string): void {
    const run = this.runsMap.get(runId);
    if (!run) return;
    this.appendLog(run, message);
    this.publishRun(run);
  }

  /**
   * Mark a run as errored from the component layer (e.g. RunCLIFetch rejection).
   */
  setRunError(runId: bigint, err: unknown): void {
    const run = this.runsMap.get(runId);
    if (!run) return;
    run.status = 'error';
    this.appendLog(run, `ERROR: ${err}`);
    this.publishRun(run);
  }

  /**
   * Mark a run as done from the component layer.
   */
  completeRun(runId: bigint, summary?: string): void {
    const run = this.runsMap.get(runId);
    if (!run) return;
    run.status = 'done';
    run.completedAt = new Date().toISOString();
    if (summary) this.appendLog(run, summary);
    this.publishRun(run);
  }

  cancelRun(runId: bigint): void {
    const run = this.runsMap.get(runId);
    if (!run) return;
    run.status = 'cancelled';
    this.appendLog(run, 'Cancellation requested');
    this.publishRun(run);
  }

  removeRun(runId: bigint): void {
    this.runsMap.delete(runId);
    this.publishAll();
  }

  async togglePause(runId: bigint): Promise<boolean> {
    const run = this.runsMap.get(runId);
    if (!run) return false;
    try {
      if (run.isPaused) {
        await ResumeManifest(run.inputFilePath);
      } else {
        await PauseManifest(run.inputFilePath);
      }
    } catch (err) {
      console.error('Failed to toggle pause', err);
    }
    return run.isPaused;
  }

  getIsPaused(runId: bigint): boolean {
    return this.runsMap.get(runId)?.isPaused ?? false;
  }

  setCollapsed(runId: bigint, collapsed: boolean): void {
    const run = this.runsMap.get(runId);
    if (!run) return;
    run.collapsed = collapsed;
    this.publishRun(run);
  }

  // ---------------------------------------------------------------------------
  // Private helpers
  // ---------------------------------------------------------------------------

  /**
   * Resolve which run an event belongs to.
   * Accepts bigint directly, or a number/string from Wails events and coerces to bigint.
   * Falls back to the most recently started run if no runId is provided
   * (backwards-compat with old single-run backend).
   */
  private resolveRun(runId?: bigint | number | string): RunInternal | undefined {
    if (runId !== undefined && runId !== null) {
      const key = BigInt(runId);
      return this.runsMap.get(key);
    }
    // Fallback: last run inserted
    const entries = Array.from(this.runsMap.values());
    return entries[entries.length - 1];
  }

  private resolveRunFromPausePayload(
    payload: string | { runId?: number | string; manifestPath?: string }
  ): RunInternal | undefined {
    if (typeof payload === 'string') {
      // Old format: payload is manifestPath — find run by inputFilePath
      return Array.from(this.runsMap.values()).find(r => r.inputFilePath === payload);
    }
    return this.resolveRun(payload.runId);
  }

  private resolveRunAndMessage(
    payload: { runId?: number | string; error?: string; summary?: string } | string,
    messageKey: 'error' | 'summary'
  ): { run?: RunInternal; message?: string } {
    if (typeof payload === 'string') {
      const run = this.resolveRun(undefined);
      return { run, message: payload };
    }
    const run = this.resolveRun(payload.runId);
    const message = messageKey === 'error' ? payload.error : payload.summary;
    return { run, message };
  }

  private appendLog(run: RunInternal, message: string): void {
    const formatted = `[${new Date().toLocaleTimeString()}] ${message}`;
    run.logs.push(formatted);
    if (run.logs.length > 200) run.logs.splice(0, run.logs.length - 200);
  }

  private publishRun(run: RunInternal): void {
    this.publishAll();
    // Auto-expand logic per run
    const overview = this.buildOverview(run);
    if (overview.total > 0 && run.collapsed && !run.hasAutoExpanded) {
      run.collapsed = false;
      run.hasAutoExpanded = true;
    }
    if (run.status === 'initializing' && overview.total > 0) {
      run.status = 'running';
    }
  }

  private publishAll(): void {
    const states: RunState[] = Array.from(this.runsMap.values()).map(run =>
      this.toRunState(run)
    );
    this.runsSubject.next(states);
  }

  private toRunState(run: RunInternal): RunState {
    const series = Array.from(run.seriesMap.values());
    const overview = this.buildOverview(run);
    let bytesDownloaded = 0;
    let hasByteSample = false;

    for (const s of series) {
      let sample: number | undefined;
      if (typeof s.uncompressedBytes === 'number' && s.uncompressedBytes >= 0) {
        sample = s.uncompressedBytes;
      } else if (typeof s.bytesDownloaded === 'number' && s.bytesDownloaded >= 0) {
        sample = s.bytesDownloaded;
      }

      if (typeof sample === 'number') {
        hasByteSample = true;
        bytesDownloaded += sample;
      }
    }

    const now = Date.now();
    if (hasByteSample) {
      const previousBytes = run.lastByteSampleValue;
      const previousAt = run.lastByteSampleAt;

      if (typeof previousBytes === 'number' && typeof previousAt === 'number') {
        const deltaBytes = bytesDownloaded - previousBytes;
        const deltaSeconds = (now - previousAt) / 1000;

        if (deltaBytes > 0 && deltaSeconds > 0) {
          const instantaneousRate = deltaBytes / deltaSeconds;
          run.bytesPerSecond =
            typeof run.bytesPerSecond === 'number'
              ? run.bytesPerSecond * 0.65 + instantaneousRate * 0.35
              : instantaneousRate;
          run.lastByteSampleValue = bytesDownloaded;
          run.lastByteSampleAt = now;
        } else if ((overview.active <= 0 || run.isPaused || run.status !== 'running') && deltaSeconds >= 0) {
          run.bytesPerSecond = undefined;
          run.lastByteSampleValue = bytesDownloaded;
          run.lastByteSampleAt = now;
        } else if (deltaSeconds >= 1.5) {
          run.bytesPerSecond = 0;
          run.lastByteSampleValue = bytesDownloaded;
          run.lastByteSampleAt = now;
        }
      } else {
        run.lastByteSampleValue = bytesDownloaded;
        run.lastByteSampleAt = now;
      }
    } else {
      run.bytesPerSecond = undefined;
      run.lastByteSampleValue = undefined;
      run.lastByteSampleAt = undefined;
    }

    return {
      runId: run.runId,
      inputFilePath: run.inputFilePath,
      outputDirPath: run.outputDirPath,
      status: run.status,
      overview,
      series,
      logs: [...run.logs],
      isPaused: run.isPaused,
      collapsed: run.collapsed,
      hasAutoExpanded: run.hasAutoExpanded,
      startedAt: run.startedAt,
      completedAt: run.completedAt,
      bytesDownloaded: hasByteSample ? bytesDownloaded : undefined,
      bytesPerSecond: run.bytesPerSecond,
      errorMessage: run.errorMessage,
    };
  }

  private buildOverview(run: RunInternal): DownloadOverviewSnapshot {
    const snapshots = Array.from(run.seriesMap.values());
    const total = snapshots.length;
    let queued = 0, active = 0, completed = 0, failed = 0, skipped = 0, cancelled = 0;
    for (const s of snapshots) {
      if (s.status === 'queued') queued++;
      else if (
        s.status === 'worker-initiated' ||
        s.status === 'pre-check' ||
        s.status === 'metadata' ||
        s.status === 'download-initiated' ||
        s.status === 'downloading' ||
        s.status === 'decompressing'
      ) active++;
      else if (s.status === 'succeeded') completed++;
      else if (s.status === 'failed') failed++;
      else if (s.status === 'skipped') skipped++;
      else if (s.status === 'cancelled') cancelled++;
    }
    const done = completed + failed + skipped + cancelled;
    return {
      total,
      queued,
      active,
      completed,
      failed,
      skipped,
      cancelled,
      progressPercent: total > 0 ? Math.round((done / total) * 100) : 0,
    };
  }

  // ---------------------------------------------------------------------------
  // Series event processing (same logic as before, now per-run)
  // ---------------------------------------------------------------------------

  private applySeriesEvent(run: RunInternal, payload: SeriesDownloadEventPayload): void {
    if (!payload?.seriesUID) return;
    run.status = 'running';

    const existing = run.seriesMap.get(payload.seriesUID);
    const snapshot: SeriesDownloadSnapshot = existing
      ? { ...existing, logs: [...existing.logs] }
      : this.createInitialSnapshot(payload);

    snapshot.status = payload.status;
    const fallbackProgress = this.resolveProgress(snapshot.progress, payload.progress, payload.status);
    snapshot.progress = fallbackProgress;
    snapshot.seriesDescription = payload.seriesDescription ?? snapshot.seriesDescription;
    snapshot.subjectID = payload.subjectID ?? snapshot.subjectID;
    snapshot.studyUID = payload.studyUID ?? snapshot.studyUID;
    snapshot.modality = payload.modality ?? snapshot.modality;

    if (typeof payload.bytesDownloaded === 'number' && payload.bytesDownloaded >= 0) {
      snapshot.bytesDownloaded = payload.bytesDownloaded;
    } else if (typeof snapshot.bytesDownloaded !== 'number') {
      snapshot.bytesDownloaded = 0;
    }
    if (typeof payload.bytesTotal === 'number' && payload.bytesTotal > 0) {
      snapshot.bytesTotal = payload.bytesTotal;
    }
    if (typeof payload.uncompressedBytes === 'number' && payload.uncompressedBytes >= 0) {
      snapshot.uncompressedBytes = payload.uncompressedBytes;
    }
    if (typeof payload.uncompressedTotal === 'number' && payload.uncompressedTotal > 0) {
      snapshot.uncompressedTotal = payload.uncompressedTotal;
    }

    snapshot.attempts = payload.attempt ?? snapshot.attempts;
    const timestamp = payload.timestamp ?? new Date().toISOString();
    snapshot.lastUpdatedAt = timestamp;

    if (!snapshot.startedAt && ACTIVE_STATUSES.has(payload.status)) {
      snapshot.startedAt = timestamp;
    }

    const resolvedPhase = this.resolvePhase(payload.status, payload.phase, snapshot.phase);
    snapshot.phase = resolvedPhase;
    snapshot.phaseProgress = this.resolvePhaseProgress(snapshot, resolvedPhase, payload.phaseProgress, fallbackProgress);

    if (TERMINAL_STATUSES.has(payload.status)) {
      snapshot.completedAt = timestamp;
      if (payload.status === 'failed') {
        snapshot.errorMessage = payload.message ?? snapshot.errorMessage;
      }
      snapshot.phaseProgress = 100;
    }

    if (payload.message) {
      this.appendSeriesLog(snapshot, payload.message, timestamp);
    }

    snapshot.progress = this.computeBlendedProgress(snapshot, fallbackProgress);
    run.seriesMap.set(payload.seriesUID, snapshot);
  }

  private ingestManifestSeriesMetadata(
    run: RunInternal,
    list: Array<{
      seriesUID: string;
      bytesTotal: number;
      seriesDescription?: string;
      studyUID?: string;
      subjectID?: string;
      modality?: string;
    }>
  ): void {
    let manifestSum = 0;
    for (const item of list) {
      if (!item?.seriesUID) continue;
      const existing = run.seriesMap.get(item.seriesUID);
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
            phase: 'queued',
            phaseProgress: 0,
            logs: [],
            bytesDownloaded: 0,
          };
      if (typeof item.bytesTotal === 'number' && item.bytesTotal > 0) {
        snapshot.bytesTotal = item.bytesTotal;
        snapshot.uncompressedTotal = item.bytesTotal;
        manifestSum += item.bytesTotal;
      }
      run.seriesMap.set(item.seriesUID, snapshot);
    }
    if (manifestSum > 0) run.manifestInitialBytesTotal = manifestSum;
  }

  // ---------------------------------------------------------------------------
  // Progress / phase helpers (unchanged logic, extracted to pure functions)
  // ---------------------------------------------------------------------------

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
      bytesDownloaded:
        typeof payload.bytesDownloaded === 'number' && payload.bytesDownloaded >= 0
          ? payload.bytesDownloaded
          : 0,
      bytesTotal:
        typeof payload.bytesTotal === 'number' && payload.bytesTotal > 0
          ? payload.bytesTotal
          : undefined,
    };
  }

  private resolveProgress(
    current: number,
    proposed: number | undefined,
    status: SeriesDownloadSnapshot['status']
  ): number {
    if (typeof proposed === 'number' && !Number.isNaN(proposed)) {
      return this.clampProgress(proposed);
    }
    const statusDefaults: Record<SeriesDownloadSnapshot['status'], number> = {
      queued: 0,
      'worker-initiated': 5,
      'pre-check': 15,
      metadata: 25,
      'download-initiated': 30,
      downloading: 35,
      decompressing: 80,
      skipped: 100, succeeded: 100, failed: 100, cancelled: 100,
    };
    return this.clampProgress(statusDefaults[status] ?? current ?? 0);
  }

  private appendSeriesLog(snapshot: SeriesDownloadSnapshot, message: string, timestamp: string): void {
    const formatted = `[${new Date(timestamp).toLocaleTimeString()}] ${message}`;
    snapshot.logs.push(formatted);
    if (snapshot.logs.length > 100) snapshot.logs.splice(0, snapshot.logs.length - 100);
  }

  private clampProgress(value: number): number {
    if (Number.isNaN(value)) return 0;
    return Math.max(0, Math.min(100, Math.round(value)));
  }

  private clampFraction(value: number | undefined): number {
    if (typeof value !== 'number' || Number.isNaN(value)) return 0;
    return Math.max(0, Math.min(1, value));
  }

  private resolvePhase(
    status: SeriesDownloadSnapshot['status'],
    incomingPhase?: SeriesDownloadPhase,
    currentPhase?: SeriesDownloadPhase
  ): SeriesDownloadPhase {
    if (incomingPhase) return incomingPhase;
    switch (status) {
      case 'queued': return 'queued';
      case 'worker-initiated':
      case 'pre-check':
        return 'queued';
      case 'metadata': return 'metadata';
      case 'download-initiated':
      case 'downloading': return 'download';
      case 'decompressing': return 'decompress';
      case 'succeeded': case 'skipped': return 'complete';
      case 'failed': case 'cancelled': return 'failed';
      default: return currentPhase ?? 'download';
    }
  }

  private resolvePhaseProgress(
    snapshot: SeriesDownloadSnapshot,
    phase: SeriesDownloadPhase,
    incomingPhaseProgress: number | undefined,
    fallbackProgress: number
  ): number | undefined {
    if (typeof incomingPhaseProgress === 'number' && !Number.isNaN(incomingPhaseProgress)) {
      return this.clampProgress(incomingPhaseProgress);
    }
    if (phase === 'download') {
      return this.calculatePercent(snapshot.bytesDownloaded, snapshot.bytesTotal, fallbackProgress);
    }
    if (phase === 'decompress') {
      const pct = this.calculatePercent(snapshot.uncompressedBytes, snapshot.uncompressedTotal, undefined);
      return typeof pct === 'number' ? pct : snapshot.phaseProgress ?? 0;
    }
    if (phase === 'complete' || phase === 'failed') return 100;
    return snapshot.phaseProgress;
  }

  private calculatePercent(done?: number, total?: number, fallback?: number): number | undefined {
    if (typeof done === 'number' && typeof total === 'number' && total > 0) {
      return this.clampProgress((done / total) * 100);
    }
    if (typeof fallback === 'number' && !Number.isNaN(fallback)) {
      return this.clampProgress(fallback);
    }
    return undefined;
  }

  private computeBlendedProgress(snapshot: SeriesDownloadSnapshot, fallbackProgress: number): number {
    if (TERMINAL_STATUSES.has(snapshot.status)) return 100;
    const dl = this.computeDownloadFraction(snapshot, fallbackProgress);
    const dec = this.computeDecompressFraction(snapshot);
    return this.clampProgress((dl * 0.8 + dec * 0.2) * 100);
  }

  private computeDownloadFraction(snapshot: SeriesDownloadSnapshot, fallbackProgress: number): number {
    if (
      typeof snapshot.bytesDownloaded === 'number' &&
      typeof snapshot.bytesTotal === 'number' &&
      snapshot.bytesTotal > 0
    ) {
      return this.clampFraction(snapshot.bytesDownloaded / snapshot.bytesTotal);
    }
    const pct =
      snapshot.phase === 'download'
        ? snapshot.phaseProgress ?? fallbackProgress
        : fallbackProgress;
    return this.clampFraction((pct ?? 0) / 100);
  }

  private computeDecompressFraction(snapshot: SeriesDownloadSnapshot): number {
    if (snapshot.phase === 'decompress') {
      if (typeof snapshot.phaseProgress === 'number') {
        return this.clampFraction(snapshot.phaseProgress / 100);
      }
      if (
        typeof snapshot.uncompressedBytes === 'number' &&
        typeof snapshot.uncompressedTotal === 'number' &&
        snapshot.uncompressedTotal > 0
      ) {
        return this.clampFraction(snapshot.uncompressedBytes / snapshot.uncompressedTotal);
      }
    }
    if (TERMINAL_STATUSES.has(snapshot.status)) return 1;
    return 0;
  }

  // ---------------------------------------------------------------------------
  // Cleanup
  // ---------------------------------------------------------------------------

  private disposeEvents(): void {
    const unsubs = [
      this.unsubscribeSeriesEvent,
      this.unsubscribeManifestMetadata,
      this.unsubscribePaused,
      this.unsubscribeResumed,
      this.unsubscribeCliError,
      this.unsubscribeCliFinished,
    ];
    for (const fn of unsubs) {
      try { fn?.(); } catch { /* ignore */ }
    }
    const events = [
      'download-series-event', 'manifest-series-metadata',
      'manifest-paused', 'manifest-resumed', 'cli-error', 'cli-finished',
    ];
    for (const ev of events) {
      try { EventsOff(ev); } catch { /* ignore */ }
    }
  }
}
