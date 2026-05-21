import { NgZone } from '@angular/core';
import { DownloadStatusService } from './download-status.service';
import { RunOptions, RunState } from '../models/run-state.model';

describe('DownloadStatusService pause/resume races', () => {
  let service: DownloadStatusService;
  let latestRuns: RunState[] = [];
  let unsubscribeRuns: { unsubscribe: () => void };
  let listeners: Record<string, Array<(payload: unknown) => void>>;
  let originalRuntime: unknown;

  const defaultOptions: RunOptions = {
    maxConnections: 2,
    maxRetries: 3,
    simultaneousDownloads: 2,
    skipExisting: false,
    downloadInParallel: true,
    authFilePath: '',
    directoryMode: 'descriptive',
  };

  function emit(eventName: string, payload: unknown): void {
    const callbacks = listeners[eventName] ?? [];
    expect(callbacks.length).withContext(`No listeners registered for ${eventName}`).toBeGreaterThan(0);
    for (const callback of callbacks) {
      callback(payload);
    }
  }

  function getRun(runId: bigint): RunState {
    const run = latestRuns.find(item => item.runId === runId);
    expect(run).withContext(`Missing run ${runId.toString()}`).toBeDefined();
    return run as RunState;
  }

  beforeEach(() => {
    listeners = {};

    const runtimeWindow = window as Window & { runtime?: any };
    originalRuntime = runtimeWindow.runtime;
    runtimeWindow.runtime = {
      EventsOnMultiple: (eventName: string, callback: (...data: any[]) => void) => {
        if (!listeners[eventName]) {
          listeners[eventName] = [];
        }
        listeners[eventName].push((payload: unknown) => callback(payload));
        return () => {};
      },
      EventsOff: () => {},
    };

    service = new DownloadStatusService(new NgZone({ enableLongStackTrace: false }));
    unsubscribeRuns = service.runs$.subscribe(runs => {
      latestRuns = runs;
    });
  });

  afterEach(() => {
    unsubscribeRuns?.unsubscribe();
    const runtimeWindow = window as Window & { runtime?: any };
    runtimeWindow.runtime = originalRuntime;
  });

  it('does not mark a paused run as done on cli-finished', () => {
    service.beginRun(1n, '/tmp/input.tcia', '/tmp/output', defaultOptions);

    emit('manifest-paused', '/tmp/input.tcia');
    emit('cli-finished', { runId: '1', summary: '' });

    const run = getRun(1n);
    expect(run.isPaused).toBeTrue();
    expect(run.status).not.toBe('done');
  });

  it('resets stale done state and terminal series when resumed', () => {
    service.beginRun(1n, '/tmp/input.tcia', '/tmp/output', defaultOptions);

    emit('manifest-series-metadata', {
      runId: '1',
      series: [{ seriesUID: 'series-1', bytesTotal: 100 }],
    });
    emit('download-series-event', {
      runId: '1',
      seriesUID: 'series-1',
      status: 'succeeded',
      progress: 100,
      timestamp: new Date().toISOString(),
    });

    // Simulate race where completion lands before paused flag.
    emit('cli-finished', { runId: '1', summary: '' });
    emit('manifest-paused', '/tmp/input.tcia');
    emit('manifest-resumed', '/tmp/input.tcia');

    const run = getRun(1n);
    expect(run.isPaused).toBeFalse();
    expect(run.status).toBe('running');
    expect(run.completedAt).toBeUndefined();
    expect(run.series.length).toBe(1);
    expect(run.series[0].status).toBe('queued');
    expect(run.series[0].progress).toBe(0);
  });

  it('does not mark an active run as done when cli-finished summary is empty', () => {
    service.beginRun(2n, '/tmp/input-2.tcia', '/tmp/output', defaultOptions);

    emit('manifest-series-metadata', {
      runId: '2',
      series: [{ seriesUID: 'series-2', bytesTotal: 100 }],
    });
    emit('download-series-event', {
      runId: '2',
      seriesUID: 'series-2',
      status: 'downloading',
      progress: 50,
      timestamp: new Date().toISOString(),
    });

    emit('cli-finished', { runId: '2', summary: '' });

    const run = getRun(2n);
    expect(run.status).toBe('running');
    expect(run.completedAt).toBeUndefined();
  });

  it('marks run as done when cli-finished summary is non-empty', () => {
    service.beginRun(3n, '/tmp/input-3.tcia', '/tmp/output', defaultOptions);

    emit('cli-finished', { runId: '3', summary: 'Download Summary: total 1, downloaded 1' });

    const run = getRun(3n);
    expect(run.status).toBe('done');
    expect(run.completedAt).toBeDefined();
  });
});
