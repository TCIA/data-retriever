import { ManifestDownloadCardComponent } from './manifest-download-card.component';
import { RunState } from '../../models/run-state.model';

describe('ManifestDownloadCardComponent', () => {
  function createRunState(overrides?: Partial<RunState>): RunState {
    const base: RunState = {
      runId: 1n,
      inputFilePath: '/tmp/input.tcia',
      outputDirPath: '/tmp/output',
      status: 'done',
      overview: {
        total: 2,
        queued: 0,
        active: 0,
        completed: 2,
        failed: 0,
        skipped: 0,
        cancelled: 0,
        progressPercent: 100,
      },
      series: [],
      logs: [],
      isPaused: false,
      collapsed: false,
      hasAutoExpanded: false,
      startedAt: new Date(0).toISOString(),
      completedAt: new Date(1000).toISOString(),
      runOptions: {
        maxConnections: 2,
        maxRetries: 3,
        simultaneousDownloads: 2,
        skipExisting: false,
        downloadInParallel: true,
        authFilePath: '',
        directoryMode: 'descriptive',
      },
    };

    return {
      ...base,
      ...overrides,
      overview: {
        ...base.overview,
        ...(overrides?.overview ?? {}),
      },
      runOptions: {
        ...base.runOptions,
        ...(overrides?.runOptions ?? {}),
      },
    };
  }

  it('shows Open Folder when all series are completed', () => {
    const component = new ManifestDownloadCardComponent();
    component.run = createRunState();

    expect(component.canOpenOutputDirectory).toBeTrue();
  });

  it('shows Open Folder when all series are skipped', () => {
    const component = new ManifestDownloadCardComponent();
    component.run = createRunState({
      overview: {
        total: 3,
        queued: 0,
        active: 0,
        completed: 0,
        failed: 0,
        skipped: 3,
        cancelled: 0,
        progressPercent: 100,
      },
    });

    expect(component.canOpenOutputDirectory).toBeTrue();
  });

  it('shows Open Folder when completed + skipped equals total', () => {
    const component = new ManifestDownloadCardComponent();
    component.run = createRunState({
      overview: {
        total: 4,
        queued: 0,
        active: 0,
        completed: 1,
        failed: 0,
        skipped: 3,
        cancelled: 0,
        progressPercent: 100,
      },
    });

    expect(component.canOpenOutputDirectory).toBeTrue();
  });

  it('hides Open Folder when any series failed', () => {
    const component = new ManifestDownloadCardComponent();
    component.run = createRunState({
      overview: {
        total: 2,
        queued: 0,
        active: 0,
        completed: 1,
        failed: 1,
        skipped: 0,
        cancelled: 0,
        progressPercent: 100,
      },
    });

    expect(component.canOpenOutputDirectory).toBeFalse();
  });

  it('hides Open Folder when any series were cancelled', () => {
    const component = new ManifestDownloadCardComponent();
    component.run = createRunState({
      overview: {
        total: 2,
        queued: 0,
        active: 0,
        completed: 2,
        failed: 0,
        skipped: 0,
        cancelled: 1,
        progressPercent: 100,
      },
    });

    expect(component.canOpenOutputDirectory).toBeFalse();
  });

  it('hides Open Folder when output path is empty', () => {
    const component = new ManifestDownloadCardComponent();
    component.run = createRunState({ outputDirPath: '' });

    expect(component.canOpenOutputDirectory).toBeFalse();
  });

  it('shows downloaded fraction as completed plus skipped over total', () => {
    const component = new ManifestDownloadCardComponent();
    component.run = createRunState({
      overview: {
        total: 5,
        queued: 0,
        active: 0,
        completed: 2,
        failed: 0,
        skipped: 1,
        cancelled: 0,
        progressPercent: 60,
      },
    });

    expect(component.completedSeriesFraction).toBe('3 / 5 downloaded');
  });

  it('shows Paused label when run is paused even if status is done', () => {
    const component = new ManifestDownloadCardComponent();
    component.run = createRunState({
      status: 'done',
      isPaused: true,
    });

    expect(component.statusLabel).toBe('Paused');
  });

  it('keeps Error label precedence over paused state', () => {
    const component = new ManifestDownloadCardComponent();
    component.run = createRunState({
      status: 'error',
      isPaused: true,
    });

    expect(component.statusLabel).toBe('Error');
  });

  it('does not show completion marker while paused', () => {
    const component = new ManifestDownloadCardComponent();
    component.run = createRunState({
      status: 'done',
      isPaused: true,
      overview: {
        total: 2,
        queued: 0,
        active: 0,
        completed: 2,
        failed: 0,
        skipped: 0,
        cancelled: 0,
        progressPercent: 100,
      },
    });

    expect(component.isCompleted).toBeFalse();
  });

  it('does not show completion marker when status is done but active series remain', () => {
    const component = new ManifestDownloadCardComponent();
    component.run = createRunState({
      status: 'done',
      isPaused: false,
      overview: {
        total: 43,
        queued: 0,
        active: 1,
        completed: 42,
        failed: 0,
        skipped: 0,
        cancelled: 0,
        progressPercent: 98,
      },
    });

    expect(component.isCompleted).toBeFalse();
  });

  it('shows Downloading label when active series remain even if status is done', () => {
    const component = new ManifestDownloadCardComponent();
    component.run = createRunState({
      status: 'done',
      isPaused: false,
      overview: {
        total: 43,
        queued: 0,
        active: 1,
        completed: 42,
        failed: 0,
        skipped: 0,
        cancelled: 0,
        progressPercent: 98,
      },
    });

    expect(component.statusLabel).toBe('Downloading');
  });
});