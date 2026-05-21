import {
  DownloadOverviewSnapshot,
  SeriesDownloadSnapshot,
} from './download-series.model';

export type RunStatus = 'initializing' | 'running' | 'done' | 'error' | 'cancelled';

export interface RunOptions {
  maxConnections: number;
  maxRetries: number;
  simultaneousDownloads: number;
  skipExisting: boolean;
  downloadInParallel: boolean;
  authFilePath: string;
  directoryMode: string;
}

export interface RunState {
  /** uint64 represented as bigint — matches the Go backend's uint64 run ID. */
  runId: bigint;
  inputFilePath: string;
  outputDirPath: string;
  status: RunStatus;
  overview: DownloadOverviewSnapshot;
  series: SeriesDownloadSnapshot[];
  logs: string[];
  isPaused: boolean;
  collapsed: boolean;
  hasAutoExpanded: boolean;
  startedAt: string;
  completedAt?: string;
  bytesDownloaded?: number;
  bytesPerSecond?: number;
  errorMessage?: string;
  runOptions: RunOptions;
}
