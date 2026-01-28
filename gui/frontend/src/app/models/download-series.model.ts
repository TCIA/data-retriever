export type SeriesDownloadStatus =
  | 'queued'
  | 'metadata'
  | 'downloading'
  | 'decompressing'
  | 'skipped'
  | 'succeeded'
  | 'failed'
  | 'cancelled';

export type SeriesDownloadPhase =
  | 'queued'
  | 'metadata'
  | 'download'
  | 'decompress'
  | 'complete'
  | 'failed';

export interface SeriesDownloadSnapshot {
  seriesUID: string;
  studyUID?: string;
  subjectID?: string;
  seriesDescription?: string;
  modality?: string;
  status: SeriesDownloadStatus;
  progress: number;
  phase?: SeriesDownloadPhase;
  phaseProgress?: number;
  logs: string[];
  bytesDownloaded?: number;
  bytesTotal?: number;
  uncompressedBytes?: number;
  uncompressedTotal?: number;
  attempts?: number;
  startedAt?: string;
  completedAt?: string;
  lastUpdatedAt?: string;
  errorMessage?: string;
}

export interface SeriesDownloadEventPayload {
  seriesUID: string;
  studyUID?: string;
  subjectID?: string;
  seriesDescription?: string;
  modality?: string;
  status: SeriesDownloadStatus;
  progress?: number;
  phase?: SeriesDownloadPhase;
  phaseProgress?: number;
  message?: string;
  bytesDownloaded?: number;
  bytesTotal?: number;
  uncompressedBytes?: number;
  uncompressedTotal?: number;
  attempt?: number;
  timestamp?: string;
}

export interface DownloadOverviewSnapshot {
  total: number;
  active: number;
  completed: number;
  failed: number;
  skipped: number;
  cancelled: number;
  progressPercent: number;
}

export interface ManifestDownloadSnapshot {
  manifestPath: string;
  total: number;
  active: number;
  completed: number;
  failed: number;
  skipped: number;
  cancelled: number;
  progressPercent: number;
  bytesDownloaded?: number;
  bytesTotal?: number;
  startedAt?: string;
  completedAt?: string;
  logs: string[];
}
