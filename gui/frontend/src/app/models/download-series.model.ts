export type SeriesDownloadStatus =
  | 'queued'
  | 'metadata'
  | 'downloading'
  | 'skipped'
  | 'succeeded'
  | 'failed'
  | 'cancelled';

export interface SeriesDownloadSnapshot {
  seriesUID: string;
  studyUID?: string;
  subjectID?: string;
  seriesDescription?: string;
  modality?: string;
  status: SeriesDownloadStatus;
  progress: number;
  logs: string[];
  bytesDownloaded?: number;
  bytesTotal?: number;
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
  message?: string;
  bytesDownloaded?: number;
  bytesTotal?: number;
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
  startedAt?: string;
  completedAt?: string;
  logs: string[];
}
