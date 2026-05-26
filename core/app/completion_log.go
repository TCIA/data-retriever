package app

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"time"
)

// CompletionLogURL is the endpoint to POST per-series download-complete events to.
// Leave empty (default) to disable. Override at build time with -ldflags
// "-X TCIA_Data_Retriever/core/app.CompletionLogURL=https://..." or set the
// TCIA_COMPLETION_LOG_URL environment variable at runtime.
//
// TODO: fill in the real logging endpoint.
var CompletionLogURL = ""

// completionLogEvent is the payload POSTed when an individual series download
// finishes successfully.
type completionLogEvent struct {
	SeriesInstanceUID string    `json:"seriesUID"`
	StudyInstanceUID  string    `json:"studyUID,omitempty"`
	PatientID         string    `json:"subjectID,omitempty"`
	Collection        string    `json:"collection,omitempty"`
	Modality          string    `json:"modality,omitempty"`
	SeriesDescription string    `json:"seriesDescription,omitempty"`
	IsSync            bool      `json:"isSync,omitempty"`
	// Source is the download mechanism the series resolved to: "IDC" (AWS S3 /
	// s5cmd), "nbia" (TCIA API), "spreadsheet" (direct URL column), or "drs"
	// (Gen3 DRS URI). See FileInfo.downloadSource.
	Source string `json:"source"`
	// SourceURL is the concrete location the series was fetched from: the s3://
	// URI (IDC), the direct/DRS URL (spreadsheet/drs), or the TCIA API endpoint
	// targeted (nbia). See FileInfo.sourceURL.
	SourceURL   string    `json:"sourceURL,omitempty"`
	CompletedAt time.Time `json:"completedAt"`
}

// postSeriesCompletionLog fires a best-effort async POST announcing that one
// series finished downloading. The call is fire-and-forget: errors are logged
// at debug level and never affect the download pipeline.
func postSeriesCompletionLog(file *FileInfo) {
	if file == nil {
		return
	}

	url := CompletionLogURL
	if envURL := os.Getenv("TCIA_COMPLETION_LOG_URL"); envURL != "" {
		url = envURL
	}
	if url == "" {
		return
	}

	evt := completionLogEvent{
		SeriesInstanceUID: file.SeriesInstanceUID,
		StudyInstanceUID:  file.StudyInstanceUID,
		PatientID:         file.PatientID,
		Collection:        file.Collection,
		Modality:          file.Modality,
		SeriesDescription: file.SeriesDescription,
		IsSync:            file.IsSyncJob,
		Source:            file.downloadSource(),
		SourceURL:         file.sourceURL(),
		CompletedAt:       time.Now().UTC(),
	}

	go func() {
		payload, err := json.Marshal(evt)
		if err != nil {
			Logger.Debugf("completion log: marshal failed: %s", err)
			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
		if err != nil {
			Logger.Debugf("completion log: build request failed: %s", err)
			return
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			Logger.Debugf("completion log: POST %s failed: %s", url, err)
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode >= 400 {
			Logger.Debugf("completion log: POST %s returned %s", url, resp.Status)
		}
	}()
}
