package app

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const (
	croissantRoleDataFile        = "data file"
	croissantRoleManifest        = "manifest"
	croissantRoleTransferPackage = "transfer package"
)

type croissantDatasetInfo struct {
	DatasetID   string
	DatasetName string
	Collection  string
}

type croissantDownloadRow struct {
	RowID                string
	DownloadID           string
	FileObjectID         string
	Name                 string
	FileName             string
	DownloadURL          string
	SearchURL            string
	DownloadArtifactRole string
	AccessMechanism      string
	AccessLevel          string
	SearchSystem         string
	DownstreamSystem     string
	DownloadRequirements string
}

func decodeCroissant(ctx context.Context, filePath string, client *http.Client, options *Options, callbacks Callbacks, s5cmdMap map[string]string) ([]*FileInfo, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if options == nil {
		return nil, fmt.Errorf("options cannot be nil")
	}

	content, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read Croissant manifest %s: %w", filePath, err)
	}

	var doc map[string]interface{}
	if err := json.Unmarshal(content, &doc); err != nil {
		return nil, fmt.Errorf("invalid Croissant JSON: %w", err)
	}

	if !looksLikeCroissantDocument(doc) {
		return nil, fmt.Errorf("invalid Croissant manifest: expected dataset with recordSet and/or distribution")
	}

	dataset := croissantDatasetFromDoc(doc)
	rows := extractCroissantRows(doc)
	if len(rows) == 0 {
		return nil, fmt.Errorf("no Croissant rows found in %s", filePath)
	}

	files := make([]*FileInfo, 0, len(rows))
	skipped := 0

	for _, row := range rows {
		row.DownloadURL = strings.TrimSpace(row.DownloadURL)
		if row.DownloadURL == "" {
			warnCroissant(callbacks, "skipping row %q: missing download URL", croissantRowLabel(row))
			skipped++
			continue
		}

		role := resolveCroissantArtifactRole(row)
		switch role {
		case croissantRoleTransferPackage:
			warnCroissant(callbacks, "skipping row %q: unsupported transfer package URL %s", croissantRowLabel(row), row.DownloadURL)
			skipped++
			continue
		case croissantRoleManifest:
			if !isCroissantDataRetrieverRow(row) {
				warnCroissant(callbacks, "skipping row %q: manifest row is not marked for TCIA Data Retriever", croissantRowLabel(row))
				skipped++
				continue
			}
			nestedFiles, nestedErr := decodeNestedCroissantManifest(ctx, row, client, options, callbacks, s5cmdMap)
			if nestedErr != nil {
				warnCroissant(callbacks, "skipping row %q: failed to decode nested manifest from %s: %v", croissantRowLabel(row), row.DownloadURL, nestedErr)
				skipped++
				continue
			}
			if len(nestedFiles) == 0 {
				warnCroissant(callbacks, "skipping row %q: nested manifest did not produce any download jobs", croissantRowLabel(row))
				skipped++
				continue
			}
			files = append(files, nestedFiles...)
		default:
			fileInfo := croissantRowToFileInfo(dataset, row)
			if fileInfo == nil {
				warnCroissant(callbacks, "skipping row %q: could not map to download file", croissantRowLabel(row))
				skipped++
				continue
			}
			files = append(files, fileInfo)
		}
	}

	files = dedupeCroissantFiles(files)
	if len(files) == 0 {
		return nil, fmt.Errorf("no actionable rows found in Croissant manifest %s", filePath)
	}

	csvPath := filepath.Join(options.Output, "metadata", "metadata.csv")
	if err := WriteAllMetadataToCSV(files, csvPath); err != nil {
		Logger.Errorf("Failed to save Croissant metadata CSV: %v", err)
	} else {
		callbacks.emitStdout(fmt.Sprintf("Saved metadata for %d files to %s\n", len(files), csvPath))
	}
	InitCompletionStatus(options.Output, files)

	if skipped > 0 {
		callbacks.emitStdout(fmt.Sprintf("Skipped %d unsupported or invalid Croissant rows\n", skipped))
	}

	return files, nil
}

func decodeNestedCroissantManifest(ctx context.Context, row croissantDownloadRow, client *http.Client, options *Options, callbacks Callbacks, s5cmdMap map[string]string) ([]*FileInfo, error) {
	nestedPath, cleanup, err := downloadCroissantArtifact(ctx, row.DownloadURL, row.FileName, client)
	if err != nil {
		return nil, err
	}
	defer cleanup()

	ext := strings.ToLower(filepath.Ext(nestedPath))
	switch ext {
	case ".tcia", ".s5cmd", ".csv", ".tsv", ".xlsx":
		files, _, err := decodeInputFileInternal(ctx, nestedPath, client, options, callbacks, s5cmdMap, false)
		if err != nil {
			return nil, err
		}
		return files, nil
	default:
		return nil, fmt.Errorf("unsupported nested manifest format: %s", ext)
	}
}

func downloadCroissantArtifact(ctx context.Context, artifactURL string, nameHint string, client *http.Client) (string, func(), error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if client == nil {
		client = http.DefaultClient
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, artifactURL, nil)
	if err != nil {
		return "", func() {}, fmt.Errorf("failed to create request for %s: %w", artifactURL, err)
	}

	resp, err := doRequest(client, req)
	if err != nil {
		return "", func() {}, fmt.Errorf("failed to fetch %s: %w", artifactURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return "", func() {}, fmt.Errorf("%s returned HTTP %d", artifactURL, resp.StatusCode)
	}

	ext := detectCroissantArtifactExtension(artifactURL, nameHint)
	if ext == "" {
		ext = ".tmp"
	}

	tmpFile, err := os.CreateTemp("", "croissant-manifest-*"+ext)
	if err != nil {
		return "", func() {}, fmt.Errorf("failed to create temp file: %w", err)
	}

	if _, err := io.Copy(tmpFile, resp.Body); err != nil {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
		return "", func() {}, fmt.Errorf("failed to save nested manifest: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		os.Remove(tmpFile.Name())
		return "", func() {}, fmt.Errorf("failed to close temp file: %w", err)
	}

	cleanup := func() {
		_ = os.Remove(tmpFile.Name())
	}

	return tmpFile.Name(), cleanup, nil
}

func detectCroissantArtifactExtension(downloadURL string, nameHint string) string {
	if parsed, err := url.Parse(downloadURL); err == nil {
		ext := strings.ToLower(filepath.Ext(parsed.Path))
		if ext != "" {
			return ext
		}
	}

	return strings.ToLower(filepath.Ext(strings.TrimSpace(nameHint)))
}

func croissantDatasetFromDoc(doc map[string]interface{}) croissantDatasetInfo {
	alternateName := strings.TrimSpace(croissantString(doc["alternateName"]))
	name := strings.TrimSpace(croissantString(doc["name"]))
	if name == "" {
		name = alternateName
	}
	if name == "" {
		name = "Croissant Dataset"
	}

	collection := sanitizeCroissantPathPart(alternateName)
	if collection == "" {
		collection = sanitizeCroissantPathPart(name)
	}
	if collection == "" {
		collection = "croissant"
	}

	datasetID := strings.TrimSpace(croissantString(doc["@id"]))
	if datasetID == "" {
		datasetID = name
	}

	return croissantDatasetInfo{
		DatasetID:   datasetID,
		DatasetName: name,
		Collection:  collection,
	}
}

func looksLikeCroissantDocument(doc map[string]interface{}) bool {
	if doc == nil {
		return false
	}

	if !croissantTypeIncludesDataset(doc["@type"]) {
		return false
	}

	return len(croissantSlice(doc["recordSet"])) > 0 || len(croissantSlice(doc["distribution"])) > 0
}

func croissantTypeIncludesDataset(value interface{}) bool {
	switch typed := value.(type) {
	case string:
		return strings.Contains(strings.ToLower(typed), "dataset")
	case []interface{}:
		for _, entry := range typed {
			if croissantTypeIncludesDataset(entry) {
				return true
			}
		}
	}
	return false
}

func extractCroissantRows(doc map[string]interface{}) []croissantDownloadRow {
	index := newCroissantRowIndex()

	for _, rawRecordSet := range croissantSlice(doc["recordSet"]) {
		recordSet := croissantMap(rawRecordSet)
		if recordSet == nil {
			continue
		}

		baseRow := croissantDownloadRow{
			RowID: strings.TrimSpace(croissantString(recordSet["@id"])),
			Name:  strings.TrimSpace(croissantString(recordSet["name"])),
		}

		dataRows := croissantSlice(recordSet["data"])
		if len(dataRows) == 0 {
			index.upsert(baseRow)
			continue
		}

		for i, rawData := range dataRows {
			data := croissantMap(rawData)
			if data == nil {
				continue
			}

			row := baseRow
			if i > 0 && row.RowID != "" {
				row.RowID = fmt.Sprintf("%s-%d", row.RowID, i+1)
			}
			applyCroissantRecordData(&row, data)
			index.upsert(row)
		}
	}

	for _, rawDistribution := range croissantSlice(doc["distribution"]) {
		distribution := croissantMap(rawDistribution)
		if distribution == nil {
			continue
		}

		row := croissantDownloadRow{
			FileObjectID: strings.TrimSpace(croissantString(distribution["@id"])),
			Name:         strings.TrimSpace(croissantString(distribution["name"])),
			FileName:     strings.TrimSpace(croissantString(distribution["name"])),
			DownloadURL:  strings.TrimSpace(croissantString(distribution["contentUrl"])),
		}

		for _, rawProperty := range croissantSlice(distribution["additionalProperty"]) {
			property := croissantMap(rawProperty)
			if property == nil {
				continue
			}

			propName := compactCroissantKey(croissantString(property["name"]))
			propValue := strings.TrimSpace(croissantString(property["value"]))
			if propValue == "" {
				continue
			}

			switch propName {
			case "tciadownloadid", "downloadid":
				row.DownloadID = propValue
				if row.RowID == "" {
					row.RowID = "download-" + propValue
				}
			case "tciadownloadartifactrole", "downloadartifactrole":
				row.DownloadArtifactRole = propValue
			case "tciaaccessmechanism", "accessmechanism":
				row.AccessMechanism = propValue
			case "tciaaccesslevel", "accesslevel":
				row.AccessLevel = propValue
			case "tciasearchaccessurl", "searchurl":
				row.SearchURL = propValue
			case "tciasearchsystem", "searchsystem":
				row.SearchSystem = propValue
			case "tciadownstreamaccesssystem", "downstreamaccesssystem":
				row.DownstreamSystem = propValue
			case "tciadownloadrequirements", "downloadrequirements":
				row.DownloadRequirements = propValue
			}
		}

		index.upsert(row)
	}

	return index.rowsInOrder()
}

func applyCroissantRecordData(row *croissantDownloadRow, data map[string]interface{}) {
	for rawKey, value := range data {
		key := compactCroissantKey(rawKey)
		text := strings.TrimSpace(croissantString(value))
		if text == "" {
			continue
		}

		switch key {
		case "downloadid":
			row.DownloadID = text
			if row.RowID == "" {
				row.RowID = "download-" + text
			}
		case "downloadurl", "contenturl":
			row.DownloadURL = text
		case "searchurl":
			row.SearchURL = text
		case "fileobjectid":
			row.FileObjectID = text
		case "downloadartifactrole":
			row.DownloadArtifactRole = text
		case "accessmechanism":
			row.AccessMechanism = text
		case "accesslevel":
			row.AccessLevel = text
		case "searchsystem":
			row.SearchSystem = text
		case "downstreamaccesssystem":
			row.DownstreamSystem = text
		case "downloadrequirements":
			row.DownloadRequirements = text
		case "name":
			if row.Name == "" {
				row.Name = text
			}
		}
	}
}

func resolveCroissantArtifactRole(row croissantDownloadRow) string {
	rawRole := strings.ToLower(strings.TrimSpace(row.DownloadArtifactRole))
	switch {
	case strings.Contains(rawRole, "transfer"):
		return croissantRoleTransferPackage
	case strings.Contains(rawRole, "manifest"):
		return croissantRoleManifest
	case strings.Contains(rawRole, "data file"), strings.Contains(rawRole, "direct"):
		return croissantRoleDataFile
	}

	lowerMechanism := strings.ToLower(strings.TrimSpace(row.AccessMechanism + " " + row.DownloadRequirements))
	switch {
	case strings.Contains(lowerMechanism, "aspera") || strings.Contains(strings.ToLower(row.DownloadURL), "faspex"):
		return croissantRoleTransferPackage
	case strings.Contains(lowerMechanism, "data retriever"):
		return croissantRoleManifest
	case looksLikeCroissantManifestURL(row.DownloadURL, row.FileName):
		return croissantRoleManifest
	default:
		return croissantRoleDataFile
	}
}

func isCroissantDataRetrieverRow(row croissantDownloadRow) bool {
	mechanism := strings.ToLower(strings.TrimSpace(row.AccessMechanism + " " + row.DownloadRequirements))
	return strings.Contains(mechanism, "data retriever")
}

func looksLikeCroissantManifestURL(downloadURL string, nameHint string) bool {
	ext := detectCroissantArtifactExtension(downloadURL, nameHint)
	switch ext {
	case ".tcia", ".s5cmd":
		return true
	default:
		return false
	}
}

func croissantRowToFileInfo(dataset croissantDatasetInfo, row croissantDownloadRow) *FileInfo {
	downloadURL := strings.TrimSpace(row.DownloadURL)
	if downloadURL == "" {
		return nil
	}

	seriesUID := stableCroissantHashID(dataset.DatasetID, row.RowID, row.FileObjectID, downloadURL)
	studyUID := stableCroissantHashID(dataset.DatasetID, row.RowID, "study")

	fileName := strings.TrimSpace(row.FileName)
	if fileName == "" {
		if parsed, err := url.Parse(downloadURL); err == nil {
			fileName = strings.TrimSpace(filepath.Base(parsed.Path))
		}
	}
	if fileName == "" || fileName == "." || fileName == "/" {
		fileName = seriesUID
	}

	seriesDescription := strings.TrimSpace(row.Name)
	if seriesDescription == "" {
		seriesDescription = fileName
	}

	fileInfo := &FileInfo{
		SeriesInstanceUID: seriesUID,
		StudyInstanceUID:  studyUID,
		SeriesDescription: seriesDescription,
		StudyDesc:         dataset.DatasetName,
		Collection:        dataset.Collection,
		PatientID:         "croissant",
		FileName:          fileName,
		StudyID:           strings.TrimSpace(row.RowID),
	}

	if strings.HasPrefix(strings.ToLower(downloadURL), "drs:") {
		fileInfo.DRSURI = downloadURL
	} else {
		fileInfo.DownloadURL = downloadURL
	}

	return fileInfo
}

func dedupeCroissantFiles(files []*FileInfo) []*FileInfo {
	if len(files) == 0 {
		return files
	}

	seen := make(map[string]struct{}, len(files))
	result := make([]*FileInfo, 0, len(files))
	for _, file := range files {
		if file == nil {
			continue
		}

		key := strings.TrimSpace(file.SeriesInstanceUID)
		if key == "" {
			key = strings.TrimSpace(file.DownloadURL)
		}
		if key == "" {
			key = strings.TrimSpace(file.DRSURI)
		}
		if key == "" {
			continue
		}

		if _, exists := seen[key]; exists {
			continue
		}

		seen[key] = struct{}{}
		result = append(result, file)
	}

	return result
}

func stableCroissantHashID(parts ...string) string {
	h := sha1.New()
	for _, part := range parts {
		_, _ = io.WriteString(h, strings.TrimSpace(part))
		_, _ = io.WriteString(h, "|")
	}
	return "croissant-" + hex.EncodeToString(h.Sum(nil))
}

func sanitizeCroissantPathPart(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}

	replacer := strings.NewReplacer("/", "_", "\\", "_", ":", "_", "\t", " ", "\n", " ", "\r", " ")
	clean := replacer.Replace(trimmed)
	clean = strings.TrimSpace(clean)
	if clean == "" {
		return ""
	}
	return clean
}

func warnCroissant(callbacks Callbacks, format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	if Logger != nil {
		Logger.Warnf("Croissant: %s", message)
	}
	callbacks.emitStderr(fmt.Sprintf("Croissant warning: %s\n", message))
}

func croissantRowLabel(row croissantDownloadRow) string {
	if strings.TrimSpace(row.RowID) != "" {
		return row.RowID
	}
	if strings.TrimSpace(row.FileObjectID) != "" {
		return row.FileObjectID
	}
	if strings.TrimSpace(row.Name) != "" {
		return row.Name
	}
	if strings.TrimSpace(row.DownloadURL) != "" {
		return row.DownloadURL
	}
	return "(unidentified row)"
}

func compactCroissantKey(key string) string {
	trimmed := strings.ToLower(strings.TrimSpace(key))
	if idx := strings.LastIndex(trimmed, "/"); idx >= 0 {
		trimmed = trimmed[idx+1:]
	}

	var b strings.Builder
	for _, r := range trimmed {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func croissantMap(value interface{}) map[string]interface{} {
	mapped, ok := value.(map[string]interface{})
	if !ok {
		return nil
	}
	return mapped
}

func croissantSlice(value interface{}) []interface{} {
	slice, ok := value.([]interface{})
	if !ok {
		return nil
	}
	return slice
}

func croissantString(value interface{}) string {
	switch typed := value.(type) {
	case string:
		return typed
	case float64:
		if typed == float64(int64(typed)) {
			return fmt.Sprintf("%d", int64(typed))
		}
		return fmt.Sprintf("%v", typed)
	case bool:
		if typed {
			return "true"
		}
		return "false"
	case []interface{}:
		parts := make([]string, 0, len(typed))
		for _, entry := range typed {
			entryText := strings.TrimSpace(croissantString(entry))
			if entryText != "" {
				parts = append(parts, entryText)
			}
		}
		return strings.Join(parts, ", ")
	case map[string]interface{}:
		if id := strings.TrimSpace(croissantString(typed["@id"])); id != "" {
			return id
		}
		if valueText := strings.TrimSpace(croissantString(typed["@value"])); valueText != "" {
			return valueText
		}
		if urlText := strings.TrimSpace(croissantString(typed["url"])); urlText != "" {
			return urlText
		}
		if name := strings.TrimSpace(croissantString(typed["name"])); name != "" {
			return name
		}
	}

	return ""
}

type croissantRowIndex struct {
	rows   map[string]croissantDownloadRow
	lookup map[string]string
	order  []string
}

func newCroissantRowIndex() *croissantRowIndex {
	return &croissantRowIndex{
		rows:   make(map[string]croissantDownloadRow),
		lookup: make(map[string]string),
		order:  make([]string, 0),
	}
}

func (idx *croissantRowIndex) upsert(row croissantDownloadRow) {
	keys := croissantRowKeys(row)

	primary := ""
	for _, key := range keys {
		if mapped, ok := idx.lookup[key]; ok {
			primary = mapped
			break
		}
	}

	if primary == "" {
		if len(keys) > 0 {
			primary = keys[0]
		} else {
			primary = fmt.Sprintf("row:%d", len(idx.order)+1)
		}
		idx.order = append(idx.order, primary)
		idx.rows[primary] = row
	} else {
		existing := idx.rows[primary]
		mergeCroissantRow(&existing, row)
		idx.rows[primary] = existing
	}

	for _, key := range keys {
		idx.lookup[key] = primary
	}
}

func (idx *croissantRowIndex) rowsInOrder() []croissantDownloadRow {
	rows := make([]croissantDownloadRow, 0, len(idx.order))
	for _, key := range idx.order {
		rows = append(rows, idx.rows[key])
	}

	sort.SliceStable(rows, func(i, j int) bool {
		return croissantRowLabel(rows[i]) < croissantRowLabel(rows[j])
	})
	return rows
}

func croissantRowKeys(row croissantDownloadRow) []string {
	keys := make([]string, 0, 4)
	if id := strings.ToLower(strings.TrimSpace(row.RowID)); id != "" {
		keys = append(keys, "row:"+id)
	}
	if id := strings.ToLower(strings.TrimSpace(row.DownloadID)); id != "" {
		keys = append(keys, "download:"+id)
	}
	if id := strings.ToLower(strings.TrimSpace(row.FileObjectID)); id != "" {
		keys = append(keys, "file:"+id)
	}
	if row.DownloadURL != "" {
		keys = append(keys, "url:"+canonicalCroissantURL(row.DownloadURL))
	}
	return keys
}

func canonicalCroissantURL(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}

	parsed, err := url.Parse(trimmed)
	if err != nil {
		return trimmed
	}
	parsed.Fragment = ""
	return parsed.String()
}

func mergeCroissantRow(dst *croissantDownloadRow, src croissantDownloadRow) {
	setCroissantStringIfEmpty(&dst.RowID, src.RowID)
	setCroissantStringIfEmpty(&dst.DownloadID, src.DownloadID)
	setCroissantStringIfEmpty(&dst.FileObjectID, src.FileObjectID)
	setCroissantStringIfEmpty(&dst.Name, src.Name)
	setCroissantStringIfEmpty(&dst.FileName, src.FileName)
	setCroissantStringIfEmpty(&dst.DownloadURL, src.DownloadURL)
	setCroissantStringIfEmpty(&dst.SearchURL, src.SearchURL)
	setCroissantStringIfEmpty(&dst.DownloadArtifactRole, src.DownloadArtifactRole)
	setCroissantStringIfEmpty(&dst.AccessMechanism, src.AccessMechanism)
	setCroissantStringIfEmpty(&dst.AccessLevel, src.AccessLevel)
	setCroissantStringIfEmpty(&dst.SearchSystem, src.SearchSystem)
	setCroissantStringIfEmpty(&dst.DownstreamSystem, src.DownstreamSystem)
	setCroissantStringIfEmpty(&dst.DownloadRequirements, src.DownloadRequirements)
}

func setCroissantStringIfEmpty(dst *string, src string) {
	if strings.TrimSpace(*dst) == "" && strings.TrimSpace(src) != "" {
		*dst = strings.TrimSpace(src)
	}
}
