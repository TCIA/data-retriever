package app

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"embed"
	"os"
	"path/filepath"
	"strings"
	"strconv"
	"context"
	"regexp"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/parquet/file"
	pqarrow "github.com/apache/arrow/go/v14/parquet/pqarrow"
	"github.com/apache/arrow/go/v14/arrow"
)

type SeriesMetadata struct {
	SeriesInstanceUID   string
	series_aws_url      string
	series_size_MB      float64
	collection_id       string
	analysis_result_id  string
	PatientID           string
	PatientAge          string
	PatientSex          string
	StudyInstanceUID    string
	StudyDate           string
	StudyDescription    string
	BodyPartExamined    string
	Modality            string
	SOPClassUID         string
	sop_class_name      string
	TransferSyntaxUID   string
	transfer_syntax_name string
	Manufacturer        string
	ManufacturerModelName string
	SeriesDate          string
	SeriesDescription   string
	SeriesNumber        string
	instanceCount       int64
	license_short_name  string
	aws_bucket          string
	crdc_series_uuid    string
	source_DOI          string
}

//go:embed parquet/prior_versions_index.parquet
//go:embed parquet/idc_index.parquet
var parquetFS embed.FS

func safeStringCol(schema *arrow.Schema, cols []arrow.Array, name string) *array.String {
	idxs := schema.FieldIndices(name)
	if len(idxs) == 0 {
		return nil
	}
	col, _ := cols[idxs[0]].(*array.String)
	return col
}

func safeFloat64Col(schema *arrow.Schema, cols []arrow.Array, name string) *array.Float64 {
	idxs := schema.FieldIndices(name)
	if len(idxs) == 0 {
		return nil
	}
	col, _ := cols[idxs[0]].(*array.Float64)
	return col
}

// stringVal returns "" if the column is missing or the cell is null.
func stringVal(col *array.String, i int) string {
	if col == nil || col.IsNull(i) {
		return ""
	}
	return col.Value(i)
}

// float64Val returns nil if the column is missing or the cell is null.
func float64Val(col *array.Float64, i int) *float64 {
	if col == nil || col.IsNull(i) {
		return nil
	}
	v := col.Value(i)
	return &v
}

func safeInt64Col(schema *arrow.Schema, cols []arrow.Array, name string) *array.Int64 {
	idxs := schema.FieldIndices(name)
	if len(idxs) == 0 {
		return nil
	}
	col, _ := cols[idxs[0]].(*array.Int64)
	return col
}

func int64Val(col *array.Int64, i int) *int64 {
	if col == nil || col.IsNull(i) {
		return nil
	}
	v := col.Value(i)
	return &v
}

func loadSeriesMetadataFromParquet(
	parquetPath string,   // empty string → use embedded fallback
	embeddedPath string,  // e.g. "parquet/idc_index.parquet"
	meta map[string]*SeriesMetadata,
	metaFromSeries map[string]string,
) error {
	var (
		f    io.ReadSeekCloser
		size int64
	)

	if parquetPath != "" {
		osf, err := os.Open(parquetPath)
		if err != nil {
			return fmt.Errorf("opening parquet file %s: %w", parquetPath, err)
		}
		stat, err := osf.Stat()
		if err != nil {
			osf.Close()
			return fmt.Errorf("stat parquet file: %w", err)
		}
		f = osf
		size = stat.Size()
	} else {
		ef, err := parquetFS.Open(embeddedPath)
		if err != nil {
			return fmt.Errorf("opening embedded parquet %s: %w", embeddedPath, err)
		}
		stat, err := ef.Stat()
		if err != nil {
			ef.Close()
			return fmt.Errorf("stat embedded parquet: %w", err)
		}
		// embed.File implements ReaderAt and Seeker on its concrete type.
		rsc, ok := ef.(io.ReadSeekCloser)
		if !ok {
			ef.Close()
			return fmt.Errorf("embedded file does not implement ReadSeekCloser")
		}
		f = rsc
		size = stat.Size()
	}
	defer f.Close()

	readerAt, ok := f.(io.ReaderAt)
	if !ok {
		return fmt.Errorf("parquet source does not implement ReaderAt")
	}
	section := io.NewSectionReader(readerAt, 0, size)

	pqReader, err := file.NewParquetReader(section)
	if err != nil {
		return fmt.Errorf("failed to create parquet reader: %w", err)
	}
	defer pqReader.Close()
	for i := 0; i < pqReader.NumRowGroups(); i++ {
		rg := pqReader.RowGroup(i)
		logger.Debugf("row group %d: num rows = %d, num columns = %d", i, rg.NumRows(), rg.NumColumns())
	}

	mem := memory.NewGoAllocator()

	props := pqarrow.ArrowReadProperties{
		BatchSize: 8192, // THIS is where batch size goes
	}

	arrowReader, err := pqarrow.NewFileReader(pqReader, props, mem)
	if err != nil {
		return fmt.Errorf("failed to create Arrow reader: %w", err)
	}

	recReader, err := arrowReader.GetRecordReader(
		context.Background(),
		nil, // all columns
		nil, // all row groups
	)

	if err != nil {
		return fmt.Errorf("failed to get record reader: %w", err)
	}
	defer recReader.Release()

	logger.Debugf("record reader schema: %v", recReader.Schema())
	schema := recReader.Schema()
	for _, f := range schema.Fields() {
		logger.Debugf("Column: %s, Type: %v", f.Name, f.Type)
	}

	logger.Debugf("parquet row groups: %d", pqReader.NumRowGroups())
	for recReader.Next() {
		rec := recReader.Record()
		schema := rec.Schema()
		cols := rec.Columns()

		uidCol                := safeStringCol(schema, cols, "SeriesInstanceUID")
		urlCol                := safeStringCol(schema, cols, "series_aws_url")
		fileSizeCol           := safeFloat64Col(schema, cols, "series_size_MB")
		collectionIDCol       := safeStringCol(schema, cols, "collection_id")
		analysisResultCol     := safeStringCol(schema, cols, "analysis_result_id")
		patientIDCol          := safeStringCol(schema, cols, "PatientID")
		patientAgeCol         := safeStringCol(schema, cols, "PatientAge")
		patientSexCol         := safeStringCol(schema, cols, "PatientSex")
		studyUIDCol           := safeStringCol(schema, cols, "StudyInstanceUID")
		studyDateCol          := safeStringCol(schema, cols, "StudyDate")
		studyDescCol          := safeStringCol(schema, cols, "StudyDescription")
		bodyPartCol           := safeStringCol(schema, cols, "BodyPartExamined")
		modalityCol           := safeStringCol(schema, cols, "Modality")
		sopClassUIDCol        := safeStringCol(schema, cols, "SOPClassUID")
		sopClassNameCol       := safeStringCol(schema, cols, "sop_class_name")
		transferSyntaxUIDCol  := safeStringCol(schema, cols, "TransferSyntaxUID")
		transferSyntaxNameCol := safeStringCol(schema, cols, "transfer_syntax_name")
		manufacturerCol       := safeStringCol(schema, cols, "Manufacturer")
		manufacturerModelCol  := safeStringCol(schema, cols, "ManufacturerModelName")
		seriesDateCol         := safeStringCol(schema, cols, "SeriesDate")
		seriesNumCol          := safeStringCol(schema, cols, "SeriesNumber")
		seriesDescCol         := safeStringCol(schema, cols, "SeriesDescription")
		instanceCountCol      := safeInt64Col(schema, cols, "instanceCount")
		licenseCol            := safeStringCol(schema, cols, "license_short_name")
		awsBucketCol          := safeStringCol(schema, cols, "aws_bucket")
		crdcUUIDCol           := safeStringCol(schema, cols, "crdc_series_uuid")
		sourceDOICol          := safeStringCol(schema, cols, "source_DOI")

		rows := int(rec.NumRows())
		for i := 0; i < rows; i++ {
			if uidCol.IsNull(i) || urlCol.IsNull(i) {
				continue
			}
			uid := uidCol.Value(i)
			url := urlCol.Value(i)

			// Skip if already present — preserves existing entries
			if _, exists := meta[url]; exists {
				continue
			}

			fileSize     := float64Val(fileSizeCol, i)
			instanceCnt  := int64Val(instanceCountCol, i)
			
			entry := SeriesMetadata{
			    SeriesInstanceUID:     uid,
			    series_aws_url:        url,
			    collection_id:         stringVal(collectionIDCol, i),
			    analysis_result_id:    stringVal(analysisResultCol, i),
			    PatientID:             stringVal(patientIDCol, i),
			    PatientAge:            stringVal(patientAgeCol, i),
			    PatientSex:            stringVal(patientSexCol, i),
			    StudyInstanceUID:      stringVal(studyUIDCol, i),
			    StudyDate:             stringVal(studyDateCol, i),
			    StudyDescription:      stringVal(studyDescCol, i),
			    BodyPartExamined:      stringVal(bodyPartCol, i),
			    Modality:              stringVal(modalityCol, i),
			    SOPClassUID:           stringVal(sopClassUIDCol, i),
			    sop_class_name:        stringVal(sopClassNameCol, i),
			    TransferSyntaxUID:     stringVal(transferSyntaxUIDCol, i),
			    transfer_syntax_name:  stringVal(transferSyntaxNameCol, i),
			    Manufacturer:          stringVal(manufacturerCol, i),
			    ManufacturerModelName: stringVal(manufacturerModelCol, i),
			    SeriesDate:            stringVal(seriesDateCol, i),
			    SeriesNumber:          stringVal(seriesNumCol, i),
			    SeriesDescription:     stringVal(seriesDescCol, i),
			    license_short_name:    stringVal(licenseCol, i),
			    aws_bucket:            stringVal(awsBucketCol, i),
			    crdc_series_uuid:      stringVal(crdcUUIDCol, i),
			    source_DOI:            stringVal(sourceDOICol, i),
			}
			if fileSize != nil {
			    entry.series_size_MB = *fileSize
			}
			if instanceCnt != nil {
			    entry.instanceCount = *instanceCnt
			}

			meta[url] = &entry
			metaFromSeries[uid] = url
		}
		rec.Release()
	}

	if err := recReader.Err(); err != nil && err != io.EOF {
		return fmt.Errorf("error reading Parquet records from %s: %w", parquetPath, err)
	}

	return nil
}


// loadS5cmdSeriesMapFromCSVs scans all '*-metadata.csv' files in the metadata
// directory to build a map of previously downloaded s5cmd series.
func loadS5cmdSeriesMapFromCSVs(outputDir string) (map[string]string, error) {
	seriesMap := make(map[string]string)
	metaDir := filepath.Join(outputDir, "metadata")

	files, err := os.ReadDir(metaDir)
	if err != nil {
		if os.IsNotExist(err) {
			return seriesMap, nil // No metadata dir yet, so no map.
		}
		return nil, fmt.Errorf("could not read metadata directory: %w", err)
	}

	for _, file := range files {
		if file.IsDir() || !strings.HasSuffix(file.Name(), "metadata.csv") {
			continue
		}

		filePath := filepath.Join(metaDir, file.Name())
		f, err := os.Open(filePath)
		if err != nil {
			logger.Warnf("Could not open metadata CSV %s: %v", filePath, err)
			continue
		}
		defer f.Close()

		reader := csv.NewReader(f)
		header, err := reader.Read()
		if err != nil {
			logger.Warnf("Could not read header from CSV %s: %v", filePath, err)
			continue
		}

		uriIndex, uidIndex := -1, -1
		for i, colName := range header {
			if colName == "OriginalS5cmdURI" {
				uriIndex = i
			} else if colName == "SeriesInstanceUID" {
				uidIndex = i
			}
		}

		if uriIndex == -1 || uidIndex == -1 {
			logger.Warnf("Could not find required columns in %s", filePath)
			continue
		}

		for {
			record, err := reader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				logger.Warnf("Error reading record from %s: %v", filePath, err)
				continue
			}
			if len(record) > uriIndex && len(record) > uidIndex {
				seriesMap[record[uriIndex]] = record[uidIndex]
			}
		}
	}

	return seriesMap, nil
}

// loadParquetMetadata loads both parquet indexes (with embedded fallback) and
// returns the URI→metadata map plus a SeriesInstanceUID→s3 URI lookup.
func loadParquetMetadata(options *Options) (map[string]*SeriesMetadata, map[string]string) {
	seriesMeta := make(map[string]*SeriesMetadata)
	nbiaLookup := make(map[string]string)

	if err := loadSeriesMetadataFromParquet(
		options.IDCParquetPath, "parquet/idc_index.parquet",
		seriesMeta, nbiaLookup,
	); err != nil {
		logger.Warnf("Failed to load idc parquet metadata: %v", err)
	}
	if err := loadSeriesMetadataFromParquet(
		options.PriorParquetPath, "parquet/prior_versions_index.parquet",
		seriesMeta, nbiaLookup,
	); err != nil {
		logger.Warnf("Failed to load prior_versions parquet metadata: %v", err)
	}
	return seriesMeta, nbiaLookup
}

// buildS5cmdFileInfo constructs a FileInfo for a single IDC s3 URI using the
// parquet-derived metadata map. Returns (fi, isNewJob, found). When found is
// false the URI was not in the parquet and the caller should route the source
// (e.g. a SeriesInstanceUID) through the TCIA fallback instead.
func buildS5cmdFileInfo(originalURI string, seriesMeta map[string]*SeriesMetadata, outputDir string, processedSeries map[string]string, options *Options) (*FileInfo, bool, bool) {
	meta, ok := seriesMeta[originalURI]
	if !ok {
		return nil, false, false
	}

	var fi *FileInfo
	isNewJob := false
	if seriesUID, exists := processedSeries[originalURI]; exists {
		logger.Infof("Queueing sync job for existing series: %s", originalURI)
		fi = &FileInfo{
			DownloadURL:       originalURI,
			SeriesInstanceUID: seriesUID,
			OriginalS5cmdURI:  originalURI,
			IsSyncJob:         true,
		}
	} else {
		isNewJob = true
		logger.Infof("Queueing new copy job for series: %s", originalURI)
		fi = &FileInfo{
			DownloadURL:       originalURI,
			SeriesInstanceUID: originalURI,
			OriginalS5cmdURI:  originalURI,
			IsSyncJob:         false,
		}
	}

	fi.SeriesInstanceUID = meta.SeriesInstanceUID
	fi.FileSize = strconv.FormatInt(int64(meta.series_size_MB*1000*1000), 10)
	fi.PatientID = meta.PatientID
	fi.PatientAge = meta.PatientAge
	fi.PatientSex = meta.PatientSex
	fi.StudyInstanceUID = meta.StudyInstanceUID
	fi.Collection = meta.collection_id
	fi.StudyDate = meta.StudyDate
	fi.StudyDesc = meta.StudyDescription
	fi.BodyPartExamined = meta.BodyPartExamined
	fi.Modality = meta.Modality
	fi.SOPClassUID = meta.SOPClassUID
	fi.SOPClassName = meta.sop_class_name
	fi.TransferSyntaxUID = meta.TransferSyntaxUID
	fi.TransferSyntaxName = meta.transfer_syntax_name
	fi.Manufacturer = meta.Manufacturer
	fi.ManufacturerModelName = meta.ManufacturerModelName
	fi.SeriesDate = meta.SeriesDate
	fi.SeriesNumber = meta.SeriesNumber
	fi.SeriesDescription = meta.SeriesDescription
	fi.InstanceCount = meta.instanceCount
	fi.LicenseShortName = meta.license_short_name
	fi.AWSBucket = meta.aws_bucket
	fi.CRDCSeriesUUID = meta.crdc_series_uuid
	fi.SourceDOI = meta.source_DOI
	fi.AnalysisResultID = meta.analysis_result_id

	var finalDirPath string
	if options.DirectoryMode == "descriptive" {
		cleanStudyDesc := strings.ReplaceAll(fi.StudyDesc, "/", "")
		cleanSeriesDesc := strings.ReplaceAll(fi.SeriesDescription, "/", "")
		studyUID := fi.StudyInstanceUID
		if len(studyUID) > 5 {
			studyUID = studyUID[len(studyUID)-5:]
		}
		seriesUID := fi.SeriesInstanceUID
		if len(seriesUID) > 5 {
			seriesUID = seriesUID[len(seriesUID)-5:]
		}
		finalDirPath = filepath.Join(outputDir, fi.Collection, fi.PatientID,
			joinParts(fi.StudyDate, cleanStudyDesc[:min(54, len(cleanStudyDesc))], studyUID),
			joinParts(fi.SeriesNumber, cleanSeriesDesc[:min(54, len(cleanSeriesDesc))], seriesUID))
	} else {
		finalDirPath = filepath.Join(outputDir, fi.Collection, fi.PatientID, fi.StudyInstanceUID, fi.SeriesInstanceUID)
	}

	if err := os.MkdirAll(finalDirPath, 0755); err != nil {
		logger.Warnf("Could not create temp directory for %s: %v", originalURI, err)
		return nil, false, false
	}
	fi.S5cmdManifestPath = finalDirPath

	return fi, isNewJob, true
}

// decodeS5cmd parses an s5cmd or .tcia manifest. Bare SeriesInstanceUID lines
// that don't resolve via the parquet index are returned as tciaFallbackUIDs so
// the caller can route them through the TCIA streaming/batch path. When the
// fallback list is empty the function writes metadata.csv and initializes
// completion status itself, preserving prior behavior for pure IDC inputs;
// when there are fallback UIDs the caller is responsible for writing combined
// metadata after the TCIA side completes.
func decodeS5cmd(filePath string, outputDir string, processedSeries map[string]string, callbacks Callbacks, options *Options) ([]*FileInfo, int, []string) {
	file, err := os.Open(filePath)
	if err != nil {
		logger.Fatalf("could not open s5cmd manifest: %v", err)
	}
	defer file.Close()

	seriesMeta, nbiaLookup := loadParquetMetadata(options)

	var jobsToProcess []*FileInfo
	var tciaFallbackUIDs []string
	var newJobs int
	var numDotRe = regexp.MustCompile(`^[0-9][0-9.]*$`)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Fields(line)
		var originalURI string
		var sourceUID string
		if len(parts) >= 2 && parts[0] == "cp" {
			originalURI = parts[1]
		} else if len(parts) == 1 && strings.HasPrefix(parts[0], "s3://") {
			originalURI = parts[0]
		} else if len(parts) == 1 && numDotRe.MatchString(parts[0]) {
			sourceUID = parts[0]
			originalURI = nbiaLookup[sourceUID]
			if originalURI == "" {
				tciaFallbackUIDs = append(tciaFallbackUIDs, sourceUID)
				continue
			}
		} else {
			continue
		}

		fi, isNew, found := buildS5cmdFileInfo(originalURI, seriesMeta, outputDir, processedSeries, options)
		if !found {
			if sourceUID != "" {
				tciaFallbackUIDs = append(tciaFallbackUIDs, sourceUID)
			} else {
				logger.Warnf("No parquet metadata found for series %s", originalURI)
			}
			continue
		}
		if isNew {
			newJobs++
		}
		jobsToProcess = append(jobsToProcess, fi)
	}

	if err := scanner.Err(); err != nil {
		logger.Fatalf("error reading s5cmd manifest: %v", err)
	}

	if len(tciaFallbackUIDs) == 0 {
		csvPath := filepath.Join(outputDir, "metadata", "metadata.csv")
		if err := WriteAllMetadataToCSV(jobsToProcess, csvPath); err != nil {
			logger.Errorf("Failed to save combined CSV: %v", err)
		} else {
			callbacks.emitStdout(fmt.Sprintf("Saved metadata for %d files to %s\n", len(jobsToProcess), csvPath))
		}
		InitCompletionStatus(outputDir, jobsToProcess)
	}

	logger.Infof("Found %d s5cmd jobs to process (%d new, %d existing); %d UIDs deferred to TCIA",
		len(jobsToProcess), newJobs, len(jobsToProcess)-newJobs, len(tciaFallbackUIDs))
	return jobsToProcess, newJobs, tciaFallbackUIDs
}

// decodeSpreadsheetSplit reads SeriesInstanceUIDs from a CSV/TSV/XLSX file and
// partitions them: UIDs present in the parquet index become s5cmd FileInfos;
// the rest are returned for the caller to feed through the TCIA path. Never
// writes metadata.csv itself — the caller is expected to combine s5cmd and
// TCIA results and write once.
func decodeSpreadsheetSplit(filePath string, outputDir string, processedSeries map[string]string, callbacks Callbacks, options *Options) ([]*FileInfo, int, []string, error) {
	seriesUIDs, err := getSeriesInstanceUIDsFromSpreadsheet(filePath)
	if err != nil {
		return nil, 0, nil, err
	}

	seriesMeta, nbiaLookup := loadParquetMetadata(options)

	var jobsToProcess []*FileInfo
	var tciaFallbackUIDs []string
	var newJobs int
	seen := make(map[string]struct{})
	for _, uid := range seriesUIDs {
		uid = strings.TrimSpace(uid)
		if uid == "" {
			continue
		}
		if _, dup := seen[uid]; dup {
			continue
		}
		seen[uid] = struct{}{}

		originalURI := nbiaLookup[uid]
		if originalURI == "" {
			tciaFallbackUIDs = append(tciaFallbackUIDs, uid)
			continue
		}
		fi, isNew, found := buildS5cmdFileInfo(originalURI, seriesMeta, outputDir, processedSeries, options)
		if !found {
			tciaFallbackUIDs = append(tciaFallbackUIDs, uid)
			continue
		}
		if isNew {
			newJobs++
		}
		jobsToProcess = append(jobsToProcess, fi)
	}

	logger.Infof("Spreadsheet split: %d s5cmd jobs (%d new), %d UIDs deferred to TCIA",
		len(jobsToProcess), newJobs, len(tciaFallbackUIDs))
	return jobsToProcess, newJobs, tciaFallbackUIDs, nil
}
