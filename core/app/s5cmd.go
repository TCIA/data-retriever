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
)

type SeriesMetadata struct {
    SeriesInstanceUID   string
		series_aws_url			string
		series_size_MB			float64
		collection_id				string
		PatientID						string
		StudyInstanceUID		string
}

//go:embed parquet/idc_index.parquet
var parquetFS embed.FS


func loadSeriesMetadataFromParquet() (map[string]SeriesMetadata, map[string]string, error) {
		f, err := parquetFS.Open("parquet/idc_index.parquet")
		if err != nil {
		    return nil, nil, fmt.Errorf("failed to open embedded parquet: %w", err)
		}
		defer f.Close()
		
		stat, err := f.Stat()
		if err != nil {
		    return nil, nil, fmt.Errorf("failed to stat parquet file: %w", err)
		}
		
		readerAt, ok := f.(io.ReaderAt)
		if !ok {
		    return nil, nil, fmt.Errorf("embedded file does not implement ReaderAt")
		}
		
		section := io.NewSectionReader(readerAt, 0, stat.Size())
		
		pqReader, err := file.NewParquetReader(section)
		if err != nil {
		    return nil, nil, fmt.Errorf("failed to create parquet reader: %w", err)
		}
		defer pqReader.Close()

logger.Warnf("num row groups: %d", pqReader.NumRowGroups())

for i := 0; i < pqReader.NumRowGroups(); i++ {
    rg := pqReader.RowGroup(i)
    logger.Warnf("row group %d: num rows = %d, num columns = %d", i, rg.NumRows(), rg.NumColumns())
}
		
		mem := memory.NewGoAllocator()
	
		props := pqarrow.ArrowReadProperties{
		    BatchSize: 8192, // THIS is where batch size goes
		}
		
		arrowReader, err := pqarrow.NewFileReader(pqReader, props, mem)
		if err != nil {
		    return nil, nil, fmt.Errorf("failed to create Arrow reader: %w", err)
		}
		
		recReader, err := arrowReader.GetRecordReader(
		    context.Background(),
		    nil, // all columns
		    nil, // all row groups
		)
		
		if err != nil {
		    return nil, nil, fmt.Errorf("failed to get record reader: %w", err)
		}
		defer recReader.Release()

		logger.Warnf("record reader schema: %v", recReader.Schema())
		schema := recReader.Schema()
for _, f := range schema.Fields() {
    logger.Warnf("Column: %s, Type: %v", f.Name, f.Type)
}

    meta := make(map[string]SeriesMetadata)
		metaFromSeries := make(map[string]string)

		logger.Warnf("parquet row groups: %d", pqReader.NumRowGroups())
    for recReader.Next() {
        rec := recReader.Record()
				//logger.Warnf("reading record")

        // Get column indices
        uidIdxs := rec.Schema().FieldIndices("SeriesInstanceUID")
        urlIdxs := rec.Schema().FieldIndices("series_aws_url")
        patientIDIdxs := rec.Schema().FieldIndices("PatientID")
        studyUIDIdxs := rec.Schema().FieldIndices("StudyInstanceUID")
        collectionIDIdxs := rec.Schema().FieldIndices("collection_id")
        fileSizeIdxs := rec.Schema().FieldIndices("series_size_MB")
        if len(uidIdxs) == 0 || len(urlIdxs) == 0 {
            return nil, nil, fmt.Errorf("required columns not found in parquet")
        }

        uidCol := rec.Column(uidIdxs[0]).(*array.String)
        urlCol := rec.Column(urlIdxs[0]).(*array.String)
        fileSizeCol := rec.Column(fileSizeIdxs[0]).(*array.Float64)
        patientIDCol := rec.Column(patientIDIdxs[0]).(*array.String)
        studyUIDCol := rec.Column(studyUIDIdxs[0]).(*array.String)
        collectionIDCol := rec.Column(collectionIDIdxs[0]).(*array.String)

        rows := int(rec.NumRows())
        for i := 0; i < rows; i++ {
            if uidCol.IsNull(i) || urlCol.IsNull(i) {
                continue
            }
            uid := uidCol.Value(i)
            url := urlCol.Value(i)
            fileSize := fileSizeCol.Value(i)
						patientID := patientIDCol.Value(i)
						studyUID := studyUIDCol.Value(i)
						collectionID := collectionIDCol.Value(i)

            if _, exists := meta[url]; exists {
                continue
            }

						//logger.Warnf("original url: %s", url)
            meta[url] = SeriesMetadata{
                SeriesInstanceUID: uid,
                series_aws_url:    url,
								series_size_MB:		 fileSize,
								PatientID:				 patientID,
								StudyInstanceUID:	 studyUID,
								collection_id:		 collectionID,
            }
						metaFromSeries[uid] = url
        }
				rec.Release()
    }

		if err := recReader.Err(); err != nil && err != io.EOF {
		    return nil, nil, fmt.Errorf("error reading Parquet records: %w", err)
		}

    return meta, metaFromSeries, nil
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
		if file.IsDir() || !strings.HasSuffix(file.Name(), "-metadata.csv") {
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

func decodeS5cmd(filePath string, outputDir string, processedSeries map[string]string, callbacks Callbacks) ([]*FileInfo, int) {
	file, err := os.Open(filePath)
	if err != nil {
		logger.Fatalf("could not open s5cmd manifest: %v", err)
	}
	defer file.Close()



  seriesMeta, nbiaLookup, err := loadSeriesMetadataFromParquet()
  if err != nil {
      logger.Fatalf("Failed to load parquet metadata: %v", err)
  }

	var jobsToProcess []*FileInfo
	var newJobs int
	var numDotRe = regexp.MustCompile(`^[0-9][0-9.]*$`)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Fields(line)
		var originalURI string
		if len(parts) >= 2 && parts[0] == "cp" {
			originalURI = parts[1]
		} else if len(parts) == 1 && strings.HasPrefix(parts[0], "s3://") {
			originalURI = parts[0]
		} else if len(parts) == 1 && numDotRe.MatchString(parts[0]) {
			originalURI = nbiaLookup[parts[0]]
		} else {
			continue // Skip comments and invalid lines
		}

		if seriesUID, ok := processedSeries[originalURI]; ok {
			// This is a sync job for an existing series
			logger.Infof("Queueing sync job for existing series: %s", originalURI)
			finalDirPath := filepath.Join(outputDir, seriesUID)
			jobsToProcess = append(jobsToProcess, &FileInfo{
				DownloadURL:      originalURI,
				SeriesInstanceUID:        seriesUID, // We already know the final UID
				OriginalS5cmdURI: originalURI,
				S5cmdManifestPath: finalDirPath, // The final directory is the target for sync
				IsSyncJob:        true,
			})
		} else {
			// This is a new copy job
			newJobs++
			logger.Infof("Queueing new copy job for series: %s", originalURI)

			fi := &FileInfo{
				DownloadURL:      originalURI,
				SeriesInstanceUID:        originalURI, // Temporary ID for progress
				OriginalS5cmdURI: originalURI,
				IsSyncJob:        false,
			}
			//  Attach Parquet metadata if available
			if meta, ok := seriesMeta[originalURI]; ok {
			    fi.SeriesInstanceUID= meta.SeriesInstanceUID
					fi.FileSize = strconv.FormatInt(
											    int64(meta.series_size_MB*1024*1024),
											    10,
												)
					fi.PatientID = meta.PatientID
					fi.StudyInstanceUID = meta.StudyInstanceUID
					fi.Collection = meta.collection_id
			} else {
			    logger.Warnf("No parquet metadata found for series %s", originalURI)
			}

			finalDirPath := filepath.Join(outputDir, fi.Collection, fi.PatientID, fi.StudyInstanceUID, fi.SeriesInstanceUID)

			if err := os.MkdirAll(finalDirPath, 0755); err != nil {
				logger.Warnf("Could not create temp directory for %s: %v", originalURI, err)
				continue
			}

			fi.S5cmdManifestPath = finalDirPath
			jobsToProcess = append(jobsToProcess, fi)

		}
	}

	if err := scanner.Err(); err != nil {
		logger.Fatalf("error reading s5cmd manifest: %v", err)
	}


	// Save all to a single CSV file
	csvPath := filepath.Join(outputDir, "metadata.csv")
	if err := WriteAllMetadataToCSV(jobsToProcess, csvPath); err != nil {
		logger.Errorf("Failed to save combined CSV: %v", err)
	} else {
		callbacks.emitStdout(fmt.Sprintf("Saved metadata for %d files to %s\n", len(jobsToProcess), csvPath))
	}

	logger.Infof("Found %d s5cmd jobs to process (%d new, %d existing)", len(jobsToProcess), newJobs, len(jobsToProcess)-newJobs)
	return jobsToProcess, newJobs
}
