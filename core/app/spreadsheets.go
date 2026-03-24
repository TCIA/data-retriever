package app

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"github.com/tealeg/xlsx"
	"unicode"
)

// SpreadSheetDecoder defines behaviour for decoding spreadsheet files.
type SpreadSheetDecoder interface {
	Decode(file *os.File) ([][]string, error)
}

// CSVDecoder decodes CSV files.
type CSVDecoder struct{}

// TSVDecoder decodes TSV files.
type TSVDecoder struct{}

// XLSXDecoder decodes XLSX files.
type XLSXDecoder struct{}

// Decode decodes a CSV file and returns the values from the "imageUrl" or "drs_uri" column
func (d *CSVDecoder) Decode(file *os.File) ([][]string, error) {
	return decodesv(file, ',')
}

// Decode decodes a TSV file and returns the values from the "imageUrl" or "drs_uri" column
func (d *TSVDecoder) Decode(file *os.File) ([][]string, error) {
	return decodesv(file, '\t')
}

// decodesv decodes a separated value file and returns the values from the "imageUrl" or "drs_uri" column
func decodesv(file *os.File, separator rune) ([][]string, error) {
	reader := csv.NewReader(file)
	reader.Comma = separator
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	return records, nil
}

// Decode decodes an XLSX file and returns the values from the "imageUrl" or "drs_uri" column
func (d *XLSXDecoder) Decode(file *os.File) ([][]string, error) {
	stat, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("could not get file stats: %w", err)
	}
	size := stat.Size()
	xlFile, err := xlsx.OpenReaderAt(file, size)
	if err != nil {
		return nil, err
	}

	var records [][]string
	for _, sheet := range xlFile.Sheets {
		for _, row := range sheet.Rows {
			var record []string
			for _, cell := range row.Cells {
				record = append(record, cell.String())
			}
			records = append(records, record)
		}
	}


	return records, nil
}

func getSpreadsheetDecoder(filename string) (SpreadSheetDecoder, error) {
	ext := strings.ToLower(filepath.Ext(filename))
	switch ext {
	case ".csv":
		return &CSVDecoder{}, nil
	case ".tsv":
		return &TSVDecoder{}, nil
	case ".xlsx":
		return &XLSXDecoder{}, nil
	default:
		return nil, fmt.Errorf("unsupported spreadsheet format: %s", ext)
	}
}

func normalize(s string) string {
    s = strings.TrimSpace(s)
    s = strings.ToLower(s)

    // remove non letters/numbers
    var b strings.Builder
    for _, r := range s {
        if unicode.IsLetter(r) || unicode.IsDigit(r) {
            b.WriteRune(r)
        }
    }
    return b.String()
}

func decodeSpreadsheet(filePath string) ([]*FileInfo, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	decoder, err := getSpreadsheetDecoder(filePath)
	if err != nil {
		return nil, err
	}

	records, err := decoder.Decode(file)
	if err != nil {
		return nil, err
	}

	if len(records) == 0 {
		return []*FileInfo{}, nil
	}

	header := records[0]
	drsURIIndex := -1
	imageURLIndex := -1
	nameIndex := -1
	collectionIndex := -1
	patientIdIndex := -1
	studyIdIndex := -1
	studyDateIndex := -1
	studyDescIndex := -1
	for i, col := range header {
		switch normalize(col) {
		case "drsuri", "fileid":
			drsURIIndex = i
		case "imageurl","wsiimageurl" :
			imageURLIndex = i
		case "name", "filename":
			nameIndex = i
		case "collection", "collectionname":
			collectionIndex = i
		case "patient", "patientid", "subject", "subjectid":
			patientIdIndex = i
		case "studyuid", "studyid":
			studyIdIndex = i
		case "studydescription", "studydesc", "studyshortname":
			studyDescIndex = i
		case "studydate":
			studyDateIndex = i
		}
	}

	if drsURIIndex == -1 && imageURLIndex == -1 {
		return nil, fmt.Errorf("no 'drs_uri', 'imageUrl', 'SeriesInstanceUID', or 'Series UID' column found in %s", file.Name())
	}

	var fileInfos []*FileInfo
		for _, record := range records[1:] {
		var fileName string
		var collection string
		var patientId string
		var studyId string
		var studyDesc string
		var studyDate string
		if nameIndex != -1 && len(record) > nameIndex {
			fileName = record[nameIndex]
		}
		if collectionIndex != -1 && len(record) > collectionIndex {
			collection = record[collectionIndex]
		}
		if patientIdIndex != -1 && len(record) > patientIdIndex {
			patientId = record[patientIdIndex]
		}
		if studyIdIndex != -1 && len(record) > studyIdIndex {
			studyId = record[studyIdIndex]
		}
		if studyDescIndex != -1 && len(record) > studyDescIndex {
			studyDesc = record[studyDescIndex]
		}
		if studyDateIndex != -1 && len(record) > studyDateIndex {
			studyDate = record[studyDateIndex]
		}

		if drsURIIndex != -1 {
			if len(record) > drsURIIndex {
				uri := record[drsURIIndex]
				if !strings.HasPrefix(uri, "drs:") {
			    uri = "drs://nci-crdc.datacommons.io/" + uri
				}
				if fileName == "" {
					fileName = filepath.Base(uri)
				}
				base := filepath.Base(uri)
				ext := filepath.Ext(base)
				fileInfos = append(fileInfos, &FileInfo{
					DRSURI:    uri,
					SeriesInstanceUID:  strings.TrimSuffix(base, ext), 
					FileName:  fileName,
					Collection: collection,
					PatientID: patientId,
					StudyID: studyId,
					StudyDesc: studyDesc,
					StudyDate: studyDate,
				})
			}
		} else {
			if len(record) > imageURLIndex {
				url := record[imageURLIndex]
				if fileName == "" {
					fileName = filepath.Base(url)
				}
				base := filepath.Base(url)
				ext := filepath.Ext(base)
				fileInfos = append(fileInfos, &FileInfo{
					DownloadURL: url,
					SeriesInstanceUID:  strings.TrimSuffix(base, ext), 
					FileName:    fileName,
					Collection: collection,
					PatientID: patientId,
					StudyID: studyId,
					StudyDesc: studyDesc,
					StudyDate: studyDate,
				})
			}
		}
	}

	return fileInfos, nil
}


var ErrSeriesInstanceUIDColumnNotFound = fmt.Errorf("no 'SeriesInstanceUID' column found")

// getSeriesInstanceUIDsFromSpreadsheet extracts a list of SeriesInstanceUIDs from a spreadsheet
func getSeriesInstanceUIDsFromSpreadsheet(filePath string) ([]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	decoder, err := getSpreadsheetDecoder(filePath)
	if err != nil {
		return nil, err
	}

	records, err := decoder.Decode(file)
	if err != nil {
		return nil, err
	}

	if len(records) == 0 {
		return []string{}, nil
	}

	header := records[0]
	seriesInstanceUIDIndex := -1
	for i, col := range header {
		if col == "SeriesInstanceUID" || col == "Series UID" {
			seriesInstanceUIDIndex = i
			break
		}
	}

	if seriesInstanceUIDIndex == -1 {
		return nil, ErrSeriesInstanceUIDColumnNotFound
	}

	var seriesUIDs []string
	for _, record := range records[1:] {
		if len(record) > seriesInstanceUIDIndex {
			seriesUIDs = append(seriesUIDs, record[seriesInstanceUIDIndex])
		}
	}

	return seriesUIDs, nil
}
