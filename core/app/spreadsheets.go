package app

import (
	"encoding/csv"
	"fmt"
	"github.com/tealeg/xlsx"
	"os"
	"path/filepath"
	"strings"
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
	// Accept ragged records so a single row with an extra trailing delimiter
	// does not fail parsing for the entire manifest.
	reader.FieldsPerRecord = -1
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
	sizeIndex, md5Index, sha256Index := -1, -1, -1
	metadataColumns := make(map[string]int)
	for i, col := range header {
		metadataColumns[normalize(col)] = i
		switch normalize(col) {
		case "drsuri", "fileid", "access", "guid", "fileguid", "drsguid":
			drsURIIndex = i
		case "imageurl", "wsiimageurl":
			imageURLIndex = i
		case "name", "filename":
			nameIndex = i
		case "collection", "collectionname":
			collectionIndex = i
		case "patient", "patientid", "subject", "subjectid", "participantid":
			patientIdIndex = i
		case "studyuid", "studyid":
			studyIdIndex = i
		case "studydescription", "studydesc", "studyshortname", "studyname":
			studyDescIndex = i
		case "filesize", "filesizebytes", "sizebytes", "ctdcsize", "size":
			sizeIndex = i
		case "md5", "md5sum", "md5checksum":
			md5Index = i
		case "sha256", "sha256sum", "sha256checksum":
			sha256Index = i
		case "studydate":
			studyDateIndex = i
		}
	}

	if drsURIIndex == -1 && imageURLIndex == -1 {
		return nil, fmt.Errorf("no 'drs_uri', 'imageUrl', 'SeriesInstanceUID', or 'Series UID' column found in %s", file.Name())
	}

	var fileInfos []*FileInfo
	for _, record := range records[1:] {
		value := func(index int) string {
			if index >= 0 && index < len(record) {
				return strings.TrimSpace(record[index])
			}
			return ""
		}
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
					DRSURI:            uri,
					SeriesInstanceUID: strings.TrimSuffix(base, ext),
					FileName:          fileName,
					FileSize:          value(sizeIndex),
					MD5:               value(md5Index),
					SHA256:            value(sha256Index),
					Collection:        collection,
					PatientID:         patientId,
					StudyID:           studyId,
					StudyDesc:         studyDesc,
					StudyDate:         studyDate,
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
					DownloadURL:       url,
					SeriesInstanceUID: strings.TrimSuffix(base, ext),
					FileName:          fileName,
					FileSize:          value(sizeIndex),
					MD5:               value(md5Index),
					SHA256:            value(sha256Index),
					Collection:        collection,
					PatientID:         patientId,
					StudyID:           studyId,
					StudyDesc:         studyDesc,
					StudyDate:         studyDate,
				})
			}
		}
	}

	// Preserve descriptive metadata independently of the retrieval route. CSV,
	// TSV and XLSX all use this path; missing/ragged cells remain empty.
	fileIndex := 0
	for _, record := range records[1:] {
		routeIndex := imageURLIndex
		if drsURIIndex >= 0 {
			routeIndex = drsURIIndex
		}
		if routeIndex < 0 || routeIndex >= len(record) {
			continue
		}
		file := fileInfos[fileIndex]
		fileIndex++
		applySpreadsheetMetadata(file, metadataColumns, record)
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

func applySpreadsheetMetadata(file *FileInfo, columns map[string]int, record []string) {
	value := func(names ...string) string {
		for _, name := range names {
			if index, ok := columns[normalize(name)]; ok && index < len(record) && strings.TrimSpace(record[index]) != "" {
				return strings.TrimSpace(record[index])
			}
		}
		return ""
	}
	file.LicenseName = value("LicenseName", "license_label")
	file.LicenseURI = value("LicenseURI", "license_url", "license_uri")
	file.LicenseShortName = value("license_short_name")
	if license := value("license"); license != "" {
		if strings.HasPrefix(strings.ToLower(license), "https://") || strings.HasPrefix(strings.ToLower(license), "http://") {
			setCroissantStringIfEmpty(&file.LicenseURI, license)
		} else if file.LicenseName == "" && file.LicenseShortName == "" {
			file.LicenseName = license
		}
	}
	file.SourceDOI = value("source_DOI")
	file.DatasetDOI = value("DatasetDOI", "dataset_doi", "doi")
	file.DatasetCitation = value("DatasetCitation", "citeAs")
	file.DataDescriptionURI = value("DataDescriptionURI", "dataset_url")
	file.WordPressDownloadID = value("WordPressDownloadID", "download_id", "wordpress_download_id")
	file.ManifestURL = value("ManifestURL", "manifest_url")
	if file.LicenseName != "" || file.LicenseURI != "" || file.LicenseShortName != "" {
		file.LicenseSource = "Manifest row"
	}
}

// seriesManifestMetadata also handles UID-only spreadsheets, whose payload
// metadata comes from IDC/NBIA rather than decodeSpreadsheet.
func seriesManifestMetadata(path string) map[string]*FileInfo {
	decoder, err := getSpreadsheetDecoder(path)
	if err != nil {
		return nil
	}
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()
	rows, err := decoder.Decode(f)
	if err != nil || len(rows) == 0 {
		return nil
	}
	columns := make(map[string]int)
	for i, col := range rows[0] {
		columns[normalize(col)] = i
	}
	index, ok := columns["seriesinstanceuid"]
	if !ok {
		index, ok = columns["seriesuid"]
	}
	if !ok {
		return nil
	}
	result := make(map[string]*FileInfo)
	for _, row := range rows[1:] {
		if index >= len(row) || strings.TrimSpace(row[index]) == "" {
			continue
		}
		file := &FileInfo{}
		applySpreadsheetMetadata(file, columns, row)
		result[strings.TrimSpace(row[index])] = file
	}
	return result
}

func applySeriesManifestMetadata(file *FileInfo, metadata map[string]*FileInfo) {
	if file == nil {
		return
	}
	row := metadata[file.SeriesInstanceUID]
	if row == nil {
		return
	}
	applyLicenseMetadata(file, row.LicenseName, row.LicenseURI, "Manifest row")
	if file.LicenseName == "" && file.LicenseURI == "" {
		setCroissantStringIfEmpty(&file.LicenseShortName, row.LicenseShortName)
	}
	setCroissantStringIfEmpty(&file.SourceDOI, row.SourceDOI)
	setCroissantStringIfEmpty(&file.DatasetDOI, row.DatasetDOI)
	setCroissantStringIfEmpty(&file.DatasetCitation, row.DatasetCitation)
	setCroissantStringIfEmpty(&file.DataDescriptionURI, row.DataDescriptionURI)
	setCroissantStringIfEmpty(&file.WordPressDownloadID, row.WordPressDownloadID)
	setCroissantStringIfEmpty(&file.ManifestURL, row.ManifestURL)
}
