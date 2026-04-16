package app

import (
	"archive/tar"
	"encoding/json"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"

	"github.com/rs/zerolog/log"
)

// UnTar extracts a tar archive into the destination directory.
func UnTar(dst string, r io.Reader) error {
	tr := tar.NewReader(r)

	for {
		header, err := tr.Next()
		switch {
		case err == io.EOF:
			return nil
		case err != nil:
			return err
		case header == nil:
			continue
		}

		target := filepath.Join(dst, header.Name)

		switch header.Typeflag {
		case tar.TypeDir:
			if _, err := os.Stat(target); err != nil {
				if err := os.MkdirAll(target, 0755); err != nil {
					return err
				}
			}
		case tar.TypeReg:
			f, err := os.OpenFile(target, os.O_CREATE|os.O_RDWR, os.FileMode(header.Mode))
			if err != nil {
				return err
			}

			if _, err := io.Copy(f, tr); err != nil {
				f.Close()
				return err
			}

			if err := f.Close(); err != nil {
				return err
			}
		}
	}
}

// ToJSON writes the provided file infos as JSON to disk.
func ToJSON(files []*FileInfo, output string) {
	rankingsJSON, _ := json.MarshalIndent(files, "", "    ")
	if err := os.WriteFile(output, rankingsJSON, 0644); err != nil {
		log.Error().Msgf("%v", err)
	}
}

// copyFile copies a file from src to dst.
func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()

	if _, err = io.Copy(out, in); err != nil {
		return err
	}
	return out.Close()
}

func WriteAllMetadataToCSV(files []*FileInfo, outPath string) error {
	if len(files) == 0 {
		return nil
	}

	// Open file
	f, err := os.Create(outPath)
	if err != nil {
		return err
	}
	defer f.Close()

	writer := csv.NewWriter(f)
	defer writer.Flush()

	// Build header from struct tags
	fileType := reflect.TypeOf(FileInfo{})
	numFields := fileType.NumField()

	// First pass: determine which columns have at least one non-empty value
	nonEmpty := make([]bool, numFields)
	for _, file := range files {
		v := reflect.ValueOf(file).Elem()
		for i := 0; i < numFields; i++ {
			if !nonEmpty[i] {
				field := v.Field(i)
				if field.Kind() == reflect.String && field.String() != "" {
					nonEmpty[i] = true
				}
			}
		}
	}

	// Build filtered header using only non-empty columns
	header := []string{}
	activeIndices := []int{}
	for i := 0; i < numFields; i++ {
		if nonEmpty[i] {
			field := fileType.Field(i)
			name := field.Tag.Get("csv")
			if name == "" {
				name = field.Name
			}
			header = append(header, name)
			activeIndices = append(activeIndices, i)
		}
	}

	// Write header
	if err := writer.Write(header); err != nil {
		return err
	}

	// Write data rows using only active (non-empty) columns
	for _, file := range files {
		v := reflect.ValueOf(file).Elem()
		row := []string{}
		for _, i := range activeIndices {
			field := v.Field(i)
			if field.Kind() == reflect.String {
				row = append(row, field.String())
			} else {
				row = append(row, "")
			}
		}
		if err := writer.Write(row); err != nil {
			return err
		}
	}

	return nil
}


// writeMetadataToCSV writes/appends a slice of FileInfo structs to a CSV file.
func writeMetadataToCSV(filePath string, fileInfos []*FileInfo) error {
	// Check if file exists to determine if we need to write a header
	stat, err := os.Stat(filePath)
	writeHeader := os.IsNotExist(err)

	// Open with read/write/create permissions.
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("could not open/create CSV file: %w", err)
	}
	defer file.Close()

	// If the file is not new and not empty, check for a trailing newline.
	if !writeHeader && stat.Size() > 0 {
		buf := make([]byte, 1)
		// Read the last byte.
		if _, err := file.ReadAt(buf, stat.Size()-1); err == nil {
			// If it's not a newline, we need to add one.
			if buf[0] != '\n' {
				// Seek to the end and write a newline.
				if _, err := file.Seek(0, io.SeekEnd); err != nil {
					return fmt.Errorf("could not seek to end of file to add newline: %w", err)
				}
				if _, err := file.WriteString("\n"); err != nil {
					return fmt.Errorf("failed to write missing newline: %w", err)
				}
			}
		}
	}

	// Ensure we are at the end of the file before letting the CSV writer take over.
	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("could not seek to end of file for writing: %w", err)
	}

	writer := csv.NewWriter(file)
	defer writer.Flush()

	header := []string{
		"SeriesInstanceUID", "PatientID", "Collection", "Modality",
		"StudyInstanceUID", "SeriesDescription", "SeriesNumber",
		"Manufacturer", "ImageCount", "FileSize", 
		"OriginalS5cmdURI",
	}

	if writeHeader {
		if err := writer.Write(header); err != nil {
			return fmt.Errorf("failed to write CSV header: %w", err)
		}
	}

	// Write rows
	for _, info := range fileInfos {
		record := []string{
			info.SeriesInstanceUID,
			info.Collection,
			info.Modality,
			info.StudyInstanceUID,
			info.SeriesDescription,
			info.SeriesNumber,
			info.Manufacturer,
			info.ImageCount,
			info.FileSize,
			info.OriginalS5cmdURI,
		}
		if err := writer.Write(record); err != nil {
			return fmt.Errorf("failed to write CSV record for series %s: %w", info.SeriesInstanceUID, err)
		}
	}

	return nil
}

// InitCompletionStatus creates the completion status CSV with all series set to "incomplete".
func InitCompletionStatus(outDir string, files []*FileInfo) error {
    statusMu.Lock()
    defer statusMu.Unlock()

    filePath := filepath.Join(outDir, "metadata", "completion_status.csv")

    f, err := os.Create(filePath)
    if err != nil {
        return fmt.Errorf("could not create completion status file: %w", err)
    }
    defer f.Close()

    w := csv.NewWriter(f)
    defer w.Flush()

    if err := w.Write([]string{"SeriesInstanceUID", "completion_status"}); err != nil {
        return fmt.Errorf("failed to write header: %w", err)
    }

    for _, file := range files {
        if err := w.Write([]string{file.SeriesInstanceUID, "incomplete"}); err != nil {
            return fmt.Errorf("failed to write row for %s: %w", file.SeriesInstanceUID, err)
        }
    }

    return nil
}

func AppendCompletionStatus(outDir string, seriesUID string, dlErr error, skipped bool) error {
    statusMu.Lock()
    defer statusMu.Unlock()

    var status string
    switch {
    case skipped:
        status = StatusSkipped
    case dlErr == nil:
        status = StatusSuccess
    default:
        status = fmt.Sprintf("error: %v", dlErr)
    }

    filePath := filepath.Join(outDir, "metadata", "completion_status.csv")

    f, err := os.Open(filePath)
    if err != nil {
        return fmt.Errorf("could not open completion status file: %w", err)
    }
    records, err := csv.NewReader(f).ReadAll()
    f.Close()
    if err != nil {
        return fmt.Errorf("could not read completion status file: %w", err)
    }

    // Find and update the matching row
    for i, row := range records[1:] {
        if len(row) >= 1 && row[0] == seriesUID {
            records[i+1][1] = status
            break
        }
    }

    // Write back atomically
    tmpPath := filePath + ".tmp"
    out, err := os.Create(tmpPath)
    if err != nil {
        return fmt.Errorf("could not create temp file: %w", err)
    }
    w := csv.NewWriter(out)
    if err := w.WriteAll(records); err != nil {
        out.Close()
        os.Remove(tmpPath)
        return fmt.Errorf("could not write completion status file: %w", err)
    }
    w.Flush()
    out.Close()

    return os.Rename(tmpPath, filePath)
}
