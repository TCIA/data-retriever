package app

import (
	"archive/tar"
	"encoding/json"
	"io"
	"os"
	"path/filepath"

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
