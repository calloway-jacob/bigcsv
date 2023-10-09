package bigcsv

import (
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

// Stream is the interface which provides a CSV file to process.
type Stream interface {
	Open() (io.ReadCloser, error)
}

// HTTPStream provides a reader for the CSV stream directly via HTTP(s).
type HTTPStream string

func (hs HTTPStream) Open() (io.ReadCloser, error) {
	req, err := http.NewRequest("GET", string(hs), nil)
	if err != nil {
		return nil, fmt.Errorf("could not create request: %w", err)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("could not request: %w", err)
	}
	r := res.Body

	// Detect gzip
	if strings.Contains(res.Header.Get("content-type"), "gzip") {
		r, err = gzip.NewReader(res.Body)
		if err != nil {
			return nil, fmt.Errorf("could not read gzip body: %w", err)
		}
	}
	return r, nil
}

// FileStream provides a reader for CSV processing from the filesystem.
//
// FileStream will only
type FileStream string

func (fs FileStream) Open() (io.ReadCloser, error) {
	var r io.ReadCloser
	var err error
	ext := strings.ToLower(filepath.Ext(string(fs)))

	r, err = os.Open(string(fs))
	if err != nil {
		return nil, fmt.Errorf("could open file '%s': %w", fs, err)
	}

	// Detect gzip in filename.
	if ext == ".gz" {
		gz, err := gzip.NewReader(r)
		if err != nil {
			r.Close()
			return nil, fmt.Errorf("gzip failed for '%s': %w", fs, err)
		}
		r = gz
	}
	return r, nil
}
