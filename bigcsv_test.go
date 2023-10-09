// This examples shows how we can re-use a parsing function for a particulary
// CSV file.
//
// I picked the "National Walkability Index" from the website:
// https://catalog.data.gov/dataset/?res_format=CSV and after downloading it
// looked at the first 20 lines.
//
// Examples here show using a single parsing function for different purposes, as
// well as the use of context to cancel. The TestFileStream will only run if you
// download the CSV file and set an environment variable.
package bigcsv_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/calloway-jacob/bigcsv"
)

// https://www.epa.gov/smartgrowth/national-walkability-index-user-guide-and-methodology

const CSV_URL = "https://edg.epa.gov/EPADataCommons/public/OA/EPA_SmartLocationDatabase_V3_Jan_2021_Final.csv"

// A simple struct with part of the data from the CSV.
type Place struct {
	Name        string
	Population  int
	Walkability float64
	Area        float64
}

func ParsePlace(row []string) (Place, error) {
	var err error
	var e error
	place := Place{}
	if len(row) < 117 {
		return place, fmt.Errorf("got %d columns, need 117 at least", len(row))
	}
	place.Name = row[10]
	if place.Population, e = strconv.Atoi(row[18]); e != nil {
		err = errors.Join(err, e)
	}
	if place.Walkability, e = strconv.ParseFloat(row[114], 64); e != nil {
		err = errors.Join(err, e)
	}
	if place.Area, e = strconv.ParseFloat(row[116], 64); e != nil {
		err = errors.Join(err, e)
	}
	return place, err
}

// TestHTTPStream tests that the CSV parsing works with the HTTP stream.
func TestHTTPStream(t *testing.T) {
	parser, err := bigcsv.New[Place](bigcsv.HTTPStream(CSV_URL))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	processedRows := &atomic.Int32{}
	// Data parsing function.
	parser.Parse = ParsePlace
	// Handling parsed data, in this case just logging it.
	parser.OnData = func(p Place) error {
		t.Logf(
			"Place '%s', pop. %d, area %.1f, walkability %.1f\n",
			p.Name, p.Population, p.Area, p.Walkability,
		)
		if processedRows.Add(1) > 100 {
			cancel()
		}
		return nil
	}
	// Handle parsing / data errors.
	parser.OnError = func(err error) {
		t.Error(err)
	}
	// Ignore CSV headers (first line).
	headers, err := parser.Reader.Read()
	if err != nil {
		t.Fatal(err)
	}
	if len(headers) < 117 {
		t.Fatal(fmt.Errorf("CSV did not have enough headers"))
	}
	// Run the parser with 5 parallel workers. Note: this is for demonstration,
	// it's unlikely that workers will speed things up for HTTP streams.
	if err = parser.Run(ctx, 5); err != nil {
		t.Fatal(err)
	}
}

// TestFileStream tests that the CSV parsing works with FileStream. Test will
// only run when setting environment variable BIGCSV_FILE, which is the path to
// the CSV (or gzipped CSV) downloaded from CSV_URL above.
func TestFileStream(t *testing.T) {
	filename, ok := os.LookupEnv("BIGCSV_FILE")
	if !ok {
		t.Skipf("No environment var BIGCSV_FILE set")
	}
	parser, err := bigcsv.New[Place](bigcsv.FileStream(filename))
	if err != nil {
		t.Fatal(err)
	}
	parser.Parse = ParsePlace
	processedRows := &atomic.Int32{}
	parser.OnData = func(p Place) error {
		processedRows.Add(1)
		return nil
	}
	parser.OnError = func(err error) {
		t.Fatal(err)
	}
	// Ignore CSV headers (first line).
	if _, err = parser.Reader.Read(); err != nil {
		t.Fatal(err)
	}
	if err = parser.Run(context.Background(), 10); err != nil {
		t.Fatal(err)
	}
	t.Logf("Parsed and processed %d rows", processedRows.Load())
}
