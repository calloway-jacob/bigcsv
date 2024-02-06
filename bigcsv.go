package bigcsv

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"sync"
)

// ErrOnRow is passed to OnError when OnRow returns an error.
var ErrOnRow = errors.New("OnRow error")

// ErrParse is passed to OnError when Parse returns an error.
var ErrParse = errors.New("Parse error")

// ErrOnData is passed to OnError when OnData returns an error.
var ErrOnData = errors.New("OnData error")

// Parser provides streaming CSV parsing. It must be created with New.
type Parser[T any] struct {
	// closer is kept from the Stream.Open() to close after processing.
	closer io.Closer

	// Reader is the CSV reader which can be modified prior to processing.
	//
	// To change CSV settings, use the Reader directly after creating the Parser
	// and prior to calling Run
	Reader *csv.Reader

	// OnRow accepts a CSV row prior to parsing.
	//
	// If an error is returned, the OnError function is called and the row is
	// skipped.
	OnRow func(row []string) error

	// Parse should parse the raw row from the CSV and return the data type.
	Parse func(row []string) (T, error)

	// OnData accepts a processed CSV row as a Report.
	//
	// The return value signals whether to stop ALL further processing. Note
	// that when using multiple workers, already started workers will continue
	// to process their row until this signal is received.
	OnData func(data T) error

	// OnError handles errors arising during processing.
	//
	// If the Parse method returns an error, this method will receive it.
	// Other, errors from the underlying *csv.Reader will be passed here, too.
	OnError func(error)
}

// NewParser opens the given stream and starts the CSV reader.
func New[T any](stream Stream) (*Parser[T], error) {
	// Open our CSV stream.
	r, err := stream.Open()
	if err != nil {
		return nil, fmt.Errorf("could not open stream: %w", err)
	}

	// Create the CSV reader.
	return &Parser[T]{
		closer: r,
		Reader: csv.NewReader(r),
	}, nil
}

// Run begins parsing the CSV records, invoking the configured functions.
//
// This method will not return until all workers have finished processing.
func (p *Parser[T]) Run(ctx context.Context, workers int) error {
	defer p.closer.Close()
	if p.OnData != nil && p.Parse == nil {
		return fmt.Errorf("cannot call OnData without Parse")
	}
	if workers < 1 {
		return fmt.Errorf("invalid number of workers: %d", workers)
	}

	// It is safe to reuse records with 1 worker.
	p.Reader.ReuseRecord = workers == 1

	wg := &sync.WaitGroup{}
	sem := make(chan struct{}, workers)

LoopOverRows:
	for ixRow := 1; ixRow > 0; ixRow++ { // NOTE: breaks on EOF intentionally
		// Check each iteration whether the parser has been stopped.
		select {
		case <-ctx.Done():
			break LoopOverRows
		case sem <- struct{}{}:
			row, err := p.Reader.Read()
			if errors.Is(err, io.EOF) {
				break LoopOverRows
			} else if err != nil {
				if p.OnError != nil {
					p.OnError(fmt.Errorf("could not read line #%d: %w", ixRow, err))
				}
				<-sem
				continue LoopOverRows
			}

			wg.Add(1)
			go p.processRow(wg, sem, ixRow, row)
		}
	}
	wg.Wait()
	return nil
}

// processRow handles a single row according to parser settings.
func (p *Parser[T]) processRow(wg *sync.WaitGroup, sem <-chan struct{}, ix int, row []string) {
	defer func() {
		<-sem
		wg.Done()
	}()

	// Hook for raw row processing.
	if p.OnRow != nil {
		if err := p.OnRow(row); err != nil {
			if p.OnError != nil {
				p.OnError(fmt.Errorf("%w: line %d: %w", ErrOnRow, ix, err))
			}
			return
		}
	}

	// Bail early if only dealing with raw rows.
	if p.Parse == nil { // nil Parse implies nil OnData
		return
	}

	data, err := p.Parse(row)
	if err != nil {
		if p.OnError != nil {
			p.OnError(fmt.Errorf("%w: line %d: %w", ErrParse, ix, err))
		}
		return
	}

	// OnData handler.
	if p.OnData == nil {
		return
	}

	if err = p.OnData(data); err != nil {
		if p.OnError != nil {
			p.OnError(fmt.Errorf("%w: line %d: %w", ErrOnData, ix, err))
		}
	}
}
