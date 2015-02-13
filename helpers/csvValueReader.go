package helpers

import (
	"encoding/csv"
	"io"
	"github.com/gocodo/bloomsource"
)

type CsvReader struct {
	reader  *csv.Reader
	headers map[string]int
}

type CsvRow struct {
	reader *CsvReader
	record []string
}

func NewCsvReaderNoHeaders(r io.Reader, headers []string) *CsvReader {
	mappedHeaders := map[string]int{}
	for index, header := range headers {
		mappedHeaders[header] = index
	}

	return &CsvReader{
		reader:  csv.NewReader(r),
		headers: mappedHeaders,
	}
}

func NewCsvReader(r io.Reader) *CsvReader {
	return &CsvReader{
		reader:  csv.NewReader(r),
		headers: make(map[string]int),
	}	
}

func (r *CsvReader) Read() (bloomsource.Valuable, error) {
	if len(r.headers) == 0 {
		row, err := r.reader.Read()
		if err != nil {
			return nil, err
		}

		for index, value := range row {
			r.headers[value] = index
		}
	}

	row, err := r.reader.Read()
	if err != nil {
		return nil, err
	}

	return &CsvRow{
		reader: r,
		record: row,
	}, nil
}

func (r *CsvRow) Value(index string) (string, bool) {
	rowIndex, ok := r.reader.headers[index]
	return r.record[rowIndex], ok
}
