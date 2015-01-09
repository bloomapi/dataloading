package helpers

import (
	"encoding/csv"
	"io"
	"github.com/gocodo/bloomsource"
)

type Reader struct {
	reader  *csv.Reader
	headers map[string]int
}

type Row struct {
	reader *Reader
	record []string
}

func NewCsvReaderNoHeaders(r io.Reader, headers []string) *Reader {
	mappedHeaders := map[string]int{}
	for index, header := range headers {
		mappedHeaders[header] = index
	}

	return &Reader{
		reader:  csv.NewReader(r),
		headers: mappedHeaders,
	}
}

func NewCsvReader(r io.Reader) *Reader {
	return &Reader{
		reader:  csv.NewReader(r),
		headers: make(map[string]int),
	}	
}

func (r *Reader) Read() (bloomsource.Valuable, error) {
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

	return &Row{
		reader: r,
		record: row,
	}, nil
}

func (r *Row) Value(index string) string {
	rowIndex := r.reader.headers[index]
	return r.record[rowIndex]
}
