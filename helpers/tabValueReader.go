package helpers

import (
  "encoding/csv"
  "io"
  "github.com/bloomapi/dataloading"
)

type CsvTabReader struct {
  reader  *csv.Reader
  headers map[string]int
}

type CsvTabRow struct {
  reader *CsvTabReader
  record []string
}

func NewCsvTabReaderNoHeaders(r io.Reader, headers []string) *CsvTabReader {
  mappedHeaders := map[string]int{}
  for index, header := range headers {
    mappedHeaders[header] = index
  }

  reader := CsvTabReader{
    reader:  csv.NewReader(r),
    headers: mappedHeaders,
  }

  reader.reader.Comma = '\t'

  return &reader
}

func NewCsvTabReader(r io.Reader) *CsvTabReader {
  reader := CsvTabReader{
    reader:  csv.NewReader(r),
    headers: make(map[string]int),
  }

  reader.reader.Comma = '\t'

  return &reader
}

func (r *CsvTabReader) Read() (dataloading.Valuable, error) {
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

  return &CsvTabRow{
    reader: r,
    record: row,
  }, nil
}

func (r *CsvTabRow) Value(index string) (string, bool) {
  rowIndex, ok := r.reader.headers[index]
  return r.record[rowIndex], ok
}
