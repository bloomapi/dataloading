package helpers

import (
	"io"
	"errors"
	"archive/zip"
	"bitbucket.org/gocodo/bloomsource"
	"github.com/tealeg/xlsx"
)

type XlxsReader struct {
	sheet *xlsx.Sheet
	headers map[string]int
	currentRow int
}

type XlxsRow struct {
	reader *XlxsReader
	cells []*xlsx.Cell
}

func NewXlxsReader(file *zip.Reader, sheetName string) (*XlxsReader, error) {
	var selectedSheet *xlsx.Sheet

	xlFile, err := xlsx.ReadZipReader(file)
	if err != nil {
		return nil, err
	}

	for _, sheet := range xlFile.Sheets {
		if sheet.Name == sheetName {
			selectedSheet = sheet
			break
		}
	}

	if selectedSheet == nil {
		return nil, errors.New("Unable to find sheet " + sheetName)
	}

	row := selectedSheet.Rows[0]
	headers := map[string]int{}
	for i, cell := range row.Cells {
		headers[cell.Value] = i
	}

	return &XlxsReader{
		sheet: selectedSheet,
		headers: headers,
		currentRow: 1,
	}, nil
}

func (r *XlxsReader) Headers() []string {
	headers := make([]string, len(r.headers))
	i := 0
	for key, _ := range r.headers {
		headers[i] = key
		i += 1
	}

	return headers
}

func (r *XlxsReader) Read() (bloomsource.Valuable, error) {
	if r.currentRow >= len(r.sheet.Rows) {
		return nil, io.EOF
	}

	row := r.sheet.Rows[r.currentRow]
	r.currentRow += 1

	return &XlxsRow{
		reader: r,
		cells: row.Cells,
	}, nil
}

func (r *XlxsRow) Value(index string) (string, bool) {
	rowIndex, ok := r.reader.headers[index]
	return r.cells[rowIndex].Value, ok
}
