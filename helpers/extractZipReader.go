package helpers

import (
	"archive/zip"
	"io"
	"regexp"
	"errors"
)

type ReadCloser struct {
	zipReader *zip.ReadCloser
	fileReader *io.ReadCloser
	Filename string
}

func matchPattern(r *zip.ReadCloser, filePattern *regexp.Regexp) (*zip.File, error) {
	for _, f := range r.File {
		if filePattern.MatchString(f.Name) {
			return f, nil
		}
	}

	return nil, errors.New("File not found in zip, " + filePattern.String())
}

func OpenExtractZipReader(filename string, filePattern *regexp.Regexp) (*ReadCloser, error) {
	r, err := zip.OpenReader(filename)
	if err != nil {
		return nil, err
	}

	file, err := matchPattern(r, filePattern)
	if err != nil {
		return nil, err
	}

	rc, err := file.Open()
	if err != nil {
		return nil, err
	}

	return &ReadCloser{r, &rc, file.Name}, nil
}

func (r *ReadCloser) Close() error {
	fileErr := (*r.fileReader).Close()
	zipErr := (*r.zipReader).Close()

	if fileErr != nil {
		return fileErr
	}

	if zipErr != nil {
		return zipErr
	}

	return nil
}

func (r *ReadCloser) Read(p []byte) (n int, err error) {
	return (*r.fileReader).Read(p)
}