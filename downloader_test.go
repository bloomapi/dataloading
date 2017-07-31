package dataloading

import (
	"strings"
	"os"
	"io"
	"io/ioutil"
	"testing"
	"github.com/bloomapi/dataloading/tests"
)

type mockReaderCloser struct {
	reader *strings.Reader
}

var mockReader = &mockReaderCloser{
	strings.NewReader("Hello World"),
}

func (r *mockReaderCloser) Read(p []byte) (int, error) {
	return r.reader.Read(p)
}

func (r *mockReaderCloser) Close() error {
	return nil
}

func mockFetcher (url string) (io.ReadCloser, error) {
	return mockReader, nil
}

func TestFetch(t *testing.T) {
	spec := tests.Spec(t)

	downloader := NewDownloader("data/", mockFetcher)

	// Gets a file using Fetcher
	firstPath, err := downloader.Fetch("Fake/uri")
	spec.Expect(err).ToEqual(nil)
	cont, err := ioutil.ReadFile(firstPath)
	spec.Expect(err).ToEqual(nil)

	spec.Expect(string(cont)).ToEqual("Hello World")

	// Gets a file from a local cache
	secondPath, err := downloader.Fetch("Fake/uri")
	spec.Expect(err).ToEqual(nil)
	spec.Expect(secondPath).ToEqual(firstPath)

	// Gets a different file
	thirdPath, err := downloader.Fetch("Fake/uri2")
	spec.Expect(err).ToEqual(nil)
	spec.Expect(thirdPath).ToNotEqual(firstPath)

	downloader.Clear()
}