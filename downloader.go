package dataloading

import (
	"os"
	"time"
	"math/rand"
	"io"
	"net/http"
	"encoding/csv"
)

type Fetcher func(url string) (io.ReadCloser, error)

type Downloader struct {
	directory string
	downloader Fetcher
	cache map[string]string
}

// randSeq from http://stackoverflow.com/questions/22892120/how-to-generate-a-random-string-of-a-fixed-length-in-golang
var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func loadDownloaderCache(directory string) map[string]string {
	ce := map[string]string{}

	file, err := os.Open(directory + "/cache.csv")
	if err != nil {
		return ce
	}
	defer file.Close()

	csvReader := csv.NewReader(file)
	records, err := csvReader.ReadAll()
	if err != nil {
		return ce
	}

	for _, record := range records {
		ce[record[0]] = record[1]
	}

	return ce
}

func saveDownloaderCache(directory string, cache map[string]string) error {
	file, err := os.Create(directory + "/cache.csv")
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	for key, value := range cache {
		writer.Write([]string{ key, value })
	}
	writer.Flush()

	return nil
}

func defaultFetcher(url string) (io.ReadCloser, error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}

	return resp.Body, nil
}

func NewDownloader(directory string, fetcher Fetcher) *Downloader {
	if fetcher == nil {
		fetcher = defaultFetcher
	}

	rand.Seed(time.Now().UTC().UnixNano())

	if _, err := os.Stat(directory); err != nil {
		if os.IsNotExist(err) {
			os.MkdirAll(directory, 0755)
		}
	}

	cache := loadDownloaderCache(directory)

	return &Downloader{
		directory,
		fetcher,
		cache,
	}
}

func (d *Downloader) getCached(url string) string {
	return d.cache[url]
}

func (d *Downloader) updateCache(url string, path string) {
	d.cache[url] = path

	// Eat error as nothing to be done/ not fatal
	_ = saveDownloaderCache(d.directory, d.cache)

	return 
}

func (d *Downloader) Fetch(url string) (string, error) {
	cachedPath := d.getCached(url)
	if cachedPath != "" {
		return cachedPath, nil
	}

	path := randSeq(32)
	outputPath := d.directory + path

	out, err := os.Create(outputPath)
	if err != nil {
		return "", err
	}
	defer out.Close()

	rc, err := d.downloader(url)
	if err != nil {
		return "", err
	}
	defer rc.Close()

	_, err = io.Copy(out, rc)
	if err != nil {
		return "", err
	}

	d.updateCache(url, outputPath)

	return outputPath, nil
}

func (d *Downloader) Clear() error {
	err := os.RemoveAll(d.directory)
	return err
}