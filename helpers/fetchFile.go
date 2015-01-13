package helpers

import (
	"net/http"
	"io"
	"os"
)

func FetchFile(uri string) (string, error) {
	outputPath := "data/" + randSeq(32)

	out, err := os.Create(outputPath)
	if err != nil {
		return "", err
	}
	defer out.Close()

	resp, err := http.Get(uri)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	_, err = io.Copy(out, resp.Body)
	if err != nil {
		return "", err
	}

	return outputPath, nil
}