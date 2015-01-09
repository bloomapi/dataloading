package helpers

import (
	"net/http"
	"io"
	"os"
	"math/rand"
)

// randSeq from http://stackoverflow.com/questions/22892120/how-to-generate-a-random-string-of-a-fixed-length-in-golang
var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

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