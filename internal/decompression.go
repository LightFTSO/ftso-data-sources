package internal

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"io"
)

func DecompressGzip(compressedData []byte) ([]byte, error) {
	buf := bytes.NewBuffer(compressedData)
	r, err := gzip.NewReader(buf)
	if err != nil {
		return []byte{}, err
	}
	defer r.Close()
	data, _ := io.ReadAll(r)

	return data, nil
}

func DecompressFlate(compressedData []byte) ([]byte, error) {
	r := flate.NewReader(bytes.NewReader(compressedData))
	defer r.Close()

	data, err := io.ReadAll(r)

	return data, err
}
