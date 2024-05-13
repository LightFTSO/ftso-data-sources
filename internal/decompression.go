package internal

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"compress/zlib"
	"io"
)

func DecompressGzip(decompressedData []byte) ([]byte, error) {
	buf := bytes.NewBuffer(decompressedData)
	r, err := gzip.NewReader(buf)
	if err != nil {
		return []byte{}, err
	}
	defer r.Close()
	data, _ := io.ReadAll(r)

	return data, nil
}

func DecompressZlib(decompressedData []byte) ([]byte, error) {
	buf := bytes.NewBuffer(decompressedData)
	r, err := zlib.NewReader(buf)
	if err != nil {
		return []byte{}, err
	}
	defer r.Close()
	data, _ := io.ReadAll(r)

	return data, nil
}

func DecompressFlate(decompressedData []byte) ([]byte, error) {
	r := flate.NewReader(bytes.NewReader(decompressedData))
	defer r.Close()

	data, err := io.ReadAll(r)

	return data, err
}
