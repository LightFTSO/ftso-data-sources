package internal

import (
	"fmt"
	"net/http"
)

var HttpServer *http.Server

func InitializeHttpServer(host string, port int) error {
	HttpServer = &http.Server{
		Addr:    fmt.Sprintf("%s:%d", host, port),
		Handler: http.DefaultServeMux,
	}

	err := HttpServer.ListenAndServe()

	return err
}
