package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"go.uber.org/zap"
)

func main() {
	target := flag.String("target", "http://127.0.0.1:80", "target host")
	certPath := flag.String("certPath", "./client.crt", "target host")
	keyPath := flag.String("keyPath", "./client.key", "target host")
	debug := flag.Bool("debug", false, "show debug messages")
	flag.Parse()

	var logger *zap.Logger
	if *debug {
		var err error
		logger, err = zap.NewDevelopment()
		if err != nil {
			panic(err)
		}
	} else {
		var err error
		logger, err = zap.NewProduction()
		if err != nil {
			panic(err)
		}
	}

	handler, err := newHandler(*target, *certPath, *keyPath)
	if err != nil {
		logger.Error("Listening on HTTP endpoint", zap.Error(err))
		os.Exit(1)
	}
	http.HandleFunc("/", handler)

	err = http.ListenAndServe(":8181", nil)
	if err != nil {
		logger.Error("Listening on HTTP endpoint", zap.Error(err))
		os.Exit(1)
	}
}

func transfer(destination io.WriteCloser, source io.ReadCloser) {
	defer destination.Close()
	defer source.Close()
	io.Copy(destination, source)
}

type readCloser struct {
	io.Reader
	io.Closer
}

func newHandler(host, certPath, keyPath string) (func(http.ResponseWriter, *http.Request), error) {
	cer, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, err
	}

	tr := &http.Transport{
		MaxIdleConns:       10,
		IdleConnTimeout:    30 * time.Second,
		DisableCompression: true,
		TLSClientConfig: &tls.Config{
			Certificates:       []tls.Certificate{cer},
			InsecureSkipVerify: true,
		},
	}

	return func(w http.ResponseWriter, req *http.Request) {

		fmt.Printf("---------- REQUEST ----------\n")

		u := host + req.URL.Path
		if req.URL.RawQuery != "" {
			u += "?" + req.URL.RawQuery
		}
		proxyReq, err := http.NewRequest(req.Method, u, readCloser{io.TeeReader(req.Body, os.Stdout), req.Body})
		if err != nil {
			fmt.Printf("Creating request: %s\n", err)
		}
		copyHeader(req.Header, proxyReq.Header)
		fmt.Printf("Request: %+v\n", proxyReq)

		fmt.Printf("---------- RESPONSE ----------\n")
		client := &http.Client{Transport: tr}
		resp, err := client.Do(proxyReq)
		if err != nil {
			fmt.Printf("Fetching url: %s\n", err)
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		defer resp.Body.Close()
		copyHeader(resp.Header, w.Header())
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, io.TeeReader(resp.Body, os.Stdout))
	}, nil
}

func copyHeader(src, dst http.Header) {
	for k, vv := range src {
		for _, v := range vv {
			dst.Add(k, v)
		}
	}
}
