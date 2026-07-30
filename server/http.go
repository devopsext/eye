package server

import (
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/devopsext/eye/common"
	sreCommon "github.com/devopsext/sre/common"
	"github.com/devopsext/utils"
)

type HttpServerOptions struct {
	ServerName string
	Listen     string
	Tls        bool
	Insecure   bool
	CA         string
	Crt        string
	Key        string
	Timeout    int
}

type HttpServer struct {
	options  HttpServerOptions
	handlers *common.Handlers
	logger   sreCommon.Logger
	meter    sreCommon.Meter
}

type HttpProcessHandleFunc = func(w http.ResponseWriter, r *http.Request)

func (h *HttpServer) processPath(path string, mux *http.ServeMux, p common.HttpHandler) {

	paths := strings.Split(path, ",")
	for _, path := range paths {

		labels := make(sreCommon.Labels)
		labels["path"] = path

		requests := h.meter.Counter("HttpServer", "requests", "Count of all http server requests", labels, "http", "server")
		errors := h.meter.Counter("HttpServer", "errors", "Count of all server input errors", labels, "http", "server")

		mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {

			requests.Inc()
			err := p.HandleHttpRequest(w, r)
			if err != nil {
				errors.Inc()
			}
		})
	}
}

func (h *HttpServer) Start(wg *sync.WaitGroup) {

	wg.Add(1)
	go func(wg *sync.WaitGroup) {

		defer wg.Done()
		h.logger.Info("Start http server...")

		var caPool *x509.CertPool
		var certificates []tls.Certificate

		if h.options.Tls {

			// load certififcate
			var cert []byte
			if _, err := os.Stat(h.options.Crt); err == nil {

				cert, err = os.ReadFile(h.options.Crt)
				if err != nil {
					h.logger.Panic(err)
				}
			} else {
				cert = []byte(h.options.Crt)
			}

			// load key
			var key []byte
			if _, err := os.Stat(h.options.Key); err == nil {
				key, err = os.ReadFile(h.options.Key)
				if err != nil {
					h.logger.Panic(err)
				}
			} else {
				key = []byte(h.options.Key)
			}

			// make pair from certificate and pair
			pair, err := tls.X509KeyPair(cert, key)
			if err != nil {
				h.logger.Panic(err)
			}

			certificates = append(certificates, pair)

			// load CA
			var ca []byte
			if _, err := os.Stat(h.options.CA); err == nil {
				ca, err = os.ReadFile(h.options.CA)
				if err != nil {
					h.logger.Panic(err)
				}
			} else {
				ca = []byte(h.options.CA)
			}

			// make pool of CA
			caPool = x509.NewCertPool()
			if !caPool.AppendCertsFromPEM(ca) {
				h.logger.Debug("Http Server CA is invalid")
			}
		}

		mux := http.NewServeMux()

		for _, p := range h.handlers.Items() {
			hp, _ := p.(common.HttpHandler)
			if !utils.IsEmpty(hp) {
				h.processPath(hp.URL(), mux, hp)
			}
		}

		listener, err := net.Listen("tcp", h.options.Listen)
		if err != nil {
			h.logger.Panic(err)
		}

		h.logger.Info("Http server is up. Listening...")

		srv := &http.Server{
			Handler:  mux,
			ErrorLog: nil,
		}

		if h.options.Tls {

			srv.TLSConfig = &tls.Config{
				Certificates:       certificates,
				RootCAs:            caPool,
				InsecureSkipVerify: h.options.Insecure,
				ServerName:         h.options.ServerName,
			}

			err = srv.ServeTLS(listener, "", "")
			if err != nil {
				h.logger.Panic(err)
			}
		} else {
			err = srv.Serve(listener)
			if err != nil {
				h.logger.Panic(err)
			}
		}
	}(wg)
}

func NewHttpServer(options HttpServerOptions, handlers *common.Handlers, observability *common.Observability) *HttpServer {

	meter := observability.Metrics()

	return &HttpServer{
		options:  options,
		handlers: handlers,
		logger:   observability.Logs(),
		meter:    meter,
	}
}
