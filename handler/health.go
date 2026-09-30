package handler

import (
	"fmt"
	"net/http"

	"github.com/devopsext/eye/common"
	sreCommon "github.com/devopsext/sre/common"
)

type HttpHealthHandlerOptions struct {
	Path string
}

type HttpHealthHandler struct {
	options       HttpHealthHandlerOptions
	observability *common.Observability
	logger        sreCommon.Logger
	meter         sreCommon.Meter
}

func (h *HttpHealthHandler) Name() string {
	return "Health"
}

func (h *HttpHealthHandler) Path() string {
	return h.options.Path
}

func (h *HttpHealthHandler) HandleHttpRequest(w http.ResponseWriter, r *http.Request) error {

	_, err := w.Write([]byte("OK"))

	if err != nil {
		http.Error(w, fmt.Sprintf("HTTP Server could not write response: %v", err), http.StatusInternalServerError)
		return err
	}
	return nil
}

func NewHttpHealthHandler(options HttpHealthHandlerOptions, observability *common.Observability) *HttpHealthHandler {

	return &HttpHealthHandler{
		options:       options,
		observability: observability,
		logger:        observability.Logs(),
		meter:         observability.Metrics(),
	}
}
