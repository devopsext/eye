package handler

import (
	"fmt"
	"net/http"

	"github.com/go-playground/form"

	"github.com/devopsext/eye/common"
	sreCommon "github.com/devopsext/sre/common"
)

type HttpInspectHandlerRequest struct {
	ID    string `form:"id"`
	Model string `form:"model"`
}

type HttpInspectHandlerResponse struct {
	Request *HttpInspectHandlerRequest `json:"request"`
}

type HttpInspectHandlerOptions struct {
	Path string
}

type HttpInspectHandler struct {
	options       HttpInspectHandlerOptions
	observability *common.Observability
	logger        sreCommon.Logger
	meter         sreCommon.Meter
}

func (h *HttpInspectHandler) Name() string {
	return "Inspect"
}

func (h *HttpInspectHandler) Path() string {
	return h.options.Path
}

func (h *HttpInspectHandler) HandleHttpRequest(w http.ResponseWriter, r *http.Request) error {

	labels := make(sreCommon.Labels)

	requests := h.meter.Counter(h.Name(), "requests", "Count of all remote_write Handler requests", labels, "remote_write", "Handler")
	errs := h.meter.Counter(h.Name(), "errors", "Count of all remote_write Handler errors", labels, "remote_write", "Handler")

	requests.Inc()

	err := r.ParseForm()
	if err != nil {
		errs.Inc()
		http.Error(w, fmt.Sprintf("could not parse form: %v", err), http.StatusInternalServerError)
		return err
	}

	decoder := form.NewDecoder()

	var request HttpInspectHandlerRequest
	err = decoder.Decode(&request, r.Form)
	if err != nil {
		errs.Inc()
		http.Error(w, fmt.Sprintf("could not decode form: %v", err), http.StatusInternalServerError)
		return err
	}

	var data []byte

	if err != nil {
		errs.Inc()
		http.Error(w, fmt.Sprintf("could not make image: %v", err), http.StatusInternalServerError)
		return err
	}

	if _, err := w.Write(data); err != nil {
		errs.Inc()
		http.Error(w, fmt.Sprintf("could not write response: %v", err), http.StatusInternalServerError)
		return err
	}
	return nil
}

func NewHttpInspectHandler(options HttpInspectHandlerOptions, observability *common.Observability) *HttpInspectHandler {

	return &HttpInspectHandler{
		options:       options,
		observability: observability,
		logger:        observability.Logs(),
		meter:         observability.Metrics(),
	}
}
