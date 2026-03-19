package handler

import (
	"fmt"
	"net/http"

	"github.com/go-playground/form"

	"github.com/devopsext/eye/common"
	sreCommon "github.com/devopsext/sre/common"
)

type HttpMetricHandlerRequest struct {
}

type HttpMetricHandlerResponse struct {
	Request *HttpMetricHandlerRequest `json:"request"`
}

type HttpMetricHandlerOptions struct {
	URL string
}

type HttpMetricHandler struct {
	options       HttpMetricHandlerOptions
	observability *common.Observability
	logger        sreCommon.Logger
	meter         sreCommon.Meter
}

func (h *HttpMetricHandler) Name() string {
	return "Metric"
}

func (h *HttpMetricHandler) URL() string {
	return h.options.URL
}

func (h *HttpMetricHandler) HandleHttpRequest(w http.ResponseWriter, r *http.Request) error {

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

	var request HttpMetricHandlerRequest
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

func NewHttpMetricHandler(options HttpMetricHandlerOptions, observability *common.Observability) *HttpMetricHandler {

	return &HttpMetricHandler{
		options:       options,
		observability: observability,
		logger:        observability.Logs(),
		meter:         observability.Metrics(),
	}
}
