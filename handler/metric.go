package Handler

import (
	"fmt"
	"net/http"

	"github.com/go-playground/form"

	"github.com/devopsext/eye/common"
	sreCommon "github.com/devopsext/sre/common"
)

type MetricHandlerRequest struct {
}

type MetricHandlerResponse struct {
	Request *MetricHandlerRequest `json:"request"`
}

type MetricHandlerOptions struct {
	URL string
}

type MetricHandler struct {
	options       MetricHandlerOptions
	observability *common.Observability
	logger        sreCommon.Logger
	meter         sreCommon.Meter
}

func MetricHandlerName() string {
	return "Metric"
}

func (c *MetricHandler) Name() string {
	return MetricHandlerName()
}

func (c *MetricHandler) URL() string {
	return c.options.URL
}

func (c *MetricHandler) HandleHttpRequest(w http.ResponseWriter, r *http.Request) error {

	labels := make(sreCommon.Labels)

	requests := c.meter.Counter(c.Name(), "requests", "Count of all remote_write Handler requests", labels, "remote_write", "Handler")
	errs := c.meter.Counter(c.Name(), "errors", "Count of all remote_write Handler errors", labels, "remote_write", "Handler")

	requests.Inc()

	err := r.ParseForm()
	if err != nil {
		errs.Inc()
		http.Error(w, fmt.Sprintf("could not parse form: %v", err), http.StatusInternalServerError)
		return err
	}

	decoder := form.NewDecoder()

	var request MetricHandlerRequest
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

func NewMetricHandler(options MetricHandlerOptions, observability *common.Observability) *MetricHandler {

	return &MetricHandler{
		options:       options,
		observability: observability,
		logger:        observability.Logs(),
		meter:         observability.Metrics(),
	}
}
