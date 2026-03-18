package Handler

import (
	"fmt"
	"net/http"

	"github.com/go-playground/form"

	"github.com/devopsext/eye/common"
	sreCommon "github.com/devopsext/sre/common"
)

type RemoteWriteHandlerRequest struct {
}

type RemoteWriteHandlerResponse struct {
	Request *RemoteWriteHandlerRequest `json:"request"`
}

type RemoteWriteHandlerOptions struct {
}

type RemoteWriteHandler struct {
	options       RemoteWriteHandlerOptions
	observability *common.Observability
	logger        sreCommon.Logger
	meter         sreCommon.Meter
}

func RemoteWriteHandlerType() string {
	return "RemoteWrite"
}

func (p *RemoteWriteHandler) Type() string {
	return RemoteWriteHandlerType()
}

func (p *RemoteWriteHandler) HandleHttpRequest(w http.ResponseWriter, r *http.Request) error {

	labels := make(sreCommon.Labels)

	requests := p.meter.Counter(p.Type(), "requests", "Count of all remote_write Handler requests", labels, "remote_write", "Handler")
	errs := p.meter.Counter(p.Type(), "errors", "Count of all remote_write Handler errors", labels, "remote_write", "Handler")

	requests.Inc()

	err := r.ParseForm()
	if err != nil {
		errs.Inc()
		http.Error(w, fmt.Sprintf("could not parse form: %v", err), http.StatusInternalServerError)
		return err
	}

	decoder := form.NewDecoder()

	var request RemoteWriteHandlerRequest
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

func NewRemoteWriteHandler(options RemoteWriteHandlerOptions, observability *common.Observability) *RemoteWriteHandler {

	return &RemoteWriteHandler{
		options:       options,
		observability: observability,
		logger:        observability.Logs(),
		meter:         observability.Metrics(),
	}
}
