package processor

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/go-playground/form"

	"github.com/devopsext/eye/common"
	sreCommon "github.com/devopsext/sre/common"
)

type RemoteWriteProcessorRequest struct {
}

type RemoteWriteProcessorResponse struct {
	Request *RemoteWriteProcessorRequest `json:"request"`
}

type RemoteWriteProcessorOptions struct {
}

type RemoteWriteProcessor struct {
	options       RemoteWriteProcessorOptions
	observability *common.Observability
	logger        sreCommon.Logger
	meter         sreCommon.Meter
}

func RemoteWriteProcessorType() string {
	return "RemoteWrite"
}

func (p *RemoteWriteProcessor) Type() string {
	return RemoteWriteProcessorType()
}

func (p *RemoteWriteProcessor) HandleHttpRequest(w http.ResponseWriter, r *http.Request) error {

	channel := strings.TrimLeft(r.URL.Path, "/")

	labels := make(sreCommon.Labels)
	labels["channel"] = channel

	requests := p.meter.Counter("remote_write", "requests", "Count of all remote_write processor requests", labels, "remote_write", "processor")
	errs := p.meter.Counter("remote_write", "errors", "Count of all remote_write processor errors", labels, "remote_write", "processor")

	requests.Inc()

	err := r.ParseForm()
	if err != nil {
		errs.Inc()
		http.Error(w, fmt.Sprintf("could not parse form: %v", err), http.StatusInternalServerError)
		return err
	}

	decoder := form.NewDecoder()

	var request RemoteWriteProcessorRequest
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

func NewRemoteWriteProcessor(options RemoteWriteProcessorOptions, observability *common.Observability) *RemoteWriteProcessor {

	return &RemoteWriteProcessor{
		options:       options,
		observability: observability,
		logger:        observability.Logs(),
		meter:         observability.Metrics(),
	}
}
