package processor

import (
	"fmt"
	"net/http"

	"github.com/devopsext/eye/common"
	sreCommon "github.com/devopsext/sre/common"
)

type HealthProcessorOptions struct {
	Path string
}

type HealthProcessor struct {
	options       HealthProcessorOptions
	observability *common.Observability
	logger        sreCommon.Logger
	meter         sreCommon.Meter
}

func HealthProcessorType() string {
	return "Health"
}

func (p *HealthProcessor) Type() string {
	return HealthProcessorType()
}

func (p *HealthProcessor) Path() string {
	return p.options.Path
}

func (p *HealthProcessor) HandleHttpRequest(w http.ResponseWriter, r *http.Request) error {

	_, err := w.Write([]byte("OK"))

	if err != nil {
		http.Error(w, fmt.Sprintf("HTTP Server could not write response: %v", err), http.StatusInternalServerError)
		return err
	}
	return nil
}

func NewHealthProcessor(options HealthProcessorOptions, observability *common.Observability) *HealthProcessor {

	return &HealthProcessor{
		options:       options,
		observability: observability,
		logger:        observability.Logs(),
		meter:         observability.Metrics(),
	}
}
