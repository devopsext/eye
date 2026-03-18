package Handler

import (
	"fmt"
	"net/http"

	"github.com/devopsext/eye/common"
	sreCommon "github.com/devopsext/sre/common"
)

type HealthHandlerOptions struct {
	Path string
}

type HealthHandler struct {
	options       HealthHandlerOptions
	observability *common.Observability
	logger        sreCommon.Logger
	meter         sreCommon.Meter
}

func HealthHandlerType() string {
	return "Health"
}

func (p *HealthHandler) Type() string {
	return HealthHandlerType()
}

func (p *HealthHandler) Path() string {
	return p.options.Path
}

func (p *HealthHandler) HandleHttpRequest(w http.ResponseWriter, r *http.Request) error {

	_, err := w.Write([]byte("OK"))

	if err != nil {
		http.Error(w, fmt.Sprintf("HTTP Server could not write response: %v", err), http.StatusInternalServerError)
		return err
	}
	return nil
}

func NewHealthHandler(options HealthHandlerOptions, observability *common.Observability) *HealthHandler {

	return &HealthHandler{
		options:       options,
		observability: observability,
		logger:        observability.Logs(),
		meter:         observability.Metrics(),
	}
}
