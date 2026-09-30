package handler

import (
	"net/http"
	"text/template"

	"github.com/devopsext/eye/common"
	sreCommon "github.com/devopsext/sre/common"
	"github.com/devopsext/utils"
)

type HttpApplicationHandlerOptions struct {
	Path string
	Page string
}

type HttpApplicationHandler struct {
	options       HttpApplicationHandlerOptions
	observability *common.Observability
	logger        sreCommon.Logger
	meter         sreCommon.Meter
	page          string
}

func (h *HttpApplicationHandler) Name() string {
	return "Application"
}

func (h *HttpApplicationHandler) Path() string {
	return h.options.Path
}

func (h *HttpApplicationHandler) Detection(detection common.ModelDetection) {
	h.logger.Debug("asasdasdas")
}

func (h *HttpApplicationHandler) handlePage(w http.ResponseWriter) error {

	data, err := utils.Content(h.options.Page)
	if err != nil {
		return err
	}
	pageTemplate, err := template.New("application").Parse(string(data))
	if err != nil {
		h.logger.Error("%s: Page %s has error: %s", h.options.Page, err)
		return err
	}

	return pageTemplate.Execute(w, nil)
}

func (h *HttpApplicationHandler) HandleHttpRequest(w http.ResponseWriter, r *http.Request) error {

	url := r.URL
	if url == nil {
		return h.handlePage(w)
	}

	switch url.Path {
	case h.options.Path:
		return h.handlePage(w)
	}

	return nil
}

func NewHttpApplicationHandler(options HttpApplicationHandlerOptions, observability *common.Observability) *HttpApplicationHandler {

	logger := observability.Logs()

	h := &HttpApplicationHandler{
		options: options,
		logger:  logger,
	}

	name := h.Name()

	if utils.IsEmpty(options.Page) {
		logger.Debug("%s: Page is not defined.", name)
		return nil
	}

	return &HttpApplicationHandler{
		options:       options,
		observability: observability,
		logger:        observability.Logs(),
		meter:         observability.Metrics(),
	}
}
