package handler

import (
	"encoding/json"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/devopsext/eye/common"
	sreCommon "github.com/devopsext/sre/common"
	"github.com/devopsext/utils"
	"github.com/gorilla/websocket"
)

type HttpApplicationHandlerOptions struct {
	Path string
	Page string
}

type HttpApplicationHandlerDataPoint struct {
	Timestamp time.Time
	Metric    string
	Value     float64
	Max       float64
	Min       float64
}

type HttpApplicationHandlerData struct {
	Ident     string
	Timestamp time.Time
	Points    []HttpApplicationHandlerDataPoint
}

type HttpApplicationHandler struct {
	options       HttpApplicationHandlerOptions
	observability *common.Observability
	logger        sreCommon.Logger
	meter         sreCommon.Meter
	page          string
	//
	wsMutex     sync.Mutex
	wsClients   map[*websocket.Conn]bool
	wsUpgrader  websocket.Upgrader
	datachannel chan HttpApplicationHandlerData
	//
	stMutex sync.Mutex
	stMap   map[string]string
}

const (
	HttpApplicationHandlerWebsocketPath = "/ws"
	HttpApplicationHandlerApiStatePath  = "/api/state"
)

// HttpApplicationHandler

func (h *HttpApplicationHandler) Name() string {
	return "Application"
}

func (h *HttpApplicationHandler) Path() string {
	return h.options.Path
}

func (h *HttpApplicationHandler) updateWsClients() {

	for {
		data := <-h.datachannel
		h.wsMutex.Lock()
		for client := range h.wsClients {
			_ = client.WriteJSON(data)
		}
		h.wsMutex.Unlock()
	}
}

func (h *HttpApplicationHandler) Start() {
	go h.updateWsClients()
}

func (h *HttpApplicationHandler) Frame(frame common.ModelFrame) {

	if utils.IsEmpty(frame) {
		return
	}

	ident := frame.Ident()
	if utils.IsEmpty(ident) {
		return
	}

	fPoints := frame.Points()
	if len(fPoints) == 0 {
		return
	}

	points := []HttpApplicationHandlerDataPoint{}
	for _, p := range fPoints {

		metric := p.Metric()
		if utils.IsEmpty(metric) {
			continue
		}
		point := HttpApplicationHandlerDataPoint{
			Timestamp: p.Timestamp(),
			Metric:    p.Metric(),
			Value:     p.Value(),
			Max:       p.Max(),
			Min:       p.Min(),
		}
		points = append(points, point)
	}

	h.datachannel <- HttpApplicationHandlerData{
		Ident:     ident,
		Timestamp: time.Now(),
		Points:    points,
	}
}

func (h *HttpApplicationHandler) State(model common.Model, state common.ModelState) {

	if utils.IsEmpty(model) {
		return
	}

	h.stMutex.Lock()
	defer h.stMutex.Unlock()

	h.stMap[model.Name()] = common.ModelStateToString(state)
}

func (h *HttpApplicationHandler) handlePage(path string, w http.ResponseWriter) error {

	data, err := utils.Content(h.options.Page)
	if err != nil {
		return err
	}
	pageTemplate, err := template.New("application").Parse(string(data))
	if err != nil {
		h.logger.Error("%s: Page %s has error: %s", h.options.Page, err)
		return err
	}

	type tpl struct {
		WebsocketPath string
		ApiStatePath  string
	}

	websocketPath, _ := url.JoinPath(path, HttpApplicationHandlerWebsocketPath)
	apiStatePath, _ := url.JoinPath(path, HttpApplicationHandlerApiStatePath)

	return pageTemplate.Execute(w, &tpl{
		WebsocketPath: websocketPath,
		ApiStatePath:  apiStatePath,
	})
}

func (h *HttpApplicationHandler) handleWebsocket(w http.ResponseWriter, r *http.Request) error {

	ws, err := h.wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		return nil
	}
	defer ws.Close()

	h.wsMutex.Lock()
	h.wsClients[ws] = true
	h.wsMutex.Unlock()

	name := h.Name()

	for {
		_, msg, err := ws.ReadMessage()
		if err != nil {
			h.wsMutex.Lock()
			delete(h.wsClients, ws)
			h.wsMutex.Unlock()
			break
		}

		var payload struct {
			Type        string `json:"type"`
			Application string `json:"application"`
		}
		err = json.Unmarshal(msg, &payload)
		if err != nil {
			h.logger.Error("%s: Cannot unmarshal message: %s, error: %s", name, msg, err)
			break
		}
		if payload.Type == "filter" && payload.Application != "" {
			//setActiveService(payload.Service)
		}
	}
	return nil
}

func (h *HttpApplicationHandler) handleApiState(w http.ResponseWriter) error {

	h.stMutex.Lock()
	defer h.stMutex.Unlock()

	model := "Unknown"
	state := "unknown"

	w.Header().Set("Content-Type", "application/json")

	type response struct {
		Model string `json:"model"`
		State string `json:"state"`
	}

	keys := slices.Collect(maps.Keys(h.stMap))
	if len(keys) > 0 {
		model = keys[0]
		state = h.stMap[model]
	}

	data, err := json.Marshal(&response{
		Model: model,
		State: state,
	})
	if err != nil {
		return err
	}
	_, err = w.Write(data)
	return err
}

func (h *HttpApplicationHandler) HandleHttpRequest(path string, w http.ResponseWriter, r *http.Request) error {

	u := r.URL
	if u == nil {
		return h.handlePage(path, w)
	}

	rest, found := strings.CutPrefix(u.Path, path)
	if !found {
		return h.handlePage(path, w)
	}
	if !strings.HasPrefix(rest, "/") {
		rest = "/" + rest
	}

	switch rest {
	case HttpApplicationHandlerWebsocketPath:
		return h.handleWebsocket(w, r)
	case HttpApplicationHandlerApiStatePath:
		return h.handleApiState(w)
	default:
		return h.handlePage(path, w)
	}
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

		//
		wsClients: make(map[*websocket.Conn]bool),
		wsUpgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool { return true },
		},
		//
		stMap: make(map[string]string),
	}
}
