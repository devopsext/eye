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
	Timestamp time.Time `json:"timestamp"`
	Metric    string    `json:"metric"`
	Value     float64   `json:"value"`
	Max       float64   `json:"max"`
	Min       float64   `json:"min"`
}

type HttpApplicationHandlerDataVerdict struct {
	Name      string `json:"name"`
	Impact    string `json:"impact"`
	Score     int    `json:"score"`
	Category  string `json:"category"`
	RootCause string `json:"root_cause"`
}

type HttpApplicationHandlerData struct {
	Ident     string                             `json:"ident"`
	Timestamp time.Time                          `json:"timestamp"`
	Points    []HttpApplicationHandlerDataPoint  `json:"points"`
	Verdict   *HttpApplicationHandlerDataVerdict `json:"verdict"`
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

// HttpApplicationHandlerDataPoint

func NewHttpApplicationHandlerDataPoint(timestamp time.Time, metric string, value, min, max float64) HttpApplicationHandlerDataPoint {
	return HttpApplicationHandlerDataPoint{
		Timestamp: timestamp,
		Metric:    metric,
		Value:     value,
		Min:       min,
		Max:       max,
	}
}

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

	af, ok := frame.(common.ModelApplicationFrame)
	if !ok {
		h.logger.Debug("%s: Frame %s is not supported", frame.Ident(), h.Name())
		return
	}

	ident := af.Ident()
	if utils.IsEmpty(ident) {
		return
	}

	ts := af.Timestamp()

	points := []HttpApplicationHandlerDataPoint{}

	inReq := af.InRequests()
	if !utils.IsEmpty(inReq) {
		points = append(points, NewHttpApplicationHandlerDataPoint(ts, "InRequests", inReq.Value(), inReq.Min(), inReq.Max()))
	}
	inThru := af.InThroughput()
	if !utils.IsEmpty(inThru) {
		points = append(points, NewHttpApplicationHandlerDataPoint(ts, "InThroughput", inThru.Value(), inThru.Min(), inThru.Max()))
	}
	inLat := af.InLatency()
	if !utils.IsEmpty(inLat) {
		points = append(points, NewHttpApplicationHandlerDataPoint(ts, "InLatency", inLat.Value(), inLat.Min(), inLat.Max()))
	}
	inErr := af.InErrors()
	if !utils.IsEmpty(inErr) {
		points = append(points, NewHttpApplicationHandlerDataPoint(ts, "InErrors", inErr.Value(), inErr.Min(), inErr.Max()))
	}
	//
	outReq := af.OutRequests()
	if !utils.IsEmpty(outReq) {
		points = append(points, NewHttpApplicationHandlerDataPoint(ts, "OutRequests", outReq.Value(), outReq.Min(), outReq.Max()))
	}
	outThru := af.OutThroughput()
	if !utils.IsEmpty(outThru) {
		points = append(points, NewHttpApplicationHandlerDataPoint(ts, "OutThroughput", outThru.Value(), outThru.Min(), outThru.Max()))
	}
	outLat := af.OutLatency()
	if !utils.IsEmpty(outLat) {
		points = append(points, NewHttpApplicationHandlerDataPoint(ts, "OutLatency", outLat.Value(), outLat.Min(), outLat.Max()))
	}
	outErr := af.OutErrors()
	if !utils.IsEmpty(outErr) {
		points = append(points, NewHttpApplicationHandlerDataPoint(ts, "OutErrors", outErr.Value(), outErr.Min(), outErr.Max()))
	}
	//
	appCPU := af.CPU()
	if !utils.IsEmpty(appCPU) {
		points = append(points, NewHttpApplicationHandlerDataPoint(ts, "ApplicationCPU", appCPU.Value(), appCPU.Min(), appCPU.Max()))
	}
	appMem := af.Memory()
	if !utils.IsEmpty(appMem) {
		points = append(points, NewHttpApplicationHandlerDataPoint(ts, "ApplicationMemory", appMem.Value(), appMem.Min(), appMem.Max()))
	}
	hostCPU := af.HostCPU()
	if !utils.IsEmpty(hostCPU) {
		points = append(points, NewHttpApplicationHandlerDataPoint(ts, "HostCPU", hostCPU.Value(), hostCPU.Min(), hostCPU.Max()))
	}
	hostMem := af.HostMemory()
	if !utils.IsEmpty(hostMem) {
		points = append(points, NewHttpApplicationHandlerDataPoint(ts, "HostMemory", hostMem.Value(), hostMem.Min(), hostMem.Max()))
	}

	if len(points) == 0 {
		return
	}

	var verdict *HttpApplicationHandlerDataVerdict

	v := af.Verdict()
	if !utils.IsEmpty(v) {

		impact := common.CaseImpactToString(v.Impact())
		if !utils.IsEmpty(impact) {

			verdict = &HttpApplicationHandlerDataVerdict{
				Name:      v.Name(),
				Impact:    impact,
				Score:     v.Score(),
				Category:  common.CaseCategoryToString(v.Category()),
				RootCause: v.RootCause(),
			}
		}
	}

	h.datachannel <- HttpApplicationHandlerData{
		Ident:     ident,
		Timestamp: af.Timestamp(),
		Points:    points,
		Verdict:   verdict,
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
		WebsocketPath     string
		ApiStatePath      string
		CaseImpactSevere  string
		CaseImpactAverage string
	}

	websocketPath, _ := url.JoinPath(path, HttpApplicationHandlerWebsocketPath)
	apiStatePath, _ := url.JoinPath(path, HttpApplicationHandlerApiStatePath)

	return pageTemplate.Execute(w, &tpl{
		WebsocketPath:     websocketPath,
		ApiStatePath:      apiStatePath,
		CaseImpactSevere:  common.CaseImpactToString(common.CaseImpactSevere),
		CaseImpactAverage: common.CaseImpactToString(common.CaseImpactAverage),
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
		stMap:       make(map[string]string),
		datachannel: make(chan HttpApplicationHandlerData),
	}
}
