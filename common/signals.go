package common

import (
	"sync"

	"github.com/devopsext/utils"
)

type SignalKind = int

const (
	SignalTraffic = iota
	SignalErrors
	SignalLatency
	SignalSaturation
)

/*type HostKind = int

const (
	HostKindVM = iota
	HostKindEC2
	HostKindBaremetal
	HostKindEsxi
)*/

const (
	HostName = "name"
	HostOn   = "server"
)

type Host struct {
	//Kind      HostKind
	On     *Host
	Labels map[string]string
}

type Hosts struct {
	mu    sync.Mutex
	items map[int64]map[string]*Host
}

const (
	ApplicationName = "name"
)

type Application struct {
	Labels map[string]string
}

type Applications struct {
	mu    sync.Mutex
	items map[int64]map[string]*Application
}

type TrafficKind = int

const (
	TrafficKindRps = iota // request per second
	TrafficKindBps        // byte per second
	TrafficKindQps        // query per second
)

type SaturationKind = int

const (
	SaturationKindCpu = iota
	SaturationKindMemory
	SaturationKindDisk
)

type Saturation = float64

const (
	HostSignalName = "name"
)

type HostSignal struct {
	Host       *Host
	Saturation map[SaturationKind]*Saturation
}

type Traffic = float64
type Errors = float64
type Latency = float64

type IncomingTraffic struct {
	Values map[TrafficKind]*Traffic
	Labels map[string]string
}

type IncomingErrors struct {
	Value  *Errors
	Labels map[string]string
}

type IncomingLatency struct {
	Value  *Latency
	Labels map[string]string
}

type OutgoingTraffic struct {
	Traffic
	Application
}

type OutgoingErrors struct {
	Errors
	Application
}

type OutgoingLatency struct {
	Latency
	Application
}

const (
	ApplicationSignalName = "application"
	ApplicationSignalHost = "host"
)

type ApplicationSignal struct {
	Application *Application
	Host        *Host
	Labels      map[string]string

	IncomingTraffic *IncomingTraffic
	IncomingErrors  *IncomingErrors
	IncomingLatency *IncomingLatency

	OutgoingTraffic map[TrafficKind]*OutgoingTraffic
	OutgoingErrors  *OutgoingErrors
	Outgoinglatency *OutgoingLatency

	Saturation map[SaturationKind]*Saturation
}

type Signal interface {
	Name() string
	Merge(s Signal)
}

type Signals struct {
	items map[string]Signal
	mu    sync.Mutex
}

type Measurements struct {
	items map[int64]*Signals
	mu    sync.Mutex
}

func SignalKindToString(kind SignalKind) string {

	switch kind {
	case SignalTraffic:
		return "traffic"
	case SignalErrors:
		return "errors"
	case SignalLatency:
		return "latency"
	case SignalSaturation:
		return "saturation"
	}
	return ""
}

// IncomingTraffic

func NewIncomingTraffic(values map[TrafficKind]*Traffic, labels map[string]string) *IncomingTraffic {

	return &IncomingTraffic{
		Values: values,
		Labels: labels,
	}
}

// IncomingErrors

func NewIncomingErrors(value *Errors, labels map[string]string) *IncomingErrors {

	return &IncomingErrors{
		Value:  value,
		Labels: labels,
	}
}

// IncomingLatency

func NewIncomingLatency(value *Latency, labels map[string]string) *IncomingLatency {

	return &IncomingLatency{
		Value:  value,
		Labels: labels,
	}
}

// Host

func (h *Host) Name() string {

	lbs := h.Labels
	if lbs == nil {
		return ""
	}
	return h.Labels[HostName]
}

func (h *Host) Same(host *Host) bool {

	if host == nil {
		return false
	}

	if h == host {
		return true
	}

	if h.Name() != host.Name() {
		return false
	}
	return true
}

func (h *Host) Copy(host *Host) {

	if h.Same(host) {
		return
	}

	if h.Labels == nil && len(host.Labels) > 0 {
		h.Labels = make(map[string]string)
	}

	for k, v := range host.Labels {

		v2 := h.Labels[k]
		if utils.IsEmpty(v2) {
			h.Labels[k] = v
			continue
		}

		if v2 == v {
			continue
		}
		h.Labels[k] = v
	}
}

func NewHost(labels map[string]string, on *Host) *Host {

	return &Host{
		Labels: labels,
		On:     on,
	}
}

// Hosts

func (hs *Hosts) AddOrUpdate(t int64, h *Host) {

	hs.mu.Lock()
	defer hs.mu.Unlock()

	if h == nil {
		return
	}

	n := h.Name()
	if utils.IsEmpty(n) {
		return
	}

	if hs.items == nil {
		hs.items = make(map[int64]map[string]*Host)
	}

	m := hs.items[t]
	if m == nil {
		m = make(map[string]*Host)
		hs.items[t] = m
	}

	old := m[n]
	if old == nil {
		m[n] = h
	} else {
		old.Copy(h)
	}
}

func (hs *Hosts) unsafeLookBack(t int64, name string, tolerance int) *Host {

	tb := t
	for {
		m := hs.items[tb]
		if m == nil || tb < (t-int64(tolerance)) {
			continue
		}
		a := m[name]
		if a != nil {
			return a
		}
		tb--
	}
}

func (hs *Hosts) Find(t int64, name string, tolerance int) *Host {

	hs.mu.Lock()
	defer hs.mu.Unlock()

	tc := hs.items[t]
	if tc == nil {

		if tolerance == 0 {
			return nil
		}

		if tolerance < 0 {
			a := hs.unsafeLookBack(t, name, tolerance)
			if a != nil {
				return a
			}
		}
	}
	return tc[name]
}

func (hs *Hosts) Sizes() (int, int) {
	hs.mu.Lock()
	defer hs.mu.Unlock()

	r := 0
	for _, v := range hs.items {
		r += len(v)
	}
	return len(hs.items), r
}

func (hs *Hosts) Merge(hosts *Hosts) {

	if hosts == nil {
		return
	}

	hs.mu.Lock()
	defer hs.mu.Unlock()

	hosts.mu.Lock()
	defer hosts.mu.Unlock()

	if hs.items == nil {
		hs.items = make(map[int64]map[string]*Host)
	}

	for t, m := range hosts.items {

		mOld := hs.items[t]
		if mOld == nil {
			hs.items[t] = m
		}
	}
}

func NewHosts() *Hosts {

	return &Hosts{
		items: make(map[int64]map[string]*Host),
	}
}

// Application

func (a *Application) Name() string {

	lbs := a.Labels
	if lbs == nil {
		return ""
	}
	return a.Labels[ApplicationName]
}

func (a *Application) Same(app *Application) bool {

	if app == nil {
		return false
	}

	if a == app {
		return true
	}

	if a.Name() != app.Name() {
		return false
	}
	return true
}

func (a *Application) Copy(app *Application) {

	if a.Same(app) {
		return
	}

	if a.Labels == nil && len(app.Labels) > 0 {
		a.Labels = make(map[string]string)
	}

	for k, v := range app.Labels {

		v2 := a.Labels[k]
		if utils.IsEmpty(v2) {
			a.Labels[k] = v
			continue
		}

		if v2 == v {
			continue
		}
		a.Labels[k] = v
	}
}

func NewApplication(labels map[string]string) *Application {

	return &Application{
		Labels: labels,
	}
}

// Applications

func (as *Applications) AddOrUpdate(t int64, a *Application) {

	as.mu.Lock()
	defer as.mu.Unlock()

	if a == nil {
		return
	}

	n := a.Name()
	if utils.IsEmpty(n) {
		return
	}

	if as.items == nil {
		as.items = make(map[int64]map[string]*Application)
	}

	m := as.items[t]
	if m == nil {
		m = make(map[string]*Application)
		as.items[t] = m
	}

	old := m[n]
	if old == nil {
		m[n] = a
	} else {
		old.Copy(a)
	}
}

func (as *Applications) unsafeLookBack(t int64, name string, tolerance int) *Application {

	tb := t
	for {
		m := as.items[tb]
		if m == nil || tb < (t-int64(tolerance)) {
			continue
		}
		a := m[name]
		if a != nil {
			return a
		}
		tb--
	}
}

func (as *Applications) Find(t int64, name string, tolerance int) *Application {

	as.mu.Lock()
	defer as.mu.Unlock()

	tc := as.items[t]
	if tc == nil {

		if tolerance == 0 {
			return nil
		}

		if tolerance < 0 {
			a := as.unsafeLookBack(t, name, tolerance)
			if a != nil {
				return a
			}
		}
	}
	return tc[name]
}

func (as *Applications) Sizes() (int, int) {
	as.mu.Lock()
	defer as.mu.Unlock()

	r := 0
	for _, v := range as.items {
		r += len(v)
	}
	return len(as.items), r
}

func (as *Applications) Merge(apps *Applications) {

	if apps == nil {
		return
	}

	as.mu.Lock()
	defer as.mu.Unlock()

	apps.mu.Lock()
	defer apps.mu.Unlock()

	if as.items == nil {
		as.items = make(map[int64]map[string]*Application)
	}

	for t, m := range apps.items {

		mOld := as.items[t]
		if mOld == nil {
			as.items[t] = m
		}
	}
}

func NewApplications() *Applications {

	return &Applications{
		items: make(map[int64]map[string]*Application),
	}
}

// HostSignal

func (hs *HostSignal) Name() string {
	return hs.Host.Name()
}

func (hs *HostSignal) Merge(s Signal) {
	// should check all fields of HostSignal
}

// ApplicationSignal

func (as *ApplicationSignal) Name() string {
	return as.Application.Name()
}

func (as *ApplicationSignal) Merge(s Signal) {
	// should check all fields of ApplicationSignal
}

func NewApplicationSignal(app *Application, host *Host) *ApplicationSignal {

	return &ApplicationSignal{
		Application: app,
		Host:        host,

		IncomingTraffic: nil,
		IncomingErrors:  nil,
		IncomingLatency: nil,

		OutgoingTraffic: nil,
		OutgoingErrors:  nil,
		Outgoinglatency: nil,

		Saturation: nil,
	}
}

// Signals

func (ss *Signals) AddOrUpdate(s Signal) {

	ss.mu.Lock()
	defer ss.mu.Unlock()

	if utils.IsEmpty(s) {
		return
	}

	n := s.Name()
	if utils.IsEmpty(n) {
		return
	}

	sOld := ss.items[n]
	if utils.IsEmpty(sOld) {
		ss.items[n] = s
		return
	}
	sOld.Merge(s)
}

func NewSignals() *Signals {

	return &Signals{
		items: make(map[string]Signal),
	}
}

// Measurement

func (ms *Measurements) AddOrUpdate(t int64, s Signal) {

	ms.mu.Lock()
	defer ms.mu.Unlock()

	if s == nil {
		return
	}

	ss := ms.items[t]
	if ss == nil {
		ss = NewSignals()
		ms.items[t] = ss
	}

	ss.AddOrUpdate(s)
	ms.items[t] = ss
}

func NewMeasurements() *Measurements {

	return &Measurements{
		items: make(map[int64]*Signals),
	}
}
