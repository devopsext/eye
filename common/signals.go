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

type HostKind = int

const (
	HostKindVM = iota
	HostKindEC2
	HostKindBaremetal
	HostKindEsxi
)

type Host struct {
	Name string
	Rack string // for bare metal, esxi
	Kind HostKind
	On   *Host
}

type Application struct {
	Name   string
	Host   *Host
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

type HostSignal struct {
	Host
	Saturation map[SaturationKind]*Saturation
}

type Traffic = float64
type Errors = float64
type Latency = float64

type IncomingTraffic = Traffic
type IncomingErrors = Errors
type IncomingLatency = Latency

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

type ApplicationSignal struct {
	Application *Application

	IncomingTraffic map[TrafficKind]*IncomingTraffic
	IncomingErrors  *IncomingErrors
	IncomingLatency *IncomingLatency

	OutgoingTraffic map[TrafficKind]*OutgoingTraffic
	OutgoingErrors  *OutgoingErrors
	Outgoinglatency *OutgoingLatency

	Saturation map[SaturationKind]*Saturation
}

type Signal interface {
	Name() string
}

type Signals struct {
	items map[string]Signal
}

type Measurements struct {
	items map[int64]Signals
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

// Application

func (a *Application) Same(app *Application) bool {

	if app == nil {
		return false
	}

	if a == app {
		return true
	}

	if a.Name != app.Name {
		return false
	}

	if a.Host != nil && app.Host != nil &&
		a.Host.Name != app.Host.Name {
		return false
	}
	return true
}

func (a *Application) Copy(app *Application) {

	if a.Same(app) {
		return
	}

	if a.Labels == nil {
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

// Applications

func (as *Applications) Add(t int64, a *Application) {

	as.mu.Lock()
	defer as.mu.Unlock()

	if a == nil {
		return
	}

	if utils.IsEmpty(a.Name) {
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

	old := m[a.Name]
	if old == nil {
		m[a.Name] = a
	} else {
		old.Copy(a)
	}
}

/*func (as *Applications) unsafeLookBack(t int64) (int64, map[string]*Application) {

	tb := t - 1
	for {
		m := as.items[tb]
		if m != nil {
      return
		}
	}
	return tb
}*/

func (as *Applications) Find(t int64, name string) *Application {

	as.mu.Lock()
	defer as.mu.Unlock()

	tc := as.items[t]
	if tc == nil {
		return nil
	}
	return tc[name]
}

// HostSignal

func (hs *HostSignal) Name() string {
	return hs.Host.Name
}

// ApplicationSignal

func (as *ApplicationSignal) Name() string {
	return as.Application.Name
}

// Signals

// Measurement

func (ms *Measurements) Add(t int64, s Signal) {

	ms.mu.Lock()
	defer ms.mu.Unlock()

	if utils.IsEmpty(s) {
		return
	}

	ss := ms.items[t]

	if utils.IsEmpty(ss) {
		ss = Signals{
			items: make(map[string]Signal),
		}
		ms.items[t] = ss
	}

	n := s.Name()
	sn := ss.items[n]
	if utils.IsEmpty(sn) {
		ss.items[n] = s
	}
}

func (ms *Measurements) FindByApplication(t int64, app *Application) *ApplicationSignal {

	if utils.IsEmpty(app) {
		return nil
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()

	ss := ms.items[t]
	for _, v := range ss.items {

		as, ok := v.(*ApplicationSignal)
		if !ok {
			continue
		}

		if as.Application == nil {
			continue
		}

		if !as.Application.Same(app) {
			continue
		}

		return as
	}
	return nil
}

func NewMeasurements() *Measurements {
	return &Measurements{
		items: make(map[int64]Signals),
	}
}
