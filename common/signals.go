package common

import "github.com/devopsext/utils"

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
	Name string
	Host *Host
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
	Application Application

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

func NewMeasurements() *Measurements {
	return &Measurements{
		items: make(map[int64]Signals),
	}
}
