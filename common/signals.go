package common

import (
	"fmt"
	"math"
	"reflect"
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

type Hashes struct {
	items map[uint64]map[string]string
	mu    sync.Mutex
}

type Host struct {
	On         *Host
	hashes     *Hashes
	labelsHash uint64
}

type Hosts struct {
	mu    sync.Mutex
	items map[int64]map[string]*Host
}

const (
	ApplicationName = "name"
)

type Application struct {
	hashes     *Hashes
	labelsHash uint64
}

type Applications struct {
	mu    sync.Mutex
	items map[int64]map[string]*Application
}

type TrafficKind = int

const (
	TrafficKindUnknown = iota // unknown
	TrafficKindRps            // request per second
	TrafficKindBps            // byte per second
	TrafficKindQps            // query per second
)

const (
	TrafficKindRpsName = "rps"
	TrafficKindBpsName = "bps"
	TrafficKindQpsName = "qps"
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

type Traffic struct {
	Value  float64
	Labels map[string]string
}
type Errors struct {
	Value  float64
	Labels map[string]string
}

type IncomingTraffic struct {
	Values map[TrafficKind]map[uint64]*Traffic
}

type IncomingErrors struct {
	Values map[uint64]*Errors
}

type IncomingLatency struct {
	items map[uint64]float64
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
}

const (
	ApplicationSignalName        = "application"
	ApplicationSignalHost        = "host"
	ApplicationSignalTrafficKind = "kind"
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

func TrafficKindByName(kind string) TrafficKind {

	switch kind {
	case TrafficKindRpsName:
		return TrafficKindRps
	case TrafficKindBpsName:
		return TrafficKindBps
	case TrafficKindQpsName:
		return TrafficKindQps
	}
	return TrafficKindUnknown
}

// Hashes

func (hs *Hashes) Hash(labels map[string]string) uint64 {

	if labels == nil {
		return 0
	}
	return MapFNV(labels)
}

func (hs *Hashes) AddOrUpdate(labels map[string]string) {

	hash := MapFNV(labels)
	if hash == 0 {
		return
	}

	hs.mu.Lock()
	defer hs.mu.Unlock()

	if hs.items == nil {
		hs.items = make(map[uint64]map[string]string)
	}

	lbs := hs.items[hash]
	if lbs == nil {
		hs.items[hash] = labels
	}
}

func (hs *Hashes) Find(hash uint64) map[string]string {

	hs.mu.Lock()
	defer hs.mu.Unlock()

	return hs.items[hash]
}

func NewHashes() *Hashes {

	return &Hashes{
		items: make(map[uint64]map[string]string),
	}
}

// IncomingTraffic

func (it *IncomingTraffic) AddOrUpdate(kind TrafficKind, value float64, labels map[string]string) {

	hash := MapFNV(labels)

	if it.Values == nil {
		it.Values = make(map[TrafficKind]map[uint64]*Traffic)
	}

	values := it.Values[kind]
	if values == nil {

		values = make(map[uint64]*Traffic)
		values[hash] = &Traffic{
			Value:  value,
			Labels: labels,
		}
	} else {

		traffic := values[hash]
		if traffic == nil {
			values[hash] = &Traffic{
				Value:  value,
				Labels: labels,
			}
		} else {
			traffic.Value = (traffic.Value + value) / 2
		}
	}
	it.Values[kind] = values
}

func NewIncomingTraffic() *IncomingTraffic {

	return &IncomingTraffic{
		Values: make(map[TrafficKind]map[uint64]*Traffic),
	}
}

// IncomingErrors

func (ie *IncomingErrors) AddOrUpdate(value float64, labels map[string]string) {

	hash := MapFNV(labels)

	if ie.Values == nil {
		ie.Values = make(map[uint64]*Errors)
	}

	errors := ie.Values[hash]
	if errors == nil {
		ie.Values[hash] = &Errors{
			Value:  value,
			Labels: labels,
		}
	} else {
		errors.Value = (errors.Value + value) / 2
	}
}

func NewIncomingErrors() *IncomingErrors {

	return &IncomingErrors{
		Values: make(map[uint64]*Errors),
	}
}

// IncomingLatency

func (il *IncomingLatency) AddOrUpdate(value float64, labels map[string]string) {

	hash := MapFNV(labels)

	if il.items == nil {
		il.items = make(map[uint64]float64)
	}

	l, ok := il.items[hash]
	if !ok {
		il.items[hash] = value
	} else {
		l = (l + value) / 2
		il.items[hash] = l
	}
}

func NewIncomingLatency() *IncomingLatency {

	return &IncomingLatency{
		items: make(map[uint64]float64),
	}
}

// Host

func (h *Host) Labels() map[string]string {

	lhs := h.hashes
	if lhs == nil {
		return nil
	}
	lbs := lhs.Find(h.labelsHash)
	if lbs == nil {
		return nil
	}
	return lbs
}

func (h *Host) Name() string {

	lbs := h.Labels()
	if lbs == nil {
		return ""
	}
	return lbs[HostName]
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

	/*labels := host.Labels()
	if h.labels == nil && len(labels) > 0 {
		h.Labels = make(map[string]string)
	}

	for k, v := range labels {

		v2 := h.Labels[k]
		if utils.IsEmpty(v2) {
			h.Labels[k] = v
			continue
		}

		if v2 == v {
			continue
		}
		h.Labels[k] = v
	}*/
}

func NewHost(hashes *Hashes, labels map[string]string, on *Host) *Host {

	labelsHash := hashes.Hash(labels)
	hashes.AddOrUpdate(labels)

	return &Host{
		hashes:     hashes,
		labelsHash: labelsHash,
		On:         on,
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
	abs := math.Abs(float64(tolerance))
	for {
		tb--
		m := hs.items[tb]
		diff := t - tb
		if m == nil && diff < int64(abs) {
			continue
		}
		if m == nil {
			return nil
		}
		a := m[name]
		if a != nil {
			return a
		}
		return nil
	}
}

func (hs *Hosts) FindWithTolerance(t int64, name string, tolerance int) *Host {

	if utils.IsEmpty(name) {
		return nil
	}

	hs.mu.Lock()
	defer hs.mu.Unlock()

	tc := hs.items[t]

	if tolerance == 0 && tc == nil {
		return nil
	}

	if tolerance < 0 {
		h := hs.unsafeLookBack(t, name, tolerance)
		if h != nil {
			return h
		}
	}

	return tc[name]
}

func (hs *Hosts) Find(t int64, name string) *Host {

	return hs.FindWithTolerance(t, name, 0)
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

func (a *Application) Labels() map[string]string {

	lhs := a.hashes
	if lhs == nil {
		return nil
	}
	lbs := lhs.Find(a.labelsHash)
	if lbs == nil {
		return nil
	}
	return lbs
}

func (a *Application) Name() string {

	lbs := a.Labels()
	if lbs == nil {
		return ""
	}
	return lbs[ApplicationName]
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

	/*if a.Labels == nil && len(app.Labels) > 0 {
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
	}*/
}

func NewApplication(hashes *Hashes, labels map[string]string) *Application {

	labelsHash := hashes.Hash(labels)
	hashes.AddOrUpdate(labels)

	return &Application{
		hashes:     hashes,
		labelsHash: labelsHash,
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
	abs := math.Abs(float64(tolerance))
	for {
		tb--
		m := as.items[tb]
		diff := t - tb
		if m == nil && diff < int64(abs) {
			continue
		}
		if m == nil {
			return nil
		}
		a := m[name]
		if a != nil {
			return a
		}
		return nil
	}
}

func (as *Applications) FindWithTolerance(t int64, name string, tolerance int) *Application {

	if utils.IsEmpty(name) {
		return nil
	}

	as.mu.Lock()
	defer as.mu.Unlock()

	tc := as.items[t]
	if tolerance == 0 && tc == nil {
		return nil
	}

	if tolerance < 0 {
		a := as.unsafeLookBack(t, name, tolerance)
		if a != nil {
			return a
		}
	}
	return tc[name]
}

func (as *Applications) Find(t int64, name string) *Application {
	return as.FindWithTolerance(t, name, 0)
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

	name := ""
	if hs.Host != nil {
		name = hs.Host.Name()
	}
	return name
}

func (hs *HostSignal) Merge(s Signal) {
	// should check all fields of HostSignal
}

// ApplicationSignal

func BuildApplicationSignalName(app, host string) string {

	if utils.IsEmpty(app) {
		return ""
	}
	return fmt.Sprintf("%s/%s", app, host)
}

func (as *ApplicationSignal) Name() string {

	appName := ""
	if as.Application != nil {
		appName = as.Application.Name()
	}

	hostName := ""
	if as.Host != nil {
		hostName = as.Host.Name()
	}
	return BuildApplicationSignalName(appName, hostName)
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

func (ss *Signals) Find(name string) Signal {

	ss.mu.Lock()
	defer ss.mu.Unlock()

	return ss.items[name]
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

func (ms *Measurements) unsafeLookBackByType(t int64, name string, tolerance int, typ reflect.Type) Signal {

	tb := t
	abs := math.Abs(float64(tolerance))
	for {
		tb--
		ss := ms.items[tb]
		diff := t - tb
		if ss == nil && diff < int64(abs) {
			continue
		}
		if ss == nil {
			return nil
		}
		s := ss.items[name]
		if utils.IsEmpty(s) {
			continue
		}
		ts := reflect.TypeOf(s)
		if ts.ConvertibleTo(typ) {
			return s
		}
		return nil
	}
}

func (ms *Measurements) FindWithToleranceByType(t int64, name string, tolerance int, typ reflect.Type) Signal {

	if utils.IsEmpty(name) {
		return nil
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()

	ss := ms.items[t]

	if ss == nil || ss.items == nil {
		return nil
	}

	if tolerance < 0 {
		s := ms.unsafeLookBackByType(t, name, tolerance, typ)
		if utils.IsEmpty(s) {
			return s
		}
	}

	return ss.items[name]
}

func (ms *Measurements) FindApplicationSignalWithTolerance(t int64, name string, tolerance int) *ApplicationSignal {

	typ := reflect.TypeFor[*ApplicationSignal]()
	s := ms.FindWithToleranceByType(t, name, tolerance, typ)
	if utils.IsEmpty(s) {
		return nil
	}
	as, ok := s.(*ApplicationSignal)
	if !ok {
		return nil
	}
	return as
}

func (ms *Measurements) FindApplicationSignal(t int64, name string) *ApplicationSignal {
	return ms.FindApplicationSignalWithTolerance(t, name, 0)
}

func NewMeasurements() *Measurements {

	return &Measurements{
		items: make(map[int64]*Signals),
	}
}
