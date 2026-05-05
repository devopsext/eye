package common

import (
	"fmt"
	"math"
	"reflect"
	"sync"

	"github.com/devopsext/utils"
)

type Labels = map[string]string

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

type Hash = uint64

type Hashes struct {
	items map[Hash]Labels
	mu    sync.Mutex
}

type Host struct {
	On         *Host
	hashes     *Hashes
	labelsHash Hash
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
	labelsHash Hash
}

type Applications struct {
	mu    sync.Mutex
	items map[int64]map[string]*Application
}

type Traffic = float64
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

type Errors = float64
type Latency = float64
type Saturation = float64

type ApplicationSaturationKind = int

const (
	ApplicationSaturationKindUnknown = iota
	ApplicationSaturationKindConnections
)

const (
	ApplicationSaturationKindConnectionsName = "connections"
)

type HostSaturationKind = int

const (
	HostSaturationKindUnknown = iota
	HostSaturationKindCPU
	HostSaturationKindMemory
	HostSaturationKindDisk
)

const (
	HostSaturationKindCPUName    = "cpu"
	HostSaturationKindMemoryName = "memory"
	HostSaturationKindDiskName   = "disk"
)

type IncomingTraffic struct {
	mu     sync.Mutex
	hashes *Hashes
	items  map[TrafficKind]map[Hash]Traffic
}

type IncomingErrors struct {
	mu     sync.Mutex
	hashes *Hashes
	items  map[Hash]Errors
}

type IncomingLatency struct {
	mu     sync.Mutex
	hashes *Hashes
	items  map[Hash]Latency
}

type OutgoingTraffic struct {
	mu     sync.Mutex
	hashes *Hashes
	items  map[TrafficKind]map[Hash]Traffic
}

type OutgoingErrors struct {
	mu     sync.Mutex
	hashes *Hashes
	items  map[Hash]Errors
}

type OutgoingLatency struct {
	mu     sync.Mutex
	hashes *Hashes
	items  map[Hash]Latency
}

type ApplicationSaturation struct {
	mu     sync.Mutex
	hashes *Hashes
	items  map[ApplicationSaturationKind]map[Hash]Saturation
}

type HostSaturation struct {
	mu     sync.Mutex
	hashes *Hashes
	items  map[HostSaturationKind]map[Hash]Saturation
}

const (
	//HostSignalName           = "name"
	HostSignalHost           = "host"
	HostSignalSaturationKind = "kind"
)

type HostSignal struct {
	Host       *Host
	Saturation *HostSaturation
}

const (
	ApplicationSignalName           = "application"
	ApplicationSignalHost           = "host"
	ApplicationSignalTrafficKind    = "kind"
	ApplicationSignalSaturationKind = "kind"
)

type ApplicationSignal struct {
	Application *Application
	Host        *Host
	Labels      Labels

	IncomingTraffic *IncomingTraffic
	IncomingErrors  *IncomingErrors
	IncomingLatency *IncomingLatency

	OutgoingTraffic *OutgoingTraffic
	OutgoingErrors  *OutgoingErrors
	OutgoingLatency *OutgoingLatency

	Saturation *ApplicationSaturation
}

type Signal interface {
	Name() string
	Merge(s Signal)
}

type Signals struct {
	mu    sync.Mutex
	items map[string]Signal
}

type Measurements struct {
	mu    sync.Mutex
	items map[int64]*Signals
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

func ApplicationSaturationKindByName(kind string) ApplicationSaturationKind {

	switch kind {
	case ApplicationSaturationKindConnectionsName:
		return ApplicationSaturationKindConnections
	}
	return ApplicationSaturationKindUnknown
}

func HostSaturationKindByName(kind string) HostSaturationKind {

	switch kind {
	case HostSaturationKindCPUName:
		return HostSaturationKindCPU
	case HostSaturationKindMemoryName:
		return HostSaturationKindMemory
	case HostSaturationKindDiskName:
		return HostSaturationKindDisk
	}
	return HostSaturationKindUnknown
}

// Hashes

func (hs *Hashes) Items() map[Hash]Labels {

	hs.mu.Lock()
	defer hs.mu.Unlock()

	return hs.items
}

func (hs *Hashes) Hash(labels Labels) Hash {

	if labels == nil {
		return 0
	}
	return MapFNV(labels)
}

func (hs *Hashes) AddOrUpdate(labels Labels) Hash {

	hash := MapFNV(labels)
	if hash == 0 {
		return hash
	}

	hs.mu.Lock()
	defer hs.mu.Unlock()

	if hs.items == nil {
		hs.items = make(map[Hash]Labels)
	}

	lbs := hs.items[hash]
	if lbs == nil {
		hs.items[hash] = labels
	}
	return hash
}

func (hs *Hashes) Find(hash Hash) Labels {

	hs.mu.Lock()
	defer hs.mu.Unlock()

	return hs.items[hash]
}

func NewHashes() *Hashes {

	return &Hashes{
		items: make(map[Hash]Labels),
	}
}

// IncomingTraffic

func (it *IncomingTraffic) Value(kind TrafficKind) map[Hash]Traffic {

	it.mu.Lock()
	defer it.mu.Unlock()

	return it.items[kind]
}

func (it *IncomingTraffic) AddOrUpdate(kind TrafficKind, value float64, labels Labels) {

	hash := it.hashes.AddOrUpdate(labels)
	if hash == 0 {
		return
	}

	it.mu.Lock()
	defer it.mu.Unlock()

	if it.items == nil {
		it.items = make(map[TrafficKind]map[Hash]Traffic)
	}

	values := it.items[kind]
	if values == nil {
		values = make(map[Hash]Traffic)
		values[hash] = value
	} else {

		traffic, ok := values[hash]
		if !ok {
			values[hash] = value
		} else {
			values[hash] = (traffic + value) / 2
		}
	}
	it.items[kind] = values
}

func NewIncomingTraffic(hashes *Hashes) *IncomingTraffic {

	return &IncomingTraffic{
		hashes: hashes,
		items:  make(map[TrafficKind]map[Hash]Traffic),
	}
}

// IncomingErrors

func (ie *IncomingErrors) AddOrUpdate(value float64, labels Labels) {

	hash := ie.hashes.AddOrUpdate(labels)
	if hash == 0 {
		return
	}

	ie.mu.Lock()
	defer ie.mu.Unlock()

	if ie.items == nil {
		ie.items = make(map[Hash]Errors)
	}

	errors, ok := ie.items[hash]
	if !ok {
		ie.items[hash] = value
	} else {
		ie.items[hash] = (errors + value) / 2
	}
}

func NewIncomingErrors(hashes *Hashes) *IncomingErrors {

	return &IncomingErrors{
		hashes: hashes,
		items:  make(map[Hash]Errors),
	}
}

// IncomingLatency

func (il *IncomingLatency) AddOrUpdate(value float64, labels Labels) {

	hash := il.hashes.AddOrUpdate(labels)
	if hash == 0 {
		return
	}

	il.mu.Lock()
	defer il.mu.Unlock()

	if il.items == nil {
		il.items = make(map[Hash]Latency)
	}

	errors, ok := il.items[hash]
	if !ok {
		il.items[hash] = value
	} else {
		il.items[hash] = (errors + value) / 2
	}
}

func NewIncomingLatency(hashes *Hashes) *IncomingLatency {

	return &IncomingLatency{
		hashes: hashes,
		items:  make(map[Hash]Latency),
	}
}

// OutgoingTraffic

func (ot *OutgoingTraffic) AddOrUpdate(kind TrafficKind, value float64, labels Labels) {

	hash := ot.hashes.AddOrUpdate(labels)
	if hash == 0 {
		return
	}

	ot.mu.Lock()
	defer ot.mu.Unlock()

	if ot.items == nil {
		ot.items = make(map[TrafficKind]map[Hash]Traffic)
	}

	values := ot.items[kind]
	if values == nil {
		values = make(map[Hash]Traffic)
		values[hash] = value
	} else {

		traffic, ok := values[hash]
		if !ok {
			values[hash] = value
		} else {
			values[hash] = (traffic + value) / 2
		}
	}
	ot.items[kind] = values
}

func NewOutgoingTraffic(hashes *Hashes) *OutgoingTraffic {

	return &OutgoingTraffic{
		hashes: hashes,
		items:  make(map[TrafficKind]map[Hash]Traffic),
	}
}

// OutgoingErrors

func (oe *OutgoingErrors) AddOrUpdate(value float64, labels Labels) {

	hash := oe.hashes.AddOrUpdate(labels)
	if hash == 0 {
		return
	}

	oe.mu.Lock()
	defer oe.mu.Unlock()

	if oe.items == nil {
		oe.items = make(map[Hash]Errors)
	}

	errors, ok := oe.items[hash]
	if !ok {
		oe.items[hash] = value
	} else {
		oe.items[hash] = (errors + value) / 2
	}
}

func NewOutgoingErrors(hashes *Hashes) *OutgoingErrors {

	return &OutgoingErrors{
		hashes: hashes,
		items:  make(map[Hash]Errors),
	}
}

// OutgoingLatency

func (ol *OutgoingLatency) AddOrUpdate(value float64, labels Labels) {

	hash := ol.hashes.AddOrUpdate(labels)
	if hash == 0 {
		return
	}

	ol.mu.Lock()
	defer ol.mu.Unlock()

	if ol.items == nil {
		ol.items = make(map[Hash]Latency)
	}

	errors, ok := ol.items[hash]
	if !ok {
		ol.items[hash] = value
	} else {
		ol.items[hash] = (errors + value) / 2
	}
}

func NewOutgoingLatency(hashes *Hashes) *OutgoingLatency {

	return &OutgoingLatency{
		hashes: hashes,
		items:  make(map[Hash]Latency),
	}
}

// ApplicationSaturation

func (as *ApplicationSaturation) AddOrUpdate(kind ApplicationSaturationKind, value float64, labels Labels) {

	hash := as.hashes.AddOrUpdate(labels)
	if hash == 0 {
		return
	}

	as.mu.Lock()
	defer as.mu.Unlock()

	if as.items == nil {
		as.items = make(map[ApplicationSaturationKind]map[Hash]Saturation)
	}

	values := as.items[kind]
	if values == nil {
		values = make(map[Hash]Saturation)
		values[hash] = value
	} else {

		saturation, ok := values[hash]
		if !ok {
			values[hash] = value
		} else {
			values[hash] = (saturation + value) / 2
		}
	}
	as.items[kind] = values
}

func NewApplicationSaturation(hashes *Hashes) *ApplicationSaturation {

	return &ApplicationSaturation{
		hashes: hashes,
		items:  make(map[ApplicationSaturationKind]map[Hash]Saturation),
	}
}

// HostSaturation

func (hs *HostSaturation) AddOrUpdate(kind HostSaturationKind, value float64, labels Labels) {

	hash := hs.hashes.AddOrUpdate(labels)
	if hash == 0 {
		return
	}

	hs.mu.Lock()
	defer hs.mu.Unlock()

	if hs.items == nil {
		hs.items = make(map[HostSaturationKind]map[Hash]Saturation)
	}

	values := hs.items[kind]
	if values == nil {
		values = make(map[Hash]Saturation)
		values[hash] = value
	} else {

		saturation, ok := values[hash]
		if !ok {
			values[hash] = value
		} else {
			values[hash] = (saturation + value) / 2
		}
	}
	hs.items[kind] = values
}

func NewHostSaturation(hashes *Hashes) *HostSaturation {

	return &HostSaturation{
		hashes: hashes,
		items:  make(map[HostSaturationKind]map[Hash]Saturation),
	}
}

// Host

func (h *Host) Labels() Labels {

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
		h.Labels = make(Labels)
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

func NewHost(hashes *Hashes, labels Labels, on *Host) *Host {

	labelsHash := hashes.AddOrUpdate(labels)

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

func (a *Application) Labels() Labels {

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
		a.Labels = make(Labels)
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

func NewApplication(hashes *Hashes, labels Labels) *Application {

	labelsHash := hashes.AddOrUpdate(labels)

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

func NewHostSignal(hashes *Hashes, host *Host) *HostSignal {

	return &HostSignal{

		Host:       host,
		Saturation: NewHostSaturation(hashes),
	}
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

func NewApplicationSignal(hashes *Hashes, app *Application, host *Host) *ApplicationSignal {

	return &ApplicationSignal{

		Application: app,
		Host:        host,

		IncomingTraffic: NewIncomingTraffic(hashes),
		IncomingErrors:  NewIncomingErrors(hashes),
		IncomingLatency: NewIncomingLatency(hashes),

		OutgoingTraffic: NewOutgoingTraffic(hashes),
		OutgoingErrors:  NewOutgoingErrors(hashes),
		OutgoingLatency: NewOutgoingLatency(hashes),

		Saturation: NewApplicationSaturation(hashes),
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

func (ms *Measurements) FindHostSignalWithTolerance(t int64, name string, tolerance int) *HostSignal {

	typ := reflect.TypeFor[*HostSignal]()
	s := ms.FindWithToleranceByType(t, name, tolerance, typ)
	if utils.IsEmpty(s) {
		return nil
	}
	hs, ok := s.(*HostSignal)
	if !ok {
		return nil
	}
	return hs
}

func (ms *Measurements) FindHostSignal(t int64, name string) *HostSignal {
	return ms.FindHostSignalWithTolerance(t, name, 0)
}

func NewMeasurements() *Measurements {

	return &Measurements{
		items: make(map[int64]*Signals),
	}
}
