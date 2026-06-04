package common

import (
	"math"
	"reflect"
	"sync"

	"github.com/devopsext/utils"
)

type Labels = map[string]string
type Stamp = uint64

type SignalKind = int

const (
	SignalTraffic = iota
	SignalErrors
	SignalLatency
	SignalSaturation
)

const (
	HostName = "name"
	HostOn   = "server"
)

type Hash = uint32

type Hashes struct {
	mu    sync.Mutex
	Items map[Hash]Labels
}

type Host struct {
	hashes *Hashes
	On     *Host
	Hash   Hash
}

type Hosts struct {
	mu    sync.Mutex
	Items map[Stamp]map[string]*Host
}

const (
	ApplicationName = "name"
)

type Application struct {
	hashes *Hashes
	Hash   Hash
}

type Applications struct {
	mu    sync.Mutex
	Items map[Stamp]map[string]*Application
}

type Traffic = float64
type TrafficKind = int

const (
	TrafficKindUnknown = iota // unknown
	TrafficKindRps            // request per second
	TrafficKindBps            // byte per second
	TrafficKindQps            // query per second
)

var allTrafficKinds = []TrafficKind{TrafficKindUnknown, TrafficKindRps, TrafficKindBps, TrafficKindQps}

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
	hashes       *Hashes
	applications *Applications
	items        map[TrafficKind]map[Hash]Traffic
}

type IncomingErrors struct {
	hashes       *Hashes
	applications *Applications
	items        map[Hash]Errors
}

type IncomingLatency struct {
	hashes       *Hashes
	applications *Applications
	items        map[Hash]Latency
}

type OutgoingTraffic struct {
	hashes       *Hashes
	applications *Applications
	items        map[TrafficKind]map[Hash]Traffic
}

type OutgoingErrors struct {
	hashes       *Hashes
	applications *Applications
	items        map[Hash]Errors
}

type OutgoingLatency struct {
	hashes       *Hashes
	applications *Applications
	items        map[Hash]Latency
}

type ApplicationSaturation struct {
	hashes *Hashes
	items  map[ApplicationSaturationKind]map[Hash]Saturation
}

type HostSaturation struct {
	hashes *Hashes
	items  map[HostSaturationKind]map[Hash]Saturation
}

const (
	//HostSignalName           = "name"
	HostSignalHost           = "host"
	HostSignalSaturationKind = "kind"
)

type HostSignal struct {
	host       *Host
	saturation *HostSaturation
}

const (
	ApplicationSignalName           = "application"
	ApplicationSignalHost           = "host"
	ApplicationSignalTrafficKind    = "kind"
	ApplicationSignalSaturationKind = "kind"
	ApplicationSignalFrontend       = "frontend"
	ApplicationSignalBackend        = "backend"
)

type ApplicationSignal struct {
	measurement *Measurements

	Application *Application
	Host        *Host

	incomingTraffic *IncomingTraffic
	incomingErrors  *IncomingErrors
	incomingLatency *IncomingLatency

	outgoingTraffic *OutgoingTraffic
	outgoingErrors  *OutgoingErrors
	outgoingLatency *OutgoingLatency

	saturation *ApplicationSaturation
}

type Dependencies struct {
	items map[*Application]*Dependencies
}

type Signal interface {
	Name() string
	Merge(s Signal)
}

type Signals struct {
	mu    sync.Mutex
	Items map[string]Signal
}

type Measurements struct {
	mu           sync.Mutex
	first        Stamp
	last         Stamp
	hosts        *Hosts
	applications *Applications
	Items        map[Stamp]*Signals
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

func (hs *Hashes) IsEmpty() bool {

	hs.mu.Lock()
	defer hs.mu.Unlock()

	return len(hs.Items) == 0
}

func (hs *Hashes) GetItems() map[Hash]Labels {

	hs.mu.Lock()
	defer hs.mu.Unlock()

	return hs.Items
}

func (hs *Hashes) SetItems(items map[Hash]Labels) {

	hs.mu.Lock()
	defer hs.mu.Unlock()

	hs.Items = items
}

func (hs *Hashes) Hash(labels Labels) Hash {

	if labels == nil {
		return 0
	}
	return Map2Hash32(labels)
}

func (hs *Hashes) AddOrUpdate(labels Labels) Hash {

	hash := Map2Hash32(labels)
	if hash == 0 {
		return hash
	}

	hs.mu.Lock()
	defer hs.mu.Unlock()

	if hs.Items == nil {
		hs.Items = make(map[Hash]Labels)
	}

	lbs := hs.Items[hash]
	if lbs == nil {
		hs.Items[hash] = labels
	}
	return hash
}

func (hs *Hashes) Find(hash Hash) Labels {

	hs.mu.Lock()
	defer hs.mu.Unlock()

	return hs.Items[hash]
}

func NewHashes() *Hashes {

	return &Hashes{
		Items: make(map[Hash]Labels),
	}
}

// IncomingTraffic

func (it *IncomingTraffic) Frontends(stamp Stamp, kinds []TrafficKind) []*Application {

	apps := []*Application{}

	for _, kind := range kinds {
		for hash := range it.items[kind] {

			lbs := it.hashes.Find(hash)
			if lbs == nil {
				continue
			}

			name := lbs[ApplicationSignalFrontend]
			if utils.IsEmpty(name) {
				continue
			}

			app := it.applications.Find(stamp, name)
			if app == nil {
				continue
			}

			if !utils.Contains(apps, app) {
				apps = append(apps, app)
			}
		}
	}
	return apps
}

func (it *IncomingTraffic) Value(kind TrafficKind) map[Hash]Traffic {

	return it.items[kind]
}

func (it *IncomingTraffic) AddOrUpdate(value float64, hash Hash) {

	if it.items == nil {
		it.items = make(map[TrafficKind]map[Hash]Traffic)
	}

	kind := TrafficKindUnknown
	lbs := it.hashes.Find(hash)
	if lbs != nil {
		kind = TrafficKindByName(lbs[ApplicationSignalTrafficKind])
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

func NewIncomingTraffic(hashes *Hashes, applications *Applications) *IncomingTraffic {

	return &IncomingTraffic{
		hashes:       hashes,
		applications: applications,
		items:        make(map[TrafficKind]map[Hash]Traffic),
	}
}

// IncomingErrors

func (ie *IncomingErrors) Frontends(stamp Stamp) []*Application {

	apps := []*Application{}

	for hash := range ie.items {

		lbs := ie.hashes.Find(hash)
		if lbs == nil {
			continue
		}

		name := lbs[ApplicationSignalFrontend]
		if utils.IsEmpty(name) {
			continue
		}

		app := ie.applications.Find(stamp, name)
		if app == nil {
			continue
		}

		if !utils.Contains(apps, app) {
			apps = append(apps, app)
		}
	}
	return apps
}

func (ie *IncomingErrors) AddOrUpdate(value float64, hash Hash) {

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

func NewIncomingErrors(hashes *Hashes, applications *Applications) *IncomingErrors {

	return &IncomingErrors{
		hashes:       hashes,
		applications: applications,
		items:        make(map[Hash]Errors),
	}
}

// IncomingLatency

func (il *IncomingLatency) Frontends(stamp Stamp) []*Application {

	apps := []*Application{}

	for hash := range il.items {

		lbs := il.hashes.Find(hash)
		if lbs == nil {
			continue
		}

		name := lbs[ApplicationSignalFrontend]
		if utils.IsEmpty(name) {
			continue
		}

		app := il.applications.Find(stamp, name)
		if app == nil {
			continue
		}

		if !utils.Contains(apps, app) {
			apps = append(apps, app)
		}
	}
	return apps
}

func (il *IncomingLatency) AddOrUpdate(value float64, hash Hash) {

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

func NewIncomingLatency(hashes *Hashes, applications *Applications) *IncomingLatency {

	return &IncomingLatency{
		hashes:       hashes,
		applications: applications,
		items:        make(map[Hash]Latency),
	}
}

// OutgoingTraffic

func (ot *OutgoingTraffic) Backends(stamp Stamp, kinds []TrafficKind) []*Application {

	apps := []*Application{}

	for _, kind := range kinds {
		for hash := range ot.items[kind] {

			lbs := ot.hashes.Find(hash)
			if lbs == nil {
				continue
			}

			name := lbs[ApplicationSignalBackend]
			if utils.IsEmpty(name) {
				continue
			}

			app := ot.applications.Find(stamp, name)
			if app == nil {
				continue
			}

			if !utils.Contains(apps, app) {
				apps = append(apps, app)
			}
		}
	}
	return apps
}

func (ot *OutgoingTraffic) AddOrUpdate(value float64, hash Hash) {

	if ot.items == nil {
		ot.items = make(map[TrafficKind]map[Hash]Traffic)
	}

	kind := TrafficKindUnknown
	lbs := ot.hashes.Find(hash)
	if lbs != nil {
		kind = TrafficKindByName(lbs[ApplicationSignalTrafficKind])
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

func NewOutgoingTraffic(hashes *Hashes, applications *Applications) *OutgoingTraffic {

	return &OutgoingTraffic{
		hashes:       hashes,
		applications: applications,
		items:        make(map[TrafficKind]map[Hash]Traffic),
	}
}

// OutgoingErrors

func (oe *OutgoingErrors) Backends(stamp Stamp) []*Application {

	apps := []*Application{}

	for hash := range oe.items {

		lbs := oe.hashes.Find(hash)
		if lbs == nil {
			continue
		}

		name := lbs[ApplicationSignalBackend]
		if utils.IsEmpty(name) {
			continue
		}

		app := oe.applications.Find(stamp, name)
		if app == nil {
			continue
		}

		if !utils.Contains(apps, app) {
			apps = append(apps, app)
		}
	}
	return apps
}

func (oe *OutgoingErrors) AddOrUpdate(value float64, hash Hash) {

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

func NewOutgoingErrors(hashes *Hashes, applications *Applications) *OutgoingErrors {

	return &OutgoingErrors{
		hashes:       hashes,
		applications: applications,
		items:        make(map[Hash]Errors),
	}
}

// OutgoingLatency

func (ol *OutgoingLatency) Backends(stamp Stamp) []*Application {

	apps := []*Application{}

	for hash := range ol.items {

		lbs := ol.hashes.Find(hash)
		if lbs == nil {
			continue
		}

		name := lbs[ApplicationSignalBackend]
		if utils.IsEmpty(name) {
			continue
		}

		app := ol.applications.Find(stamp, name)
		if app == nil {
			continue
		}

		if !utils.Contains(apps, app) {
			apps = append(apps, app)
		}
	}
	return apps
}

func (ol *OutgoingLatency) AddOrUpdate(value float64, hash Hash) {

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

func NewOutgoingLatency(hashes *Hashes, applications *Applications) *OutgoingLatency {

	return &OutgoingLatency{
		hashes:       hashes,
		applications: applications,
		items:        make(map[Hash]Latency),
	}
}

// ApplicationSaturation

func (as *ApplicationSaturation) AddOrUpdate(value float64, hash Hash) {

	if as.items == nil {
		as.items = make(map[ApplicationSaturationKind]map[Hash]Saturation)
	}

	kind := ApplicationSaturationKindUnknown
	lbs := as.hashes.Find(hash)
	if lbs != nil {
		kind = ApplicationSaturationKindByName(lbs[ApplicationSignalSaturationKind])
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

func (hs *HostSaturation) AddOrUpdate(value float64, hash Hash) {

	if hs.items == nil {
		hs.items = make(map[HostSaturationKind]map[Hash]Saturation)
	}

	kind := HostSaturationKindUnknown
	lbs := hs.hashes.Find(hash)
	if lbs != nil {
		kind = HostSaturationKindByName(lbs[HostSignalSaturationKind])
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
	lbs := lhs.Find(h.Hash)
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
}

func NewHost(hashes *Hashes, hash Hash, on *Host) *Host {

	return &Host{
		hashes: hashes,
		Hash:   hash,
		On:     on,
	}
}

// Hosts

func (hs *Hosts) GetItems() map[Stamp]map[string]*Host {

	hs.mu.Lock()
	defer hs.mu.Unlock()

	return hs.Items
}

func (hs *Hosts) SetItems(items map[Stamp]map[string]*Host) {

	hs.mu.Lock()
	defer hs.mu.Unlock()

	hs.Items = items
}

func (hs *Hosts) AddOrUpdate(stamp Stamp, h *Host) {

	hs.mu.Lock()
	defer hs.mu.Unlock()

	if h == nil {
		return
	}

	n := h.Name()
	if utils.IsEmpty(n) {
		return
	}

	if hs.Items == nil {
		hs.Items = make(map[Stamp]map[string]*Host)
	}

	m := hs.Items[stamp]
	if m == nil {
		m = make(map[string]*Host)
		hs.Items[stamp] = m
	}

	old := m[n]
	if old == nil {
		m[n] = h
	} else {
		old.Copy(h)
	}
}

func (hs *Hosts) unsafeLookBack(stamp Stamp, name string, tolerance int) *Host {

	tb := stamp
	abs := math.Abs(float64(tolerance))
	for {
		tb--
		m := hs.Items[tb]
		diff := stamp - tb
		if m == nil && diff < Stamp(abs) {
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

func (hs *Hosts) FindWithTolerance(stamp Stamp, name string, tolerance int) *Host {

	if utils.IsEmpty(name) {
		return nil
	}

	hs.mu.Lock()
	defer hs.mu.Unlock()

	tc := hs.Items[stamp]

	if tolerance == 0 && tc == nil {
		return nil
	}

	if tolerance < 0 {
		h := hs.unsafeLookBack(stamp, name, tolerance)
		if h != nil {
			return h
		}
	}

	return tc[name]
}

func (hs *Hosts) Find(stamp Stamp, name string) *Host {

	return hs.FindWithTolerance(stamp, name, 0)
}

func (hs *Hosts) Sizes() (int, int) {
	hs.mu.Lock()
	defer hs.mu.Unlock()

	r := 0
	for _, v := range hs.Items {
		r += len(v)
	}
	return len(hs.Items), r
}

func (hs *Hosts) Merge(hosts *Hosts) {

	if hosts == nil {
		return
	}

	hs.mu.Lock()
	defer hs.mu.Unlock()

	hosts.mu.Lock()
	defer hosts.mu.Unlock()

	if hs.Items == nil {
		hs.Items = make(map[Stamp]map[string]*Host)
	}

	for t, m := range hosts.Items {

		mOld := hs.Items[t]
		if mOld == nil {
			hs.Items[t] = m
		}
	}
}

func NewHosts() *Hosts {

	return &Hosts{
		Items: make(map[Stamp]map[string]*Host),
	}
}

// Application

func (a *Application) Labels() Labels {

	lhs := a.hashes
	if lhs == nil {
		return nil
	}
	lbs := lhs.Find(a.Hash)
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
}

func NewApplication(hashes *Hashes, hash Hash) *Application {

	return &Application{
		hashes: hashes,
		Hash:   hash,
	}
}

// Applications

func (as *Applications) GetItems() map[Stamp]map[string]*Application {

	as.mu.Lock()
	defer as.mu.Unlock()

	return as.Items
}

func (as *Applications) SetItems(items map[Stamp]map[string]*Application) {

	as.mu.Lock()
	defer as.mu.Unlock()

	as.Items = items
}

func (as *Applications) AddOrUpdate(stamp Stamp, a *Application) {

	as.mu.Lock()
	defer as.mu.Unlock()

	if a == nil {
		return
	}

	n := a.Name()
	if utils.IsEmpty(n) {
		return
	}

	if as.Items == nil {
		as.Items = make(map[Stamp]map[string]*Application)
	}

	m := as.Items[stamp]
	if m == nil {
		m = make(map[string]*Application)
		as.Items[stamp] = m
	}

	old := m[n]
	if old == nil {
		m[n] = a
	} else {
		old.Copy(a)
	}
}

func (as *Applications) unsafeLookBack(stamp Stamp, name string, tolerance int) *Application {

	tb := stamp
	abs := math.Abs(float64(tolerance))
	for {
		tb--
		m := as.Items[tb]
		diff := stamp - tb
		if m == nil && diff < Stamp(abs) {
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

func (as *Applications) FindWithTolerance(stamp Stamp, name string, tolerance int) *Application {

	if utils.IsEmpty(name) {
		return nil
	}

	as.mu.Lock()
	defer as.mu.Unlock()

	tc := as.Items[stamp]
	if tolerance == 0 && tc == nil {
		return nil
	}

	if tolerance < 0 {
		a := as.unsafeLookBack(stamp, name, tolerance)
		if a != nil {
			return a
		}
	}
	return tc[name]
}

func (as *Applications) Find(stamp Stamp, name string) *Application {
	return as.FindWithTolerance(stamp, name, 0)
}

func (as *Applications) Sizes() (int, int) {
	as.mu.Lock()
	defer as.mu.Unlock()

	r := 0
	for _, v := range as.Items {
		r += len(v)
	}
	return len(as.Items), r
}

func (as *Applications) Merge(apps *Applications) {

	if apps == nil {
		return
	}

	as.mu.Lock()
	defer as.mu.Unlock()

	apps.mu.Lock()
	defer apps.mu.Unlock()

	if as.Items == nil {
		as.Items = make(map[Stamp]map[string]*Application)
	}

	for t, m := range apps.Items {

		mOld := as.Items[t]
		if mOld == nil {
			as.Items[t] = m
		}
	}
}

func NewApplications() *Applications {

	return &Applications{
		Items: make(map[Stamp]map[string]*Application),
	}
}

// HostSignal

func (hs *HostSignal) Name() string {

	name := ""
	if hs.host != nil {
		name = hs.host.Name()
	}
	return name
}

func (hs *HostSignal) Merge(s Signal) {
	// should check all fields of HostSignal
}

func (hs *HostSignal) Host() *Host {
	return hs.host
}

func (hs *HostSignal) Saturation() *HostSaturation {
	return hs.saturation
}

func NewHostSignal(hashes *Hashes, host *Host) *HostSignal {

	return &HostSignal{

		host:       host,
		saturation: NewHostSaturation(hashes),
	}
}

// ApplicationSignal

func BuildApplicationSignalName(app, host string) string {

	if app == "" {
		return ""
	}
	return app + "/" + host
}

func (as *ApplicationSignal) IncomingTraffic() *IncomingTraffic {
	return as.incomingTraffic
}

func (as *ApplicationSignal) IncomingErrors() *IncomingErrors {
	return as.incomingErrors
}

func (as *ApplicationSignal) IncomingLatency() *IncomingLatency {
	return as.incomingLatency
}

func (as *ApplicationSignal) OutgoingTraffic() *OutgoingTraffic {
	return as.outgoingTraffic
}

func (as *ApplicationSignal) OutgoingErrors() *OutgoingErrors {
	return as.outgoingErrors
}

func (as *ApplicationSignal) OutgoingLatency() *OutgoingLatency {
	return as.outgoingLatency
}

func (as *ApplicationSignal) Saturation() *ApplicationSaturation {
	return as.saturation
}

func (as *ApplicationSignal) Frontends(stamp Stamp) []*Application {

	traffic := as.incomingTraffic.Frontends(stamp, allTrafficKinds)
	errors := as.incomingErrors.Frontends(stamp)
	latency := as.incomingLatency.Frontends(stamp)

	arr := []*Application{}
	arr = append(arr, traffic...)
	arr = append(arr, errors...)
	arr = append(arr, latency...)

	return ApplicationsCompact(arr)
}

func (as *ApplicationSignal) Backends(stamp Stamp) []*Application {

	traffic := as.outgoingTraffic.Backends(stamp, allTrafficKinds)
	errors := as.outgoingErrors.Backends(stamp)
	latency := as.outgoingLatency.Backends(stamp)

	arr := []*Application{}
	arr = append(arr, traffic...)
	arr = append(arr, errors...)
	arr = append(arr, latency...)

	return ApplicationsCompact(arr)
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

func NewApplicationSignal(hashes *Hashes, applications *Applications, app *Application, host *Host) *ApplicationSignal {

	return &ApplicationSignal{

		Application: app,
		Host:        host,

		incomingTraffic: NewIncomingTraffic(hashes, applications),
		incomingErrors:  NewIncomingErrors(hashes, applications),
		incomingLatency: NewIncomingLatency(hashes, applications),

		outgoingTraffic: NewOutgoingTraffic(hashes, applications),
		outgoingErrors:  NewOutgoingErrors(hashes, applications),
		outgoingLatency: NewOutgoingLatency(hashes, applications),

		saturation: NewApplicationSaturation(hashes),
	}
}

// Dependencies

func (ds *Dependencies) Items() map[*Application]*Dependencies {
	return ds.items
}

func NewDependencies() *Dependencies {

	return &Dependencies{
		items: make(map[*Application]*Dependencies),
	}
}

// Signals

func (ss *Signals) GetItems() map[string]Signal {

	ss.mu.Lock()
	defer ss.mu.Unlock()

	return ss.Items
}

func (ss *Signals) SetItems(items map[string]Signal) {

	ss.mu.Lock()
	defer ss.mu.Unlock()

	ss.Items = items
}

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

	sOld := ss.Items[n]
	if utils.IsEmpty(sOld) {
		ss.Items[n] = s
		return
	}
	sOld.Merge(s)
}

func (ss *Signals) Find(name string) Signal {

	ss.mu.Lock()
	defer ss.mu.Unlock()

	return ss.Items[name]
}

func NewSignals() *Signals {

	return &Signals{
		Items: make(map[string]Signal),
	}
}

// Measurements

func (ms *Measurements) GetItems() map[Stamp]*Signals {

	ms.mu.Lock()
	defer ms.mu.Unlock()

	return ms.Items
}

func (ms *Measurements) SetItems(items map[Stamp]*Signals) {

	ms.mu.Lock()
	defer ms.mu.Unlock()

	ms.Items = items
}

func (ms *Measurements) AddOrUpdate(stamp Stamp, s Signal) {

	ms.mu.Lock()
	defer ms.mu.Unlock()

	if s == nil {
		return
	}

	if stamp > ms.last {
		ms.last = stamp
	}

	if stamp < ms.first {
		ms.first = stamp
	}

	ss := ms.Items[stamp]
	if ss == nil {
		ss = NewSignals()
		ms.Items[stamp] = ss
	}

	ss.AddOrUpdate(s)
	ms.Items[stamp] = ss
}

func (ms *Measurements) unsafeLookBackByType(stamp Stamp, name string, tolerance int, typ reflect.Type) Signal {

	tb := stamp
	abs := math.Abs(float64(tolerance))
	for {
		tb--
		ss := ms.Items[tb]
		diff := stamp - tb
		if ss == nil && diff < Stamp(abs) {
			continue
		}
		if ss == nil {
			return nil
		}
		s := ss.Items[name]
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

func (ms *Measurements) FindSignalWithToleranceByType(stamp Stamp, name string, tolerance int, typ reflect.Type) Signal {

	if utils.IsEmpty(name) {
		return nil
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()

	ss := ms.Items[stamp]

	if ss == nil || ss.Items == nil {
		return nil
	}

	if tolerance < 0 {
		s := ms.unsafeLookBackByType(stamp, name, tolerance, typ)
		if utils.IsEmpty(s) {
			return s
		}
	}

	return ss.Items[name]
}

func (ms *Measurements) FindApplicationSignalWithTolerance(stamp Stamp, name string, tolerance int) *ApplicationSignal {

	typ := reflect.TypeFor[*ApplicationSignal]()
	s := ms.FindSignalWithToleranceByType(stamp, name, tolerance, typ)
	if utils.IsEmpty(s) {
		return nil
	}
	as, ok := s.(*ApplicationSignal)
	if !ok {
		return nil
	}
	return as
}

func (ms *Measurements) FindApplicationSignal(stamp Stamp, name string) *ApplicationSignal {
	return ms.FindApplicationSignalWithTolerance(stamp, name, 0)
}

func (ms *Measurements) FindHostSignalWithTolerance(stamp Stamp, name string, tolerance int) *HostSignal {

	typ := reflect.TypeFor[*HostSignal]()
	s := ms.FindSignalWithToleranceByType(stamp, name, tolerance, typ)
	if utils.IsEmpty(s) {
		return nil
	}
	hs, ok := s.(*HostSignal)
	if !ok {
		return nil
	}
	return hs
}

func (ms *Measurements) FindHostSignal(stamp Stamp, name string) *HostSignal {
	return ms.FindHostSignalWithTolerance(stamp, name, 0)
}

func (ms *Measurements) FirstStamp() Stamp {

	if ms.first == math.MaxUint64 {
		return 0
	}
	return ms.first
}

func (ms *Measurements) LastStamp() Stamp {
	return ms.last
}

/*func (ms *Measurements) LastApplicationSignal(name string) *ApplicationSignal {

	stamp := ms.LastStamp()
	if stamp == 0 {
		return nil
	}
	return ms.FindApplicationSignal(stamp, name)
}*/

func (ms *Measurements) unsafeApplicationSignals(stamp Stamp, apps []*Application) []*ApplicationSignal {

	m := []*ApplicationSignal{}

	signals := ms.Items[stamp]
	if signals == nil {
		return m
	}

	// improve performance
	mm := make(map[*Application]struct{}, len(apps))
	for _, a := range apps {
		mm[a] = struct{}{}
	}

	for _, s := range signals.GetItems() {

		as, ok := s.(*ApplicationSignal)
		if !ok {
			continue
		}

		_, exists := mm[as.Application]
		if len(apps) == 0 || exists {
			if !utils.Contains(m, as) {
				m = append(m, as)
			}
		}
	}
	return m
}

func (ms *Measurements) ApplicationSignals(stamp Stamp, apps []*Application) []*ApplicationSignal {

	ms.mu.Lock()
	defer ms.mu.Unlock()

	return ms.unsafeApplicationSignals(stamp, apps)
}

func (ms *Measurements) ApplicationSignalsByNames(stamp Stamp, names []string) []*ApplicationSignal {

	apps := []*Application{}
	for _, n := range names {
		app := ms.applications.Find(stamp, n)
		if app == nil {
			continue
		}
		apps = append(apps, app)
	}

	return ms.unsafeApplicationSignals(stamp, apps)
}

func (ms *Measurements) HostSignals(stamp Stamp, hosts []*Host) []*HostSignal {

	ms.mu.Lock()
	defer ms.mu.Unlock()

	m := []*HostSignal{}

	signals := ms.Items[stamp]
	if signals == nil {
		return m
	}

	// improve performance
	mm := make(map[*Host]struct{}, len(hosts))
	for _, h := range hosts {
		mm[h] = struct{}{}
	}

	for _, s := range signals.GetItems() {

		hs, ok := s.(*HostSignal)
		if !ok {
			continue
		}
		_, exists := mm[hs.host]
		if len(hosts) == 0 || exists {
			if !utils.Contains(m, hs) {
				m = append(m, hs)
			}
		}
	}
	return m
}

func (ms *Measurements) HostSignalsByNames(stamp Stamp, names []string) []*HostSignal {

	hosts := []*Host{}
	for _, n := range names {
		app := ms.hosts.Find(stamp, n)
		if app == nil {
			continue
		}
		hosts = append(hosts, app)
	}

	return ms.HostSignals(stamp, hosts)
}

func (ms *Measurements) Applications(stamp Stamp, hosts []*Host) []*Application {

	ms.mu.Lock()
	defer ms.mu.Unlock()

	m := []*Application{}
	signals := ms.Items[stamp]
	if signals == nil {
		return m
	}

	// improve performance
	mm := make(map[*Host]struct{}, len(hosts))
	for _, h := range hosts {
		mm[h] = struct{}{}
	}

	for _, s := range signals.GetItems() {

		as, ok := s.(*ApplicationSignal)
		if !ok {
			continue
		}
		a := as.Application
		if a == nil {
			continue
		}
		_, exists := mm[as.Host]
		if len(hosts) == 0 || exists {
			if !utils.Contains(m, a) {
				m = append(m, a)
			}
		}
	}
	return m
}

func (ms *Measurements) Hosts(stamp Stamp, apps []*Application) []*Host {

	ms.mu.Lock()
	defer ms.mu.Unlock()

	m := []*Host{}
	signals := ms.Items[stamp]
	if signals == nil {
		return m
	}

	// improve performance
	ma := make(map[*Application]struct{}, len(apps))
	for _, a := range apps {
		ma[a] = struct{}{}
	}

	if len(apps) > 0 {
		for _, s := range signals.GetItems() {

			as, ok := s.(*ApplicationSignal)
			if !ok {
				continue
			}
			h := as.Host
			if h == nil {
				continue
			}
			if _, exists := ma[as.Application]; exists {
				if !utils.Contains(m, h) {
					m = append(m, h)
				}
			}
		}
		return m
	}

	for _, s := range signals.GetItems() {

		hs, ok1 := s.(*HostSignal)
		if ok1 {
			h := hs.host
			if h == nil {
				continue
			}
			if !utils.Contains(m, h) {
				m = append(m, h)
			}
			continue
		}

		as, ok2 := s.(*ApplicationSignal)
		if ok2 {
			h := as.Host
			if h == nil {
				continue
			}
			if !utils.Contains(m, h) {
				m = append(m, h)
			}
		}
	}
	return m
}

func (ms *Measurements) unsafeDependencies(stamp Stamp, parents []*Application, exclude []*Application) *Dependencies {

	m := NewDependencies()

	// initialize parent dependencies
	for _, a := range parents {
		m.items[a] = nil
		if !utils.Contains(exclude, a) {
			exclude = append(exclude, a)
		}
	}

	arr := ms.unsafeApplicationSignals(stamp, parents)
	for _, as := range arr {

		a := as.Application
		if a == nil {
			continue
		}

		var new *Dependencies = nil

		backends := as.Backends(stamp)
		if len(backends) > 0 {

			newbacks := []*Application{}
			for _, b := range backends {
				if utils.Contains(exclude, b) {
					continue
				}
				newbacks = append(newbacks, b)
			}

			if len(newbacks) > 0 {
				new = ms.unsafeDependencies(stamp, newbacks, exclude)
			}
		}

		old, ok := m.items[a]
		if !ok {
			m.items[a] = new
		} else {
			if old != nil && new != nil {
				deps := NewDependencies()
				deps.items = MergeMaps(old.items, new.items)
				m.items[a] = deps
			} else if new != nil {
				m.items[a] = new
			}
		}
	}

	if len(m.items) > 0 {
		return m
	}

	return nil
}

func (ms *Measurements) Dependencies(stamp Stamp, parents []*Application) *Dependencies {

	ms.mu.Lock()
	defer ms.mu.Unlock()

	exclude := []*Application{}
	return ms.unsafeDependencies(stamp, parents, exclude)
}

func (ms *Measurements) DependenciesByNames(stamp Stamp, names []string) *Dependencies {

	apps := []*Application{}
	for _, n := range names {
		app := ms.applications.Find(stamp, n)
		if app == nil {
			continue
		}
		apps = append(apps, app)
	}
	return ms.Dependencies(stamp, apps)
}

func NewMeasurements(hosts *Hosts, applications *Applications) *Measurements {

	return &Measurements{
		first:        math.MaxUint64,
		last:         0,
		hosts:        hosts,
		applications: applications,
		Items:        make(map[Stamp]*Signals),
	}
}
