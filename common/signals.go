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

type Attributes struct {
	mu    sync.Mutex
	items map[Hash]Labels
}

type Names struct {
	mu    sync.Mutex
	items map[Hash]string
}

type Host struct {
	On     *Host
	Name   Hash
	Labels Hash
}

type Hosts struct {
	mu    sync.Mutex
	items map[Stamp]map[Hash]*Host
}

const (
	ApplicationName = "name"
)

type Application struct {
	Name   Hash
	Labels Hash
}

type Applications struct {
	mu    sync.Mutex
	items map[Stamp]map[Hash]*Application
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
	items map[TrafficKind]map[Hash]Traffic
}

type IncomingErrors struct {
	items map[Hash]Errors
}

type IncomingLatency struct {
	items map[Hash]Latency
}

type OutgoingTraffic struct {
	items map[TrafficKind]map[Hash]Traffic
}

type OutgoingErrors struct {
	items map[Hash]Errors
}

type OutgoingLatency struct {
	items map[Hash]Latency
}

type ApplicationSaturation struct {
	items map[ApplicationSaturationKind]map[Hash]Saturation
}

type HostSaturation struct {
	items map[HostSaturationKind]map[Hash]Saturation
}

const (
	//HostSignalName           = "name"
	HostSignalHost           = "host"
	HostSignalSaturationKind = "kind"
)

type HostSignal struct {
	hash       Hash
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
	hash        Hash
	application *Application
	host        *Host

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
	//	Name(names *Names) Hash
	Hash() Hash
	Merge(s Signal)
}

type Signals struct {
	mu    sync.Mutex
	items map[Hash]Signal
}

type Measurements struct {
	mu    sync.Mutex
	first Stamp
	last  Stamp
	items map[Stamp]*Signals
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

// Attributes

func (hs *Attributes) IsEmpty() bool {

	hs.mu.Lock()
	defer hs.mu.Unlock()

	return len(hs.items) == 0
}

func (hs *Attributes) GetItems() map[Hash]Labels {

	hs.mu.Lock()
	defer hs.mu.Unlock()

	return hs.items
}

func (hs *Attributes) SetItems(items map[Hash]Labels) {

	hs.mu.Lock()
	defer hs.mu.Unlock()

	hs.items = items
}

func (hs *Attributes) LabelsHash(labels Labels) Hash {

	if labels == nil {
		return 0
	}
	return Map2Hash32(labels)
}

func (hs *Attributes) AddOrUpdate(labels Labels) Hash {

	hash := hs.LabelsHash(labels)
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

func (hs *Attributes) Find(hash Hash) Labels {

	hs.mu.Lock()
	defer hs.mu.Unlock()

	return hs.items[hash]
}

func NewAttributes() *Attributes {

	return &Attributes{
		items: make(map[Hash]Labels),
	}
}

// Names

func (ns *Names) IsEmpty() bool {

	ns.mu.Lock()
	defer ns.mu.Unlock()

	return len(ns.items) == 0
}

func (ns *Names) GetItems() map[Hash]string {

	ns.mu.Lock()
	defer ns.mu.Unlock()

	return ns.items
}

func (ns *Names) SetItems(items map[Hash]string) {

	ns.mu.Lock()
	defer ns.mu.Unlock()

	ns.items = items
}

func (ns *Names) NameHash(name string) Hash {

	if utils.IsEmpty(name) {
		return 0
	}
	return String2Hash32(name)
}

func (ns *Names) AddOrUpdate(name string) Hash {

	hash := ns.NameHash(name)
	if hash == 0 {
		return hash
	}

	ns.mu.Lock()
	defer ns.mu.Unlock()

	if ns.items == nil {
		ns.items = make(map[Hash]string)
	}

	_, ok := ns.items[hash]
	if !ok {
		ns.items[hash] = name
	}
	return hash
}

func (ns *Names) Find(hash Hash) string {

	ns.mu.Lock()
	defer ns.mu.Unlock()

	return ns.items[hash]
}

func NewNames() *Names {

	return &Names{
		items: make(map[Hash]string),
	}
}

// IncomingTraffic

func (it *IncomingTraffic) Frontends(stamp Stamp, kinds []TrafficKind) []*Application {

	apps := []*Application{}
	/*
		for _, kind := range kinds {
			for hash := range it.items[kind] {

				lbs := it.Attributes.Find(hash)
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
		}*/
	return apps
}

func (it *IncomingTraffic) Value(kind TrafficKind) map[Hash]Traffic {

	return it.items[kind]
}

func (it *IncomingTraffic) AddOrUpdate(value float64, hash Hash) {

	if it.items == nil {
		it.items = make(map[TrafficKind]map[Hash]Traffic)
	}
	/*
	   kind := TrafficKindUnknown
	   lbs := it.Attributes.Find(hash)

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
	*/
}

func NewIncomingTraffic() *IncomingTraffic {

	return &IncomingTraffic{
		items: make(map[TrafficKind]map[Hash]Traffic),
	}
}

// IncomingErrors

func (ie *IncomingErrors) Frontends(stamp Stamp) []*Application {

	apps := []*Application{}
	/*
		for hash := range ie.items {

			lbs := ie.Attributes.Find(hash)
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
		}*/
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

func NewIncomingErrors() *IncomingErrors {

	return &IncomingErrors{
		items: make(map[Hash]Errors),
	}
}

// IncomingLatency

func (il *IncomingLatency) Frontends(stamp Stamp) []*Application {

	apps := []*Application{}
	/*
		for hash := range il.items {

			lbs := il.Attributes.Find(hash)
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
		}*/
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

func NewIncomingLatency() *IncomingLatency {

	return &IncomingLatency{
		items: make(map[Hash]Latency),
	}
}

// OutgoingTraffic

func (ot *OutgoingTraffic) Backends(stamp Stamp, kinds []TrafficKind) []*Application {

	apps := []*Application{}
	/*
		for _, kind := range kinds {
			for hash := range ot.items[kind] {

				lbs := ot.Attributes.Find(hash)
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
		}*/
	return apps
}

func (ot *OutgoingTraffic) AddOrUpdate(value float64, hash Hash) {

	if ot.items == nil {
		ot.items = make(map[TrafficKind]map[Hash]Traffic)
	}
	/*
	   kind := TrafficKindUnknown
	   lbs := ot.Attributes.Find(hash)

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
	*/
}

func NewOutgoingTraffic() *OutgoingTraffic {

	return &OutgoingTraffic{
		items: make(map[TrafficKind]map[Hash]Traffic),
	}
}

// OutgoingErrors

func (oe *OutgoingErrors) Backends(stamp Stamp) []*Application {

	apps := []*Application{}
	/*
		for hash := range oe.items {

			lbs := oe.Attributes.Find(hash)
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
		}*/
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

func NewOutgoingErrors() *OutgoingErrors {

	return &OutgoingErrors{
		items: make(map[Hash]Errors),
	}
}

// OutgoingLatency

func (ol *OutgoingLatency) Backends(stamp Stamp) []*Application {

	apps := []*Application{}
	/*
		for hash := range ol.items {

			lbs := ol.Attributes.Find(hash)
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
		}*/
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

func NewOutgoingLatency() *OutgoingLatency {

	return &OutgoingLatency{
		items: make(map[Hash]Latency),
	}
}

// ApplicationSaturation

func (as *ApplicationSaturation) AddOrUpdate(value float64, hash Hash) {

	if as.items == nil {
		as.items = make(map[ApplicationSaturationKind]map[Hash]Saturation)
	}
	/*
	   kind := ApplicationSaturationKindUnknown
	   lbs := as.Attributes.Find(hash)

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
	*/
}

func NewApplicationSaturation() *ApplicationSaturation {

	return &ApplicationSaturation{
		items: make(map[ApplicationSaturationKind]map[Hash]Saturation),
	}
}

// HostSaturation

func (hs *HostSaturation) AddOrUpdate(value float64, hash Hash) {

	if hs.items == nil {
		hs.items = make(map[HostSaturationKind]map[Hash]Saturation)
	}
	/*
	   kind := HostSaturationKindUnknown
	   lbs := hs.Attributes.Find(hash)

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
	*/
}

func NewHostSaturation() *HostSaturation {

	return &HostSaturation{
		items: make(map[HostSaturationKind]map[Hash]Saturation),
	}
}

// Host

func (h *Host) GetLabels(attributes *Attributes) Labels {

	lhs := attributes
	if lhs == nil {
		return nil
	}
	lbs := lhs.Find(h.Labels)
	if lbs == nil {
		return nil
	}
	return lbs
}

func (h *Host) GetName(names *Names) string {

	lbs := names
	if lbs == nil {
		return ""
	}
	return names.Find(h.Name)
}

func (h *Host) Same(host *Host) bool {

	if host == nil {
		return false
	}

	if h == host {
		return true
	}

	if h.Name != host.Name {
		return false
	}
	return true
}

func (h *Host) Copy(host *Host) {

	if h.Same(host) {
		return
	}
}

func NewHost(name, labels Hash, on *Host) *Host {

	return &Host{
		Name:   name,
		Labels: labels,
		On:     on,
	}
}

// Hosts

func (hs *Hosts) GetItems() map[Stamp]map[Hash]*Host {

	hs.mu.Lock()
	defer hs.mu.Unlock()

	return hs.items
}

func (hs *Hosts) SetItems(items map[Stamp]map[Hash]*Host) {

	hs.mu.Lock()
	defer hs.mu.Unlock()

	hs.items = items
}

func (hs *Hosts) AddOrUpdate(stamp Stamp, h *Host) {

	hs.mu.Lock()
	defer hs.mu.Unlock()

	if h == nil {
		return
	}

	n := h.Name
	if utils.IsEmpty(n) {
		return
	}

	if hs.items == nil {
		hs.items = make(map[Stamp]map[Hash]*Host)
	}

	m := hs.items[stamp]
	if m == nil {
		m = make(map[Hash]*Host)
		hs.items[stamp] = m
	}

	old := m[n]
	if old == nil {
		m[n] = h
	} else {
		old.Copy(h)
	}
}

func (hs *Hosts) unsafeLookBack(stamp Stamp, hash Hash, tolerance int) *Host {

	tb := stamp
	abs := math.Abs(float64(tolerance))
	for {
		tb--
		m := hs.items[tb]
		diff := stamp - tb
		if m == nil && diff < Stamp(abs) {
			continue
		}
		if m == nil {
			return nil
		}
		a := m[hash]
		if a != nil {
			return a
		}
		return nil
	}
}

func (hs *Hosts) FindWithTolerance(stamp Stamp, hash Hash, tolerance int) *Host {

	if hash == 0 {
		return nil
	}

	hs.mu.Lock()
	defer hs.mu.Unlock()

	tc := hs.items[stamp]

	if tolerance == 0 && tc == nil {
		return nil
	}

	if tolerance < 0 {
		h := hs.unsafeLookBack(stamp, hash, tolerance)
		if h != nil {
			return h
		}
	}

	return tc[hash]
}

func (hs *Hosts) Find(stamp Stamp, hash Hash) *Host {

	return hs.FindWithTolerance(stamp, hash, 0)
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
		hs.items = make(map[Stamp]map[Hash]*Host)
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
		items: make(map[Stamp]map[Hash]*Host),
	}
}

// Application

func (a *Application) GetLabels(attributes *Attributes) Labels {

	lhs := attributes
	if lhs == nil {
		return nil
	}
	lbs := lhs.Find(a.Labels)
	if lbs == nil {
		return nil
	}
	return lbs
}

func (a *Application) GetName(names *Names) string {

	lbs := names
	if lbs == nil {
		return ""
	}
	return names.Find(a.Name)
}

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
	return true
}

func (a *Application) Copy(app *Application) {

	if a.Same(app) {
		return
	}
}

func NewApplication(name, labels Hash) *Application {

	return &Application{
		Name:   name,
		Labels: labels,
	}
}

// Applications

func (as *Applications) GetItems() map[Stamp]map[Hash]*Application {

	as.mu.Lock()
	defer as.mu.Unlock()

	return as.items
}

func (as *Applications) SetItems(items map[Stamp]map[Hash]*Application) {

	as.mu.Lock()
	defer as.mu.Unlock()

	as.items = items
}

func (as *Applications) AddOrUpdate(stamp Stamp, a *Application) {

	as.mu.Lock()
	defer as.mu.Unlock()

	if a == nil {
		return
	}

	n := a.Name
	if utils.IsEmpty(n) {
		return
	}

	if as.items == nil {
		as.items = make(map[Stamp]map[Hash]*Application)
	}

	m := as.items[stamp]
	if m == nil {
		m = make(map[Hash]*Application)
		as.items[stamp] = m
	}

	old := m[n]
	if old == nil {
		m[n] = a
	} else {
		old.Copy(a)
	}
}

func (as *Applications) unsafeLookBack(stamp Stamp, hash Hash, tolerance int) *Application {

	tb := stamp
	abs := math.Abs(float64(tolerance))
	for {
		tb--
		m := as.items[tb]
		diff := stamp - tb
		if m == nil && diff < Stamp(abs) {
			continue
		}
		if m == nil {
			return nil
		}
		a := m[hash]
		if a != nil {
			return a
		}
		return nil
	}
}

func (as *Applications) FindWithTolerance(stamp Stamp, hash Hash, tolerance int) *Application {

	if hash == 0 {
		return nil
	}

	as.mu.Lock()
	defer as.mu.Unlock()

	tc := as.items[stamp]
	if tolerance == 0 && tc == nil {
		return nil
	}

	if tolerance < 0 {
		a := as.unsafeLookBack(stamp, hash, tolerance)
		if a != nil {
			return a
		}
	}
	return tc[hash]
}

func (as *Applications) Find(stamp Stamp, hash Hash) *Application {
	return as.FindWithTolerance(stamp, hash, 0)
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
		as.items = make(map[Stamp]map[Hash]*Application)
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
		items: make(map[Stamp]map[Hash]*Application),
	}
}

// HostSignal

func (hs *HostSignal) Hash() Hash {
	return 0
}

/*func (hs *HostSignal) Name() string {

	name := ""
	if hs.host != nil {
		name = hs.host.Name()
	}
	return name
}*/

func (hs *HostSignal) GetHost() *Host {
	return hs.host
}

/*
func (hs *HostSignal) GetHostHash() Hash {
	if hs.host == nil {
		return 0
	}
	return hs.host.Hash
}
*/

func (hs *HostSignal) Merge(s Signal) {
	// should check all fields of HostSignal
}

func (hs *HostSignal) Saturation() *HostSaturation {
	return hs.saturation
}

func NewHostSignal(hash Hash, host *Host) *HostSignal {

	return &HostSignal{

		hash:       hash,
		host:       host,
		saturation: NewHostSaturation(),
	}
}

// ApplicationSignal

func (as *ApplicationSignal) Hash() Hash {
	return 0
}

func (as *ApplicationSignal) GetApplication() *Application {
	return as.application
}

func (as *ApplicationSignal) GetHost() *Host {
	return as.host
}

func (as *ApplicationSignal) GetIncomingTraffic() *IncomingTraffic {
	return as.incomingTraffic
}

func (as *ApplicationSignal) GetIncomingErrors() *IncomingErrors {
	return as.incomingErrors
}

func (as *ApplicationSignal) GetIncomingLatency() *IncomingLatency {
	return as.incomingLatency
}

func (as *ApplicationSignal) GetOutgoingTraffic() *OutgoingTraffic {
	return as.outgoingTraffic
}

func (as *ApplicationSignal) GetOutgoingErrors() *OutgoingErrors {
	return as.outgoingErrors
}

func (as *ApplicationSignal) GetOutgoingLatency() *OutgoingLatency {
	return as.outgoingLatency
}

func (as *ApplicationSignal) GetSaturation() *ApplicationSaturation {
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

/*func (as *ApplicationSignal) Name() string {

	appName := ""
	if as.application != nil {
		appName = as.application.Name()
	} else {
		return ""
	}

	hostName := ""
	if as.host != nil {
		hostName = as.host.Name()
	}
	return appName + "/" + hostName
}*/

func (as *ApplicationSignal) Merge(s Signal) {
	// should check all fields of ApplicationSignal
}

func NewApplicationSignal(hash Hash, applications *Applications, app *Application, host *Host) *ApplicationSignal {

	return &ApplicationSignal{

		hash:        hash,
		application: app,
		host:        host,

		incomingTraffic: NewIncomingTraffic(),
		incomingErrors:  NewIncomingErrors(),
		incomingLatency: NewIncomingLatency(),

		outgoingTraffic: NewOutgoingTraffic(),
		outgoingErrors:  NewOutgoingErrors(),
		outgoingLatency: NewOutgoingLatency(),

		saturation: NewApplicationSaturation(),
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

func (ss *Signals) GetItems() map[Hash]Signal {

	ss.mu.Lock()
	defer ss.mu.Unlock()

	return ss.items
}

func (ss *Signals) SetItems(items map[Hash]Signal) {

	ss.mu.Lock()
	defer ss.mu.Unlock()

	ss.items = items
}

func (ss *Signals) AddOrUpdate(s Signal) {

	ss.mu.Lock()
	defer ss.mu.Unlock()

	if utils.IsEmpty(s) {
		return
	}

	n := s.Hash()
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

func (ss *Signals) Find(hash Hash) Signal {

	ss.mu.Lock()
	defer ss.mu.Unlock()

	return ss.items[hash]
}

func NewSignals() *Signals {

	return &Signals{
		items: make(map[Hash]Signal),
	}
}

// Measurements

func (ms *Measurements) GetItems() map[Stamp]*Signals {

	ms.mu.Lock()
	defer ms.mu.Unlock()

	return ms.items
}

func (ms *Measurements) SetItems(items map[Stamp]*Signals) {

	ms.mu.Lock()
	defer ms.mu.Unlock()

	ms.items = items
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

	ss := ms.items[stamp]
	if ss == nil {
		ss = NewSignals()
		ms.items[stamp] = ss
	}

	ss.AddOrUpdate(s)
	ms.items[stamp] = ss
}

func (ms *Measurements) unsafeLookBackByType(stamp Stamp, hash Hash, tolerance int, typ reflect.Type) Signal {

	tb := stamp
	abs := math.Abs(float64(tolerance))
	for {
		tb--
		ss := ms.items[tb]
		diff := stamp - tb
		if ss == nil && diff < Stamp(abs) {
			continue
		}
		if ss == nil {
			return nil
		}
		s := ss.items[hash]
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

func (ms *Measurements) FindSignalWithToleranceByType(stamp Stamp, hash Hash, tolerance int, typ reflect.Type) Signal {

	if hash == 0 {
		return nil
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()

	ss := ms.items[stamp]

	if ss == nil || ss.items == nil {
		return nil
	}

	if tolerance < 0 {
		s := ms.unsafeLookBackByType(stamp, hash, tolerance, typ)
		if utils.IsEmpty(s) {
			return s
		}
	}

	return ss.items[hash]
}

func (ms *Measurements) FindApplicationSignalWithTolerance(stamp Stamp, hash Hash, tolerance int) *ApplicationSignal {

	typ := reflect.TypeFor[*ApplicationSignal]()
	s := ms.FindSignalWithToleranceByType(stamp, hash, tolerance, typ)
	if utils.IsEmpty(s) {
		return nil
	}
	as, ok := s.(*ApplicationSignal)
	if !ok {
		return nil
	}
	return as
}

func (ms *Measurements) FindApplicationSignal(stamp Stamp, hash Hash) *ApplicationSignal {
	return ms.FindApplicationSignalWithTolerance(stamp, hash, 0)
}

func (ms *Measurements) FindHostSignalWithTolerance(stamp Stamp, hash Hash, tolerance int) *HostSignal {

	typ := reflect.TypeFor[*HostSignal]()
	s := ms.FindSignalWithToleranceByType(stamp, hash, tolerance, typ)
	if utils.IsEmpty(s) {
		return nil
	}
	hs, ok := s.(*HostSignal)
	if !ok {
		return nil
	}
	return hs
}

func (ms *Measurements) FindHostSignal(stamp Stamp, hash Hash) *HostSignal {
	return ms.FindHostSignalWithTolerance(stamp, hash, 0)
}

func (ms *Measurements) GetFirst() Stamp {

	if ms.first == math.MaxUint64 {
		return 0
	}
	return ms.first
}

func (ms *Measurements) SetFirst(stamp Stamp) {
	ms.first = stamp
}

func (ms *Measurements) GetLast() Stamp {
	return ms.last
}

func (ms *Measurements) SetLast(stamp Stamp) {
	ms.last = stamp
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

	signals := ms.items[stamp]
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

		_, exists := mm[as.application]
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

/*func (ms *Measurements) ApplicationSignalsByNames(stamp Stamp, names []string) []*ApplicationSignal {

	apps := []*Application{}
	for _, n := range names {
		app := ms.applications.Find(stamp, n)
		if app == nil {
			continue
		}
		apps = append(apps, app)
	}

	return ms.unsafeApplicationSignals(stamp, apps)
}*/

func (ms *Measurements) HostSignals(stamp Stamp, hosts []*Host) []*HostSignal {

	ms.mu.Lock()
	defer ms.mu.Unlock()

	m := []*HostSignal{}

	signals := ms.items[stamp]
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

/*func (ms *Measurements) HostSignalsByNames(stamp Stamp, names []string) []*HostSignal {

	hosts := []*Host{}
	for _, n := range names {
		app := ms.hosts.Find(stamp, n)
		if app == nil {
			continue
		}
		hosts = append(hosts, app)
	}

	return ms.HostSignals(stamp, hosts)
}*/

func (ms *Measurements) Applications(stamp Stamp, hosts []*Host) []*Application {

	ms.mu.Lock()
	defer ms.mu.Unlock()

	m := []*Application{}
	signals := ms.items[stamp]
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
		a := as.application
		if a == nil {
			continue
		}
		_, exists := mm[as.host]
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
	signals := ms.items[stamp]
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
			h := as.host
			if h == nil {
				continue
			}
			if _, exists := ma[as.application]; exists {
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
			h := as.host
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

		a := as.application
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

func (ms *Measurements) DependenciesByNames(applications *Applications, stamp Stamp, hashes []Hash) *Dependencies {

	apps := []*Application{}
	for _, n := range hashes {
		app := applications.Find(stamp, n)
		if app == nil {
			continue
		}
		apps = append(apps, app)
	}
	return ms.Dependencies(stamp, apps)
}

func NewMeasurements() *Measurements {

	return &Measurements{
		first: math.MaxUint64,
		last:  0,
		items: make(map[Stamp]*Signals),
	}
}
