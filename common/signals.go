package common

import (
	"math"
	"reflect"
	"sync"

	"github.com/devopsext/utils"
	"github.com/puzpuzpuz/xsync/v4"
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

/*
type NamesItems = map[Hash]string
type Names struct {
	mu    sync.Mutex
	items NamesItems
}*/

type NamesItems = *xsync.Map[Hash, string]
type Names struct {
	items NamesItems
}

/*type AttributesItems = map[Hash]Labels
type Attributes struct {
	mu    sync.Mutex
	items AttributesItems
}*/

type AttributesItems = *xsync.Map[Hash, Labels]
type Attributes struct {
	items AttributesItems
}

type Host struct {
	On     *Host
	Name   Hash
	Labels Hash
}

type HostsItems = map[Stamp]map[Hash]*Host
type Hosts struct {
	mu    sync.Mutex
	items HostsItems
}

const (
	ApplicationName = "name"
)

type Application struct {
	Name   Hash
	Labels Hash
}

type ApplicationsItems = map[Stamp]map[Hash]*Application
type Applications struct {
	mu    sync.Mutex
	items ApplicationsItems
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

type IncomingTrafficItems = map[TrafficKind]map[Hash]Traffic
type IncomingTraffic struct {
	Items IncomingTrafficItems
}

type IncomingErrorsItems = map[Hash]Errors
type IncomingErrors struct {
	Items IncomingErrorsItems
}

type IncomingLatencyItems = map[Hash]Latency
type IncomingLatency struct {
	Items IncomingLatencyItems
}

type OutgoingTrafficItems = map[TrafficKind]map[Hash]Traffic
type OutgoingTraffic struct {
	Items OutgoingTrafficItems
}

type OutgoingErrorsItems = map[Hash]Errors
type OutgoingErrors struct {
	Items OutgoingErrorsItems
}

type OutgoingLatencyItems = map[Hash]Latency
type OutgoingLatency struct {
	Items OutgoingLatencyItems
}

type ApplicationSaturationItems = map[ApplicationSaturationKind]map[Hash]Saturation
type ApplicationSaturation struct {
	Items ApplicationSaturationItems
}

type HostSaturationItems = map[HostSaturationKind]map[Hash]Saturation
type HostSaturation struct {
	Items HostSaturationItems
}

const (
	//HostSignalName           = "name"
	HostSignalHost           = "host"
	HostSignalSaturationKind = "kind"
)

type HostSignal struct {
	Name       Hash
	host       *Host
	Saturation HostSaturation
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
	Name        Hash
	Application Hash
	Host        Hash

	IncomingTraffic IncomingTraffic
	IncomingErrors  IncomingErrors
	IncomingLatency IncomingLatency

	OutgoingTraffic OutgoingTraffic
	OutgoingErrors  OutgoingErrors
	OutgoingLatency OutgoingLatency

	Saturation ApplicationSaturation
}

type Dependencies struct {
	items map[*Application]*Dependencies
}

type Signal interface {
	GetName() Hash
	Merge(s Signal)
}

/*
type Signals struct {
	mu    sync.Mutex
	Items map[Hash]Signal
}
*/

type MeasurementsItems = map[Stamp]map[Hash]Signal
type Measurements struct {
	mu    sync.Mutex
	first Stamp
	last  Stamp
	items MeasurementsItems
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

/*func (hs *Attributes) IsEmpty() bool {

	hs.mu.Lock()
	defer hs.mu.Unlock()

	return len(hs.items) == 0
}

func (hs *Attributes) GetItems() AttributesItems {

	hs.mu.Lock()
	defer hs.mu.Unlock()

	return hs.items
}

func (hs *Attributes) SetItems(items AttributesItems) {

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
		hs.items = make(AttributesItems)
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
		items: make(AttributesItems),
	}
}*/

func (as *Attributes) IsEmpty() bool {

	if as.items == nil {
		return true
	}
	return as.items.Size() == 0
}

func (as *Attributes) GetItems() AttributesItems {

	return as.items
}

func (as *Attributes) SetItems(items AttributesItems) {

	as.items = items
}

func (as *Attributes) LabelsHash(labels Labels) Hash {
	if len(labels) == 0 {
		return 0
	}
	return Map2Hash32(labels)
}

func (as *Attributes) AddOrUpdate(labels Labels) Hash {

	hash := as.LabelsHash(labels)
	if hash == 0 {
		return hash
	}

	if as.items == nil {
		as.items = xsync.NewMap[Hash, Labels]()
	}

	// Perform the map operation outside the heavy struct lock
	// because xsync handles its internal concurrency safely.
	as.items.Store(hash, labels)
	return hash
}

func (as *Attributes) Find(hash Hash) Labels {

	if as.items == nil {
		return nil
	}

	v, ok := as.items.Load(hash)
	if !ok {
		return nil
	}
	return v
}

func NewAttributes() *Attributes {
	return &Attributes{
		items: xsync.NewMap[Hash, Labels](),
	}
}

// Names

/*
func (ns *Names) IsEmpty() bool {

	ns.mu.Lock()
	defer ns.mu.Unlock()

	return len(ns.items) == 0
}

func (ns *Names) GetItems() NamesItems {

	ns.mu.Lock()
	defer ns.mu.Unlock()

	return ns.items
}

func (ns *Names) SetItems(items NamesItems) {

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
		ns.items = make(NamesItems)
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
		items: make(NamesItems),
	}
}*/

func (ns *Names) IsEmpty() bool {

	if ns.items == nil {
		return true
	}
	return ns.items.Size() == 0
}

func (ns *Names) GetItems() NamesItems {

	return ns.items
}

func (ns *Names) SetItems(items NamesItems) {

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

	if ns.items == nil {
		ns.items = xsync.NewMap[Hash, string]()
	}

	// Perform the map operation outside the heavy struct lock
	// because xsync handles its internal concurrency safely.
	ns.items.Store(hash, name)
	return hash
}

func (ns *Names) Find(hash Hash) string {

	if ns.items == nil {
		return ""
	}

	v, ok := ns.items.Load(hash)
	if !ok {
		return ""
	}
	return v
}

func NewNames() *Names {
	return &Names{
		items: xsync.NewMap[Hash, string](),
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

	return it.Items[kind]
}

func (it *IncomingTraffic) AddOrUpdate(value float64, hash Hash) {

	if it.Items == nil {
		it.Items = make(map[TrafficKind]map[Hash]Traffic)
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

	if ie.Items == nil {
		ie.Items = make(map[Hash]Errors)
	}

	errors, ok := ie.Items[hash]
	if !ok {
		ie.Items[hash] = value
	} else {
		ie.Items[hash] = (errors + value) / 2
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

	if il.Items == nil {
		il.Items = make(map[Hash]Latency)
	}

	errors, ok := il.Items[hash]
	if !ok {
		il.Items[hash] = value
	} else {
		il.Items[hash] = (errors + value) / 2
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

	if ot.Items == nil {
		ot.Items = make(map[TrafficKind]map[Hash]Traffic)
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

	if oe.Items == nil {
		oe.Items = make(map[Hash]Errors)
	}

	errors, ok := oe.Items[hash]
	if !ok {
		oe.Items[hash] = value
	} else {
		oe.Items[hash] = (errors + value) / 2
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

	if ol.Items == nil {
		ol.Items = make(map[Hash]Latency)
	}

	errors, ok := ol.Items[hash]
	if !ok {
		ol.Items[hash] = value
	} else {
		ol.Items[hash] = (errors + value) / 2
	}
}

// ApplicationSaturation

func (as *ApplicationSaturation) AddOrUpdate(value float64, hash Hash) {

	if as.Items == nil {
		as.Items = make(map[ApplicationSaturationKind]map[Hash]Saturation)
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

// HostSaturation

func (hs *HostSaturation) AddOrUpdate(value float64, hash Hash) {

	if hs.Items == nil {
		hs.Items = make(map[HostSaturationKind]map[Hash]Saturation)
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

func (hs *Hosts) GetItems() HostsItems {

	hs.mu.Lock()
	defer hs.mu.Unlock()

	return hs.items
}

func (hs *Hosts) SetItems(items HostsItems) {

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
	if n == 0 {
		return
	}

	if hs.items == nil {
		hs.items = make(HostsItems)
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
		hs.items = make(HostsItems)
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
		items: make(HostsItems),
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
	if n == 0 {
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

func (hs *HostSignal) GetName() Hash {
	return hs.Name
}

func (hs *HostSignal) GetHost() *Host {
	return hs.host
}

func (hs *HostSignal) Merge(s Signal) {
	// should check all fields of HostSignal
}

// ApplicationSignal

func (as *ApplicationSignal) GetName() Hash {
	return as.Name
}

func (as *ApplicationSignal) GetApplication() Hash {
	return as.Application
}

func (as *ApplicationSignal) GetHost() Hash {
	return as.Host
}

func (as *ApplicationSignal) Frontends(stamp Stamp) []*Application {

	traffic := as.IncomingTraffic.Frontends(stamp, allTrafficKinds)
	errors := as.IncomingErrors.Frontends(stamp)
	latency := as.IncomingLatency.Frontends(stamp)

	arr := []*Application{}
	arr = append(arr, traffic...)
	arr = append(arr, errors...)
	arr = append(arr, latency...)

	return ApplicationsCompact(arr)
}

func (as *ApplicationSignal) Backends(stamp Stamp) []*Application {

	traffic := as.OutgoingTraffic.Backends(stamp, allTrafficKinds)
	errors := as.OutgoingErrors.Backends(stamp)
	latency := as.OutgoingLatency.Backends(stamp)

	arr := []*Application{}
	arr = append(arr, traffic...)
	arr = append(arr, errors...)
	arr = append(arr, latency...)

	return ApplicationsCompact(arr)
}

func (as *ApplicationSignal) Merge(s Signal) {
	// should check all fields of ApplicationSignal
}

func NewApplicationSignal(name Hash, app Hash, host Hash) *ApplicationSignal {

	return &ApplicationSignal{

		Name:        name,
		Application: app,
		Host:        host,
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

/*
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

	n := s.GetName()
	if n == 0 {
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
*/
// Measurements

func (ms *Measurements) GetItems() MeasurementsItems {

	ms.mu.Lock()
	defer ms.mu.Unlock()

	return ms.items
}

func (ms *Measurements) SetItems(items MeasurementsItems) {

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

	n := s.GetName()
	if n == 0 {
		return
	}

	ss := ms.items[stamp]
	if ss == nil {
		ss = make(map[Hash]Signal)
		ms.items[stamp] = ss
	}

	sOld := ss[n]
	if utils.IsEmpty(sOld) {
		ss[n] = s
		return
	}
	sOld.Merge(s)

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
		s := ss[hash]
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

	if ss == nil || len(ss) == 0 {
		return nil
	}

	if tolerance < 0 {
		s := ms.unsafeLookBackByType(stamp, hash, tolerance, typ)
		if utils.IsEmpty(s) {
			return s
		}
	}

	return ss[hash]
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

/*
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
*/
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

	for _, s := range signals {

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

/*
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
*/
func (ms *Measurements) BuildApplicationSignalName(app, host string) string {

	if app == "" {
		return ""
	}
	return app + "/" + host
}

func NewMeasurements() *Measurements {

	return &Measurements{
		first: math.MaxUint64,
		last:  0,
		items: make(MeasurementsItems),
	}
}
