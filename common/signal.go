package common

import (
	"math"
	"sync"

	"github.com/devopsext/utils"
	"github.com/puzpuzpuz/xsync/v4"
)

type Labels = map[string]string
type Stamp = uint64 // in milliseconds

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
	on     *Host
	hash   Hash
	labels Hash
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
	hash   Hash
	labels Hash
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
	hash       Hash
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
	hash        Hash
	application Hash
	host        Hash

	IncomingTraffic IncomingTraffic
	IncomingErrors  IncomingErrors
	IncomingLatency IncomingLatency

	OutgoingTraffic OutgoingTraffic
	OutgoingErrors  OutgoingErrors
	OutgoingLatency OutgoingLatency

	Saturation ApplicationSaturation
}

type Dependencies struct {
	items map[Hash]*Dependencies
}

type Signal interface {
	Hash() Hash
	Merge(s Signal)
	ContainsAny(hashs []Hash) bool
	AsApplicationSignal() *ApplicationSignal
	AsHostSignal() *HostSignal
}

type MeasurementsItems = map[Hash]map[Stamp]Signal
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

func (ns *Names) FindByHash(hash Hash) string {

	if ns.items == nil {
		return ""
	}

	v, ok := ns.items.Load(hash)
	if !ok {
		return ""
	}
	return v
}

func (ns *Names) FindByHashes(hashes []Hash) []string {

	r := []string{}

	for _, h := range hashes {
		n := ns.FindByHash(h)
		r = append(r, n)
	}
	return r
}

func (ns *Names) FindByName(name string) Hash {

	if ns.items == nil {
		return 0
	}

	hash := ns.NameHash(name)
	if hash == 0 {
		return 0
	}

	n := ns.FindByHash(hash)
	if n != name {
		return 0
	}
	return hash
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

func (h *Host) Labels(attributes *Attributes) Labels {

	lhs := attributes
	if lhs == nil {
		return nil
	}
	lbs := lhs.Find(h.labels)
	if lbs == nil {
		return nil
	}
	return lbs
}

func (h *Host) Name(names *Names) string {

	lbs := names
	if lbs == nil {
		return ""
	}
	return names.FindByHash(h.hash)
}

func (h *Host) Same(host *Host) bool {

	if host == nil {
		return false
	}

	if h == host {
		return true
	}

	if h.hash != host.hash {
		return false
	}
	return true
}

func (h *Host) Copy(host *Host) {

	if h.Same(host) {
		return
	}
}

func NewHost(hash, labels Hash, on *Host) *Host {

	return &Host{
		hash:   hash,
		labels: labels,
		on:     on,
	}
}

// Hosts

func (hs *Hosts) Items() HostsItems {

	hs.mu.Lock()
	defer hs.mu.Unlock()

	return hs.items
}

func (hs *Hosts) AddOrUpdate(stamp Stamp, h *Host) {

	hs.mu.Lock()
	defer hs.mu.Unlock()

	if h == nil {
		return
	}

	n := h.hash
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

func (hs *Hosts) unsafeFind(stamp Stamp, hash Hash) *Host {

	if hash == 0 {
		return nil
	}

	tc := hs.items[stamp]

	if tc == nil {
		return nil
	}
	return tc[hash]
}

func (hs *Hosts) Find(stamp Stamp, hash Hash) *Host {

	hs.mu.Lock()
	defer hs.mu.Unlock()

	return hs.unsafeFind(stamp, hash)
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

func (a *Application) Labels(attributes *Attributes) Labels {

	lhs := attributes
	if lhs == nil {
		return nil
	}
	lbs := lhs.Find(a.labels)
	if lbs == nil {
		return nil
	}
	return lbs
}

func (a *Application) Name(names *Names) string {

	lbs := names
	if lbs == nil {
		return ""
	}
	return names.FindByHash(a.hash)
}

func (a *Application) Same(app *Application) bool {

	if app == nil {
		return false
	}

	if a == app {
		return true
	}

	if a.hash != app.hash {
		return false
	}
	return true
}

func (a *Application) Copy(app *Application) {

	if a.Same(app) {
		return
	}
}

func NewApplication(hash, labels Hash) *Application {

	return &Application{
		hash:   hash,
		labels: labels,
	}
}

// Applications

func (as *Applications) Items() map[Stamp]map[Hash]*Application {

	as.mu.Lock()
	defer as.mu.Unlock()

	return as.items
}

func (as *Applications) AddOrUpdate(stamp Stamp, a *Application) {

	as.mu.Lock()
	defer as.mu.Unlock()

	if a == nil {
		return
	}

	n := a.hash
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

func (as *Applications) unsafeFind(stamp Stamp, hash Hash) *Application {

	if hash == 0 {
		return nil
	}

	tc := as.items[stamp]
	if tc == nil {
		return nil
	}
	return tc[hash]
}

func (as *Applications) Find(stamp Stamp, hash Hash) *Application {

	as.mu.Lock()
	defer as.mu.Unlock()

	return as.unsafeFind(stamp, hash)
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
	return hs.hash
}

func (hs *HostSignal) Merge(s Signal) {
	// should check all fields of HostSignal
}

func (hs *HostSignal) ContainsAny(hashes []Hash) bool {

	for _, h := range hashes {

		if hs.hash == h {
			return true
		}
	}
	return false
}

func (hs *HostSignal) AsApplicationSignal() *ApplicationSignal {
	return nil
}

func (hs *HostSignal) AsHostSignal() *HostSignal {
	return hs
}

func NewHostSignal(hash Hash) *HostSignal {

	return &HostSignal{
		hash: hash,
	}
}

// ApplicationSignal

func (as *ApplicationSignal) Hash() Hash {
	return as.hash
}

func (as *ApplicationSignal) Application() Hash {
	return as.application
}

func (as *ApplicationSignal) Host() Hash {
	return as.host
}

func (as *ApplicationSignal) ContainsAny(hashes []Hash) bool {

	for _, h := range hashes {

		if as.application == h || as.host == h {
			return true
		}
	}
	return false
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

func (as *ApplicationSignal) AsApplicationSignal() *ApplicationSignal {
	return as
}

func (as *ApplicationSignal) AsHostSignal() *HostSignal {
	return nil
}

func NewApplicationSignal(hash Hash, app Hash, host Hash) *ApplicationSignal {

	return &ApplicationSignal{
		hash:        hash,
		application: app,
		host:        host,
	}
}

// Dependencies

func (ds *Dependencies) Items() map[Hash]*Dependencies {
	return ds.items
}

func (ds *Dependencies) Contains(hash Hash) bool {

	_, ok := ds.items[hash]
	if ok {
		return true
	}
	return false
}

func (ds *Dependencies) AddOrUpdate(hash Hash, children *Dependencies) {
	ds.items[hash] = children
}

func NewDependencies() *Dependencies {

	return &Dependencies{
		items: make(map[Hash]*Dependencies),
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

	hash := s.Hash()
	if hash == 0 {
		return
	}

	ss := ms.items[hash]
	if ss == nil {
		ss = make(map[Stamp]Signal)
		ms.items[hash] = ss
	}

	sOld := ss[stamp]
	if utils.IsEmpty(sOld) {
		ss[stamp] = s
		return
	}
	sOld.Merge(s)

	ms.items[hash] = ss
}

/*func (ms *Measurements) unsafeLookBackByType(stamp Stamp, hash Hash, tolerance int, typ reflect.Type) Signal {

	tb := stamp
	abs := math.Abs(float64(tolerance))
	for {
		tb--
		ss := ms.items[hash]
		diff := stamp - tb
		if ss == nil && diff < Stamp(abs) {
			continue
		}
		if ss == nil {
			return nil
		}
		s := ss[stamp]
		if utils.IsEmpty(s) {
			continue
		}
		ts := reflect.TypeOf(s)
		if ts.ConvertibleTo(typ) {
			return s
		}
		return nil
	}
}*/

func (ms *Measurements) unsafeFindSignal(stamp Stamp, hash Hash) Signal {

	if hash == 0 {
		return nil
	}
	ss := ms.items[hash]

	if len(ss) == 0 {
		return nil
	}
	return ss[stamp]
}

func (ms *Measurements) FindSignal(stamp Stamp, hash Hash) Signal {

	ms.mu.Lock()
	defer ms.mu.Unlock()

	return ms.unsafeFindSignal(stamp, hash)
}

func (ms *Measurements) unsafeFindSignals(stamp Stamp, hashes []Hash) []Signal {

	r := []Signal{}
	l := len(hashes)

	for _, signals := range ms.items {

		s, ok := signals[stamp]
		if !ok || utils.IsEmpty(s) {
			continue
		}

		if l > 0 && !s.ContainsAny(hashes) {
			continue
		}
		r = append(r, s)
	}

	return r
}

func (ms *Measurements) FindSignals(stamp Stamp, hashes []Hash) []Signal {

	ms.mu.Lock()
	defer ms.mu.Unlock()

	return ms.unsafeFindSignals(stamp, hashes)
}

func (ms *Measurements) FindApplicationSignal(stamp Stamp, hash Hash) *ApplicationSignal {

	ms.mu.Lock()
	defer ms.mu.Unlock()

	s := ms.unsafeFindSignal(stamp, hash)
	if s == nil {
		return nil
	}
	return s.AsApplicationSignal()
}

func (ms *Measurements) FindHostSignal(stamp Stamp, hash Hash) *HostSignal {

	ms.mu.Lock()
	defer ms.mu.Unlock()

	s := ms.unsafeFindSignal(stamp, hash)
	if s == nil {
		return nil
	}

	return s.AsHostSignal()
}

func (ms *Measurements) First() Stamp {

	if ms.first == math.MaxUint64 {
		return 0
	}
	return ms.first
}

func (ms *Measurements) Last() Stamp {
	return ms.last
}

func (ms *Measurements) unsafeDependencies(stamp Stamp, parents []Hash, exclude []Hash) *Dependencies {

	if len(parents) == 0 {
		return nil
	}

	m := NewDependencies()

	// initialize parent dependencies
	for _, h := range parents {
		m.items[h] = nil
		if !utils.Contains(exclude, h) {
			exclude = append(exclude, h)
		}
	}

	signals := ms.unsafeFindSignals(stamp, parents)
	for _, s := range signals {

		var new *Dependencies = nil
		h := Hash(0)

		as := s.AsApplicationSignal()
		if as != nil {

			h = as.Hash()
			backends := as.Backends(stamp)
			if len(backends) > 0 {

				newparents := []Hash{}
				for _, b := range backends {
					if utils.Contains(exclude, b) {
						continue
					}
					newparents = append(newparents, b.hash)
				}
				new = ms.unsafeDependencies(stamp, newparents, exclude)
			}
		}

		if h == 0 {
			continue
		}

		old, ok := m.items[h]
		if !ok {
			m.items[h] = new
		} else {
			if old != nil && new != nil {
				deps := NewDependencies()
				deps.items = MergeMaps(old.items, new.items)
				m.items[h] = deps
			} else if new != nil {
				m.items[h] = new
			}
		}
	}

	if len(m.items) > 0 {
		return m
	}
	return nil
}

func (ms *Measurements) Dependencies(stamp Stamp, parents []Hash) *Dependencies {

	ms.mu.Lock()
	defer ms.mu.Unlock()

	exclude := []Hash{}
	return ms.unsafeDependencies(stamp, parents, exclude)
}

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
