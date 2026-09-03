package common

import (
	"maps"
	"math"
	"slices"
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

type NamesItems = *xsync.Map[Hash, string]
type Names struct {
	items NamesItems
}

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
	TrafficKindQps            // query per second
	TrafficKindCps            // connection per second
	TrafficKindMps            // message per second
	TrafficKindTps            // transaction per second
	TrafficKindBps            // byte per second
)

var AllTrafficKinds = []TrafficKind{TrafficKindUnknown, TrafficKindRps, TrafficKindQps, TrafficKindCps, TrafficKindMps, TrafficKindTps, TrafficKindBps}
var RequestsTrafficKinds = []TrafficKind{TrafficKindRps, TrafficKindQps, TrafficKindCps, TrafficKindMps, TrafficKindTps}
var ThroughputTrafficKinds = []TrafficKind{TrafficKindBps}

const (
	TrafficKindRpsName = "rps"
	TrafficKindQpsName = "qps"
	TrafficKindCpsName = "cps"
	TrafficKindMpsName = "mps"
	TrafficKindTpsName = "tps"
	TrafficKindBpsName = "bps"
)

type Errors = float64
type Latency = float64
type Saturation = float64

type SaturationKind = int

const (
	SaturationKindUnknown = iota
	SaturationKindCPU
	SaturationKindMemory
	SaturationKindDisk
	SaturationKindConnections
)

const (
	SaturationKindCPUName         = "cpu"
	SaturationKindMemoryName      = "memory"
	SaturationKindDiskName        = "disk"
	SaturationKindConnectionsName = "connections"
)

type IncomingTrafficItems = map[TrafficKind]map[Hash][]Traffic
type IncomingTraffic struct {
	items IncomingTrafficItems
}

type IncomingErrorsItems = map[Hash][]Errors
type IncomingErrors struct {
	items IncomingErrorsItems
}

type IncomingLatencyItems = map[Hash][]Latency
type IncomingLatency struct {
	items IncomingLatencyItems
}

type OutgoingTrafficItems = map[TrafficKind]map[Hash][]Traffic
type OutgoingTraffic struct {
	items OutgoingTrafficItems
}

type OutgoingErrorsItems = map[Hash][]Errors
type OutgoingErrors struct {
	items OutgoingErrorsItems
}

type OutgoingLatencyItems = map[Hash][]Latency
type OutgoingLatency struct {
	items OutgoingLatencyItems
}

type SaturationItems = map[SaturationKind]map[Hash][]Saturation

type ApplicationSaturation struct {
	items SaturationItems
}

type HostSaturation struct {
	items SaturationItems
}

const (
	//HostSignalName           = "name"
	HostSignalHost = "host"
)

type HostSignal struct {
	hash       Hash
	Saturation HostSaturation
}

const (
	ApplicationSignalName        = "application"
	ApplicationSignalHost        = "host"
	ApplicationSignalTrafficKind = "kind"
	ApplicationSignalFrontend    = "frontend"
	ApplicationSignalBackend     = "backend"
)

const (
	SignalSaturationKind = "kind"
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
	case TrafficKindQpsName:
		return TrafficKindQps
	case TrafficKindCpsName:
		return TrafficKindCps
	case TrafficKindMpsName:
		return TrafficKindMps
	case TrafficKindTpsName:
		return TrafficKindTps
	case TrafficKindBpsName:
		return TrafficKindBps
	}
	return TrafficKindUnknown
}

func SaturationKindByName(kind string) SaturationKind {

	switch kind {
	case SaturationKindCPUName:
		return SaturationKindCPU
	case SaturationKindMemoryName:
		return SaturationKindMemory
	case SaturationKindDiskName:
		return SaturationKindDisk
	case SaturationKindConnectionsName:
		return SaturationKindConnections
	}
	return SaturationKindUnknown
}

// Attributes

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

func (as *Attributes) FindTrafficKind(hash Hash) TrafficKind {

	kind := TrafficKindUnknown
	lbs := as.Find(hash)
	if lbs != nil {
		kind = TrafficKindByName(lbs[ApplicationSignalTrafficKind])
	}
	return kind
}

func (as *Attributes) FindSaturationKind(hash Hash) SaturationKind {

	kind := SaturationKindUnknown
	lbs := as.Find(hash)
	if lbs != nil {
		kind = SaturationKindByName(lbs[SignalSaturationKind])
	}
	return kind
}

func NewAttributes() *Attributes {
	return &Attributes{
		items: xsync.NewMap[Hash, Labels](),
	}
}

// Names

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

func (it *IncomingTraffic) Frontends(kinds []TrafficKind) []Hash {

	hashes := []Hash{}

	for _, kind := range kinds {
		for hash := range it.items[kind] {
			hashes = append(hashes, hash)
		}
	}
	return hashes
}

func (it *IncomingTraffic) aggregateByKinds(kinds []TrafficKind, aggregator Aggregator[Traffic]) map[TrafficKind]*Traffic {

	r := make(map[TrafficKind]*Traffic)

	for _, kind := range kinds {
		values := []Traffic{}
		for _, vls := range it.items[kind] {
			values = append(values, vls...)
		}
		if len(values) == 0 {
			r[kind] = nil
			continue
		}
		v := aggregator(values)
		r[kind] = &v
	}
	return r
}

func (it *IncomingTraffic) AvgByKinds(kinds []TrafficKind) map[TrafficKind]*Traffic {
	return it.aggregateByKinds(kinds, Avg)
}

func (it *IncomingTraffic) AddOrUpdate(value float64, attribute Hash, kind TrafficKind) {

	if it.items == nil {
		it.items = make(IncomingTrafficItems)
	}

	values := it.items[kind]

	if values == nil {
		values = make(map[Hash][]Traffic)
		values[attribute] = append(values[attribute], value)
	} else {

		traffic, ok := values[attribute]
		if !ok {
			traffic = []Traffic{value}
		} else {
			traffic = append(traffic, value)
		}
		values[attribute] = traffic
	}
	it.items[kind] = values
}

// IncomingErrors

func (ie *IncomingErrors) Frontends() []Hash {
	return slices.Collect(maps.Keys(ie.items))
}

func (ie *IncomingErrors) aggregate(aggregator Aggregator[Errors]) *Errors {

	var r *Errors
	values := []Errors{}
	for _, vls := range ie.items {
		values = append(values, vls...)
	}
	if len(values) == 0 {
		return r
	}
	v := aggregator(values)
	return &v
}

func (ie *IncomingErrors) Sum() *Errors {
	return ie.aggregate(Sum)
}

func (ie *IncomingErrors) AddOrUpdate(value float64, attribute Hash) {

	if ie.items == nil {
		ie.items = make(IncomingErrorsItems)
	}

	errors, ok := ie.items[attribute]
	if !ok {
		errors = []Errors{value}
	} else {
		errors = append(errors, value)
	}
	ie.items[attribute] = errors
}

// IncomingLatency

func (il *IncomingLatency) Frontends() []Hash {
	return slices.Collect(maps.Keys(il.items))
}

func (il *IncomingLatency) aggregate(aggregator Aggregator[Latency]) *Latency {

	var r *Latency
	values := []Latency{}
	for _, vls := range il.items {
		values = append(values, vls...)
	}
	if len(values) == 0 {
		return r
	}
	v := aggregator(values)
	return &v
}

func (il *IncomingLatency) Avg() *Latency {
	return il.aggregate(Avg)
}

func (il *IncomingLatency) AddOrUpdate(value float64, attribute Hash) {

	if il.items == nil {
		il.items = make(IncomingLatencyItems)
	}

	latency, ok := il.items[attribute]
	if !ok {
		latency = []Latency{value}
	} else {
		latency = append(latency, value)
	}
	il.items[attribute] = latency
}

// OutgoingTraffic

func (ot *OutgoingTraffic) Backends(kinds []TrafficKind) []Hash {

	hashes := []Hash{}

	for _, kind := range kinds {
		for hash := range ot.items[kind] {
			hashes = append(hashes, hash)
		}
	}
	return hashes
}

func (ot *OutgoingTraffic) aggregateByKinds(kinds []TrafficKind, aggregator Aggregator[Traffic]) map[TrafficKind]*Traffic {

	r := make(map[TrafficKind]*Traffic)

	for _, kind := range kinds {
		values := []Traffic{}
		for _, vls := range ot.items[kind] {
			values = append(values, vls...)
		}
		if len(values) == 0 {
			r[kind] = nil
			continue
		}
		v := aggregator(values)
		r[kind] = &v
	}
	return r
}

func (ot *OutgoingTraffic) AvgByKinds(kinds []TrafficKind) map[TrafficKind]*Traffic {
	return ot.aggregateByKinds(kinds, Avg)
}

func (ot *OutgoingTraffic) AddOrUpdate(value float64, attribute Hash, kind TrafficKind) {

	if ot.items == nil {
		ot.items = make(OutgoingTrafficItems)
	}

	values := ot.items[kind]

	if values == nil {
		values = make(map[Hash][]Traffic)
		values[attribute] = append(values[attribute], value)
	} else {

		traffic, ok := values[attribute]
		if !ok {
			traffic = []Traffic{value}
		} else {
			traffic = append(traffic, value)
		}
		values[attribute] = traffic
	}
	ot.items[kind] = values
}

// OutgoingErrors

func (oe *OutgoingErrors) Backends() []Hash {
	return slices.Collect(maps.Keys(oe.items))
}

func (oe *OutgoingErrors) aggregate(aggregator Aggregator[Errors]) *Errors {

	var r *Errors
	values := []Errors{}
	for _, vls := range oe.items {
		values = append(values, vls...)
	}
	if len(values) == 0 {
		return r
	}
	v := aggregator(values)
	return &v
}

func (oe *OutgoingErrors) Sum() *Errors {
	return oe.aggregate(Sum)
}

func (oe *OutgoingErrors) AddOrUpdate(value float64, attribute Hash) {

	if oe.items == nil {
		oe.items = make(OutgoingErrorsItems)
	}

	errors, ok := oe.items[attribute]
	if !ok {
		errors = []Errors{value}
	} else {
		errors = append(errors, value)
	}
	oe.items[attribute] = errors
}

// OutgoingLatency

func (ol *OutgoingLatency) Backends() []Hash {
	return slices.Collect(maps.Keys(ol.items))
}

func (ol *OutgoingLatency) aggregate(aggregator Aggregator[Latency]) *Latency {

	var r *Latency
	values := []Latency{}
	for _, vls := range ol.items {
		values = append(values, vls...)
	}
	if len(values) == 0 {
		return r
	}
	v := aggregator(values)
	return &v
}

func (ol *OutgoingLatency) Avg() *Latency {
	return ol.aggregate(Avg)
}

func (ol *OutgoingLatency) AddOrUpdate(value float64, attribute Hash) {

	if ol.items == nil {
		ol.items = make(OutgoingLatencyItems)
	}

	latency, ok := ol.items[attribute]
	if !ok {
		latency = []Latency{value}
	} else {
		latency = append(latency, value)
	}
	ol.items[attribute] = latency
}

// ApplicationSaturation

func (as *ApplicationSaturation) aggregateByKind(kind SaturationKind, aggregator Aggregator[Saturation]) *Saturation {

	var r *Saturation

	values := []Saturation{}
	for _, vls := range as.items[kind] {
		values = append(values, vls...)
	}
	if len(values) == 0 {
		return r
	}
	v := aggregator(values)
	r = &v
	return r
}

func (as *ApplicationSaturation) MaxByKind(kind TrafficKind) *Saturation {
	return as.aggregateByKind(kind, Max)
}

func (as *ApplicationSaturation) AddOrUpdate(value float64, attribute Hash, kind SaturationKind) {

	if as.items == nil {
		as.items = make(SaturationItems)
	}

	values := as.items[kind]

	if values == nil {
		values = make(map[Hash][]Saturation)
		values[attribute] = append(values[attribute], value)
	} else {

		traffic, ok := values[attribute]
		if !ok {
			traffic = []Traffic{value}
		} else {
			traffic = append(traffic, value)
		}
		values[attribute] = traffic
	}
	as.items[kind] = values
}

// HostSaturation

func (hs *HostSaturation) AddOrUpdate(value float64, attribute Hash, kind SaturationKind) {

	if hs.items == nil {
		hs.items = make(SaturationItems)
	}

	values := hs.items[kind]

	if values == nil {
		values = make(map[Hash][]Saturation)
		values[attribute] = append(values[attribute], value)
	} else {

		traffic, ok := values[attribute]
		if !ok {
			traffic = []Traffic{value}
		} else {
			traffic = append(traffic, value)
		}
		values[attribute] = traffic
	}
	hs.items[kind] = values
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

func (as *ApplicationSignal) Frontends() []Hash {

	traffic := as.IncomingTraffic.Frontends(AllTrafficKinds)
	errors := as.IncomingErrors.Frontends()
	latency := as.IncomingLatency.Frontends()

	hashes := []Hash{}
	hashes = append(hashes, traffic...)
	hashes = append(hashes, errors...)
	hashes = append(hashes, latency...)

	return slices.Compact(hashes)
}

func (as *ApplicationSignal) Backends() []Hash {

	traffic := as.OutgoingTraffic.Backends(AllTrafficKinds)
	errors := as.OutgoingErrors.Backends()
	latency := as.OutgoingLatency.Backends()

	hashes := []Hash{}
	hashes = append(hashes, traffic...)
	hashes = append(hashes, errors...)
	hashes = append(hashes, latency...)

	return slices.Compact(hashes)
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

// Measurements

func (ms *Measurements) Items() MeasurementsItems {

	ms.mu.Lock()
	defer ms.mu.Unlock()

	return ms.items
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
			backends := as.Backends()
			if len(backends) > 0 {

				newparents := []Hash{}
				for _, b := range backends {
					if utils.Contains(exclude, b) {
						continue
					}
					newparents = append(newparents, b)
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

	if len(parents) == 0 {
		return nil
	}

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
