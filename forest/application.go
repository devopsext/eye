package forest

import (
	"bufio"
	"cmp"
	"encoding/gob"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"time"

	"github.com/devopsext/eye/common"
	"github.com/devopsext/utils"
	"github.com/e-XpertSolutions/go-iforest/v2/iforest"
)

type ApplicationCasePattern struct {
	inRequests   common.CaseLevel
	inThroughput common.CaseLevel
	inLatency    common.CaseLevel
	inErrors     common.CaseLevel

	outRequests   common.CaseLevel
	outThroughput common.CaseLevel
	outLatency    common.CaseLevel
	outErrors     common.CaseLevel

	appCPU  common.CaseLevel
	appMem  common.CaseLevel
	hostCPU common.CaseLevel
	hostMem common.CaseLevel
}

type ApplicationCaseVerdict struct {
	acase *ApplicationCase
	score int
}

type ApplicationCase struct {
	name        string
	description string
	rootCause   string
	impact      common.CaseImpact
	category    common.CaseCategory
	pattern     ApplicationCasePattern
}

type ApplicationCases struct {
	list []*ApplicationCase
}

type ApplicationTrafficValues map[common.TrafficKind]*common.Traffic
type ApplicationLatencyValue *common.Latency
type ApplicationErrorsValue *common.Errors
type ApplicationSaturationValue *common.Saturation
type HostSaturationValue *common.Saturation

type ApplicationFrames []*ApplicationFrame

type ApplicationFramePoint struct {
	timestamp time.Time
	value     float64
	max       float64
	min       float64
}

type ApplicationFramePoints struct {
	inRequests    *ApplicationFramePoint
	inThroughput  *ApplicationFramePoint
	inLatency     *ApplicationFramePoint
	inErrors      *ApplicationFramePoint
	outRequests   *ApplicationFramePoint
	outThroughput *ApplicationFramePoint
	outLatency    *ApplicationFramePoint
	outErrors     *ApplicationFramePoint
	appCPU        *ApplicationFramePoint
	appMem        *ApplicationFramePoint
	hostCPU       *ApplicationFramePoint
	hostMem       *ApplicationFramePoint
}

type ApplicationFrame struct {
	//
	application common.Hash
	host        common.Hash
	//
	stamp common.Stamp
	begin common.Stamp
	end   common.Stamp
	//
	inRequests   ApplicationTrafficValues
	inThroughput ApplicationTrafficValues
	inLatency    ApplicationLatencyValue
	inErrors     ApplicationErrorsValue
	//
	appCPU  ApplicationSaturationValue
	appMem  ApplicationSaturationValue
	hostCPU HostSaturationValue
	hostMem HostSaturationValue
	//
	outRequests   ApplicationTrafficValues
	outThroughput ApplicationTrafficValues
	outLatency    ApplicationLatencyValue
	outErrors     ApplicationErrorsValue
}

type ApplicationFrameLimits struct {
	From  common.Stamp
	First common.Stamp
	Last  common.Stamp
}

type ApplicationTrafficSlots struct {
	MaxSlots  int
	KeyToSlot map[common.TrafficKind]int
	SlotToKey []common.TrafficKind
	IsFitted  bool
}

type ApplicationFeatureStats struct {
	Median float64
	Q1     float64
	Q3     float64
	IQR    float64
}

// K = 1.5 (Default / Standard Outlier Threshold)
// Corresponds to standard statistical "mild outliers" (roughly equivalent to 2.7sigma on a normal distribution).
// Anything exceeding $Q3 + 1.5 * IQR is immediately labeled High without needing the Isolation Forest's confirmation.

// K = 3.0 ("Extreme" Outlier Threshold)
// Expands the normal band significantly (roughly 4.7 sigma). Only massive spikes trigger direct High
// leaving subtle or multi-metric correlations to be decided by the Isolation Forest

// Lowering K (e.g., 1.0): Tightens the envelope, making the engine more sensitive to smaller deviations.
// Raising K (e.g., 2.0 - 3.0): Broadens the envelope, reducing alert noise for services with naturally spiky or high-variance diurnal traffic patterns.

// Quartile Profiler Conditioned on 168 Hourly Buckets (7 Days x 24 Hours)
type ApplicationProfilerBucketsKind = int

const (
	ApplicationProfilerBucketsKindDay = iota
	ApplicationProfilerBucketsKindHour
	ApplicationProfilerBucketsKindHalfHour
	ApplicationProfilerBucketsKind15Minutes
	ApplicationProfilerBucketsKind10Minutes
	ApplicationProfilerBucketsKind5Minutes
	ApplicationProfilerBucketsKindMinute
)

type ApplicationProfiler struct {
	Buckets  [][]ApplicationFeatureStats
	Exists   []bool
	K        float64
	IsFitted bool
	Kind     ApplicationProfilerBucketsKind
}

type ApplicationEngineOptions struct {
	Path                  string
	TreesNumber           int
	SubsampleSize         int
	OutlierRatio          float64
	InRequestsMaxSlots    int
	InThroughputMaxSlots  int
	OutRequestsMaxSlots   int
	OutThroughputMaxSlots int
	RangeMultiplier       float64
	ProfileBucketsKind    int
	MinScore              int
	Categories            string
	Impacts               string
}

type ApplicationEngineFileIncoming struct {
	ReqSlots  *ApplicationTrafficSlots
	ThruSlots *ApplicationTrafficSlots
	Forest    *iforest.Forest
	Profile   *ApplicationProfiler
	Data      [][]float64
}

type ApplicationEngineFileOutgoing = ApplicationEngineFileIncoming

type ApplicationEngineFileSaturation struct {
	Forest  *iforest.Forest
	Profile *ApplicationProfiler
	Data    [][]float64
}

type ApplicationEngineFileV001 struct {
	Stamps     []common.Stamp
	Minutes    []int
	Incoming   ApplicationEngineFileIncoming
	Saturation ApplicationEngineFileSaturation
	Outgoing   ApplicationEngineFileOutgoing
}

type ApplicationEngine struct {
	id      common.Hash
	options *ApplicationEngineOptions

	inReqSlots  *ApplicationTrafficSlots
	inThruSlots *ApplicationTrafficSlots

	outReqSlots  *ApplicationTrafficSlots
	outThruSlots *ApplicationTrafficSlots

	stamps  []common.Stamp
	minutes []int
	inData  [][]float64
	satData [][]float64
	outData [][]float64

	inForest  *iforest.Forest
	satForest *iforest.Forest
	outForest *iforest.Forest

	inProfiler  *ApplicationProfiler
	satProfiler *ApplicationProfiler
	outProfiler *ApplicationProfiler

	categories []common.CaseCategory
	impacts    []common.CaseImpact
}

const (
	ApplicationEngineV001 = "0.0.1"
)

// ApplicationCase

func (c *ApplicationCase) SamePattern(pattern ApplicationCasePattern) bool {

	return c.pattern.inRequests == pattern.inRequests &&
		c.pattern.inThroughput == pattern.inThroughput &&
		c.pattern.inLatency == pattern.inLatency &&
		c.pattern.inErrors == pattern.inErrors &&
		c.pattern.outRequests == pattern.outRequests &&
		c.pattern.outThroughput == pattern.outThroughput &&
		c.pattern.outLatency == pattern.outLatency &&
		c.pattern.outErrors == pattern.outErrors &&
		c.pattern.appCPU == pattern.appCPU &&
		c.pattern.appMem == pattern.appMem &&
		c.pattern.hostCPU == pattern.hostCPU &&
		c.pattern.hostMem == pattern.hostMem
}

func NewApplicationCase(impact common.CaseImpact, category common.CaseCategory, name, description, rootCause string, pattern ApplicationCasePattern) *ApplicationCase {

	return &ApplicationCase{
		name:        name,
		description: description,
		rootCause:   rootCause,
		impact:      impact,
		category:    category,
		pattern:     pattern,
	}
}

// ApplicationCases

func (ac *ApplicationCases) Items() []*ApplicationCase {
	return ac.list
}

func (ac *ApplicationCases) FindBypattern(pattern ApplicationCasePattern) *ApplicationCase {

	var r *ApplicationCase
	for _, c := range ac.list {

		if c.SamePattern(pattern) {
			return c
		}
	}
	return r
}

func (ac *ApplicationCases) Add(impact common.CaseImpact, category common.CaseCategory, name, description, rootCause string, pattern ApplicationCasePattern) *ApplicationCase {

	c := ac.FindBypattern(pattern)
	if !utils.IsEmpty(c) {
		return c
	}
	c = NewApplicationCase(impact, category, name, description, rootCause, pattern)
	ac.list = append(ac.list, c)
	return c
}

func (ac *ApplicationCases) getScore(observed, expected *ApplicationCasePattern) int {

	score := 0
	check := func(obs, exp common.CaseLevel) {
		if obs == exp {
			score++
		}
	}

	check(observed.inRequests, expected.inRequests)
	check(observed.inThroughput, expected.inThroughput)
	check(observed.inLatency, expected.inLatency)
	check(observed.inErrors, expected.inErrors)
	check(observed.appCPU, expected.appCPU)
	check(observed.appMem, expected.appMem)
	check(observed.hostCPU, expected.hostCPU)
	check(observed.hostMem, expected.hostMem)
	check(observed.outRequests, expected.outRequests)
	check(observed.outThroughput, expected.outThroughput)
	check(observed.outLatency, expected.outLatency)
	check(observed.outErrors, expected.outErrors)

	return score
}

func (ac *ApplicationCases) Match(pattern *ApplicationCasePattern) (*ApplicationCase, int) {

	bestScore := -1
	var bestCase *ApplicationCase

	if pattern == nil {
		return bestCase, bestScore
	}

	for _, c := range ac.list {
		score := ac.getScore(pattern, &c.pattern)
		if score > bestScore {
			bestScore = score
			bestCase = c
		}
	}

	return bestCase, bestScore
}

func NewApplicationCases() *ApplicationCases {

	cases := &ApplicationCases{}

	cases.Add(common.CaseImpactZero, common.CaseCategoryHealthy,
		"Healthy Steady State",
		"All metrics inside baseline quantiles",
		"Nominal operating conditions",
		ApplicationCasePattern{
			inRequests:    common.CaseLevelNormal,
			inThroughput:  common.CaseLevelNormal,
			inLatency:     common.CaseLevelNormal,
			inErrors:      common.CaseLevelNormal,
			appCPU:        common.CaseLevelNormal,
			appMem:        common.CaseLevelNormal,
			hostCPU:       common.CaseLevelNormal,
			hostMem:       common.CaseLevelNormal,
			outRequests:   common.CaseLevelNormal,
			outThroughput: common.CaseLevelNormal,
			outLatency:    common.CaseLevelNormal,
			outErrors:     common.CaseLevelNormal,
		})

	cases.Add(common.CaseImpactZero, common.CaseCategoryHealthy,
		"Healthy Load Scaling",
		"Proportional out/in scaling without latency or error growth",
		"Organic client traffic surge absorbed cleanly",
		ApplicationCasePattern{
			inRequests:    common.CaseLevelHigh,
			inThroughput:  common.CaseLevelHigh,
			inLatency:     common.CaseLevelNormal,
			inErrors:      common.CaseLevelNormal,
			appCPU:        common.CaseLevelNormal,
			appMem:        common.CaseLevelNormal,
			hostCPU:       common.CaseLevelNormal,
			hostMem:       common.CaseLevelNormal,
			outRequests:   common.CaseLevelHigh,
			outThroughput: common.CaseLevelHigh,
			outLatency:    common.CaseLevelNormal,
			outErrors:     common.CaseLevelNormal,
		})

	cases.Add(common.CaseImpactZero, common.CaseCategoryHealthy,
		"Healthy Off-Peak Drop",
		"Proportional traffic decline with stable latencies",
		"Diurnal or scheduled off-peak traffic drop",
		ApplicationCasePattern{
			inRequests:    common.CaseLevelLow,
			inThroughput:  common.CaseLevelLow,
			inLatency:     common.CaseLevelNormal,
			inErrors:      common.CaseLevelNormal,
			appCPU:        common.CaseLevelNormal,
			appMem:        common.CaseLevelNormal,
			hostCPU:       common.CaseLevelNormal,
			hostMem:       common.CaseLevelNormal,
			outRequests:   common.CaseLevelLow,
			outThroughput: common.CaseLevelLow,
			outLatency:    common.CaseLevelNormal,
			outErrors:     common.CaseLevelNormal,
		})

	cases.Add(common.CaseImpactZero, common.CaseCategoryHealthy,
		"Cache Hit Absorption",
		"High ingress with low egress and nominal latencies/errors",
		"In-memory cache absorbing reads cleanly (e.g. Redis / CDN)",
		ApplicationCasePattern{
			inRequests:    common.CaseLevelHigh,
			inThroughput:  common.CaseLevelHigh,
			inLatency:     common.CaseLevelLow,
			inErrors:      common.CaseLevelNormal,
			appCPU:        common.CaseLevelNormal,
			appMem:        common.CaseLevelNormal,
			hostCPU:       common.CaseLevelNormal,
			hostMem:       common.CaseLevelNormal,
			outRequests:   common.CaseLevelLow,
			outThroughput: common.CaseLevelLow,
			outLatency:    common.CaseLevelNormal,
			outErrors:     common.CaseLevelNormal,
		})

	cases.Add(common.CaseImpactZero, common.CaseCategoryHealthy,
		"Ultra-Low Footprint Zero Idle",
		"Metrics fully functional with saturation floor near 0% under lightweight async load",
		"Efficient non-blocking event loop or runtime idling at true zero consumption",
		ApplicationCasePattern{
			inRequests:    common.CaseLevelNormal,
			inThroughput:  common.CaseLevelNormal,
			inLatency:     common.CaseLevelLow,
			inErrors:      common.CaseLevelLow,
			appCPU:        common.CaseLevelLow,
			appMem:        common.CaseLevelLow,
			hostCPU:       common.CaseLevelLow,
			hostMem:       common.CaseLevelLow,
			outRequests:   common.CaseLevelNormal,
			outThroughput: common.CaseLevelNormal,
			outLatency:    common.CaseLevelLow,
			outErrors:     common.CaseLevelLow,
		})

	cases.Add(common.CaseImpactZero, common.CaseCategoryOperational,
		"Client Inactivity (Full Idle)",
		"All metrics idle down to baseline floor without errors",
		"Clean cessation of incoming requests",
		ApplicationCasePattern{
			inRequests:    common.CaseLevelLow,
			inThroughput:  common.CaseLevelLow,
			inLatency:     common.CaseLevelLow,
			inErrors:      common.CaseLevelLow,
			appCPU:        common.CaseLevelLow,
			appMem:        common.CaseLevelLow,
			hostCPU:       common.CaseLevelLow,
			hostMem:       common.CaseLevelLow,
			outRequests:   common.CaseLevelLow,
			outThroughput: common.CaseLevelLow,
			outLatency:    common.CaseLevelLow,
			outErrors:     common.CaseLevelLow,
		})

	cases.Add(common.CaseImpactZero, common.CaseCategoryOperational,
		"Async Queue / Worker Drain",
		"Zero ingress traffic with high CPU and high outbound traffic",
		"Background job processing Kafka consumer drain batch sync",
		ApplicationCasePattern{
			inRequests:    common.CaseLevelLow,
			inThroughput:  common.CaseLevelLow,
			inLatency:     common.CaseLevelNormal,
			inErrors:      common.CaseLevelNormal,
			appCPU:        common.CaseLevelHigh,
			appMem:        common.CaseLevelNormal,
			hostCPU:       common.CaseLevelHigh,
			hostMem:       common.CaseLevelNormal,
			outRequests:   common.CaseLevelHigh,
			outThroughput: common.CaseLevelHigh,
			outLatency:    common.CaseLevelNormal,
			outErrors:     common.CaseLevelNormal,
		})

	cases.Add(common.CaseImpactZero, common.CaseCategoryOperational,
		"Target Drainage / Rolling Deploy",
		"Clean linear decline of traffic without error spike during deployment cycle",
		"Target group registration draining or pod termination grace period",
		ApplicationCasePattern{
			inRequests:    common.CaseLevelLow,
			inThroughput:  common.CaseLevelLow,
			inLatency:     common.CaseLevelNormal,
			inErrors:      common.CaseLevelLow,
			appCPU:        common.CaseLevelLow,
			appMem:        common.CaseLevelLow,
			hostCPU:       common.CaseLevelLow,
			hostMem:       common.CaseLevelLow,
			outRequests:   common.CaseLevelLow,
			outThroughput: common.CaseLevelLow,
			outLatency:    common.CaseLevelNormal,
			outErrors:     common.CaseLevelLow,
		})

	cases.Add(common.CaseImpactAverage, common.CaseCategoryFailure,
		"App CPU Limit / Cgroup Throttle",
		"App CPU saturated while host CPU and memory remain normal",
		"Container cgroup CPU quota reached or thread-pool exhaustion",
		ApplicationCasePattern{
			inRequests:    common.CaseLevelHigh,
			inThroughput:  common.CaseLevelHigh,
			inLatency:     common.CaseLevelHigh,
			inErrors:      common.CaseLevelHigh,
			appCPU:        common.CaseLevelHigh,
			appMem:        common.CaseLevelNormal,
			hostCPU:       common.CaseLevelNormal,
			hostMem:       common.CaseLevelNormal,
			outRequests:   common.CaseLevelLow,
			outThroughput: common.CaseLevelLow,
			outLatency:    common.CaseLevelLow,
			outErrors:     common.CaseLevelLow,
		})

	cases.Add(common.CaseImpactAverage, common.CaseCategoryFailure,
		"Noisy Neighbor CPU Steal",
		"Host CPU high but App CPU normal/low; outbound drops",
		"External rogue process or container on host stealing CPU cores",
		ApplicationCasePattern{
			inRequests:    common.CaseLevelNormal,
			inThroughput:  common.CaseLevelNormal,
			inLatency:     common.CaseLevelHigh,
			inErrors:      common.CaseLevelHigh,
			appCPU:        common.CaseLevelNormal,
			appMem:        common.CaseLevelNormal,
			hostCPU:       common.CaseLevelHigh,
			hostMem:       common.CaseLevelNormal,
			outRequests:   common.CaseLevelLow,
			outThroughput: common.CaseLevelLow,
			outLatency:    common.CaseLevelLow,
			outErrors:     common.CaseLevelLow,
		})

	cases.Add(common.CaseImpactAverage, common.CaseCategoryFailure,
		"Edge Rejection / Fast Fail",
		"Inbound errors high while inbound latency drops to minimum",
		"Credential stuffing WAF blocking scraper flood 401/403/429",
		ApplicationCasePattern{
			inRequests:    common.CaseLevelHigh,
			inThroughput:  common.CaseLevelNormal,
			inLatency:     common.CaseLevelLow,
			inErrors:      common.CaseLevelHigh,
			appCPU:        common.CaseLevelNormal,
			appMem:        common.CaseLevelNormal,
			hostCPU:       common.CaseLevelNormal,
			hostMem:       common.CaseLevelNormal,
			outRequests:   common.CaseLevelLow,
			outThroughput: common.CaseLevelLow,
			outLatency:    common.CaseLevelLow,
			outErrors:     common.CaseLevelLow,
		})

	cases.Add(common.CaseImpactAverage, common.CaseCategoryFailure,
		"Heavy Payload / Slowloris",
		"Inbound througput high with low requests and high memory saturation",
		"Multipart file upload storm payload uncompressed JSON Slowloris attack",
		ApplicationCasePattern{
			inRequests:    common.CaseLevelLow,
			inThroughput:  common.CaseLevelHigh,
			inLatency:     common.CaseLevelHigh,
			inErrors:      common.CaseLevelHigh,
			appCPU:        common.CaseLevelNormal,
			appMem:        common.CaseLevelHigh,
			hostCPU:       common.CaseLevelNormal,
			hostMem:       common.CaseLevelHigh,
			outRequests:   common.CaseLevelLow,
			outThroughput: common.CaseLevelLow,
			outLatency:    common.CaseLevelLow,
			outErrors:     common.CaseLevelLow,
		})

	cases.Add(common.CaseImpactAverage, common.CaseCategoryFailure,
		"Fast Bypass / Regression Bug",
		"Inbound latency drops to zero with 200 OK and zero outbound calls",
		"Auth middleware bypass regression empty body return cached mock leak",
		ApplicationCasePattern{
			inRequests:    common.CaseLevelNormal,
			inThroughput:  common.CaseLevelNormal,
			inLatency:     common.CaseLevelLow,
			inErrors:      common.CaseLevelLow,
			appCPU:        common.CaseLevelLow,
			appMem:        common.CaseLevelLow,
			hostCPU:       common.CaseLevelLow,
			hostMem:       common.CaseLevelLow,
			outRequests:   common.CaseLevelLow,
			outThroughput: common.CaseLevelLow,
			outLatency:    common.CaseLevelLow,
			outErrors:     common.CaseLevelLow,
		})

	cases.Add(common.CaseImpactAverage, common.CaseCategoryFailure,
		"Egress Bandwidth Flooding",
		"Outbound throughput spikes while Outbound requests and saturation remain normal",
		"Unbounded query results large file downloads data exfiltration",
		ApplicationCasePattern{
			inRequests:    common.CaseLevelNormal,
			inThroughput:  common.CaseLevelNormal,
			inLatency:     common.CaseLevelNormal,
			inErrors:      common.CaseLevelNormal,
			appCPU:        common.CaseLevelNormal,
			appMem:        common.CaseLevelNormal,
			hostCPU:       common.CaseLevelNormal,
			hostMem:       common.CaseLevelNormal,
			outRequests:   common.CaseLevelNormal,
			outThroughput: common.CaseLevelNormal,
			outLatency:    common.CaseLevelHigh,
			outErrors:     common.CaseLevelNormal,
		})

	cases.Add(common.CaseImpactAverage, common.CaseCategoryFailure,
		"GC Stop-The-World Freeze",
		"App memory pinned at high watermark with CPU spikes from GC threads",
		"Full GC pause memory compaction freeze runtime heap lockup",
		ApplicationCasePattern{
			inRequests:    common.CaseLevelNormal,
			inThroughput:  common.CaseLevelNormal,
			inLatency:     common.CaseLevelHigh,
			inErrors:      common.CaseLevelNormal,
			appCPU:        common.CaseLevelHigh,
			appMem:        common.CaseLevelHigh,
			hostCPU:       common.CaseLevelNormal,
			hostMem:       common.CaseLevelNormal,
			outRequests:   common.CaseLevelLow,
			outThroughput: common.CaseLevelLow,
			outLatency:    common.CaseLevelLow,
			outErrors:     common.CaseLevelLow,
		})

	cases.Add(common.CaseImpactAverage, common.CaseCategoryFailure,
		"Client Disconnect / Timeout",
		"Inbound errors up (499/408) with near-zero downstream impact",
		"Client drops connection prematurely slow mobile networks",
		ApplicationCasePattern{
			inRequests:    common.CaseLevelNormal,
			inThroughput:  common.CaseLevelLow,
			inLatency:     common.CaseLevelHigh,
			inErrors:      common.CaseLevelHigh,
			appCPU:        common.CaseLevelLow,
			appMem:        common.CaseLevelLow,
			hostCPU:       common.CaseLevelLow,
			hostMem:       common.CaseLevelLow,
			outRequests:   common.CaseLevelLow,
			outThroughput: common.CaseLevelLow,
			outLatency:    common.CaseLevelLow,
			outErrors:     common.CaseLevelLow,
		})

	cases.Add(common.CaseImpactAverage, common.CaseCategoryFailure,
		"App Memory Leak / OOM Thrash",
		"App memory hits cgroup limit causing kernel page reclamation thrashing",
		"Application heap/buffer leak approaching container limit / OOM-kill",
		ApplicationCasePattern{
			inRequests:    common.CaseLevelNormal,
			inThroughput:  common.CaseLevelNormal,
			inLatency:     common.CaseLevelHigh,
			inErrors:      common.CaseLevelHigh,
			appCPU:        common.CaseLevelNormal,
			appMem:        common.CaseLevelHigh,
			hostCPU:       common.CaseLevelNormal,
			hostMem:       common.CaseLevelNormal,
			outRequests:   common.CaseLevelLow,
			outThroughput: common.CaseLevelLow,
			outLatency:    common.CaseLevelLow,
			outErrors:     common.CaseLevelLow,
		})

	cases.Add(common.CaseImpactAverage, common.CaseCategoryFailure,
		"Noisy Neighbor Memory Hog",
		"Host memory high while App memory is normal; app starved of cache/buffers",
		"Co-located container eating host RAM triggering kernel page scanning",
		ApplicationCasePattern{
			inRequests:    common.CaseLevelNormal,
			inThroughput:  common.CaseLevelNormal,
			inLatency:     common.CaseLevelHigh,
			inErrors:      common.CaseLevelHigh,
			appCPU:        common.CaseLevelNormal,
			appMem:        common.CaseLevelNormal,
			hostCPU:       common.CaseLevelNormal,
			hostMem:       common.CaseLevelHigh,
			outRequests:   common.CaseLevelLow,
			outThroughput: common.CaseLevelLow,
			outLatency:    common.CaseLevelLow,
			outErrors:     common.CaseLevelLow,
		})

	cases.Add(common.CaseImpactAverage, common.CaseCategoryFailure,
		"Hyper-Aggressive GC / Memory Starvation",
		"App memory pinned near 0% while App CPU is pinned at 100%",
		"GOMEMLIMIT/heap target set too low; runtime in continuous GC thrash",
		ApplicationCasePattern{
			inRequests:    common.CaseLevelNormal,
			inThroughput:  common.CaseLevelNormal,
			inLatency:     common.CaseLevelHigh,
			inErrors:      common.CaseLevelNormal,
			appCPU:        common.CaseLevelHigh,
			appMem:        common.CaseLevelLow,
			hostCPU:       common.CaseLevelNormal,
			hostMem:       common.CaseLevelNormal,
			outRequests:   common.CaseLevelLow,
			outThroughput: common.CaseLevelLow,
			outLatency:    common.CaseLevelNormal,
			outErrors:     common.CaseLevelNormal,
		})

	cases.Add(common.CaseImpactAverage, common.CaseCategoryFailure,
		"Client 499 Abort / Orphaned Compute",
		"High inbound 499s while outbound traffic continues running normally",
		"Missing context cancellation; backend work proceeds for cancelled client requests",
		ApplicationCasePattern{
			inRequests:    common.CaseLevelNormal,
			inThroughput:  common.CaseLevelNormal,
			inLatency:     common.CaseLevelHigh,
			inErrors:      common.CaseLevelHigh,
			appCPU:        common.CaseLevelHigh,
			appMem:        common.CaseLevelNormal,
			hostCPU:       common.CaseLevelNormal,
			hostMem:       common.CaseLevelNormal,
			outRequests:   common.CaseLevelHigh,
			outThroughput: common.CaseLevelHigh,
			outLatency:    common.CaseLevelNormal,
			outErrors:     common.CaseLevelNormal,
		})

	cases.Add(common.CaseImpactAverage, common.CaseCategoryFailure,
		"Poison Pill / DLQ Redrive Loop",
		"Zero inbound traffic with high CPU and surging outbound broker retry errors",
		"Unparseable queue message repeatedly nack-ed and redriven by workers",
		ApplicationCasePattern{
			inRequests:    common.CaseLevelLow,
			inThroughput:  common.CaseLevelLow,
			inLatency:     common.CaseLevelNormal,
			inErrors:      common.CaseLevelNormal,
			appCPU:        common.CaseLevelHigh,
			appMem:        common.CaseLevelHigh,
			hostCPU:       common.CaseLevelNormal,
			hostMem:       common.CaseLevelNormal,
			outRequests:   common.CaseLevelHigh,
			outThroughput: common.CaseLevelHigh,
			outLatency:    common.CaseLevelLow,
			outErrors:     common.CaseLevelHigh,
		})

	cases.Add(common.CaseImpactAverage, common.CaseCategoryFailure,
		"App Metric Collector Outage",
		"Traffic flows normally but container cgroup exporter drops out",
		"cgroup fs mount error kubelet stats provider stall or scraper timeout",
		ApplicationCasePattern{
			inRequests:    common.CaseLevelNormal,
			inThroughput:  common.CaseLevelNormal,
			inLatency:     common.CaseLevelNormal,
			inErrors:      common.CaseLevelNormal,
			appCPU:        common.CaseLevelUnknown,
			appMem:        common.CaseLevelUnknown,
			hostCPU:       common.CaseLevelNormal,
			hostMem:       common.CaseLevelNormal,
			outRequests:   common.CaseLevelNormal,
			outThroughput: common.CaseLevelNormal,
			outLatency:    common.CaseLevelNormal,
			outErrors:     common.CaseLevelNormal,
		})

	cases.Add(common.CaseImpactAverage, common.CaseCategoryFailure,
		"Host Agent Transport Failure",
		"Traffic and App metrics healthy but node-level hardware metrics missing",
		"host exporter daemon down crash or host firewall drop",
		ApplicationCasePattern{
			inRequests:    common.CaseLevelNormal,
			inThroughput:  common.CaseLevelNormal,
			inLatency:     common.CaseLevelNormal,
			inErrors:      common.CaseLevelNormal,
			appCPU:        common.CaseLevelNormal,
			appMem:        common.CaseLevelNormal,
			hostCPU:       common.CaseLevelUnknown,
			hostMem:       common.CaseLevelUnknown,
			outRequests:   common.CaseLevelNormal,
			outThroughput: common.CaseLevelNormal,
			outLatency:    common.CaseLevelNormal,
			outErrors:     common.CaseLevelNormal,
		})

	cases.Add(common.CaseImpactAverage, common.CaseCategoryOperational,
		"Circuit Breaker Fallback",
		"Outbound completely stopped but inbound returns fast healthy 200s",
		"Circuit breaker tripped serving degraded local cache or empty response",
		ApplicationCasePattern{
			inRequests:    common.CaseLevelHigh,
			inThroughput:  common.CaseLevelNormal,
			inLatency:     common.CaseLevelLow,
			inErrors:      common.CaseLevelNormal,
			appCPU:        common.CaseLevelNormal,
			appMem:        common.CaseLevelNormal,
			hostCPU:       common.CaseLevelNormal,
			hostMem:       common.CaseLevelNormal,
			outRequests:   common.CaseLevelLow,
			outThroughput: common.CaseLevelLow,
			outLatency:    common.CaseLevelLow,
			outErrors:     common.CaseLevelLow,
		})

	cases.Add(common.CaseImpactSevere, common.CaseCategoryFailure,
		"Host CPU Exhaustion by App",
		"Both App and Host CPU pinned near 100% while memory is stable",
		"Inbound compute volume exceeded physical host CPU cores",
		ApplicationCasePattern{
			inRequests:    common.CaseLevelHigh,
			inThroughput:  common.CaseLevelHigh,
			inLatency:     common.CaseLevelHigh,
			inErrors:      common.CaseLevelHigh,
			appCPU:        common.CaseLevelHigh,
			appMem:        common.CaseLevelNormal,
			hostCPU:       common.CaseLevelHigh,
			hostMem:       common.CaseLevelNormal,
			outRequests:   common.CaseLevelLow,
			outThroughput: common.CaseLevelLow,
			outLatency:    common.CaseLevelLow,
			outErrors:     common.CaseLevelLow,
		})

	cases.Add(common.CaseImpactSevere, common.CaseCategoryFailure,
		"Downstream / Backend Stall",
		"Outbound latency/errors surge and backpressure into ingress",
		"Downstream API DB lock contention or remote timeout",
		ApplicationCasePattern{
			inRequests:    common.CaseLevelHigh,
			inThroughput:  common.CaseLevelHigh,
			inLatency:     common.CaseLevelHigh,
			inErrors:      common.CaseLevelHigh,
			appCPU:        common.CaseLevelNormal,
			appMem:        common.CaseLevelNormal,
			hostCPU:       common.CaseLevelNormal,
			hostMem:       common.CaseLevelNormal,
			outRequests:   common.CaseLevelLow,
			outThroughput: common.CaseLevelLow,
			outLatency:    common.CaseLevelHigh,
			outErrors:     common.CaseLevelHigh,
		})

	cases.Add(common.CaseImpactSevere, common.CaseCategoryFailure,
		"Thread Deadlock / Pool Hang",
		"Extreme inbound latency with zero CPU and no 5xx errors",
		"Deadlock mutex starvation connection pool leak uncompleted requests",
		ApplicationCasePattern{
			inRequests:    common.CaseLevelHigh,
			inThroughput:  common.CaseLevelNormal,
			inLatency:     common.CaseLevelHigh,
			inErrors:      common.CaseLevelNormal,
			appCPU:        common.CaseLevelLow,
			appMem:        common.CaseLevelNormal,
			hostCPU:       common.CaseLevelNormal,
			hostMem:       common.CaseLevelNormal,
			outRequests:   common.CaseLevelLow,
			outThroughput: common.CaseLevelLow,
			outLatency:    common.CaseLevelLow,
			outErrors:     common.CaseLevelLow,
		})

	cases.Add(common.CaseImpactSevere, common.CaseCategoryFailure,
		"Retry Amplification Storm",
		"Out requests to In requests ratio spikes far above historical norm",
		"Cascading client retries or unbounded fan-out loops",
		ApplicationCasePattern{
			inRequests:    common.CaseLevelNormal,
			inThroughput:  common.CaseLevelNormal,
			inLatency:     common.CaseLevelHigh,
			inErrors:      common.CaseLevelHigh,
			appCPU:        common.CaseLevelHigh,
			appMem:        common.CaseLevelNormal,
			hostCPU:       common.CaseLevelHigh,
			hostMem:       common.CaseLevelNormal,
			outRequests:   common.CaseLevelHigh,
			outThroughput: common.CaseLevelHigh,
			outLatency:    common.CaseLevelHigh,
			outErrors:     common.CaseLevelHigh,
		})

	cases.Add(common.CaseImpactSevere, common.CaseCategoryFailure,
		"Network Partition / TCP Blackhole",
		"Outbound attempts stay up while outbound timeouts explode",
		"Switch/router failure egress security group drop routing loop",
		ApplicationCasePattern{
			inRequests:    common.CaseLevelNormal,
			inThroughput:  common.CaseLevelNormal,
			inLatency:     common.CaseLevelHigh,
			inErrors:      common.CaseLevelHigh,
			appCPU:        common.CaseLevelNormal,
			appMem:        common.CaseLevelNormal,
			hostCPU:       common.CaseLevelNormal,
			hostMem:       common.CaseLevelNormal,
			outRequests:   common.CaseLevelNormal,
			outThroughput: common.CaseLevelNormal,
			outLatency:    common.CaseLevelHigh,
			outErrors:     common.CaseLevelHigh,
		})

	cases.Add(common.CaseImpactSevere, common.CaseCategoryFailure,
		"Host Memory Exhaustion / Swap Thrashing",
		"Host memory exhausted causing OS page swing; CPU drops due to I/O wait",
		"Host swing pages to disk; disk I/O blocks runloops across containers / processes",
		ApplicationCasePattern{
			inRequests:    common.CaseLevelNormal,
			inThroughput:  common.CaseLevelNormal,
			inLatency:     common.CaseLevelHigh,
			inErrors:      common.CaseLevelHigh,
			appCPU:        common.CaseLevelLow,
			appMem:        common.CaseLevelNormal,
			hostCPU:       common.CaseLevelLow,
			hostMem:       common.CaseLevelHigh,
			outRequests:   common.CaseLevelLow,
			outThroughput: common.CaseLevelLow,
			outLatency:    common.CaseLevelLow,
			outErrors:     common.CaseLevelLow,
		})

	cases.Add(common.CaseImpactSevere, common.CaseCategoryFailure,
		"App OOM-Kill CrashLoop",
		"App memory drops to near 0% after 100% breach; crash loop restarts",
		"Kernel OOM-killer terminated container (SIGKILL); pod restarting",
		ApplicationCasePattern{
			inRequests:    common.CaseLevelNormal,
			inThroughput:  common.CaseLevelNormal,
			inLatency:     common.CaseLevelHigh,
			inErrors:      common.CaseLevelHigh,
			appCPU:        common.CaseLevelLow,
			appMem:        common.CaseLevelLow,
			hostCPU:       common.CaseLevelNormal,
			hostMem:       common.CaseLevelNormal,
			outRequests:   common.CaseLevelLow,
			outThroughput: common.CaseLevelLow,
			outLatency:    common.CaseLevelLow,
			outErrors:     common.CaseLevelLow,
		})

	cases.Add(common.CaseImpactSevere, common.CaseCategoryFailure,
		"Host Kernel OOM Lockup",
		"Host memory pinned at 100%; kswapd pegs Host CPU at 100%; total freeze",
		"Host physical RAM exhausted; kswapd thrashing kernel page alloc locks",
		ApplicationCasePattern{
			inRequests:    common.CaseLevelNormal,
			inThroughput:  common.CaseLevelNormal,
			inLatency:     common.CaseLevelHigh,
			inErrors:      common.CaseLevelHigh,
			appCPU:        common.CaseLevelHigh,
			appMem:        common.CaseLevelNormal,
			hostCPU:       common.CaseLevelHigh,
			hostMem:       common.CaseLevelHigh,
			outRequests:   common.CaseLevelLow,
			outThroughput: common.CaseLevelLow,
			outLatency:    common.CaseLevelHigh,
			outErrors:     common.CaseLevelHigh,
		})

	cases.Add(common.CaseImpactSevere, common.CaseCategoryFailure,
		"Zombie Worker / Cold Init Deadlock",
		"App memory flat near 0% with 0% CPU; incoming requests fail instantly (502)",
		"Process deadlocked in pre-main init phase; runtime heap unallocated",
		ApplicationCasePattern{
			inRequests:    common.CaseLevelNormal,
			inThroughput:  common.CaseLevelNormal,
			inLatency:     common.CaseLevelLow,
			inErrors:      common.CaseLevelHigh,
			appCPU:        common.CaseLevelLow,
			appMem:        common.CaseLevelLow,
			hostCPU:       common.CaseLevelNormal,
			hostMem:       common.CaseLevelNormal,
			outRequests:   common.CaseLevelLow,
			outThroughput: common.CaseLevelLow,
			outLatency:    common.CaseLevelLow,
			outErrors:     common.CaseLevelLow,
		})

	cases.Add(common.CaseImpactSevere, common.CaseCategoryFailure,
		"Host OOM-Killer Cascading Storm",
		"Host memory at 100% with random service restarts and network drops",
		"OOM-killer killing random background processes and sidecars",
		ApplicationCasePattern{
			inRequests:    common.CaseLevelNormal,
			inThroughput:  common.CaseLevelNormal,
			inLatency:     common.CaseLevelHigh,
			inErrors:      common.CaseLevelHigh,
			appCPU:        common.CaseLevelLow,
			appMem:        common.CaseLevelNormal,
			hostCPU:       common.CaseLevelHigh,
			hostMem:       common.CaseLevelHigh,
			outRequests:   common.CaseLevelLow,
			outThroughput: common.CaseLevelLow,
			outLatency:    common.CaseLevelLow,
			outErrors:     common.CaseLevelLow,
		})

	cases.Add(common.CaseImpactSevere, common.CaseCategoryFailure,
		"FD / Ephemeral Port Exhaustion",
		"Fast failure on inbound/outbound connection creation with normal CPU/memory",
		"EMFILE or EADDRNOTAVAIL socket leak connection pool exhausting ulimit",
		ApplicationCasePattern{
			inRequests:    common.CaseLevelNormal,
			inThroughput:  common.CaseLevelNormal,
			inLatency:     common.CaseLevelLow,
			inErrors:      common.CaseLevelHigh,
			appCPU:        common.CaseLevelNormal,
			appMem:        common.CaseLevelNormal,
			hostCPU:       common.CaseLevelNormal,
			hostMem:       common.CaseLevelNormal,
			outRequests:   common.CaseLevelLow,
			outThroughput: common.CaseLevelLow,
			outLatency:    common.CaseLevelLow,
			outErrors:     common.CaseLevelHigh,
		})

	cases.Add(common.CaseImpactSevere, common.CaseCategoryFailure,
		"DNS Resolution Outage",
		"Outbound traffic drops with fast DNS resolution failures; ingress times out",
		"CoreDNS outage VPC resolver throttle corrupted resolv.conf",
		ApplicationCasePattern{
			inRequests:    common.CaseLevelNormal,
			inThroughput:  common.CaseLevelNormal,
			inLatency:     common.CaseLevelHigh,
			inErrors:      common.CaseLevelHigh,
			appCPU:        common.CaseLevelLow,
			appMem:        common.CaseLevelNormal,
			hostCPU:       common.CaseLevelLow,
			hostMem:       common.CaseLevelNormal,
			outRequests:   common.CaseLevelLow,
			outThroughput: common.CaseLevelLow,
			outLatency:    common.CaseLevelLow,
			outErrors:     common.CaseLevelHigh,
		})

	cases.Add(common.CaseImpactSevere, common.CaseCategoryFailure,
		"Cloud Hypervisor Steal / Throttling",
		"Latency surges despite zero App/Host CPU usage; hypervisor steal time pinned",
		"AWS burst credit balance exhaustion or noisy hypervisor neighbor",
		ApplicationCasePattern{
			inRequests:    common.CaseLevelNormal,
			inThroughput:  common.CaseLevelNormal,
			inLatency:     common.CaseLevelHigh,
			inErrors:      common.CaseLevelHigh,
			appCPU:        common.CaseLevelLow,
			appMem:        common.CaseLevelNormal,
			hostCPU:       common.CaseLevelLow,
			hostMem:       common.CaseLevelNormal,
			outRequests:   common.CaseLevelLow,
			outThroughput: common.CaseLevelLow,
			outLatency:    common.CaseLevelLow,
			outErrors:     common.CaseLevelLow,
		})

	cases.Add(common.CaseImpactSevere, common.CaseCategoryFailure,
		"Unobservable Bottleneck (Telemetry Dropout)",
		"Severe inbound queueing and outbound collapse while saturation is entirely unknown",
		"exporter or cgroup telemetry crash during critical CPU/memory exhaustion",
		ApplicationCasePattern{
			inRequests:    common.CaseLevelHigh,
			inThroughput:  common.CaseLevelHigh,
			inLatency:     common.CaseLevelHigh,
			inErrors:      common.CaseLevelHigh,
			appCPU:        common.CaseLevelUnknown,
			appMem:        common.CaseLevelUnknown,
			hostCPU:       common.CaseLevelUnknown,
			hostMem:       common.CaseLevelUnknown,
			outRequests:   common.CaseLevelLow,
			outThroughput: common.CaseLevelLow,
			outLatency:    common.CaseLevelLow,
			outErrors:     common.CaseLevelLow,
		})

	return cases
}

// ApplicationCaseVerdict

func (acv *ApplicationCaseVerdict) Name() string {
	return acv.acase.name
}

func (acv *ApplicationCaseVerdict) Description() string {
	return acv.acase.description
}

func (acv *ApplicationCaseVerdict) RootCause() string {
	return acv.acase.rootCause
}

func (acv *ApplicationCaseVerdict) Category() common.CaseCategory {
	return acv.acase.impact
}

func (acv *ApplicationCaseVerdict) Impact() common.CaseImpact {
	return acv.acase.category
}

func (acv *ApplicationCaseVerdict) Score() int {
	return acv.score
}

// ApplicationTrafficValues

func (tvs ApplicationTrafficValues) Sum() (common.Traffic, bool) {
	tot := 0.0
	hasValid := false
	for _, v := range tvs {
		if v != nil {
			tot += *v
			hasValid = true
		}
	}
	return tot, hasValid
}

// ApplicationFramePoint

func (fp *ApplicationFramePoint) Timestamp() time.Time {
	return fp.timestamp
}

func (fp *ApplicationFramePoint) Value() float64 {
	return fp.value
}

func (fp *ApplicationFramePoint) Max() float64 {
	return fp.max
}

func (fp *ApplicationFramePoint) Min() float64 {
	return fp.min
}

func NewApplicationFramePoint(timestamp time.Time, value, min, max float64) *ApplicationFramePoint {

	return &ApplicationFramePoint{
		timestamp: timestamp,
		value:     value,
		max:       max,
		min:       min,
	}
}

// ApplicationFramePoints

func (fp *ApplicationFramePoints) IsEmpty() bool {

	return fp.inRequests == nil &&
		fp.inThroughput == nil &&
		fp.inLatency == nil &&
		fp.inErrors == nil &&
		fp.outRequests == nil &&
		fp.outThroughput == nil &&
		fp.outLatency == nil &&
		fp.outErrors == nil &&
		fp.appCPU == nil &&
		fp.appMem == nil &&
		fp.hostCPU == nil &&
		fp.hostMem == nil
}

func (fp *ApplicationFramePoints) InRequets() *ApplicationFramePoint {
	return fp.inRequests
}

func (fp *ApplicationFramePoints) InThroughput() *ApplicationFramePoint {
	return fp.inThroughput
}

func (fp *ApplicationFramePoints) InLatency() *ApplicationFramePoint {
	return fp.inLatency
}

func (fp *ApplicationFramePoints) InErrors() *ApplicationFramePoint {
	return fp.inErrors
}

func (fp *ApplicationFramePoints) OutRequets() *ApplicationFramePoint {
	return fp.outRequests
}

func (fp *ApplicationFramePoints) OutThroughput() *ApplicationFramePoint {
	return fp.outThroughput
}

func (fp *ApplicationFramePoints) OutLatency() *ApplicationFramePoint {
	return fp.outLatency
}

func (fp *ApplicationFramePoints) OutErrors() *ApplicationFramePoint {
	return fp.outErrors
}

func (fp *ApplicationFramePoints) CPU() *ApplicationFramePoint {
	return fp.appCPU
}

func (fp *ApplicationFramePoints) Memory() *ApplicationFramePoint {
	return fp.appMem
}

func (fp *ApplicationFramePoints) HostCPU() *ApplicationFramePoint {
	return fp.hostCPU
}

func (fp *ApplicationFramePoints) HostMemory() *ApplicationFramePoint {
	return fp.hostMem
}

// ApplicationFrame

func (af *ApplicationFrame) Valid() bool {

	if af.stamp == 0 {
		return false
	}
	return true
}

func (af *ApplicationFrame) Stamp() common.Stamp {
	return af.stamp
}

func (af *ApplicationFrame) Begin() common.Stamp {
	return af.begin
}

func (af *ApplicationFrame) End() common.Stamp {
	return af.end
}

func (af *ApplicationFrame) Application() common.Hash {
	return af.application
}

func (af *ApplicationFrame) Host() common.Hash {
	return af.host
}

func NewApplicationFrame(stamp common.Stamp, appSignal *common.ApplicationSignal, hostSignal *common.HostSignal, host common.Hash) *ApplicationFrame {

	r := &ApplicationFrame{
		application: appSignal.Application(),
		host:        host,
		//
		stamp: stamp,
		begin: stamp,
		end:   stamp,
		//
		inRequests:   appSignal.IncomingTraffic.RequestsAvg(),
		inThroughput: appSignal.IncomingTraffic.ThroughputAvg(),
		inLatency:    appSignal.IncomingLatency.Avg(),
		inErrors:     appSignal.IncomingErrors.Sum(),
		//
		appCPU: appSignal.Saturation.CPUMax(),
		appMem: appSignal.Saturation.MemoryMax(),
		//
		outRequests:   appSignal.OutgoingTraffic.RequestsAvg(),
		outThroughput: appSignal.OutgoingTraffic.ThroughputAvg(),
		outLatency:    appSignal.OutgoingLatency.Avg(),
		outErrors:     appSignal.OutgoingErrors.Sum(),
	}

	if !utils.IsEmpty(hostSignal) {
		if r.host == common.Hash(0) {
			r.host = hostSignal.Hash()
		}
		r.hostCPU = hostSignal.Saturation.CPUMax()
		r.hostMem = hostSignal.Saturation.MemoryMax()
	}
	return r
}

// ApplicationTrafficSlots

func (ss *ApplicationTrafficSlots) Fit(samples []ApplicationTrafficValues) {

	volMap := make(map[common.TrafficKind]common.Traffic)
	for _, s := range samples {
		for k, v := range s {
			if v != nil {
				volMap[k] += *v
			}
		}
	}

	type kv struct {
		k common.TrafficKind
		v common.Traffic
	}
	var pairs []kv
	for k, v := range volMap {
		pairs = append(pairs, kv{k, v})
	}

	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].v > pairs[j].v
	})

	ss.KeyToSlot = make(map[common.TrafficKind]int)
	ss.SlotToKey = make([]common.TrafficKind, ss.MaxSlots)

	limit := len(pairs)
	if limit > ss.MaxSlots {
		limit = ss.MaxSlots
	}

	for i := 0; i < limit; i++ {
		ss.SlotToKey[i] = pairs[i].k
		ss.KeyToSlot[pairs[i].k] = i
	}
	ss.IsFitted = true
}

func (ss *ApplicationTrafficSlots) VectorizeTraffic(avs ApplicationTrafficValues, useMaxForTotal bool, medians []float64) ([]float64, []bool) {

	vec := make([]float64, ss.MaxSlots+2)
	valid := make([]bool, ss.MaxSlots+2)
	otherIdx := ss.MaxSlots
	totalIdx := ss.MaxSlots + 1

	for k, v := range avs {
		if v == nil {
			continue
		}

		value := *v
		if useMaxForTotal {
			if !valid[totalIdx] || value > vec[totalIdx] {
				vec[totalIdx] = value
			}
		} else {
			vec[totalIdx] += value
		}
		valid[totalIdx] = true

		if slot, ok := ss.KeyToSlot[k]; ok {
			if useMaxForTotal {
				if !valid[slot] || value > vec[slot] {
					vec[slot] = value
				}
			} else {
				vec[slot] += value
			}
			valid[slot] = true
		} else {
			if useMaxForTotal {
				if !valid[otherIdx] || value > vec[otherIdx] {
					vec[otherIdx] = value
				}
			} else {
				vec[otherIdx] += value
			}
			valid[otherIdx] = true
		}
	}

	if len(medians) == len(vec) {
		for i := range vec {
			if !valid[i] {
				vec[i] = medians[i]
			}
		}
	}

	return vec, valid
}

func NewApplicationTrafficSlots(maxSlots int) *ApplicationTrafficSlots {

	return &ApplicationTrafficSlots{
		MaxSlots:  maxSlots,
		KeyToSlot: make(map[common.TrafficKind]int),
		SlotToKey: make([]common.TrafficKind, maxSlots),
	}
}

// ApplicationProfiler

func TimeWindowKey(t time.Time, kind ApplicationProfilerBucketsKind) int {

	t = t.UTC()
	tm := int(t.Weekday())*24 + t.Hour()*60 + t.Minute()
	switch kind {
	case ApplicationProfilerBucketsKindDay:
		return int(t.Weekday())
	case ApplicationProfilerBucketsKindHour:
		return tm / 60
	case ApplicationProfilerBucketsKindHalfHour:
		return tm / 30
	case ApplicationProfilerBucketsKind15Minutes:
		tm := int(t.Weekday())*24*60 + t.Hour()*60 + t.Minute()
		return tm / 15
	case ApplicationProfilerBucketsKind10Minutes:
		return tm / 10
	case ApplicationProfilerBucketsKind5Minutes:
		return tm / 5
	case ApplicationProfilerBucketsKindMinute:
		return tm
	}
	return 0
}

func Percentile(sorted []float64, p float64) float64 {
	idx := p * float64(len(sorted)-1)
	l := int(math.Floor(idx))
	u := int(math.Ceil(idx))
	w := idx - float64(l)
	return sorted[l]*(1.0-w) + sorted[u]*w
}

/*func (ap *ApplicationProfiler) Fit(stamps []common.Stamp, matrix [][]float64) {

	if len(matrix) == 0 || len(matrix) != len(stamps) {
		return
	}
	numCols := len(matrix[0])

	bucketSize := len(ap.Exists)

	bucketData := make([][][]float64, bucketSize)
	for b := range bucketData {
		bucketData[b] = make([][]float64, numCols)
	}

	for row := 0; row < len(matrix); row++ {

		t := common.StampToTime(stamps[row])
		b := TimeWindowKey(t, ap.Kind)
		for col := 0; col < numCols; col++ {
			bucketData[b][col] = append(bucketData[b][col], matrix[row][col])
		}
	}

	for b := 0; b < bucketSize; b++ {

		if !ap.Exists[b] {
			ap.Buckets[b] = make([]ApplicationFeatureStats, numCols)
		}
		for col := 0; col < numCols; col++ {
			vals := bucketData[b][col]
			if len(vals) == 0 {
				ap.Buckets[b][col] = ApplicationFeatureStats{Median: 0, Q1: 0, Q3: 0, IQR: 1.0}
				continue
			}
			sort.Float64s(vals)

			q1 := Percentile(vals, 0.25)
			med := Percentile(vals, 0.50)
			q3 := Percentile(vals, 0.75)
			iqr := q3 - q1

			if iqr == 0 {
				iqr = med * 0.05
				if iqr == 0 {
					iqr = 1.0
				}
			}

			ap.Buckets[b][col] = ApplicationFeatureStats{
				Median: med,
				Q1:     q1,
				Q3:     q3,
				IQR:    iqr,
			}
			ap.Exists[b] = true
		}
	}
	ap.IsFitted = true
}*/

func (ap *ApplicationProfiler) Fit(stamps []common.Stamp, matrix [][]float64) {
	if len(matrix) == 0 || len(matrix) != len(stamps) {
		return
	}
	numCols := len(matrix[0])
	bucketSize := len(ap.Exists)

	// Временное хранилище для сбора всех значений по бакетам из нового пакета данных
	bucketData := make([][][]float64, bucketSize)
	for b := range bucketData {
		bucketData[b] = make([][]float64, numCols)
	}

	// 1. Распределяем новые точки по бакетам с проверкой границ
	for row := 0; row < len(matrix); row++ {
		t := common.StampToTime(stamps[row])
		b := TimeWindowKey(t, ap.Kind)

		// Защита от выхода за пределы массива бакетов
		if b < 0 || b >= bucketSize {
			continue
		}

		for col := 0; col < numCols; col++ {
			bucketData[b][col] = append(bucketData[b][col], matrix[row][col])
		}
	}

	// 2. Обновляем статистику для каждого бакета
	for b := 0; b < bucketSize; b++ {
		// Проверяем, есть ли вообще данные в этом бакете за этот проход
		hasDataInBucket := false
		for col := 0; col < numCols; col++ {
			if len(bucketData[b][col]) > 0 {
				hasDataInBucket = true
				break
			}
		}

		// Если в бакет не попало новых данных:
		if !hasDataInBucket {
			if !ap.Exists[b] {
				// Если бакет пустой и никогда не заполнялся — инициализируем дефолтом
				ap.Buckets[b] = make([]ApplicationFeatureStats, numCols)
				for col := 0; col < numCols; col++ {
					ap.Buckets[b][col] = ApplicationFeatureStats{Median: 0, Q1: 0, Q3: 0, IQR: 1.0}
				}
			}
			continue
		}

		// Инициализируем слайс статистики для бакета, если он еще не существовал
		if !ap.Exists[b] {
			ap.Buckets[b] = make([]ApplicationFeatureStats, numCols)
		}

		// 3. Считаем квантили по каждой колонке
		for col := 0; col < numCols; col++ {
			vals := bucketData[b][col]
			if len(vals) == 0 {
				// Если для конкретной колонки нет новых данных, но бакет уже существовал —
				// оставляем старые значения (ничего не перезаписываем)
				continue
			}

			sort.Float64s(vals)

			q1 := Percentile(vals, 0.25)
			med := Percentile(vals, 0.50)
			q3 := Percentile(vals, 0.75)
			iqr := q3 - q1

			if iqr == 0 {
				iqr = med * 0.05
				if iqr == 0 {
					iqr = 1.0
				}
			}

			ap.Buckets[b][col] = ApplicationFeatureStats{
				Median: med,
				Q1:     q1,
				Q3:     q3,
				IQR:    iqr,
			}
		}

		// Помечаем бакет как существующий/активный
		ap.Exists[b] = true
	}
	ap.IsFitted = true
}

func (ap *ApplicationProfiler) Classify(b int, col int, val float64, isAnomaly, isValid bool) common.CaseLevel {

	if !isValid {
		return common.CaseLevelUnknown
	}

	st := ap.Buckets[b][col]
	highT := st.Q3 + (ap.K * st.IQR)
	lowT := st.Q1 - (ap.K * st.IQR)

	if val >= highT {
		return common.CaseLevelHigh
	}
	if val <= lowT {
		return common.CaseLevelLow
	}
	if isAnomaly {
		if val > st.Median {
			return common.CaseLevelHigh
		}
		return common.CaseLevelLow
	}
	return common.CaseLevelNormal
}

func NewApplicationProfiler(k float64, kind ApplicationProfilerBucketsKind) *ApplicationProfiler {

	size := 0
	switch kind {
	case ApplicationProfilerBucketsKindDay:
		size = 7
	case ApplicationProfilerBucketsKindHour:
		size = 7 * 24
	case ApplicationProfilerBucketsKindHalfHour:
		size = 7 * 24 * 2
	case ApplicationProfilerBucketsKind15Minutes:
		size = 7 * 24 * 4
	case ApplicationProfilerBucketsKind10Minutes:
		size = 7 * 24 * 6
	case ApplicationProfilerBucketsKind5Minutes:
		size = 7 * 24 * 12
	case ApplicationProfilerBucketsKindMinute:
		size = 7 * 24 * 60
	}
	return &ApplicationProfiler{
		Buckets: make([][]ApplicationFeatureStats, size),
		Exists:  make([]bool, size),
		K:       k,
		Kind:    kind,
	}
}

// ApplicationEngine

func (ae *ApplicationEngine) Name() string {
	return "ForestApplicationEngine"
}

func (ae *ApplicationEngine) getMedians(profiler *ApplicationProfiler, t time.Time, startIdx, count int) []float64 {

	meds := make([]float64, count)
	if !profiler.IsFitted {
		return meds
	}
	b := TimeWindowKey(t, profiler.Kind)
	if len(profiler.Buckets[b]) < startIdx+count {
		return meds
	}
	for i := 0; i < count; i++ {
		meds[i] = profiler.Buckets[b][startIdx+i].Median
	}
	return meds
}

/*
func (ae *ApplicationEngine) extractTimeVectors(t time.Time) []float64 {

	m := float64(t.Minute()) + float64(t.Second())/60.0
	h := float64(t.Hour()) + m/60.0
	wd := float64(t.Weekday()) + h/24.0
	md := float64(t.Day()-1) + h/24.0

	twoPi := 2.0 * math.Pi

	return []float64{
		math.Sin(twoPi * m / 60.0), math.Cos(twoPi * m / 60.0), // Minute (fractional)
		math.Sin(twoPi * h / 24.0), math.Cos(twoPi * h / 24.0), // Hour (fractional)
		math.Sin(twoPi * wd / 7.0), math.Cos(twoPi * wd / 7.0), // Day of Week
		math.Sin(twoPi * md / 31.0), math.Cos(twoPi * md / 31.0), // Day of Month
	}
}
*/

func (ae *ApplicationEngine) extractTimeVectors(t time.Time) []float64 {

	t = t.UTC()

	m := float64(t.Minute())
	h := float64(t.Hour()) + m/60.0
	wd := float64(t.Weekday()) + h/24.0
	md := float64(t.Day() - 1)

	twoPi := 2.0 * math.Pi

	return []float64{
		math.Sin(twoPi * m / 60.0), math.Cos(twoPi * m / 60.0), // Minute of hour (0-59)
		math.Sin(twoPi * h / 24.0), math.Cos(twoPi * h / 24.0), // Hour of day (0-23)
		math.Sin(twoPi * wd / 7.0), math.Cos(twoPi * wd / 7.0), // Weekday (0-6)
		math.Sin(twoPi * md / 31.0), math.Cos(twoPi * md / 31.0), // Day of month (1-31)
	}
}

func (ae *ApplicationEngine) weeklyMinuteKey(t time.Time) int {

	t = t.UTC()

	weekday := int(t.Weekday()) // Sunday = 0, Monday = 1, ..., Saturday = 6
	hour := t.Hour()            // 0 - 23
	minute := t.Minute()        // 0 - 59

	return (weekday * 24 * 60) + (hour * 60) + minute
}

type ApplicationEngineVectors struct {
	time                                                                            time.Time
	bucket                                                                          int
	inVec, satVec, outVec                                                           []float64
	inValid, satValid, outValid                                                     []bool
	inReqIdx, inThruIdx, inLatIdx, inErrIdx                                         int
	appCPUIdx, appMemIdx, hostCPUIdx, hostMemIdx, cpuDiffIdx, memDiffIdx            int
	outReqIdx, outThruIdx, outLatIdx, outErrIdx, flowReqIdx, flowThruIdx, latDivIdx int
}

func (ae *ApplicationEngine) extractVectors(t time.Time, frame *ApplicationFrame) (vectors ApplicationEngineVectors) {

	t = t.UTC()
	vectors.time = t
	timeVec := ae.extractTimeVectors(t)

	inSlotWidth := ae.inReqSlots.MaxSlots + 2

	inReqMeds := ae.getMedians(ae.inProfiler, t, 0, inSlotWidth)
	inThruMeds := ae.getMedians(ae.inProfiler, t, inSlotWidth, inSlotWidth)

	inReqSlots, inReqVal := ae.inReqSlots.VectorizeTraffic(frame.inRequests, false, inReqMeds)
	vectors.inErrIdx = len(inReqSlots) - 1

	inThruSlots, inThruVal := ae.inThruSlots.VectorizeTraffic(frame.inThroughput, false, inThruMeds)
	vectors.inThruIdx = len(inReqSlots) + len(inThruSlots) - 1

	vectors.inLatIdx = vectors.inThruIdx + 1
	vectors.inErrIdx = vectors.inLatIdx + 1

	b := TimeWindowKey(t, ae.inProfiler.Kind)
	vectors.bucket = b

	inLatValid := frame.inLatency != nil
	inLat := 0.0
	if inLatValid {
		inLat = *frame.inLatency
	}
	if !inLatValid && ae.inProfiler.IsFitted && len(ae.inProfiler.Buckets[b]) > vectors.inLatIdx {
		inLat = ae.inProfiler.Buckets[b][vectors.inLatIdx].Median
	}

	inErrValid := frame.inErrors != nil
	inErr := 0.0
	if inErrValid {
		inErr = *frame.inErrors
	}
	if !inErrValid && ae.inProfiler.IsFitted && len(ae.inProfiler.Buckets[b]) > vectors.inErrIdx {
		inErr = ae.inProfiler.Buckets[b][vectors.inErrIdx].Median
	}

	vectors.inVec = append(vectors.inVec, inReqSlots...)
	vectors.inVec = append(vectors.inVec, inThruSlots...)
	vectors.inVec = append(vectors.inVec, inLat, inErr)
	vectors.inVec = append(vectors.inVec, timeVec...)

	vectors.inValid = append(vectors.inValid, inReqVal...)
	vectors.inValid = append(vectors.inValid, inThruVal...)
	vectors.inValid = append(vectors.inValid, inLatValid, inErrValid)
	for i := 0; i < len(timeVec); i++ {
		vectors.inValid = append(vectors.inValid, true) // Time is always valid
	}

	vectors.appCPUIdx = 0
	appCPUValid := frame.appCPU != nil
	appCPU := 0.0
	if appCPUValid {
		appCPU = *frame.appCPU
	}
	if !appCPUValid && ae.satProfiler.IsFitted && len(ae.satProfiler.Buckets[b]) > vectors.appCPUIdx {
		appCPU = ae.satProfiler.Buckets[b][vectors.appCPUIdx].Median
	}

	vectors.appMemIdx = vectors.appCPUIdx + 1
	appMemValid := frame.appMem != nil
	appMem := 0.0
	if appMemValid {
		appMem = *frame.appMem
	}
	if !appMemValid && ae.satProfiler.IsFitted && len(ae.satProfiler.Buckets[b]) > vectors.appMemIdx {
		appMem = ae.satProfiler.Buckets[b][vectors.appMemIdx].Median
	}

	vectors.hostCPUIdx = vectors.appMemIdx + 1
	hostCPUValid := frame.hostCPU != nil
	hostCPU := 0.0
	if hostCPUValid {
		hostCPU = *frame.hostCPU
	}
	if !hostCPUValid && ae.satProfiler.IsFitted && len(ae.satProfiler.Buckets[b]) > vectors.hostCPUIdx {
		hostCPU = ae.satProfiler.Buckets[b][vectors.hostCPUIdx].Median
	}

	vectors.hostMemIdx = vectors.hostCPUIdx + 1
	hostMemValid := frame.hostMem != nil
	hostMem := 0.0
	if hostMemValid {
		hostMem = *frame.hostMem
	}
	if !hostMemValid && ae.satProfiler.IsFitted && len(ae.satProfiler.Buckets[b]) > vectors.hostMemIdx {
		hostMem = ae.satProfiler.Buckets[b][vectors.hostMemIdx].Median
	}

	vectors.cpuDiffIdx = vectors.hostMemIdx + 1
	cpuDiffVal := false
	cpuDiff := 0.0
	if hostCPUValid && appCPUValid {
		cpuDiffVal = true
		cpuDiff = hostCPU - appCPU
	} else if ae.satProfiler.IsFitted && len(ae.satProfiler.Buckets[b]) > vectors.cpuDiffIdx {
		cpuDiff = ae.satProfiler.Buckets[b][vectors.cpuDiffIdx].Median
	}

	vectors.memDiffIdx = vectors.cpuDiffIdx + 1
	memDiffVal := false
	memDiff := 0.0
	if hostMemValid && appMemValid {
		memDiffVal = true
		memDiff = hostMem - appMem
	} else if ae.satProfiler.IsFitted && len(ae.satProfiler.Buckets[b]) > vectors.memDiffIdx {
		memDiff = ae.satProfiler.Buckets[b][vectors.memDiffIdx].Median
	}

	vectors.satVec = []float64{appCPU, appMem, hostCPU, hostMem, cpuDiff, memDiff}
	vectors.satVec = append(vectors.satVec, timeVec...)

	vectors.satValid = []bool{appCPUValid, appMemValid, hostCPUValid, hostMemValid, cpuDiffVal, memDiffVal}
	for i := 0; i < len(timeVec); i++ {
		vectors.satValid = append(vectors.satValid, true) // Time is always valid
	}

	outSlotWidth := ae.outReqSlots.MaxSlots + 2

	outReqMeds := ae.getMedians(ae.outProfiler, t, 0, outSlotWidth)
	outThruMeds := ae.getMedians(ae.outProfiler, t, outSlotWidth, outSlotWidth)

	outReqSlots, outReqVal := ae.outReqSlots.VectorizeTraffic(frame.outRequests, false, outReqMeds)
	vectors.outReqIdx = len(outReqSlots) - 1

	outThruSlots, outThruVal := ae.outThruSlots.VectorizeTraffic(frame.outThroughput, false, outThruMeds)
	vectors.outThruIdx = len(outReqSlots) + len(outThruSlots) - 1

	vectors.outLatIdx = vectors.outThruIdx + 1
	vectors.outErrIdx = vectors.outLatIdx + 1

	outLatValid := frame.outLatency != nil
	outLat := 0.0
	if outLatValid {
		outLat = *frame.outLatency
	}
	if !outLatValid && ae.outProfiler.IsFitted && len(ae.outProfiler.Buckets[b]) > vectors.outLatIdx {
		outLat = ae.outProfiler.Buckets[b][vectors.outLatIdx].Median
	}

	outErrValid := frame.outErrors != nil
	outErr := 0.0
	if outErrValid {
		outErr = *frame.outErrors
	}
	if !outErrValid && ae.outProfiler.IsFitted && len(ae.outProfiler.Buckets[b]) > vectors.outErrIdx {
		outErr = ae.outProfiler.Buckets[b][vectors.outErrIdx].Median
	}

	inReqSum, inReqKnown := frame.inRequests.Sum()
	outReqSum, outReqKnown := frame.outRequests.Sum()
	vectors.flowReqIdx = vectors.outErrIdx + 1
	flowReqValid := false
	flowReqRatio := 0.0
	if inReqKnown && outReqKnown && inReqSum > 0 {
		flowReqValid = true
		flowReqRatio = outReqSum / inReqSum
	} else if ae.outProfiler.IsFitted && len(ae.outProfiler.Buckets[b]) > vectors.flowReqIdx {
		flowReqRatio = ae.outProfiler.Buckets[b][vectors.flowReqIdx].Median
	}

	inThruSum, inThruKnown := frame.inThroughput.Sum()
	outThruSum, outThruKnown := frame.outThroughput.Sum()
	vectors.flowThruIdx = vectors.flowReqIdx + 1
	flowThruValid := false
	flowThruRatio := 0.0
	if inThruKnown && outThruKnown && inThruSum > 0 {
		flowThruValid = true
		flowThruRatio = outThruSum / inThruSum
	} else if ae.outProfiler.IsFitted && len(ae.outProfiler.Buckets[b]) > vectors.flowThruIdx {
		flowThruRatio = ae.outProfiler.Buckets[b][vectors.flowThruIdx].Median
	}

	vectors.latDivIdx = vectors.flowThruIdx + 1
	latDivValid := false
	latDiv := 0.0
	if inLatValid && outLatValid {
		latDivValid = true
		latDiv = inLat - outLat
	} else if ae.outProfiler.IsFitted && len(ae.outProfiler.Buckets[b]) > vectors.latDivIdx {
		latDiv = ae.outProfiler.Buckets[b][vectors.latDivIdx].Median
	}

	vectors.outVec = append(vectors.outVec, outReqSlots...)
	vectors.outVec = append(vectors.outVec, outThruSlots...)
	vectors.outVec = append(vectors.outVec, outLat, outErr)
	vectors.outVec = append(vectors.outVec, flowReqRatio, flowThruRatio, latDiv)
	vectors.outVec = append(vectors.outVec, timeVec...)

	vectors.outValid = append(vectors.outValid, outReqVal...)
	vectors.outValid = append(vectors.outValid, outThruVal...)
	vectors.outValid = append(vectors.outValid, outLatValid, outErrValid)
	vectors.outValid = append(vectors.outValid, flowReqValid, flowThruValid, latDivValid)
	for i := 0; i < len(timeVec); i++ {
		vectors.outValid = append(vectors.outValid, true) // Time is always valid
	}

	return vectors
}

func (ae *ApplicationEngine) splitTimesData(from, first, last common.Stamp) (tL, tR []common.Stamp, inL, satL, outL, inR, satR, outR [][]float64) {

	lstamps := len(ae.stamps)
	if lstamps != len(ae.inData) ||
		lstamps != len(ae.satData) ||
		lstamps != len(ae.outData) {
		return tL, tR, inL, satL, outL, inR, satR, outR
	}

	for k, stamp := range ae.stamps {

		// skip outdated data
		if stamp < from {
			continue
		}

		// add only data outside of time limits
		if stamp < first {
			inL = append(inL, ae.inData[k])
			satL = append(satL, ae.satData[k])
			outL = append(outL, ae.outData[k])
			tL = append(tL, stamp)
		} else if stamp > last {
			inR = append(inR, ae.inData[k])
			satR = append(satR, ae.satData[k])
			outR = append(outR, ae.outData[k])
			tR = append(tR, stamp)
		}
	}
	return tL, tR, inL, satL, outL, inR, satR, outR
}

func (ae *ApplicationEngine) Train(frames ApplicationFrames, limits ApplicationFrameLimits) error {

	var inReq, inThru []ApplicationTrafficValues
	var outReq, outThru []ApplicationTrafficValues

	// sort frames by time stamp
	slices.SortFunc(frames, func(a *ApplicationFrame, b *ApplicationFrame) int {
		return cmp.Compare(a.Stamp(), b.Stamp())
	})

	temp := []*ApplicationFrame{}

	for _, f := range frames {

		index := slices.Index(ae.stamps, f.stamp)
		if index > -1 {
			continue
		}
		temp = append(temp, f)
		inReq = append(inReq, f.inRequests)
		inThru = append(inThru, f.inThroughput)
		outReq = append(outReq, f.outRequests)
		outThru = append(outThru, f.outThroughput)
	}
	// need to rethink
	ae.inReqSlots.Fit(inReq)
	ae.inThruSlots.Fit(inThru)
	ae.outReqSlots.Fit(outReq)
	ae.outThruSlots.Fit(outThru)

	tL, tR, inL, satL, outL, inR, satR, outR := ae.splitTimesData(limits.From, limits.First, limits.Last)

	stamps := []common.Stamp{}
	inData := [][]float64{}
	satData := [][]float64{}
	outData := [][]float64{}

	for _, f := range temp {

		t := common.StampToTime(f.stamp)

		vectors := ae.extractVectors(t, f)
		stamps = append(stamps, f.stamp)
		inData = append(inData, vectors.inVec)
		satData = append(satData, vectors.satVec)
		outData = append(outData, vectors.outVec)

		key := ae.weeklyMinuteKey(t)
		index := slices.Index(ae.minutes, key)
		if index == -1 {
			ae.minutes = append(ae.minutes, key)
		}
	}

	ae.stamps = append(tL, stamps...)
	ae.stamps = append(ae.stamps, tR...)

	ae.inData = append(inL, inData...)
	ae.inData = append(ae.inData, inR...)

	ae.satData = append(satL, satData...)
	ae.satData = append(ae.satData, satR...)

	ae.outData = append(outL, outData...)
	ae.outData = append(ae.outData, outR...)

	ae.inForest.Train(ae.inData)
	err := ae.inForest.Test(ae.inData)
	if err != nil {
		return err
	}
	ae.inProfiler.Fit(ae.stamps, ae.inData)

	ae.satForest.Train(ae.satData)
	err = ae.satForest.Test(ae.satData)
	if err != nil {
		return err
	}
	ae.satProfiler.Fit(ae.stamps, ae.satData)

	ae.outForest.Train(ae.outData)
	err = ae.outForest.Test(ae.outData)
	if err != nil {
		return err
	}
	ae.outProfiler.Fit(ae.stamps, ae.outData)

	return nil
}

type ApplicationEngineFileLoadKind = int

const (
	ApplicationEngineFileLoadKindData = iota
	ApplicationEngineFileLoadKindForest
)

var AllApplicationEngineFileLoadKinds = []ApplicationEngineFileLoadKind{ApplicationEngineFileLoadKindData, ApplicationEngineFileLoadKindForest}

func (ae *ApplicationEngine) decodeV001(decoder *gob.Decoder, kinds []ApplicationEngineFileLoadKind) error {

	engineFile := ApplicationEngineFileV001{}
	err := decoder.Decode(&engineFile)
	if err != nil {
		return err
	}

	ae.stamps = engineFile.Stamps
	ae.minutes = engineFile.Minutes

	if utils.Contains(kinds, ApplicationEngineFileLoadKindData) {

		lstamps := len(engineFile.Stamps)
		if lstamps != len(engineFile.Incoming.Data) ||
			lstamps != len(engineFile.Saturation.Data) ||
			lstamps != len(engineFile.Outgoing.Data) {
			return nil
		}
		ae.inData = engineFile.Incoming.Data
		ae.satData = engineFile.Saturation.Data
		ae.outData = engineFile.Outgoing.Data
	}

	if utils.Contains(kinds, ApplicationEngineFileLoadKindForest) {
		ae.inForest = engineFile.Incoming.Forest
		ae.satForest = engineFile.Saturation.Forest
		ae.outForest = engineFile.Outgoing.Forest
	}

	ae.inReqSlots = engineFile.Incoming.ReqSlots
	ae.inThruSlots = engineFile.Incoming.ThruSlots
	ae.inProfiler = engineFile.Incoming.Profile

	ae.satProfiler = engineFile.Saturation.Profile

	ae.outReqSlots = engineFile.Outgoing.ReqSlots
	ae.outThruSlots = engineFile.Outgoing.ThruSlots
	ae.outProfiler = engineFile.Outgoing.Profile

	return nil
}

func (ae *ApplicationEngine) load(kinds []ApplicationEngineFileLoadKind) error {

	fpath := filepath.Join(ae.options.Path, fmt.Sprintf("%d.data", ae.id))

	if !utils.FileExists(fpath) {
		return nil
	}

	f, err := os.Open(fpath)
	if err != nil {
		return err
	}
	defer f.Close()

	br := bufio.NewReader(f)

	decoder := gob.NewDecoder(br)

	var version string
	if err := decoder.Decode(&version); err != nil {
		return err
	}

	switch version {
	case ApplicationEngineV001:
		err = ae.decodeV001(decoder, kinds)
	}
	return err
}

func (ae *ApplicationEngine) LoadData() error {
	return ae.load([]ApplicationEngineFileLoadKind{ApplicationEngineFileLoadKindData})
}

func (ae *ApplicationEngine) LoadForest() error {
	return ae.load([]ApplicationEngineFileLoadKind{ApplicationEngineFileLoadKindForest})
}

func (ae *ApplicationEngine) Save() error {

	optPath := ae.options.Path
	if !utils.DirExists(optPath) {
		os.MkdirAll(optPath, os.ModePerm)
	}

	fpath := filepath.Join(optPath, fmt.Sprintf("%d.data", ae.id))

	f, err := os.Create(fpath)
	if err != nil {
		return err
	}
	defer f.Close()

	bw := bufio.NewWriter(f)
	defer bw.Flush()

	encoder := gob.NewEncoder(bw)

	if err := encoder.Encode(ApplicationEngineV001); err != nil {
		return err
	}

	engineFile := ApplicationEngineFileV001{
		Stamps:  ae.stamps,
		Minutes: ae.minutes,
		Incoming: ApplicationEngineFileIncoming{
			ReqSlots:  ae.inReqSlots,
			ThruSlots: ae.inThruSlots,
			Forest:    ae.inForest,
			Profile:   ae.inProfiler,
			Data:      ae.inData,
		},
		Saturation: ApplicationEngineFileSaturation{
			Forest:  ae.satForest,
			Profile: ae.satProfiler,
			Data:    ae.satData,
		},
		Outgoing: ApplicationEngineFileOutgoing{
			ReqSlots:  ae.outReqSlots,
			ThruSlots: ae.outThruSlots,
			Forest:    ae.outForest,
			Profile:   ae.outProfiler,
			Data:      ae.outData,
		},
	}
	return encoder.Encode(&engineFile)
}

func (ae *ApplicationEngine) Clone() *ApplicationEngine {

	return &ApplicationEngine{
		id: ae.id,
	}
}

func (ae *ApplicationEngine) extractPoints(vectors ApplicationEngineVectors) ApplicationFramePoints {

	getLimits := func(profiler *ApplicationProfiler, col int, val float64, isPercentage bool) (float64, float64) {
		b := vectors.bucket
		if profiler.IsFitted && len(profiler.Buckets[b]) > col && profiler.Exists[b] {
			st := profiler.Buckets[b][col]
			upper := st.Q3 + (profiler.K * st.IQR)
			lower := st.Q1 - (profiler.K * st.IQR)
			if lower < 0 {
				lower = 0
			}
			return upper, lower
		}
		// Default for bounded saturation metrics (AppCPU, HostCPU)
		if isPercentage {
			return 100.0, 0.0
		}
		// Dynamic default derived directly from the incoming frame value:
		// Sets a dynamic corridor (e.g., ±50% of the observed baseline)
		if val > 0 {
			abs := math.Abs(val*profiler.K - val)
			return val + abs, val - abs
		}
		// Fallback if metric arrives as 0 before fitting
		return 1.0, 0.0
	}

	points := ApplicationFramePoints{}

	if len(vectors.inVec) > vectors.inReqIdx && vectors.inValid[vectors.inReqIdx] {
		val := vectors.inVec[vectors.inReqIdx]
		max, min := getLimits(ae.inProfiler, vectors.inReqIdx, val, false)
		points.inRequests = NewApplicationFramePoint(vectors.time, val, min, max)
	}

	if len(vectors.inVec) > vectors.inThruIdx && vectors.inValid[vectors.inThruIdx] {
		val := vectors.inVec[vectors.inThruIdx]
		max, min := getLimits(ae.inProfiler, vectors.inThruIdx, val, false)
		points.inThroughput = NewApplicationFramePoint(vectors.time, val, min, max)
	}

	if len(vectors.inVec) > vectors.inLatIdx && vectors.inValid[vectors.inLatIdx] {
		val := vectors.inVec[vectors.inLatIdx]
		max, min := getLimits(ae.inProfiler, vectors.inLatIdx, val, false)
		points.inLatency = NewApplicationFramePoint(vectors.time, val, min, max)
	}

	if len(vectors.inVec) > vectors.inErrIdx && vectors.inValid[vectors.inErrIdx] {
		val := vectors.inVec[vectors.inErrIdx]
		max, min := getLimits(ae.inProfiler, vectors.inErrIdx, val, false)
		points.inErrors = NewApplicationFramePoint(vectors.time, val, min, max)
	}

	if len(vectors.outVec) > vectors.outReqIdx && vectors.outValid[vectors.outReqIdx] {
		val := vectors.outVec[vectors.outReqIdx]
		max, min := getLimits(ae.outProfiler, vectors.outReqIdx, val, false)
		points.inRequests = NewApplicationFramePoint(vectors.time, val, min, max)
	}

	if len(vectors.outVec) > vectors.outThruIdx && vectors.outValid[vectors.outThruIdx] {
		val := vectors.outVec[vectors.outThruIdx]
		max, min := getLimits(ae.outProfiler, vectors.outThruIdx, val, false)
		points.inThroughput = NewApplicationFramePoint(vectors.time, val, min, max)
	}

	if len(vectors.outVec) > vectors.outLatIdx && vectors.outValid[vectors.outLatIdx] {
		val := vectors.outVec[vectors.outLatIdx]
		max, min := getLimits(ae.outProfiler, vectors.outLatIdx, val, false)
		points.outLatency = NewApplicationFramePoint(vectors.time, val, min, max)
	}

	if len(vectors.outVec) > vectors.outErrIdx && vectors.outValid[vectors.outErrIdx] {
		val := vectors.outVec[vectors.outErrIdx]
		max, min := getLimits(ae.outProfiler, vectors.outErrIdx, val, false)
		points.inErrors = NewApplicationFramePoint(vectors.time, val, min, max)
	}

	if len(vectors.satVec) > vectors.appCPUIdx && vectors.satValid[vectors.appCPUIdx] {
		val := vectors.satVec[vectors.appCPUIdx]
		max, min := getLimits(ae.satProfiler, vectors.appCPUIdx, val, true)
		points.appCPU = NewApplicationFramePoint(vectors.time, val, min, max)
	}

	if len(vectors.satVec) > vectors.appMemIdx && vectors.satValid[vectors.appMemIdx] {
		val := vectors.satVec[vectors.appMemIdx]
		max, min := getLimits(ae.satProfiler, vectors.appMemIdx, val, true)
		points.appMem = NewApplicationFramePoint(vectors.time, val, min, max)
	}

	if len(vectors.satVec) > vectors.hostCPUIdx && vectors.satValid[vectors.hostCPUIdx] {
		val := vectors.satVec[vectors.hostCPUIdx]
		max, min := getLimits(ae.satProfiler, vectors.hostCPUIdx, val, true)
		points.hostCPU = NewApplicationFramePoint(vectors.time, val, min, max)
	}

	if len(vectors.satVec) > vectors.hostMemIdx && vectors.satValid[vectors.hostMemIdx] {
		val := vectors.satVec[vectors.hostMemIdx]
		max, min := getLimits(ae.satProfiler, vectors.hostMemIdx, val, true)
		points.hostMem = NewApplicationFramePoint(vectors.time, val, min, max)
	}
	return points
}

func (ae *ApplicationEngine) predictPattern(frame *ApplicationFrame) (*ApplicationFramePoints, *ApplicationCasePattern) {

	t := common.StampToTime(frame.stamp)

	vectors := ae.extractVectors(t, frame)
	points := ae.extractPoints(vectors)
	if !points.IsEmpty() {
		return nil, nil
	}

	inLabels, _, _ := ae.inForest.Predict([][]float64{vectors.inVec})
	satLabels, _, _ := ae.satForest.Predict([][]float64{vectors.satVec})
	outLabels, _, _ := ae.outForest.Predict([][]float64{vectors.outVec})

	// no labels
	if len(inLabels) == 0 || len(satLabels) == 0 || len(outLabels) == 0 {
		return &points, nil
	}

	inAnom := inLabels[0] == 1
	satAnom := satLabels[0] == 1
	outAnom := outLabels[0] == 1

	classify := func(profiler *ApplicationProfiler, idx int, vec []float64, valid []bool, anom bool) common.CaseLevel {
		return profiler.Classify(vectors.bucket, idx, vec[idx], anom, valid[idx])
	}

	inReqLevel := classify(ae.inProfiler, vectors.inReqIdx, vectors.inVec, vectors.inValid, inAnom)
	inThruLevel := classify(ae.inProfiler, vectors.inThruIdx, vectors.inVec, vectors.inValid, inAnom)
	inLatLevel := classify(ae.inProfiler, vectors.inLatIdx, vectors.inVec, vectors.inValid, inAnom)
	inErrLevel := classify(ae.inProfiler, vectors.inErrIdx, vectors.inVec, vectors.inValid, inAnom)

	appCPULevel := classify(ae.satProfiler, vectors.appCPUIdx, vectors.satVec, vectors.satValid, satAnom)
	appMemLevel := classify(ae.satProfiler, vectors.appMemIdx, vectors.satVec, vectors.satValid, satAnom)
	hostCPULevel := classify(ae.satProfiler, vectors.hostCPUIdx, vectors.satVec, vectors.satValid, satAnom)
	hostMemLevel := classify(ae.satProfiler, vectors.hostMemIdx, vectors.satVec, vectors.satValid, satAnom)

	outReqLevel := classify(ae.outProfiler, vectors.outReqIdx, vectors.outVec, vectors.outValid, outAnom)
	outThruLevel := classify(ae.outProfiler, vectors.outThruIdx, vectors.outVec, vectors.outValid, outAnom)
	outLatLevel := classify(ae.outProfiler, vectors.outLatIdx, vectors.outVec, vectors.outValid, outAnom)
	outErrLevel := classify(ae.outProfiler, vectors.outErrIdx, vectors.outVec, vectors.outValid, outAnom)

	pattern := &ApplicationCasePattern{
		inRequests:    inReqLevel,
		inThroughput:  inThruLevel,
		inLatency:     inLatLevel,
		inErrors:      inErrLevel,
		appCPU:        appCPULevel,
		appMem:        appMemLevel,
		hostCPU:       hostCPULevel,
		hostMem:       hostMemLevel,
		outRequests:   outReqLevel,
		outThroughput: outThruLevel,
		outLatency:    outLatLevel,
		outErrors:     outErrLevel,
	}
	return &points, pattern
}

func (ae *ApplicationEngine) aggregateFrames(frames ApplicationFrames) ApplicationFrame {

	sumTraffic := func(values, sum ApplicationTrafficValues, count map[common.TrafficKind]int) {

		for k, v := range values {
			vr := sum[k]
			if v != nil {
				vn := *v
				if vr != nil {
					vn = vn + *vr
				}
				sum[k] = &vn
				vc, ok := count[k]
				if !ok {
					count[k] = 1
				} else {
					count[k] = vc + 1
				}
			}
		}
	}

	avgTraffic := func(sum ApplicationTrafficValues, count map[common.TrafficKind]int) ApplicationTrafficValues {

		r := make(ApplicationTrafficValues)
		for k, v := range sum {

			c, ok := count[k]
			if v == nil || !ok {
				continue
			}
			v1 := common.Traffic(float64(*v) / float64(c))
			r[k] = &v1
		}
		return r
	}

	maxLatency := func(value ApplicationLatencyValue, max ApplicationLatencyValue) ApplicationLatencyValue {

		if value != nil {
			vl := max
			vn := *value
			if vl == nil || (vl != nil && vn > *vl) {
				return &vn
			}
		}
		return max
	}

	maxErrors := func(value ApplicationErrorsValue, max ApplicationErrorsValue) ApplicationErrorsValue {

		if value != nil {
			ve := max
			vn := *value
			if ve == nil || (ve != nil && vn > *ve) {
				return &vn
			}
		}
		return max
	}

	maxAppSaturation := func(value ApplicationSaturationValue, max ApplicationSaturationValue) ApplicationSaturationValue {

		if value != nil {
			vs := max
			vn := *value
			if vs == nil || (vs != nil && vn > *vs) {
				return &vn
			}
		}
		return max
	}

	maxHostSaturation := func(value HostSaturationValue, max HostSaturationValue) HostSaturationValue {

		if value != nil {
			vs := max
			vn := *value
			if vs == nil || (vs != nil && vn > *vs) {
				return &vn
			}
		}
		return max
	}

	sumInRequests := make(ApplicationTrafficValues)
	cntInRequests := make(map[common.TrafficKind]int)
	sumInThroughput := make(ApplicationTrafficValues)
	cntInThroughput := make(map[common.TrafficKind]int)
	var maxInLatency ApplicationLatencyValue
	var maxInErrors ApplicationErrorsValue

	var maxAppCPU ApplicationSaturationValue
	var maxAppMem ApplicationSaturationValue
	var maxHostCPU HostSaturationValue
	var maxHostMem HostSaturationValue

	sumOutRequests := make(ApplicationTrafficValues)
	cntOutRequests := make(map[common.TrafficKind]int)
	sumOutThroughput := make(ApplicationTrafficValues)
	cntOutThroughput := make(map[common.TrafficKind]int)
	var maxOutLatency ApplicationLatencyValue
	var maxOutErrors ApplicationErrorsValue

	for _, f := range frames {

		sumTraffic(f.inRequests, sumInRequests, cntInRequests)
		sumTraffic(f.inThroughput, sumInThroughput, cntInThroughput)
		maxInLatency = maxLatency(f.inLatency, maxInLatency)
		maxInErrors = maxErrors(f.inErrors, maxInErrors)

		maxAppCPU = maxAppSaturation(f.appCPU, maxAppCPU)
		maxAppMem = maxAppSaturation(f.appMem, maxAppMem)
		maxHostCPU = maxHostSaturation(f.hostCPU, maxHostCPU)
		maxHostMem = maxHostSaturation(f.hostMem, maxHostMem)

		sumTraffic(f.outRequests, sumOutRequests, cntOutRequests)
		sumTraffic(f.outThroughput, sumOutThroughput, cntOutThroughput)
		maxOutLatency = maxLatency(f.outLatency, maxOutLatency)
		maxOutErrors = maxErrors(f.outErrors, maxOutErrors)
	}

	return ApplicationFrame{
		inRequests:   avgTraffic(sumInRequests, cntInRequests),
		inThroughput: avgTraffic(sumInThroughput, cntInThroughput),
		inLatency:    maxInLatency,
		inErrors:     maxInErrors,

		appCPU:  maxAppCPU,
		appMem:  maxAppMem,
		hostCPU: maxHostCPU,
		hostMem: maxHostMem,

		outRequests:   avgTraffic(sumOutRequests, cntOutRequests),
		outThroughput: avgTraffic(sumOutThroughput, cntOutThroughput),
		outLatency:    maxOutLatency,
		outErrors:     maxOutErrors,
	}
}

func (ae *ApplicationEngine) Ready() bool {

	if !ae.inForest.Trained || !ae.inForest.Tested ||
		!ae.satForest.Trained || !ae.satForest.Tested ||
		!ae.outForest.Trained || !ae.outForest.Tested {
		return false
	}
	return true
}

func (ae *ApplicationEngine) Exists(stamp common.Stamp) bool {

	if len(ae.minutes) == 0 {
		return false
	}

	t := common.StampToTime(stamp)
	key := ae.weeklyMinuteKey(t)
	index := slices.Index(ae.minutes, key)

	return index != -1
}

func (ae *ApplicationEngine) Consolidate(frames ApplicationFrames) *ApplicationFrame {

	if len(frames) == 0 {
		return nil
	}

	// sort frames by time stamp
	slices.SortFunc(frames, func(a *ApplicationFrame, b *ApplicationFrame) int {
		return cmp.Compare(a.Stamp(), b.Stamp())
	})

	t := time.Now().UTC()
	stamp := common.TimeToStamp(t)
	frame := ae.aggregateFrames(frames)

	frame.application = frames[0].Application()
	frame.host = frames[0].Host()

	frame.stamp = stamp
	frame.begin = frames[0].Stamp()
	frame.end = frames[len(frames)-1].Stamp()

	return &frame
}

func (ae *ApplicationEngine) Diagnose(frame *ApplicationFrame, cases *ApplicationCases) (*ApplicationFramePoints, *ApplicationCaseVerdict) {

	if frame == nil {
		return nil, nil
	}

	points, pattern := ae.predictPattern(frame)
	cs, score := cases.Match(pattern)
	if cs == nil || score <= ae.options.MinScore {
		return points, nil
	}

	if !(utils.Contains(ae.categories, cs.category) &&
		utils.Contains(ae.impacts, cs.impact)) {
		return points, nil
	}

	verdict := &ApplicationCaseVerdict{
		acase: cs,
		score: score,
	}
	return points, verdict
}

func NewApplicationEngine(id common.Hash, options *ApplicationEngineOptions) *ApplicationEngine {

	categories := common.StringToCaseCategories(options.Categories)
	if len(categories) == 0 {
		categories = common.AllCaseCategories()
	}

	impacts := common.StringToCaseImpacts(options.Impacts)
	if len(impacts) == 0 {
		impacts = common.AllCaseImpacts()
	}

	return &ApplicationEngine{

		id:      id,
		options: options,

		inReqSlots:  NewApplicationTrafficSlots(options.InRequestsMaxSlots),
		inThruSlots: NewApplicationTrafficSlots(options.InThroughputMaxSlots),

		outReqSlots:  NewApplicationTrafficSlots(options.OutRequestsMaxSlots),
		outThruSlots: NewApplicationTrafficSlots(options.OutThroughputMaxSlots),

		inForest:  iforest.NewForest(options.TreesNumber, options.SubsampleSize, options.OutlierRatio),
		satForest: iforest.NewForest(options.TreesNumber, options.SubsampleSize, options.OutlierRatio),
		outForest: iforest.NewForest(options.TreesNumber, options.SubsampleSize, options.OutlierRatio),

		inProfiler:  NewApplicationProfiler(options.RangeMultiplier, options.ProfileBucketsKind),
		satProfiler: NewApplicationProfiler(options.RangeMultiplier, options.ProfileBucketsKind),
		outProfiler: NewApplicationProfiler(options.RangeMultiplier, options.ProfileBucketsKind),

		categories: categories,
		impacts:    impacts,
	}
}
