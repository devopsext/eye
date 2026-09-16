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
	frame *ApplicationFrame
	acase *ApplicationCase
	score int
	begin common.Stamp
	end   common.Stamp
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

type ApplicationFrame struct {
	stamp common.Stamp
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
type ApplicationProfiler struct {
	Buckets  [168][]ApplicationFeatureStats
	K        float64
	isFitted bool
}

type ApplicationEngineOptions struct {
	Path            string
	TreesNumber     int
	SubsampleSize   int
	OutlierRatio    float64
	TrafficMaxSlots int
	RangeMultiplier float64
	MinScore        int
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
	inData  [][]float64
	satData [][]float64
	outData [][]float64

	inForest  *iforest.Forest
	satForest *iforest.Forest
	outForest *iforest.Forest

	inProfiler  *ApplicationProfiler
	satProfiler *ApplicationProfiler
	outProfiler *ApplicationProfiler
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

func (acv *ApplicationCaseVerdict) Begin() common.Stamp {
	return acv.begin
}

func (acv *ApplicationCaseVerdict) End() common.Stamp {
	return acv.end
}

func (acv *ApplicationCaseVerdict) Name() string {
	return acv.acase.name
}

func (acv *ApplicationCaseVerdict) Description() string {
	return acv.acase.description
}

func (acv *ApplicationCaseVerdict) RootCause() string {
	return acv.acase.rootCause
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

// ApplicationFrame

func (af *ApplicationFrame) Valid() bool {

	if af.stamp == 0 {
		return false
	}

	if len(af.inRequests) == 0 && len(af.outRequests) == 0 {
		return false
	}

	if len(af.inThroughput) == 0 && len(af.outThroughput) == 0 {
		return false
	}
	return true
}

func (af *ApplicationFrame) Stamp() common.Stamp {
	return af.stamp
}

func NewApplicationFrame(stamp common.Stamp, appSignal *common.ApplicationSignal, hostSignal *common.HostSignal) *ApplicationFrame {

	r := &ApplicationFrame{
		stamp: stamp,
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

func TimeWindowKey(t time.Time) int {
	return int(t.Weekday())*24 + t.Hour()
}

func Percentile(sorted []float64, p float64) float64 {
	idx := p * float64(len(sorted)-1)
	l := int(math.Floor(idx))
	u := int(math.Ceil(idx))
	w := idx - float64(l)
	return sorted[l]*(1.0-w) + sorted[u]*w
}

func (ap *ApplicationProfiler) Fit(timestamps []time.Time, matrix [][]float64) {

	if len(matrix) == 0 || len(matrix) != len(timestamps) {
		return
	}
	numCols := len(matrix[0])

	bucketData := make([][][]float64, 168)
	for b := range bucketData {
		bucketData[b] = make([][]float64, numCols)
	}

	for row := 0; row < len(matrix); row++ {
		b := TimeWindowKey(timestamps[row])
		for col := 0; col < numCols; col++ {
			bucketData[b][col] = append(bucketData[b][col], matrix[row][col])
		}
	}

	for b := 0; b < 168; b++ {
		ap.Buckets[b] = make([]ApplicationFeatureStats, numCols)
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
		}
	}
	ap.isFitted = true
}

func (ap *ApplicationProfiler) Classify(t time.Time, col int, val float64, isAnomaly, isValid bool) common.CaseLevel {

	if !isValid {
		return common.CaseLevelUnknown
	}

	b := TimeWindowKey(t)
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

func NewApplicationProfiler(k float64) *ApplicationProfiler {
	return &ApplicationProfiler{K: k}
}

// ApplicationEngine

func (ae *ApplicationEngine) Name() string {
	return "ForestApplicationEngine"
}

func (ae *ApplicationEngine) getMedians(profiler *ApplicationProfiler, t time.Time, startIdx, count int) []float64 {

	meds := make([]float64, count)
	if !profiler.isFitted {
		return meds
	}
	b := TimeWindowKey(t)
	if len(profiler.Buckets[b]) < startIdx+count {
		return meds
	}
	for i := 0; i < count; i++ {
		meds[i] = profiler.Buckets[b][startIdx+i].Median
	}
	return meds
}

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
func (ae *ApplicationEngine) extractVectors(f *ApplicationFrame) (inVec, satVec, outVec []float64, inValid, satValid, outValid []bool) {

	t := common.StampToTime(f.stamp)
	timeVec := ae.extractTimeVectors(t)

	inSlotWidth := ae.inReqSlots.MaxSlots + 2

	inReqMeds := ae.getMedians(ae.inProfiler, t, 0, inSlotWidth)
	inThruMeds := ae.getMedians(ae.inProfiler, t, inSlotWidth, inSlotWidth)

	inReqSlots, inReqVal := ae.inReqSlots.VectorizeTraffic(f.inRequests, false, inReqMeds)
	inThruSlots, inThruVal := ae.inThruSlots.VectorizeTraffic(f.inThroughput, false, inThruMeds)

	inLatIdx := len(inReqSlots) + len(inThruSlots)
	inErrIdx := inLatIdx + 1

	b := TimeWindowKey(t)

	inLatValid := f.inLatency != nil
	inLat := 0.0
	if inLatValid {
		inLat = *f.inLatency
	}
	if !inLatValid && ae.inProfiler.isFitted && len(ae.inProfiler.Buckets[b]) > inLatIdx {
		inLat = ae.inProfiler.Buckets[b][inLatIdx].Median
	}

	inErrValid := f.inErrors != nil
	inErr := 0.0
	if inErrValid {
		inErr = *f.inErrors
	}
	if !inErrValid && ae.inProfiler.isFitted && len(ae.inProfiler.Buckets[b]) > inErrIdx {
		inErr = ae.inProfiler.Buckets[b][inErrIdx].Median
	}

	inVec = append(inVec, inReqSlots...)
	inVec = append(inVec, inThruSlots...)
	inVec = append(inVec, inLat, inErr)
	inVec = append(inVec, timeVec...)

	inValid = append(inValid, inReqVal...)
	inValid = append(inValid, inThruVal...)
	inValid = append(inValid, inLatValid, inErrValid)
	for i := 0; i < len(timeVec); i++ {
		inValid = append(inValid, true) // Time is always valid
	}

	appCPUValid := f.appCPU != nil
	appCPU := 0.0
	if appCPUValid {
		appCPU = *f.appCPU
	}
	if !appCPUValid && ae.satProfiler.isFitted && len(ae.satProfiler.Buckets[b]) > 0 {
		appCPU = ae.satProfiler.Buckets[b][0].Median
	}

	appMemValid := f.appMem != nil
	appMem := 0.0
	if appMemValid {
		appMem = *f.appMem
	}
	if !appMemValid && ae.satProfiler.isFitted && len(ae.satProfiler.Buckets[b]) > 1 {
		appMem = ae.satProfiler.Buckets[b][1].Median
	}

	hostCPUValid := f.hostCPU != nil
	hostCPU := 0.0
	if hostCPUValid {
		hostCPU = *f.hostCPU
	}
	if !hostCPUValid && ae.satProfiler.isFitted && len(ae.satProfiler.Buckets[b]) > 2 {
		hostCPU = ae.satProfiler.Buckets[b][2].Median
	}

	hostMemValid := f.hostMem != nil
	hostMem := 0.0
	if hostMemValid {
		hostMem = *f.hostMem
	}
	if !hostMemValid && ae.satProfiler.isFitted && len(ae.satProfiler.Buckets[b]) > 3 {
		hostMem = ae.satProfiler.Buckets[b][3].Median
	}

	cpuDiffVal := false
	cpuDiff := 0.0
	if hostCPUValid && appCPUValid {
		cpuDiffVal = true
		cpuDiff = hostCPU - appCPU
	} else if ae.satProfiler.isFitted && len(ae.satProfiler.Buckets[b]) > 4 {
		cpuDiff = ae.satProfiler.Buckets[b][4].Median
	}

	memDiffVal := false
	memDiff := 0.0
	if hostMemValid && appMemValid {
		memDiffVal = true
		memDiff = hostMem - appMem
	} else if ae.satProfiler.isFitted && len(ae.satProfiler.Buckets[b]) > 5 {
		memDiff = ae.satProfiler.Buckets[b][5].Median
	}

	satVec = []float64{appCPU, appMem, hostCPU, hostMem, cpuDiff, memDiff}
	satVec = append(satVec, timeVec...)

	satValid = []bool{appCPUValid, appMemValid, hostCPUValid, hostMemValid, cpuDiffVal, memDiffVal}
	for i := 0; i < len(timeVec); i++ {
		satValid = append(satValid, true) // Time is always valid
	}

	outSlotWidth := ae.outReqSlots.MaxSlots + 2

	outReqMeds := ae.getMedians(ae.outProfiler, t, 0, outSlotWidth)
	outThruMeds := ae.getMedians(ae.outProfiler, t, outSlotWidth, outSlotWidth)

	outReqSlots, outReqVal := ae.outReqSlots.VectorizeTraffic(f.outRequests, false, outReqMeds)
	outThruSlots, outThruVal := ae.outThruSlots.VectorizeTraffic(f.outThroughput, false, outThruMeds)

	/*outLatIdx := len(outReqSlots) + len(outThruSlots)
	outErrIdx := inLatIdx + 1*/

	outLatIdx := ae.outReqSlots.MaxSlots + 2 + ae.outReqSlots.MaxSlots + 2
	outErrIdx := outLatIdx + 1

	outLatValid := f.outLatency != nil
	outLat := 0.0
	if outLatValid {
		outLat = *f.outLatency
	}
	if !outLatValid && ae.outProfiler.isFitted && len(ae.outProfiler.Buckets[b]) > outLatIdx {
		outLat = ae.outProfiler.Buckets[b][outLatIdx].Median
	}

	outErrValid := f.outErrors != nil
	outErr := 0.0
	if outErrValid {
		outErr = *f.outErrors
	}
	if !outErrValid && ae.outProfiler.isFitted && len(ae.outProfiler.Buckets[b]) > outErrIdx {
		outErr = ae.outProfiler.Buckets[b][outErrIdx].Median
	}

	inReqSum, inReqKnown := f.inRequests.Sum()
	outReqSum, outReqKnown := f.outRequests.Sum()
	flowReqValid := false
	flowReqRatio := 0.0
	if inReqKnown && outReqKnown && inReqSum > 0 {
		flowReqValid = true
		flowReqRatio = outReqSum / inReqSum
	} else if ae.outProfiler.isFitted && len(ae.outProfiler.Buckets[b]) > 4*outSlotWidth {
		flowReqRatio = ae.outProfiler.Buckets[b][4*outSlotWidth].Median
	}

	inThruSum, inThruKnown := f.inThroughput.Sum()
	outThruSum, outThruKnown := f.outThroughput.Sum()
	flowThruValid := false
	flowThruRatio := 0.0
	if inThruKnown && outThruKnown && inThruSum > 0 {
		flowThruValid = true
		flowThruRatio = outThruSum / inThruSum
	} else if ae.outProfiler.isFitted && len(ae.outProfiler.Buckets[b]) > 4*outSlotWidth+1 {
		flowThruRatio = ae.outProfiler.Buckets[b][4*outSlotWidth+1].Median
	}

	latDivValid := false
	latDiv := 0.0
	if inLatValid && outLatValid {
		latDivValid = true
		latDiv = inLat - outLat
	} else if ae.outProfiler.isFitted && len(ae.outProfiler.Buckets[b]) > 4*outSlotWidth+2 {
		latDiv = ae.outProfiler.Buckets[b][4*outSlotWidth+2].Median
	}

	outVec = append(outVec, outReqSlots...)
	outVec = append(outVec, outThruSlots...)
	outVec = append(outVec, outLat, outErr)
	outVec = append(outVec, flowReqRatio, flowThruRatio, latDiv)
	outVec = append(outVec, timeVec...)

	outValid = append(outValid, outReqVal...)
	outValid = append(outValid, outThruVal...)
	outValid = append(outValid, outLatValid, outErrValid)
	outValid = append(outValid, flowReqValid, flowThruValid, latDivValid)
	for i := 0; i < len(timeVec); i++ {
		outValid = append(outValid, true) // Time is always valid
	}

	return inVec, satVec, outVec, inValid, satValid, outValid
}

func (ae *ApplicationEngine) splitTimesData(from, first, last common.Stamp) (tL, tR []common.Stamp, inL, satL, outL, inR, satR, outR [][]float64) {

	ltimes := len(ae.stamps)
	if ltimes != len(ae.inData) ||
		ltimes != len(ae.satData) ||
		ltimes != len(ae.outData) {
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
	var times []time.Time

	// sort frames by time stamp
	slices.SortFunc(frames, func(a *ApplicationFrame, b *ApplicationFrame) int {
		return cmp.Compare(a.Stamp(), b.Stamp())
	})

	temp := []*ApplicationFrame{}

	for _, f := range frames {

		index := slices.Index(ae.stamps, f.stamp)
		if index >= 0 {
			continue
		}
		temp = append(temp, f)
		inReq = append(inReq, f.inRequests)
		inThru = append(inThru, f.inThroughput)
		outReq = append(outReq, f.outRequests)
		outThru = append(outThru, f.outThroughput)
	}

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

		iv, sv, ov, _, _, _ := ae.extractVectors(f)
		stamps = append(stamps, f.stamp)
		inData = append(inData, iv)
		satData = append(satData, sv)
		outData = append(outData, ov)
		times = append(times, common.StampToTime(f.stamp))
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
	ae.inProfiler.Fit(times, ae.inData)

	ae.satForest.Train(ae.satData)
	err = ae.satForest.Test(ae.satData)
	if err != nil {
		return err
	}
	ae.satProfiler.Fit(times, ae.satData)

	ae.outForest.Train(ae.outData)
	err = ae.outForest.Test(ae.outData)
	if err != nil {
		return err
	}
	ae.outProfiler.Fit(times, ae.outData)

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

	if utils.Contains(kinds, ApplicationEngineFileLoadKindData) {

		ltimes := len(engineFile.Stamps)
		if ltimes != len(engineFile.Incoming.Data) ||
			ltimes != len(engineFile.Saturation.Data) ||
			ltimes != len(engineFile.Outgoing.Data) {
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
		Stamps: ae.stamps,
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

func (ae *ApplicationEngine) predictPattern(frame *ApplicationFrame) *ApplicationCasePattern {

	t := common.StampToTime(frame.stamp)

	inVec, satVec, outVec, inVal, satVal, outVal := ae.extractVectors(frame)

	inLabels, _, _ := ae.inForest.Predict([][]float64{inVec})
	satLabels, _, _ := ae.satForest.Predict([][]float64{satVec})
	outLabels, _, _ := ae.outForest.Predict([][]float64{outVec})

	// no labels
	if len(inLabels) == 0 || len(satLabels) == 0 || len(outLabels) == 0 {
		return nil
	}

	inAnom := inLabels[0] == 1
	satAnom := satLabels[0] == 1
	outAnom := outLabels[0] == 1

	slotWidth := ae.inReqSlots.MaxSlots + 2
	inReqTotalIdx := slotWidth - 1
	inThruTotalIdx := inReqTotalIdx + slotWidth

	inReqLevel := ae.inProfiler.Classify(t, inReqTotalIdx, inVec[inReqTotalIdx], inAnom, inVal[inReqTotalIdx])
	inThruLevel := ae.inProfiler.Classify(t, inThruTotalIdx, inVec[inThruTotalIdx], inAnom, inVal[inThruTotalIdx])
	inLatLevel := ae.inProfiler.Classify(t, inThruTotalIdx+1, inVec[inThruTotalIdx+1], inAnom, inVal[inThruTotalIdx+1])
	inErrLevel := ae.inProfiler.Classify(t, inThruTotalIdx+2, inVec[inThruTotalIdx+2], inAnom, inVal[inThruTotalIdx+2])

	appCPULevel := ae.satProfiler.Classify(t, 0, satVec[0], satAnom, satVal[0])
	appMemLevel := ae.satProfiler.Classify(t, 1, satVec[1], satAnom, satVal[1])
	hostCPULevel := ae.satProfiler.Classify(t, 2, satVec[2], satAnom, satVal[2])
	hostMemLevel := ae.satProfiler.Classify(t, 3, satVec[3], satAnom, satVal[3])

	outSlotWidth := ae.outReqSlots.MaxSlots + 2
	outReqTotalIdx := outSlotWidth - 1
	outThruTotalIdx := outReqTotalIdx + outSlotWidth

	outReqLevel := ae.outProfiler.Classify(t, outReqTotalIdx, outVec[outReqTotalIdx], outAnom, outVal[outReqTotalIdx])
	outThruLevel := ae.outProfiler.Classify(t, outThruTotalIdx, outVec[outThruTotalIdx], outAnom, outVal[outThruTotalIdx])
	outLatLevel := ae.inProfiler.Classify(t, outThruTotalIdx+1, outVec[inThruTotalIdx+1], outAnom, outVal[inThruTotalIdx+1])
	outErrLevel := ae.inProfiler.Classify(t, outThruTotalIdx+2, outVec[inThruTotalIdx+2], outAnom, outVal[inThruTotalIdx+2])

	return &ApplicationCasePattern{
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
}

func (ae *ApplicationEngine) aggregateFrames(frames ApplicationFrames) *ApplicationFrame {

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

	return &ApplicationFrame{
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

func (ae *ApplicationEngine) Diagnose(frames ApplicationFrames, cases *ApplicationCases) *ApplicationCaseVerdict {

	if !ae.Ready() {
		return nil
	}

	// sort frames by time stamp
	slices.SortFunc(frames, func(a *ApplicationFrame, b *ApplicationFrame) int {
		return cmp.Compare(a.Stamp(), b.Stamp())
	})

	frame := ae.aggregateFrames(frames)
	if frame == nil {
		return nil
	}
	frame.stamp = common.TimeToStamp(time.Now())

	pattern := ae.predictPattern(frame)
	cs, score := cases.Match(pattern)
	if cs == nil || score < ae.options.MinScore {
		return nil
	}

	return &ApplicationCaseVerdict{
		frame: frame,
		acase: cs,
		score: score,
		begin: frames[0].Stamp(),
		end:   frames[len(frames)-1].Stamp(),
	}
}

func NewApplicationEngine(id common.Hash, options *ApplicationEngineOptions) *ApplicationEngine {

	return &ApplicationEngine{

		id:      id,
		options: options,

		inReqSlots:  NewApplicationTrafficSlots(options.TrafficMaxSlots),
		inThruSlots: NewApplicationTrafficSlots(options.TrafficMaxSlots),

		outReqSlots:  NewApplicationTrafficSlots(options.TrafficMaxSlots),
		outThruSlots: NewApplicationTrafficSlots(options.TrafficMaxSlots),

		inForest:  iforest.NewForest(options.TreesNumber, options.SubsampleSize, options.OutlierRatio),
		satForest: iforest.NewForest(options.TreesNumber, options.SubsampleSize, options.OutlierRatio),
		outForest: iforest.NewForest(options.TreesNumber, options.SubsampleSize, options.OutlierRatio),

		inProfiler:  NewApplicationProfiler(options.RangeMultiplier),
		satProfiler: NewApplicationProfiler(options.RangeMultiplier),
		outProfiler: NewApplicationProfiler(options.RangeMultiplier),
	}
}
