package common

import "github.com/devopsext/utils"

type CaseLevel = int

const (
	CaseLevelUnknown = iota
	CaseLevelNormal
	CaseLevelHigh
	CaseLevelLow
)

type CaseImpact = int

const (
	CaseImpactUnknown = iota
	CaseImpactZero
	CaseImpactAverage
	CaseImpactMaximum
)

type CaseCategory = int

const (
	CaseCategoryUnknown = iota
	CaseCategoryHealthy
	CaseCategoryOperational
	CaseCategoryFailure
)

type CaseLevels struct {
	appInRequests   CaseLevel
	appInThroughput CaseLevel
	appInLatency    CaseLevel
	appInErrors     CaseLevel

	appOutRequests   CaseLevel
	appOutThroughput CaseLevel
	appOutLatency    CaseLevel
	appOutErrors     CaseLevel

	appCPU  CaseLevel
	appMem  CaseLevel
	hostCPU CaseLevel
	hostMem CaseLevel
}

type Case struct {
	name        string
	description string
	rootCause   string
	impact      CaseImpact
	category    CaseCategory
	levels      CaseLevels
}

type Cases struct {
	list []*Case
}

func (c *Case) SameLevels(levels CaseLevels) bool {

	return c.levels.appInRequests == levels.appInRequests &&
		c.levels.appInThroughput == levels.appInThroughput &&
		c.levels.appInLatency == levels.appInLatency &&
		c.levels.appInErrors == levels.appInErrors &&
		c.levels.appOutRequests == levels.appOutRequests &&
		c.levels.appOutThroughput == levels.appOutThroughput &&
		c.levels.appOutLatency == levels.appOutLatency &&
		c.levels.appOutErrors == levels.appOutErrors &&
		c.levels.appCPU == levels.appCPU &&
		c.levels.appMem == levels.appMem &&
		c.levels.hostCPU == levels.hostCPU &&
		c.levels.hostMem == levels.hostMem
}

func NewCase(impact CaseImpact, category CaseCategory, name, description, rootCause string, levels CaseLevels) *Case {

	return &Case{
		name:        name,
		description: description,
		rootCause:   rootCause,
		impact:      impact,
		category:    category,
		levels:      levels,
	}
}

// Cases

func (cs *Cases) Items() []*Case {
	return cs.list
}

func (cs *Cases) FindByLevels(levels CaseLevels) *Case {

	var r *Case
	for _, c := range cs.list {

		if c.SameLevels(levels) {
			return c
		}
	}
	return r
}

func (cs *Cases) Add(impact CaseImpact, category CaseCategory, name, description, rootCause string, levels CaseLevels) *Case {

	c := cs.FindByLevels(levels)
	if !utils.IsEmpty(c) {
		return c
	}
	c = NewCase(impact, category, name, description, rootCause, levels)
	cs.list = append(cs.list, c)
	return c
}

func NewCases() *Cases {

	cases := &Cases{}

	cases.Add(CaseImpactZero, CaseCategoryHealthy,
		"Healthy Steady State",
		"All metrics inside baseline quantiles",
		"Nominal operating conditions",
		CaseLevels{
			appInRequests:    CaseLevelNormal,
			appInThroughput:  CaseLevelNormal,
			appInLatency:     CaseLevelNormal,
			appInErrors:      CaseLevelNormal,
			appCPU:           CaseLevelNormal,
			appMem:           CaseLevelNormal,
			hostCPU:          CaseLevelNormal,
			hostMem:          CaseLevelNormal,
			appOutRequests:   CaseLevelNormal,
			appOutThroughput: CaseLevelNormal,
			appOutLatency:    CaseLevelNormal,
			appOutErrors:     CaseLevelNormal,
		})

	cases.Add(CaseImpactZero, CaseCategoryHealthy,
		"Healthy Load Scaling",
		"Proportional out/in scaling without latency or error growth",
		"Organic client traffic surge absorbed cleanly",
		CaseLevels{
			appInRequests:    CaseLevelHigh,
			appInThroughput:  CaseLevelHigh,
			appInLatency:     CaseLevelNormal,
			appInErrors:      CaseLevelNormal,
			appCPU:           CaseLevelNormal,
			appMem:           CaseLevelNormal,
			hostCPU:          CaseLevelNormal,
			hostMem:          CaseLevelNormal,
			appOutRequests:   CaseLevelHigh,
			appOutThroughput: CaseLevelHigh,
			appOutLatency:    CaseLevelNormal,
			appOutErrors:     CaseLevelNormal,
		})

	cases.Add(CaseImpactZero, CaseCategoryHealthy,
		"Healthy Off-Peak Drop",
		"Proportional traffic decline with stable latencies",
		"Diurnal or scheduled off-peak traffic drop",
		CaseLevels{
			appInRequests:    CaseLevelLow,
			appInThroughput:  CaseLevelLow,
			appInLatency:     CaseLevelNormal,
			appInErrors:      CaseLevelNormal,
			appCPU:           CaseLevelNormal,
			appMem:           CaseLevelNormal,
			hostCPU:          CaseLevelNormal,
			hostMem:          CaseLevelNormal,
			appOutRequests:   CaseLevelLow,
			appOutThroughput: CaseLevelLow,
			appOutLatency:    CaseLevelNormal,
			appOutErrors:     CaseLevelNormal,
		})

	cases.Add(CaseImpactZero, CaseCategoryHealthy,
		"Cache Hit Absorption",
		"High ingress with low egress and nominal latencies/errors",
		"In-memory cache absorbing reads cleanly (e.g. Redis / CDN)",
		CaseLevels{
			appInRequests:    CaseLevelHigh,
			appInThroughput:  CaseLevelHigh,
			appInLatency:     CaseLevelLow,
			appInErrors:      CaseLevelNormal,
			appCPU:           CaseLevelNormal,
			appMem:           CaseLevelNormal,
			hostCPU:          CaseLevelNormal,
			hostMem:          CaseLevelNormal,
			appOutRequests:   CaseLevelLow,
			appOutThroughput: CaseLevelLow,
			appOutLatency:    CaseLevelNormal,
			appOutErrors:     CaseLevelNormal,
		})

	cases.Add(CaseImpactZero, CaseCategoryHealthy,
		"Ultra-Low Footprint Zero Idle",
		"Metrics fully functional with saturation floor near 0% under lightweight async load",
		"Efficient non-blocking event loop or runtime idling at true zero consumption",
		CaseLevels{
			appInRequests:    CaseLevelNormal,
			appInThroughput:  CaseLevelNormal,
			appInLatency:     CaseLevelLow,
			appInErrors:      CaseLevelLow,
			appCPU:           CaseLevelLow,
			appMem:           CaseLevelLow,
			hostCPU:          CaseLevelLow,
			hostMem:          CaseLevelLow,
			appOutRequests:   CaseLevelNormal,
			appOutThroughput: CaseLevelNormal,
			appOutLatency:    CaseLevelLow,
			appOutErrors:     CaseLevelLow,
		})

	cases.Add(CaseImpactZero, CaseCategoryOperational,
		"Client Inactivity (Full Idle)",
		"All metrics idle down to baseline floor without errors",
		"Clean cessation of incoming requests",
		CaseLevels{
			appInRequests:    CaseLevelLow,
			appInThroughput:  CaseLevelLow,
			appInLatency:     CaseLevelLow,
			appInErrors:      CaseLevelLow,
			appCPU:           CaseLevelLow,
			appMem:           CaseLevelLow,
			hostCPU:          CaseLevelLow,
			hostMem:          CaseLevelLow,
			appOutRequests:   CaseLevelLow,
			appOutThroughput: CaseLevelLow,
			appOutLatency:    CaseLevelLow,
			appOutErrors:     CaseLevelLow,
		})

	cases.Add(CaseImpactZero, CaseCategoryOperational,
		"Async Queue / Worker Drain",
		"Zero ingress traffic with high CPU and high outbound traffic",
		"Background job processing Kafka consumer drain batch sync",
		CaseLevels{
			appInRequests:    CaseLevelLow,
			appInThroughput:  CaseLevelLow,
			appInLatency:     CaseLevelNormal,
			appInErrors:      CaseLevelNormal,
			appCPU:           CaseLevelHigh,
			appMem:           CaseLevelNormal,
			hostCPU:          CaseLevelHigh,
			hostMem:          CaseLevelNormal,
			appOutRequests:   CaseLevelHigh,
			appOutThroughput: CaseLevelHigh,
			appOutLatency:    CaseLevelNormal,
			appOutErrors:     CaseLevelNormal,
		})

	cases.Add(CaseImpactZero, CaseCategoryOperational,
		"Target Drainage / Rolling Deploy",
		"Clean linear decline of traffic without error spike during deployment cycle",
		"Target group registration draining or pod termination grace period",
		CaseLevels{
			appInRequests:    CaseLevelLow,
			appInThroughput:  CaseLevelLow,
			appInLatency:     CaseLevelNormal,
			appInErrors:      CaseLevelLow,
			appCPU:           CaseLevelLow,
			appMem:           CaseLevelLow,
			hostCPU:          CaseLevelLow,
			hostMem:          CaseLevelLow,
			appOutRequests:   CaseLevelLow,
			appOutThroughput: CaseLevelLow,
			appOutLatency:    CaseLevelNormal,
			appOutErrors:     CaseLevelLow,
		})

	cases.Add(CaseImpactAverage, CaseCategoryFailure,
		"App CPU Limit / Cgroup Throttle",
		"App CPU saturated while host CPU and memory remain normal",
		"Container cgroup CPU quota reached or thread-pool exhaustion",
		CaseLevels{
			appInRequests:    CaseLevelHigh,
			appInThroughput:  CaseLevelHigh,
			appInLatency:     CaseLevelHigh,
			appInErrors:      CaseLevelHigh,
			appCPU:           CaseLevelHigh,
			appMem:           CaseLevelNormal,
			hostCPU:          CaseLevelNormal,
			hostMem:          CaseLevelNormal,
			appOutRequests:   CaseLevelLow,
			appOutThroughput: CaseLevelLow,
			appOutLatency:    CaseLevelLow,
			appOutErrors:     CaseLevelLow,
		})

	cases.Add(CaseImpactAverage, CaseCategoryFailure,
		"Noisy Neighbor CPU Steal",
		"Host CPU high but App CPU normal/low; outbound drops",
		"External rogue process or container on host stealing CPU cores",
		CaseLevels{
			appInRequests:    CaseLevelNormal,
			appInThroughput:  CaseLevelNormal,
			appInLatency:     CaseLevelHigh,
			appInErrors:      CaseLevelHigh,
			appCPU:           CaseLevelNormal,
			appMem:           CaseLevelNormal,
			hostCPU:          CaseLevelHigh,
			hostMem:          CaseLevelNormal,
			appOutRequests:   CaseLevelLow,
			appOutThroughput: CaseLevelLow,
			appOutLatency:    CaseLevelLow,
			appOutErrors:     CaseLevelLow,
		})

	return cases
}
