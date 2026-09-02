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
	CaseImpactSevere
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

	cases.Add(CaseImpactAverage, CaseCategoryFailure,
		"Edge Rejection / Fast Fail",
		"Inbound errors high while inbound latency drops to minimum",
		"Credential stuffing WAF blocking scraper flood 401/403/429",
		CaseLevels{
			appInRequests:    CaseLevelHigh,
			appInThroughput:  CaseLevelNormal,
			appInLatency:     CaseLevelLow,
			appInErrors:      CaseLevelHigh,
			appCPU:           CaseLevelNormal,
			appMem:           CaseLevelNormal,
			hostCPU:          CaseLevelNormal,
			hostMem:          CaseLevelNormal,
			appOutRequests:   CaseLevelLow,
			appOutThroughput: CaseLevelLow,
			appOutLatency:    CaseLevelLow,
			appOutErrors:     CaseLevelLow,
		})

	cases.Add(CaseImpactAverage, CaseCategoryFailure,
		"Heavy Payload / Slowloris",
		"Inbound througput high with low requests and high memory saturation",
		"Multipart file upload storm payload uncompressed JSON Slowloris attack",
		CaseLevels{
			appInRequests:    CaseLevelLow,
			appInThroughput:  CaseLevelHigh,
			appInLatency:     CaseLevelHigh,
			appInErrors:      CaseLevelHigh,
			appCPU:           CaseLevelNormal,
			appMem:           CaseLevelHigh,
			hostCPU:          CaseLevelNormal,
			hostMem:          CaseLevelHigh,
			appOutRequests:   CaseLevelLow,
			appOutThroughput: CaseLevelLow,
			appOutLatency:    CaseLevelLow,
			appOutErrors:     CaseLevelLow,
		})

	cases.Add(CaseImpactAverage, CaseCategoryFailure,
		"Fast Bypass / Regression Bug",
		"Inbound latency drops to zero with 200 OK and zero outbound calls",
		"Auth middleware bypass regression empty body return cached mock leak",
		CaseLevels{
			appInRequests:    CaseLevelNormal,
			appInThroughput:  CaseLevelNormal,
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

	cases.Add(CaseImpactAverage, CaseCategoryFailure,
		"Egress Bandwidth Flooding",
		"Outbound throughput spikes while Outbound requests and saturation remain normal",
		"Unbounded query results large file downloads data exfiltration",
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
			appOutLatency:    CaseLevelHigh,
			appOutErrors:     CaseLevelNormal,
		})

	cases.Add(CaseImpactAverage, CaseCategoryFailure,
		"GC Stop-The-World Freeze",
		"App memory pinned at high watermark with CPU spikes from GC threads",
		"Full GC pause memory compaction freeze runtime heap lockup",
		CaseLevels{
			appInRequests:    CaseLevelNormal,
			appInThroughput:  CaseLevelNormal,
			appInLatency:     CaseLevelHigh,
			appInErrors:      CaseLevelNormal,
			appCPU:           CaseLevelHigh,
			appMem:           CaseLevelHigh,
			hostCPU:          CaseLevelNormal,
			hostMem:          CaseLevelNormal,
			appOutRequests:   CaseLevelLow,
			appOutThroughput: CaseLevelLow,
			appOutLatency:    CaseLevelLow,
			appOutErrors:     CaseLevelLow,
		})

	cases.Add(CaseImpactAverage, CaseCategoryFailure,
		"Client Disconnect / Timeout",
		"Inbound errors up (499/408) with near-zero downstream impact",
		"Client drops connection prematurely slow mobile networks",
		CaseLevels{
			appInRequests:    CaseLevelNormal,
			appInThroughput:  CaseLevelLow,
			appInLatency:     CaseLevelHigh,
			appInErrors:      CaseLevelHigh,
			appCPU:           CaseLevelLow,
			appMem:           CaseLevelLow,
			hostCPU:          CaseLevelLow,
			hostMem:          CaseLevelLow,
			appOutRequests:   CaseLevelLow,
			appOutThroughput: CaseLevelLow,
			appOutLatency:    CaseLevelLow,
			appOutErrors:     CaseLevelLow,
		})

	cases.Add(CaseImpactAverage, CaseCategoryFailure,
		"App Memory Leak / OOM Thrash",
		"App memory hits cgroup limit causing kernel page reclamation thrashing",
		"Application heap/buffer leak approaching container limit / OOM-kill",
		CaseLevels{
			appInRequests:    CaseLevelNormal,
			appInThroughput:  CaseLevelNormal,
			appInLatency:     CaseLevelHigh,
			appInErrors:      CaseLevelHigh,
			appCPU:           CaseLevelNormal,
			appMem:           CaseLevelHigh,
			hostCPU:          CaseLevelNormal,
			hostMem:          CaseLevelNormal,
			appOutRequests:   CaseLevelLow,
			appOutThroughput: CaseLevelLow,
			appOutLatency:    CaseLevelLow,
			appOutErrors:     CaseLevelLow,
		})

	cases.Add(CaseImpactAverage, CaseCategoryFailure,
		"Noisy Neighbor Memory Hog",
		"Host memory high while App memory is normal; app starved of cache/buffers",
		"Co-located container eating host RAM triggering kernel page scanning",
		CaseLevels{
			appInRequests:    CaseLevelNormal,
			appInThroughput:  CaseLevelNormal,
			appInLatency:     CaseLevelHigh,
			appInErrors:      CaseLevelHigh,
			appCPU:           CaseLevelNormal,
			appMem:           CaseLevelNormal,
			hostCPU:          CaseLevelNormal,
			hostMem:          CaseLevelHigh,
			appOutRequests:   CaseLevelLow,
			appOutThroughput: CaseLevelLow,
			appOutLatency:    CaseLevelLow,
			appOutErrors:     CaseLevelLow,
		})

	cases.Add(CaseImpactAverage, CaseCategoryFailure,
		"Hyper-Aggressive GC / Memory Starvation",
		"App memory pinned near 0% while App CPU is pinned at 100%",
		"GOMEMLIMIT/heap target set too low; runtime in continuous GC thrash",
		CaseLevels{
			appInRequests:    CaseLevelNormal,
			appInThroughput:  CaseLevelNormal,
			appInLatency:     CaseLevelHigh,
			appInErrors:      CaseLevelNormal,
			appCPU:           CaseLevelHigh,
			appMem:           CaseLevelLow,
			hostCPU:          CaseLevelNormal,
			hostMem:          CaseLevelNormal,
			appOutRequests:   CaseLevelLow,
			appOutThroughput: CaseLevelLow,
			appOutLatency:    CaseLevelNormal,
			appOutErrors:     CaseLevelNormal,
		})

	cases.Add(CaseImpactAverage, CaseCategoryFailure,
		"Client 499 Abort / Orphaned Compute",
		"High inbound 499s while outbound traffic continues running normally",
		"Missing context cancellation; backend work proceeds for cancelled client requests",
		CaseLevels{
			appInRequests:    CaseLevelNormal,
			appInThroughput:  CaseLevelNormal,
			appInLatency:     CaseLevelHigh,
			appInErrors:      CaseLevelHigh,
			appCPU:           CaseLevelHigh,
			appMem:           CaseLevelNormal,
			hostCPU:          CaseLevelNormal,
			hostMem:          CaseLevelNormal,
			appOutRequests:   CaseLevelHigh,
			appOutThroughput: CaseLevelHigh,
			appOutLatency:    CaseLevelNormal,
			appOutErrors:     CaseLevelNormal,
		})

	cases.Add(CaseImpactAverage, CaseCategoryFailure,
		"Poison Pill / DLQ Redrive Loop",
		"Zero inbound traffic with high CPU and surging outbound broker retry errors",
		"Unparseable queue message repeatedly nack-ed and redriven by workers",
		CaseLevels{
			appInRequests:    CaseLevelLow,
			appInThroughput:  CaseLevelLow,
			appInLatency:     CaseLevelNormal,
			appInErrors:      CaseLevelNormal,
			appCPU:           CaseLevelHigh,
			appMem:           CaseLevelHigh,
			hostCPU:          CaseLevelNormal,
			hostMem:          CaseLevelNormal,
			appOutRequests:   CaseLevelHigh,
			appOutThroughput: CaseLevelHigh,
			appOutLatency:    CaseLevelLow,
			appOutErrors:     CaseLevelHigh,
		})

	cases.Add(CaseImpactAverage, CaseCategoryFailure,
		"App Metric Collector Outage",
		"Traffic flows normally but container cgroup exporter drops out",
		"cgroup fs mount error kubelet stats provider stall or scraper timeout",
		CaseLevels{
			appInRequests:    CaseLevelNormal,
			appInThroughput:  CaseLevelNormal,
			appInLatency:     CaseLevelNormal,
			appInErrors:      CaseLevelNormal,
			appCPU:           CaseLevelUnknown,
			appMem:           CaseLevelUnknown,
			hostCPU:          CaseLevelNormal,
			hostMem:          CaseLevelNormal,
			appOutRequests:   CaseLevelNormal,
			appOutThroughput: CaseLevelNormal,
			appOutLatency:    CaseLevelNormal,
			appOutErrors:     CaseLevelNormal,
		})

	cases.Add(CaseImpactAverage, CaseCategoryFailure,
		"Host Agent Transport Failure",
		"Traffic and App metrics healthy but node-level hardware metrics missing",
		"host exporter daemon down crash or host firewall drop",
		CaseLevels{
			appInRequests:    CaseLevelNormal,
			appInThroughput:  CaseLevelNormal,
			appInLatency:     CaseLevelNormal,
			appInErrors:      CaseLevelNormal,
			appCPU:           CaseLevelNormal,
			appMem:           CaseLevelNormal,
			hostCPU:          CaseLevelUnknown,
			hostMem:          CaseLevelUnknown,
			appOutRequests:   CaseLevelNormal,
			appOutThroughput: CaseLevelNormal,
			appOutLatency:    CaseLevelNormal,
			appOutErrors:     CaseLevelNormal,
		})

	cases.Add(CaseImpactAverage, CaseCategoryOperational,
		"Circuit Breaker Fallback",
		"Outbound completely stopped but inbound returns fast healthy 200s",
		"Circuit breaker tripped serving degraded local cache or empty response",
		CaseLevels{
			appInRequests:    CaseLevelHigh,
			appInThroughput:  CaseLevelNormal,
			appInLatency:     CaseLevelLow,
			appInErrors:      CaseLevelNormal,
			appCPU:           CaseLevelNormal,
			appMem:           CaseLevelNormal,
			hostCPU:          CaseLevelNormal,
			hostMem:          CaseLevelNormal,
			appOutRequests:   CaseLevelLow,
			appOutThroughput: CaseLevelLow,
			appOutLatency:    CaseLevelLow,
			appOutErrors:     CaseLevelLow,
		})

	cases.Add(CaseImpactSevere, CaseCategoryFailure,
		"Host CPU Exhaustion by App",
		"Both App and Host CPU pinned near 100% while memory is stable",
		"Inbound compute volume exceeded physical host CPU cores",
		CaseLevels{
			appInRequests:    CaseLevelHigh,
			appInThroughput:  CaseLevelHigh,
			appInLatency:     CaseLevelHigh,
			appInErrors:      CaseLevelHigh,
			appCPU:           CaseLevelHigh,
			appMem:           CaseLevelNormal,
			hostCPU:          CaseLevelHigh,
			hostMem:          CaseLevelNormal,
			appOutRequests:   CaseLevelLow,
			appOutThroughput: CaseLevelLow,
			appOutLatency:    CaseLevelLow,
			appOutErrors:     CaseLevelLow,
		})

	cases.Add(CaseImpactSevere, CaseCategoryFailure,
		"Downstream / Backend Stall",
		"Outbound latency/errors surge and backpressure into ingress",
		"Downstream API DB lock contention or remote timeout",
		CaseLevels{
			appInRequests:    CaseLevelHigh,
			appInThroughput:  CaseLevelHigh,
			appInLatency:     CaseLevelHigh,
			appInErrors:      CaseLevelHigh,
			appCPU:           CaseLevelNormal,
			appMem:           CaseLevelNormal,
			hostCPU:          CaseLevelNormal,
			hostMem:          CaseLevelNormal,
			appOutRequests:   CaseLevelLow,
			appOutThroughput: CaseLevelLow,
			appOutLatency:    CaseLevelHigh,
			appOutErrors:     CaseLevelHigh,
		})

	cases.Add(CaseImpactSevere, CaseCategoryFailure,
		"Thread Deadlock / Pool Hang",
		"Extreme inbound latency with zero CPU and no 5xx errors",
		"Deadlock mutex starvation connection pool leak uncompleted requests",
		CaseLevels{
			appInRequests:    CaseLevelHigh,
			appInThroughput:  CaseLevelNormal,
			appInLatency:     CaseLevelHigh,
			appInErrors:      CaseLevelNormal,
			appCPU:           CaseLevelLow,
			appMem:           CaseLevelNormal,
			hostCPU:          CaseLevelNormal,
			hostMem:          CaseLevelNormal,
			appOutRequests:   CaseLevelLow,
			appOutThroughput: CaseLevelLow,
			appOutLatency:    CaseLevelLow,
			appOutErrors:     CaseLevelLow,
		})

	cases.Add(CaseImpactSevere, CaseCategoryFailure,
		"Retry Amplification Storm",
		"Out requests to In requests ratio spikes far above historical norm",
		"Cascading client retries or unbounded fan-out loops",
		CaseLevels{
			appInRequests:    CaseLevelNormal,
			appInThroughput:  CaseLevelNormal,
			appInLatency:     CaseLevelHigh,
			appInErrors:      CaseLevelHigh,
			appCPU:           CaseLevelHigh,
			appMem:           CaseLevelNormal,
			hostCPU:          CaseLevelHigh,
			hostMem:          CaseLevelNormal,
			appOutRequests:   CaseLevelHigh,
			appOutThroughput: CaseLevelHigh,
			appOutLatency:    CaseLevelHigh,
			appOutErrors:     CaseLevelHigh,
		})

	cases.Add(CaseImpactSevere, CaseCategoryFailure,
		"Network Partition / TCP Blackhole",
		"Outbound attempts stay up while outbound timeouts explode",
		"Switch/router failure egress security group drop routing loop",
		CaseLevels{
			appInRequests:    CaseLevelNormal,
			appInThroughput:  CaseLevelNormal,
			appInLatency:     CaseLevelHigh,
			appInErrors:      CaseLevelHigh,
			appCPU:           CaseLevelNormal,
			appMem:           CaseLevelNormal,
			hostCPU:          CaseLevelNormal,
			hostMem:          CaseLevelNormal,
			appOutRequests:   CaseLevelNormal,
			appOutThroughput: CaseLevelNormal,
			appOutLatency:    CaseLevelHigh,
			appOutErrors:     CaseLevelHigh,
		})

	cases.Add(CaseImpactSevere, CaseCategoryFailure,
		"Host Memory Exhaustion / Swap Thrashing",
		"Host memory exhausted causing OS page swapping; CPU drops due to I/O wait",
		"Host swapping pages to disk; disk I/O blocks runloops across containers / processes",
		CaseLevels{
			appInRequests:    CaseLevelNormal,
			appInThroughput:  CaseLevelNormal,
			appInLatency:     CaseLevelHigh,
			appInErrors:      CaseLevelHigh,
			appCPU:           CaseLevelLow,
			appMem:           CaseLevelNormal,
			hostCPU:          CaseLevelLow,
			hostMem:          CaseLevelHigh,
			appOutRequests:   CaseLevelLow,
			appOutThroughput: CaseLevelLow,
			appOutLatency:    CaseLevelLow,
			appOutErrors:     CaseLevelLow,
		})

	cases.Add(CaseImpactSevere, CaseCategoryFailure,
		"App OOM-Kill CrashLoop",
		"App memory drops to near 0% after 100% breach; crash loop restarts",
		"Kernel OOM-killer terminated container (SIGKILL); pod restarting",
		CaseLevels{
			appInRequests:    CaseLevelNormal,
			appInThroughput:  CaseLevelNormal,
			appInLatency:     CaseLevelHigh,
			appInErrors:      CaseLevelHigh,
			appCPU:           CaseLevelLow,
			appMem:           CaseLevelLow,
			hostCPU:          CaseLevelNormal,
			hostMem:          CaseLevelNormal,
			appOutRequests:   CaseLevelLow,
			appOutThroughput: CaseLevelLow,
			appOutLatency:    CaseLevelLow,
			appOutErrors:     CaseLevelLow,
		})

	cases.Add(CaseImpactSevere, CaseCategoryFailure,
		"Host Kernel OOM Lockup",
		"Host memory pinned at 100%; kswapd pegs Host CPU at 100%; total freeze",
		"Host physical RAM exhausted; kswapd thrashing kernel page alloc locks",
		CaseLevels{
			appInRequests:    CaseLevelNormal,
			appInThroughput:  CaseLevelNormal,
			appInLatency:     CaseLevelHigh,
			appInErrors:      CaseLevelHigh,
			appCPU:           CaseLevelHigh,
			appMem:           CaseLevelNormal,
			hostCPU:          CaseLevelHigh,
			hostMem:          CaseLevelHigh,
			appOutRequests:   CaseLevelLow,
			appOutThroughput: CaseLevelLow,
			appOutLatency:    CaseLevelHigh,
			appOutErrors:     CaseLevelHigh,
		})

	cases.Add(CaseImpactSevere, CaseCategoryFailure,
		"Zombie Worker / Cold Init Deadlock",
		"App memory flat near 0% with 0% CPU; incoming requests fail instantly (502)",
		"Process deadlocked in pre-main init phase; runtime heap unallocated",
		CaseLevels{
			appInRequests:    CaseLevelNormal,
			appInThroughput:  CaseLevelNormal,
			appInLatency:     CaseLevelLow,
			appInErrors:      CaseLevelHigh,
			appCPU:           CaseLevelLow,
			appMem:           CaseLevelLow,
			hostCPU:          CaseLevelNormal,
			hostMem:          CaseLevelNormal,
			appOutRequests:   CaseLevelLow,
			appOutThroughput: CaseLevelLow,
			appOutLatency:    CaseLevelLow,
			appOutErrors:     CaseLevelLow,
		})

	cases.Add(CaseImpactSevere, CaseCategoryFailure,
		"Host OOM-Killer Cascading Storm",
		"Host memory at 100% with random service restarts and network drops",
		"OOM-killer killing random background processes and sidecars",
		CaseLevels{
			appInRequests:    CaseLevelNormal,
			appInThroughput:  CaseLevelNormal,
			appInLatency:     CaseLevelHigh,
			appInErrors:      CaseLevelHigh,
			appCPU:           CaseLevelLow,
			appMem:           CaseLevelNormal,
			hostCPU:          CaseLevelHigh,
			hostMem:          CaseLevelHigh,
			appOutRequests:   CaseLevelLow,
			appOutThroughput: CaseLevelLow,
			appOutLatency:    CaseLevelLow,
			appOutErrors:     CaseLevelLow,
		})

	cases.Add(CaseImpactSevere, CaseCategoryFailure,
		"FD / Ephemeral Port Exhaustion",
		"Fast failure on inbound/outbound connection creation with normal CPU/memory",
		"EMFILE or EADDRNOTAVAIL socket leak connection pool exhausting ulimit",
		CaseLevels{
			appInRequests:    CaseLevelNormal,
			appInThroughput:  CaseLevelNormal,
			appInLatency:     CaseLevelLow,
			appInErrors:      CaseLevelHigh,
			appCPU:           CaseLevelNormal,
			appMem:           CaseLevelNormal,
			hostCPU:          CaseLevelNormal,
			hostMem:          CaseLevelNormal,
			appOutRequests:   CaseLevelLow,
			appOutThroughput: CaseLevelLow,
			appOutLatency:    CaseLevelLow,
			appOutErrors:     CaseLevelHigh,
		})

	cases.Add(CaseImpactSevere, CaseCategoryFailure,
		"DNS Resolution Outage",
		"Outbound traffic drops with fast DNS resolution failures; ingress times out",
		"CoreDNS outage VPC resolver throttle corrupted resolv.conf",
		CaseLevels{
			appInRequests:    CaseLevelNormal,
			appInThroughput:  CaseLevelNormal,
			appInLatency:     CaseLevelHigh,
			appInErrors:      CaseLevelHigh,
			appCPU:           CaseLevelLow,
			appMem:           CaseLevelNormal,
			hostCPU:          CaseLevelLow,
			hostMem:          CaseLevelNormal,
			appOutRequests:   CaseLevelLow,
			appOutThroughput: CaseLevelLow,
			appOutLatency:    CaseLevelLow,
			appOutErrors:     CaseLevelHigh,
		})

	cases.Add(CaseImpactSevere, CaseCategoryFailure,
		"Cloud Hypervisor Steal / Throttling",
		"Latency surges despite zero App/Host CPU usage; hypervisor steal time pinned",
		"AWS burst credit balance exhaustion or noisy hypervisor neighbor",
		CaseLevels{
			appInRequests:    CaseLevelNormal,
			appInThroughput:  CaseLevelNormal,
			appInLatency:     CaseLevelHigh,
			appInErrors:      CaseLevelHigh,
			appCPU:           CaseLevelLow,
			appMem:           CaseLevelNormal,
			hostCPU:          CaseLevelLow,
			hostMem:          CaseLevelNormal,
			appOutRequests:   CaseLevelLow,
			appOutThroughput: CaseLevelLow,
			appOutLatency:    CaseLevelLow,
			appOutErrors:     CaseLevelLow,
		})

	cases.Add(CaseImpactSevere, CaseCategoryFailure,
		"Unobservable Bottleneck (Telemetry Dropout)",
		"Severe inbound queueing and outbound collapse while saturation is entirely unknown",
		"exporter or cgroup telemetry crash during critical CPU/memory exhaustion",
		CaseLevels{
			appInRequests:    CaseLevelHigh,
			appInThroughput:  CaseLevelHigh,
			appInLatency:     CaseLevelHigh,
			appInErrors:      CaseLevelHigh,
			appCPU:           CaseLevelUnknown,
			appMem:           CaseLevelUnknown,
			hostCPU:          CaseLevelUnknown,
			hostMem:          CaseLevelUnknown,
			appOutRequests:   CaseLevelLow,
			appOutThroughput: CaseLevelLow,
			appOutLatency:    CaseLevelLow,
			appOutErrors:     CaseLevelLow,
		})

	return cases
}
