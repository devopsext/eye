package forest

import (
	"github.com/devopsext/eye/common"
	"github.com/devopsext/utils"
)

type ApplicationTrafficValues map[common.TrafficKind]*common.Traffic
type ApplicationLatencyValue *common.Latency
type ApplicationErrorsValue *common.Errors
type ApplicationSaturationValue *common.Saturation

type ApplicationFrames map[common.Hash][]*ApplicationFrame

type ApplicationFrame struct {
	stamp common.Stamp
	hash  common.Hash
	//
	appInRequests   ApplicationTrafficValues
	appInThroughput ApplicationTrafficValues
	appInLatency    ApplicationLatencyValue
	appInErrors     ApplicationErrorsValue
	//
	appCPU  ApplicationSaturationValue
	appMem  ApplicationSaturationValue
	hostCPU ApplicationSaturationValue
	hostMem ApplicationSaturationValue
	//
	appOutRequests   ApplicationTrafficValues
	appOutThroughput ApplicationTrafficValues
	appOutLatency    ApplicationLatencyValue
	appOutErrors     ApplicationErrorsValue
}

type ApplicationEngine struct {
	/*InReqRegistry  *DynamicStreamRegistry
	InThruRegistry *DynamicStreamRegistry
	InLatRegistry  *DynamicStreamRegistry
	InErrRegistry  *DynamicStreamRegistry

	OutReqRegistry  *DynamicStreamRegistry
	OutThruRegistry *DynamicStreamRegistry
	OutLatRegistry  *DynamicStreamRegistry
	OutErrRegistry  *DynamicStreamRegistry

	InForest  *iforest.Forest
	SatForest *iforest.Forest
	OutForest *iforest.Forest

	InProfiler  *DynamicProfiler
	SatProfiler *DynamicProfiler
	OutProfiler *DynamicProfiler*/

	cases *common.Cases
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

func (tvs ApplicationTrafficValues) Max() (common.Traffic, bool) {
	maxVal := -1.0
	hasValid := false
	for _, v := range tvs {
		if v != nil {
			value := *v
			if !hasValid || value > maxVal {
				maxVal = value
			}
			hasValid = true
		}
	}
	if !hasValid {
		return 0.0, false
	}
	return maxVal, true
}

// ApplicationFrame

func (af *ApplicationFrame) Valid() bool {

	if af.stamp == 0 {
		return false
	}

	if len(af.appInRequests) == 0 && len(af.appOutRequests) == 0 {
		return false
	}

	if len(af.appInThroughput) == 0 && len(af.appOutThroughput) == 0 {
		return false
	}

	return true
}

func NewApplicationFrame(stamp common.Stamp, hash common.Hash, appSignal *common.ApplicationSignal, hostSignal *common.HostSignal) *ApplicationFrame {

	r := &ApplicationFrame{
		stamp: stamp,
		hash:  hash,
		//
		appInRequests:   appSignal.IncomingTraffic.RequestsAvg(),
		appInThroughput: appSignal.IncomingTraffic.ThroughputAvg(),
		appInLatency:    appSignal.IncomingLatency.Avg(),
		appInErrors:     appSignal.IncomingErrors.Sum(),
		//
		appCPU: appSignal.Saturation.CPUMax(),
		appMem: appSignal.Saturation.MemoryMax(),
		//
		appOutRequests:   appSignal.OutgoingTraffic.RequestsAvg(),
		appOutThroughput: appSignal.OutgoingTraffic.ThroughputAvg(),
		appOutLatency:    appSignal.OutgoingLatency.Avg(),
		appOutErrors:     appSignal.OutgoingErrors.Sum(),
	}

	if !utils.IsEmpty(hostSignal) {
		r.hostCPU = hostSignal.Saturation.CPUMax()
		r.hostMem = hostSignal.Saturation.MemoryMax()
	}
	return r
}

// ApplicationEngine

func (ae *ApplicationEngine) Train(frames ApplicationFrames) error {
	return nil
}

func NewApplicationEngine(maxSlotsPerDimension int, cases *common.Cases) *ApplicationEngine {

	return &ApplicationEngine{
		/*InReqRegistry:   NewDynamicStreamRegistry(maxSlotsPerDimension),
		InThruRegistry:  NewDynamicStreamRegistry(maxSlotsPerDimension),
		InLatRegistry:   NewDynamicStreamRegistry(maxSlotsPerDimension),
		InErrRegistry:   NewDynamicStreamRegistry(maxSlotsPerDimension),
		OutReqRegistry:  NewDynamicStreamRegistry(maxSlotsPerDimension),
		OutThruRegistry: NewDynamicStreamRegistry(maxSlotsPerDimension),
		OutLatRegistry:  NewDynamicStreamRegistry(maxSlotsPerDimension),
		OutErrRegistry:  NewDynamicStreamRegistry(maxSlotsPerDimension),

		InForest:    iforest.NewForest(100, 256, 0.01),
		SatForest:   iforest.NewForest(100, 256, 0.01),
		OutForest:   iforest.NewForest(100, 256, 0.01),
		InProfiler:  NewDynamicProfiler(1.5),
		SatProfiler: NewDynamicProfiler(1.5),
		OutProfiler: NewDynamicProfiler(1.5),*/

		cases: cases,
	}
}
