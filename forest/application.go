package forest

import (
	"math"
	"sort"

	"github.com/devopsext/eye/common"
	"github.com/devopsext/utils"
)

type ApplicationTrafficValues map[common.TrafficKind]*common.Traffic
type ApplicationLatencyValue *common.Latency
type ApplicationErrorsValue *common.Errors
type ApplicationSaturationValue *common.Saturation

type ApplicationFrames []*ApplicationFrame

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

type ApplicationTrafficSlots struct {
	maxSlots  int
	keyToSlot map[common.TrafficKind]int
	slotToKey []common.TrafficKind
	isFitted  bool
}

type ApplicationFeatureStats struct {
	Median float64
	Q1     float64
	Q3     float64
	IQR    float64
}

type ApplicationProfiler struct {
	Stats    []ApplicationFeatureStats
	K        float64
	isFitted bool
}

type ApplicationEngine struct {
	appInReqSlots  *ApplicationTrafficSlots
	appInThruSlots *ApplicationTrafficSlots

	appOutReqSlots  *ApplicationTrafficSlots
	appOutThruSlots *ApplicationTrafficSlots

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

	InProfiler *ApplicationProfiler
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

	ss.keyToSlot = make(map[common.TrafficKind]int)
	ss.slotToKey = make([]common.TrafficKind, ss.maxSlots)

	limit := len(pairs)
	if limit > ss.maxSlots {
		limit = ss.maxSlots
	}

	for i := 0; i < limit; i++ {
		ss.slotToKey[i] = pairs[i].k
		ss.keyToSlot[pairs[i].k] = i
	}
	ss.isFitted = true
}

func (ss *ApplicationTrafficSlots) VectorizeTraffic(avs ApplicationTrafficValues, useMaxForTotal bool, medians []float64) ([]float64, []bool) {

	vec := make([]float64, ss.maxSlots+2)
	valid := make([]bool, ss.maxSlots+2)
	otherIdx := ss.maxSlots
	totalIdx := ss.maxSlots + 1

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

		if slot, ok := ss.keyToSlot[k]; ok {
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
		maxSlots:  maxSlots,
		keyToSlot: make(map[common.TrafficKind]int),
		slotToKey: make([]common.TrafficKind, maxSlots),
	}
}

// ApplicationProfiler

func (ap *ApplicationProfiler) Fit(matrix [][]float64) {
	if len(matrix) == 0 {
		return
	}

	numCols := len(matrix[0])
	ap.Stats = make([]ApplicationFeatureStats, numCols)

	percentile := func(sorted []float64, p float64) float64 {
		idx := p * float64(len(sorted)-1)
		l := int(math.Floor(idx))
		u := int(math.Ceil(idx))
		w := idx - float64(l)
		return sorted[l]*(1.0-w) + sorted[u]*w
	}

	for col := 0; col < numCols; col++ {
		vals := make([]float64, len(matrix))
		for row := 0; row < len(matrix); row++ {
			vals[row] = matrix[row][col]
		}
		sort.Float64s(vals)

		q1 := percentile(vals, 0.25)
		med := percentile(vals, 0.50)
		q3 := percentile(vals, 0.75)
		iqr := q3 - q1

		if iqr == 0 {
			iqr = med * 0.05
			if iqr == 0 {
				iqr = 1.0
			}
		}

		ap.Stats[col] = ApplicationFeatureStats{
			Median: med,
			Q1:     q1,
			Q3:     q3,
			IQR:    iqr,
		}
	}
	ap.isFitted = true
}

func (ap *ApplicationProfiler) Classify(col int, val float64, isAnomaly, isValid bool) common.CaseLevel {

	if !isValid {
		return common.CaseLevelUnknown
	}

	st := ap.Stats[col]
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

func (ae *ApplicationEngine) getMedians(profiler *ApplicationProfiler, startIdx, count int) []float64 {
	meds := make([]float64, count)
	if !profiler.isFitted || len(profiler.Stats) < startIdx+count {
		return meds
	}
	for i := 0; i < count; i++ {
		meds[i] = profiler.Stats[startIdx+i].Median
	}
	return meds
}

func (ae *ApplicationEngine) extractVectors(f *ApplicationFrame) (inVec, satVec, outVec []float64, inValid, satValid, outValid []bool) {

	slotWidth := ae.appInReqSlots.maxSlots + 2

	appInReqMeds := ae.getMedians(ae.InProfiler, 0, slotWidth)
	appInThruMeds := ae.getMedians(ae.InProfiler, slotWidth, slotWidth)

	appInReqSlots, appInReqVal := ae.appInReqSlots.VectorizeTraffic(f.appInRequests, false, appInReqMeds)
	appInThruSlots, appInThruVal := ae.appInThruSlots.VectorizeTraffic(f.appInThroughput, false, appInThruMeds)

	appInLatIdx := len(appInReqSlots) + len(appInThruSlots)
	appInErrIdx := appInLatIdx + 1

	appInLatVal := f.appInLatency != nil
	appInLat := *f.appInLatency
	if f.appInLatency == nil && ae.InProfiler.isFitted && len(ae.InProfiler.Stats) > appInLatIdx {
		appInLat = ae.InProfiler.Stats[appInLatIdx].Median
	}

	appInErrVal := f.appInErrors != nil
	appInErr := *f.appInErrors
	if f.appInErrors == nil && ae.InProfiler.isFitted && len(ae.InProfiler.Stats) > appInErrIdx {
		appInErr = ae.InProfiler.Stats[appInErrIdx].Median
	}

	inVec = append(inVec, appInReqSlots...)
	inVec = append(inVec, appInThruSlots...)
	inVec = append(inVec, appInLat, appInErr)

	inValid = append(inValid, appInReqVal...)
	inValid = append(inValid, appInThruVal...)
	inValid = append(inValid, appInLatVal, appInErrVal)

	/*	inValid = append(inValid, inLatVal...)
		inValid = append(inValid, inErrVal...)

		appCPU := f.AppCPU.Value
		if !f.AppCPU.Valid && e.SatProfiler.isFitted {
			appCPU = e.SatProfiler.Stats[0].Median
		}
		appMem := f.AppMem.Value
		if !f.AppMem.Valid && e.SatProfiler.isFitted {
			appMem = e.SatProfiler.Stats[1].Median
		}
		hostCPU := f.HostCPU.Value
		if !f.HostCPU.Valid && e.SatProfiler.isFitted {
			hostCPU = e.SatProfiler.Stats[2].Median
		}
		hostMem := f.HostMem.Value
		if !f.HostMem.Valid && e.SatProfiler.isFitted {
			hostMem = e.SatProfiler.Stats[3].Median
		}

		cpuDiff := 0.0
		cpuDiffVal := false
		if f.HostCPU.Valid && f.AppCPU.Valid {
			cpuDiff = f.HostCPU.Value - f.AppCPU.Value
			cpuDiffVal = true
		} else if e.SatProfiler.isFitted {
			cpuDiff = e.SatProfiler.Stats[4].Median
		}

		memDiff := 0.0
		memDiffVal := false
		if f.HostMem.Valid && f.AppMem.Valid {
			memDiff = f.HostMem.Value - f.AppMem.Value
			memDiffVal = true
		} else if e.SatProfiler.isFitted {
			memDiff = e.SatProfiler.Stats[5].Median
		}

		satVec = []float64{appCPU, appMem, hostCPU, hostMem, cpuDiff, memDiff}
		satValid = []bool{f.AppCPU.Valid, f.AppMem.Valid, f.HostCPU.Valid, f.HostMem.Valid, cpuDiffVal, memDiffVal}

		outSlotWidth := e.OutReqRegistry.maxSlots + 2
		outReqMeds := e.getMedians(e.OutProfiler, 0, outSlotWidth)
		outThruMeds := e.getMedians(e.OutProfiler, outSlotWidth, outSlotWidth)
		outLatMeds := e.getMedians(e.OutProfiler, 2*outSlotWidth, outSlotWidth)
		outErrMeds := e.getMedians(e.OutProfiler, 3*outSlotWidth, outSlotWidth)

		outReqSlots, outReqVal := e.OutReqRegistry.Vectorize(f.OutRequests, false, outReqMeds)
		outThruSlots, outThruVal := e.OutThruRegistry.Vectorize(f.OutThroughput, false, outThruMeds)
		outLatSlots, outLatVal := e.OutLatRegistry.Vectorize(f.OutLatency, true, outLatMeds)
		outErrSlots, outErrVal := e.OutErrRegistry.Vectorize(f.OutErrors, false, outErrMeds)

		inReqSum, inReqKnown := f.InRequests.Sum()
		outReqSum, outReqKnown := f.OutRequests.Sum()
		flowReqRatio := 0.0
		flowReqKnown := false
		if inReqKnown && outReqKnown && inReqSum > 0 {
			flowReqRatio = outReqSum / inReqSum
			flowReqKnown = true
		} else if e.OutProfiler.isFitted {
			flowReqRatio = e.OutProfiler.Stats[4*outSlotWidth].Median
		}

		inThruSum, inThruKnown := f.InThroughput.Sum()
		outThruSum, outThruKnown := f.OutThroughput.Sum()
		flowThruRatio := 0.0
		flowThruKnown := false
		if inThruKnown && outThruKnown && inThruSum > 0 {
			flowThruRatio = outThruSum / inThruSum
			flowThruKnown = true
		} else if e.OutProfiler.isFitted {
			flowThruRatio = e.OutProfiler.Stats[4*outSlotWidth+1].Median
		}

		inMaxLat, inLatKnown := f.InLatency.Max()
		outMaxLat, outLatKnown := f.OutLatency.Max()
		latDiv := 0.0
		latDivKnown := false
		if inLatKnown && outLatKnown {
			latDiv = inMaxLat - outMaxLat
			latDivKnown = true
		} else if e.OutProfiler.isFitted {
			latDiv = e.OutProfiler.Stats[4*outSlotWidth+2].Median
		}

		outVec = append(outVec, outReqSlots...)
		outVec = append(outVec, outThruSlots...)
		outVec = append(outVec, outLatSlots...)
		outVec = append(outVec, outErrSlots...)
		outVec = append(outVec, flowReqRatio, flowThruRatio, latDiv)

		outValid = append(outValid, outReqVal...)
		outValid = append(outValid, outThruVal...)
		outValid = append(outValid, outLatVal...)
		outValid = append(outValid, outErrVal...)
		outValid = append(outValid, flowReqKnown, flowThruKnown, latDivKnown)
	*/
	return inVec, satVec, outVec, inValid, satValid, outValid
}

func (ae *ApplicationEngine) Train(frames ApplicationFrames) error {

	var appInReq, appInThru []ApplicationTrafficValues
	var appOutReq, appOutThru []ApplicationTrafficValues

	for _, f := range frames {
		appInReq = append(appInReq, f.appInRequests)
		appInThru = append(appInThru, f.appInThroughput)
		appOutReq = append(appOutReq, f.appOutRequests)
		appOutThru = append(appOutThru, f.appOutThroughput)
	}

	ae.appInReqSlots.Fit(appInReq)
	ae.appInThruSlots.Fit(appInThru)
	ae.appOutReqSlots.Fit(appOutReq)
	ae.appOutThruSlots.Fit(appOutThru)

	var inMatrix, satMatrix, outMatrix [][]float64
	for _, f := range frames {
		iv, sv, ov, _, _, _ := ae.extractVectors(f)
		inMatrix = append(inMatrix, iv)
		satMatrix = append(satMatrix, sv)
		outMatrix = append(outMatrix, ov)
	}

	/*
		e.InForest.Train(inMatrix)
		e.InForest.Test(inMatrix)
		e.InProfiler.Fit(inMatrix)

		e.SatForest.Train(satMatrix)
		e.SatForest.Test(satMatrix)
		e.SatProfiler.Fit(satMatrix)

		e.OutForest.Train(outMatrix)
		e.OutForest.Test(outMatrix)
		e.OutProfiler.Fit(outMatrix)
	*/
	return nil
}

func NewApplicationEngine() *ApplicationEngine {

	maxSlotsPerDimension := 4
	return &ApplicationEngine{
		appInReqSlots:  NewApplicationTrafficSlots(maxSlotsPerDimension),
		appInThruSlots: NewApplicationTrafficSlots(maxSlotsPerDimension),

		appOutReqSlots:  NewApplicationTrafficSlots(maxSlotsPerDimension),
		appOutThruSlots: NewApplicationTrafficSlots(maxSlotsPerDimension),

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

	}
}
