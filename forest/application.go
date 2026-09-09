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

type ApplicationTrafficValues map[common.TrafficKind]*common.Traffic
type ApplicationLatencyValue *common.Latency
type ApplicationErrorsValue *common.Errors
type ApplicationSaturationValue *common.Saturation

type ApplicationFrames []*ApplicationFrame

type ApplicationFrame struct {
	stamp common.Stamp
	time  time.Time
	//
	inRequests   ApplicationTrafficValues
	inThroughput ApplicationTrafficValues
	inLatency    ApplicationLatencyValue
	inErrors     ApplicationErrorsValue
	//
	appCPU  ApplicationSaturationValue
	appMem  ApplicationSaturationValue
	hostCPU ApplicationSaturationValue
	hostMem ApplicationSaturationValue
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

// K = 1.5 (Default / Standard Outlier Threshold)
// Corresponds to standard statistical "mild outliers" (roughly equivalent to 2.7sigma on a normal distribution).
// Anything exceeding $Q3 + 1.5 * IQR is immediately labeled High without needing the Isolation Forest's confirmation.

// K = 3.0 ("Extreme" Outlier Threshold)
// Expands the normal band significantly (roughly 4.7 sigma). Only massive spikes trigger direct High
// leaving subtle or multi-metric correlations to be decided by the Isolation Forest

// Lowering K (e.g., 1.0): Tightens the envelope, making the engine more sensitive to smaller deviations.
// Raising K (e.g., 2.0 - 3.0): Broadens the envelope, reducing alert noise for services with naturally spiky or high-variance diurnal traffic patterns.

type ApplicationProfiler struct {
	Stats    []ApplicationFeatureStats
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
}

type ApplicationEngineFileEntry struct {
	Forest *iforest.Forest
	Data   [][]float64
}

type ApplicationEngineFile struct {
	Times      []common.Stamp
	Incoming   ApplicationEngineFileEntry
	Saturation ApplicationEngineFileEntry
	Outgoing   ApplicationEngineFileEntry
}

type ApplicationEngine struct {
	id      common.Hash
	options *ApplicationEngineOptions

	inReqSlots  *ApplicationTrafficSlots
	inThruSlots *ApplicationTrafficSlots

	outReqSlots  *ApplicationTrafficSlots
	outThruSlots *ApplicationTrafficSlots

	times   []common.Stamp
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

func (ae *ApplicationEngine) Name() string {
	return "ForestApplicationEngine"
}

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

func (ae *ApplicationEngine) extractTimeVectors(stamp common.Stamp) []float64 {

	t := common.StampToTime(stamp)

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

	timeVec := ae.extractTimeVectors(f.stamp)

	inSlotWidth := ae.inReqSlots.maxSlots + 2

	inReqMeds := ae.getMedians(ae.inProfiler, 0, inSlotWidth)
	inThruMeds := ae.getMedians(ae.inProfiler, inSlotWidth, inSlotWidth)

	inReqSlots, inReqVal := ae.inReqSlots.VectorizeTraffic(f.inRequests, false, inReqMeds)
	inThruSlots, inThruVal := ae.inThruSlots.VectorizeTraffic(f.inThroughput, false, inThruMeds)

	inLatIdx := len(inReqSlots) + len(inThruSlots)
	inErrIdx := inLatIdx + 1

	inLatValid := f.inLatency != nil
	inLat := 0.0
	if inLatValid {
		inLat = *f.inLatency
	}
	if !inLatValid && ae.inProfiler.isFitted && len(ae.inProfiler.Stats) > inLatIdx {
		inLat = ae.inProfiler.Stats[inLatIdx].Median
	}

	inErrValid := f.inErrors != nil
	inErr := 0.0
	if inErrValid {
		inErr = *f.inErrors
	}
	if !inErrValid && ae.inProfiler.isFitted && len(ae.inProfiler.Stats) > inErrIdx {
		inErr = ae.inProfiler.Stats[inErrIdx].Median
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
	if !appCPUValid && ae.satProfiler.isFitted {
		appCPU = ae.satProfiler.Stats[0].Median
	}

	appMemValid := f.appMem != nil
	appMem := 0.0
	if appMemValid {
		appMem = *f.appMem
	}
	if !appMemValid && ae.satProfiler.isFitted {
		appMem = ae.satProfiler.Stats[1].Median
	}

	hostCPUValid := f.hostCPU != nil
	hostCPU := 0.0
	if hostCPUValid {
		hostCPU = *f.hostCPU
	}
	if !hostCPUValid && ae.satProfiler.isFitted {
		hostCPU = ae.satProfiler.Stats[2].Median
	}

	hostMemValid := f.hostMem != nil
	hostMem := 0.0
	if hostMemValid {
		hostMem = *f.hostMem
	}
	if !hostMemValid && ae.satProfiler.isFitted {
		hostMem = ae.satProfiler.Stats[3].Median
	}

	cpuDiffVal := false
	cpuDiff := 0.0
	if hostCPUValid && appCPUValid {
		cpuDiffVal = true
		cpuDiff = hostCPU - appCPU
	} else if ae.satProfiler.isFitted {
		cpuDiff = ae.satProfiler.Stats[4].Median
	}

	memDiffVal := false
	memDiff := 0.0
	if hostMemValid && appMemValid {
		memDiffVal = true
		memDiff = hostMem - appMem
	} else if ae.satProfiler.isFitted {
		memDiff = ae.satProfiler.Stats[5].Median
	}

	satVec = []float64{appCPU, appMem, hostCPU, hostMem, cpuDiff, memDiff}
	satVec = append(satVec, timeVec...)

	satValid = []bool{appCPUValid, appMemValid, hostCPUValid, hostMemValid, cpuDiffVal, memDiffVal}
	for i := 0; i < len(timeVec); i++ {
		satValid = append(satValid, true) // Time is always valid
	}

	outSlotWidth := ae.outReqSlots.maxSlots + 2

	outReqMeds := ae.getMedians(ae.outProfiler, 0, outSlotWidth)
	outThruMeds := ae.getMedians(ae.outProfiler, outSlotWidth, outSlotWidth)

	outReqSlots, outReqVal := ae.outReqSlots.VectorizeTraffic(f.outRequests, false, outReqMeds)
	outThruSlots, outThruVal := ae.outThruSlots.VectorizeTraffic(f.outThroughput, false, outThruMeds)

	/*outLatIdx := len(outReqSlots) + len(outThruSlots)
	outErrIdx := inLatIdx + 1*/

	outLatIdx := ae.outReqSlots.maxSlots + 2 + ae.outReqSlots.maxSlots + 2
	outErrIdx := outLatIdx + 1

	outLatValid := f.outLatency != nil
	outLat := 0.0
	if outLatValid {
		outLat = *f.outLatency
	}
	if !outLatValid && ae.outProfiler.isFitted && len(ae.outProfiler.Stats) > outLatIdx {
		outLat = ae.outProfiler.Stats[outLatIdx].Median
	}

	outErrValid := f.outErrors != nil
	outErr := 0.0
	if outErrValid {
		outErr = *f.outErrors
	}
	if !outErrValid && ae.outProfiler.isFitted && len(ae.outProfiler.Stats) > outErrIdx {
		outErr = ae.outProfiler.Stats[outErrIdx].Median
	}

	inReqSum, inReqKnown := f.inRequests.Sum()
	outReqSum, outReqKnown := f.outRequests.Sum()
	flowReqValid := false
	flowReqRatio := 0.0
	if inReqKnown && outReqKnown && inReqSum > 0 {
		flowReqValid = true
		flowReqRatio = outReqSum / inReqSum
	} else if ae.outProfiler.isFitted {
		flowReqRatio = ae.outProfiler.Stats[4*outSlotWidth].Median
	}

	inThruSum, inThruKnown := f.inThroughput.Sum()
	outThruSum, outThruKnown := f.outThroughput.Sum()
	flowThruValid := false
	flowThruRatio := 0.0
	if inThruKnown && outThruKnown && inThruSum > 0 {
		flowThruValid = true
		flowThruRatio = outThruSum / inThruSum
	} else if ae.outProfiler.isFitted {
		flowThruRatio = ae.outProfiler.Stats[4*outSlotWidth+1].Median
	}

	latDivValid := false
	latDiv := 0.0
	if inLatValid && outLatValid {
		latDivValid = true
		latDiv = inLat - outLat
	} else if ae.outProfiler.isFitted {
		latDiv = ae.outProfiler.Stats[4*outSlotWidth+2].Median
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

func (ae *ApplicationEngine) splitData(from, first, last common.Stamp) (tL, tR []common.Stamp, inL, satL, outL, inR, satR, outR [][]float64) {

	ltimes := len(ae.times)
	if ltimes != len(ae.inData) ||
		ltimes != len(ae.satData) ||
		ltimes != len(ae.outData) {
		return tL, tR, inL, satL, outL, inR, satR, outR
	}

	for k, stamp := range ae.times {

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

	for _, f := range frames {
		inReq = append(inReq, f.inRequests)
		inThru = append(inThru, f.inThroughput)
		outReq = append(outReq, f.outRequests)
		outThru = append(outThru, f.outThroughput)
	}

	ae.inReqSlots.Fit(inReq)
	ae.inThruSlots.Fit(inThru)
	ae.outReqSlots.Fit(outReq)
	ae.outThruSlots.Fit(outThru)

	tL, tR, inL, satL, outL, inR, satR, outR := ae.splitData(limits.From, limits.First, limits.Last)

	times := []common.Stamp{}
	inData := [][]float64{}
	satData := [][]float64{}
	outData := [][]float64{}

	for _, f := range frames {

		iv, sv, ov, _, _, _ := ae.extractVectors(f)

		index := slices.Index(ae.times, f.stamp)
		if index >= 0 {
			continue
		}
		times = append(times, f.stamp)
		inData = append(inData, iv)
		satData = append(satData, sv)
		outData = append(outData, ov)
	}

	ae.times = append(tL, times...)
	ae.times = append(ae.times, tR...)

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
	ae.inProfiler.Fit(ae.inData)

	ae.satForest.Train(ae.satData)
	err = ae.satForest.Test(ae.satData)
	if err != nil {
		return err
	}
	ae.satProfiler.Fit(ae.satData)

	ae.outForest.Train(ae.outData)
	err = ae.outForest.Test(ae.outData)
	if err != nil {
		return err
	}
	ae.outProfiler.Fit(ae.outData)

	return nil
}

func (ae *ApplicationEngine) decodeV001(decoder *gob.Decoder, onlyData, onlyForest bool) error {

	engineFile := ApplicationEngineFile{}
	err := decoder.Decode(&engineFile)
	if err != nil {
		return err
	}

	ae.times = engineFile.Times

	if onlyData {

		ltimes := len(engineFile.Times)
		if ltimes != len(engineFile.Incoming.Data) ||
			ltimes != len(engineFile.Saturation.Data) ||
			ltimes != len(engineFile.Outgoing.Data) {
			return nil
		}
		ae.inData = engineFile.Incoming.Data
		ae.satData = engineFile.Saturation.Data
		ae.outData = engineFile.Outgoing.Data
	}

	if onlyForest {
		ae.inForest = engineFile.Incoming.Forest
		ae.satForest = engineFile.Saturation.Forest
		ae.outForest = engineFile.Outgoing.Forest
	}

	return nil
}

func (ae *ApplicationEngine) load(onlyData, onlyForest bool) error {

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
		err = ae.decodeV001(decoder, onlyData, onlyForest)
	}
	return err
}

func (ae *ApplicationEngine) Load() error {
	return ae.load(true, false)
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

	engineFile := ApplicationEngineFile{
		Times: ae.times,
		Incoming: ApplicationEngineFileEntry{
			Forest: ae.inForest,
			Data:   ae.inData,
		},
		Saturation: ApplicationEngineFileEntry{
			Forest: ae.satForest,
			Data:   ae.satData,
		},
		Outgoing: ApplicationEngineFileEntry{
			Forest: ae.outForest,
			Data:   ae.outData,
		},
	}
	return encoder.Encode(&engineFile)
}

func (ae *ApplicationEngine) Clone() *ApplicationEngine {

	return &ApplicationEngine{
		id: ae.id,
	}
}

func (ae *ApplicationEngine) Diagnose(frames ApplicationFrames) {

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

func LoadApplicationEngine(id common.Hash, options *ApplicationEngineOptions) *ApplicationEngine {

	return &ApplicationEngine{}
}
