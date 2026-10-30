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
	sreCommon "github.com/devopsext/sre/common"
	"github.com/devopsext/utils"
	"github.com/e-XpertSolutions/go-iforest/v2/iforest"
)

type HostCasePattern struct {
	hostCPU common.CaseLevel
	hostMem common.CaseLevel
}

type HostCase struct {
	name        string
	description string
	impact      common.CaseImpact
	category    common.CaseCategory
	pattern     HostCasePattern
}

type HostCaseVerdict struct {
	hcase *HostCase
	score int
}

type HostCases struct {
	list []*HostCase
}

type HostSaturationValue *common.Saturation

type HostFrames []*HostFrame

type HostFramePoint struct {
	timestamp time.Time
	value     float64
	max       float64
	min       float64
	med       float64
}

type HostFramePoints struct {
	hostCPU *HostFramePoint
	hostMem *HostFramePoint
}

type HostFrame struct {
	//
	hash common.Hash
	//
	stamp common.Stamp
	begin common.Stamp
	end   common.Stamp
	//
	hostCPU HostSaturationValue
	hostMem HostSaturationValue
}

type HostTrafficSlots struct {
	MaxSlots  int
	KeyToSlot map[common.TrafficKind]int
	SlotToKey []common.TrafficKind
	IsFitted  bool
}

type HostFeatureStats struct {
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
type HostProfilerBucketsKind = int

const (
	HostProfilerBucketsKindDay = iota
	HostProfilerBucketsKindHour
	HostProfilerBucketsKindHalfHour
	HostProfilerBucketsKind15Minutes
	HostProfilerBucketsKind10Minutes
	HostProfilerBucketsKind5Minutes
	HostProfilerBucketsKindMinute
)

type HostProfiler struct {
	Buckets [][]*HostFeatureStats
	K       float64
	Kind    HostProfilerBucketsKind
}

type HostEngineOptions struct {
	Path               string
	TreesNumber        int
	SubsampleSize      int
	OutlierRatio       float64
	RangeMultiplier    float64
	ProfileBucketsKind int
}

type HostEngineFileSaturation struct {
	Forest  *iforest.Forest
	Profile *HostProfiler
	Data    [][]float64
}

type HostEngineFileV001 struct {
	Stamps     []common.Stamp
	Minutes    []int
	Saturation HostEngineFileSaturation
}

type HostEngine struct {
	id      common.Hash
	options *HostEngineOptions

	stamps  []common.Stamp
	minutes []int
	satData [][]float64

	satForest   *iforest.Forest
	satProfiler *HostProfiler

	logger sreCommon.Logger
}

const (
	HostEngineV001 = "0.0.1"
)

// HostCase

func (c *HostCase) SamePattern(pattern HostCasePattern) bool {

	return c.pattern.hostCPU == pattern.hostCPU &&
		c.pattern.hostMem == pattern.hostMem
}

func NewHostCase(impact common.CaseImpact, category common.CaseCategory, name, description string, pattern HostCasePattern) *HostCase {

	return &HostCase{
		name:        name,
		description: description,
		impact:      impact,
		category:    category,
		pattern:     pattern,
	}
}

// HostCases

func (ac *HostCases) Items() []*HostCase {
	return ac.list
}

func (ac *HostCases) FindBypattern(pattern HostCasePattern) *HostCase {

	var r *HostCase
	for _, c := range ac.list {

		if c.SamePattern(pattern) {
			return c
		}
	}
	return r
}

func (ac *HostCases) Add(impact common.CaseImpact, category common.CaseCategory, name, description string, pattern HostCasePattern) *HostCase {

	c := ac.FindBypattern(pattern)
	if !utils.IsEmpty(c) {
		return c
	}
	c = NewHostCase(impact, category, name, description, pattern)
	ac.list = append(ac.list, c)
	return c
}

func (ac *HostCases) getScore(observed, expected *HostCasePattern) int {

	score := 0
	check := func(obs, exp common.CaseLevel) {
		if obs == exp {
			score++
		}
	}

	check(observed.hostCPU, expected.hostCPU)
	check(observed.hostMem, expected.hostMem)

	return score
}

func (ac *HostCases) Match(pattern *HostCasePattern) (*HostCase, int) {

	bestScore := -1
	var bestCase *HostCase

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

func NewHostCases() *HostCases {

	cases := &HostCases{}

	cases.Add(common.CaseImpactZero, common.CaseCategoryHealthy,
		"Healthy Steady State",
		"All metrics inside baseline quantiles",
		HostCasePattern{
			hostCPU: common.CaseLevelNormal,
			hostMem: common.CaseLevelNormal,
		})

	cases.Add(common.CaseImpactZero, common.CaseCategoryHealthy,
		"Ultra-Low Footprint Zero Idle",
		"Metrics fully functional with saturation floor near 0% under lightweight async load",
		HostCasePattern{
			hostCPU: common.CaseLevelLow,
			hostMem: common.CaseLevelLow,
		})

	cases.Add(common.CaseImpactSevere, common.CaseCategoryFailure,
		"Host Kernel OOM Lockup",
		"Host memory pinned at 100%; total freeze",
		HostCasePattern{
			hostCPU: common.CaseLevelHigh,
			hostMem: common.CaseLevelHigh,
		})

	cases.Add(common.CaseImpactAverage, common.CaseCategoryOperational,
		"Host Agent Transport Failure",
		"Lack of host metrics",
		HostCasePattern{
			hostCPU: common.CaseLevelUnknown,
			hostMem: common.CaseLevelUnknown,
		})

	return cases
}

// HostCaseVerdict

func (acv *HostCaseVerdict) Name() string {
	return acv.hcase.name
}

func (acv *HostCaseVerdict) Description() string {
	return acv.hcase.description
}

func (acv *HostCaseVerdict) Category() common.CaseCategory {
	return acv.hcase.impact
}

func (acv *HostCaseVerdict) Impact() common.CaseImpact {
	return acv.hcase.category
}

func (acv *HostCaseVerdict) Score() int {
	return acv.score
}

// HostFramePoint

func (fp *HostFramePoint) Timestamp() time.Time {
	return fp.timestamp
}

func (fp *HostFramePoint) Value() float64 {
	return fp.value
}

func (fp *HostFramePoint) Max() float64 {
	return fp.max
}

func (fp *HostFramePoint) Min() float64 {
	return fp.min
}

func (fp *HostFramePoint) Median() float64 {
	return fp.med
}

func NewHostFramePoint(timestamp time.Time, value, min, max, med float64) *HostFramePoint {

	return &HostFramePoint{
		timestamp: timestamp,
		value:     value,
		max:       max,
		min:       min,
		med:       med,
	}
}

// HostFramePoints

func (fp *HostFramePoints) IsEmpty() bool {

	return fp.hostCPU == nil &&
		fp.hostMem == nil
}

func (fp *HostFramePoints) CPU() *HostFramePoint {
	return fp.hostCPU
}

func (fp *HostFramePoints) Memory() *HostFramePoint {
	return fp.hostMem
}

// HostFrame

func (af *HostFrame) Valid() bool {

	if af.stamp == 0 {
		return false
	}
	return true
}

func (af *HostFrame) Stamp() common.Stamp {
	return af.stamp
}

func (af *HostFrame) Begin() common.Stamp {
	return af.begin
}

func (af *HostFrame) End() common.Stamp {
	return af.end
}

func (af *HostFrame) Hash() common.Hash {
	return af.hash
}

func NewHostFrame(stamp common.Stamp, signal *common.HostSignal) *HostFrame {

	r := &HostFrame{
		hash: signal.Hash(),
		//
		stamp: stamp,
		begin: stamp,
		end:   stamp,
		//
		hostCPU: signal.Saturation.CPUMax(),
		hostMem: signal.Saturation.MemoryMax(),
	}
	return r
}

// HostProfiler

func (ap *HostProfiler) TimeWindowKey(t time.Time, kind HostProfilerBucketsKind) (int, float64) {

	t = t.UTC()
	tm := int(t.Weekday())*24*60 + t.Hour()*60 + t.Minute()
	tms := float64(t.Weekday())*24*60 + float64(t.Hour())*60 + float64(t.Minute()) + float64(t.Second())/60.0
	switch kind {
	case HostProfilerBucketsKindDay:
		return int(t.Weekday()), float64(int(t.Weekday()) / 1.0)
	case HostProfilerBucketsKindHour:
		return tm / 60, tms / 60.0
	case HostProfilerBucketsKindHalfHour:
		return tm / 30, tms / 30.0
	case HostProfilerBucketsKind15Minutes:
		tm = int(t.Weekday())*24*60 + t.Hour()*60 + t.Minute()
		return tm / 15, tms / 15.0
	case HostProfilerBucketsKind10Minutes:
		return tm / 10, tms / 10.0
	case HostProfilerBucketsKind5Minutes:
		return tm / 5, tms / 5.0
	case HostProfilerBucketsKindMinute:
		return tm, tms
	}
	return tm, tms
}

func (ap *HostProfiler) Percentile(sorted []float64, p float64) float64 {
	idx := p * float64(len(sorted)-1)
	l := int(math.Floor(idx))
	u := int(math.Ceil(idx))
	w := idx - float64(l)
	return sorted[l]*(1.0-w) + sorted[u]*w
}

func (ap *HostProfiler) FilledCount() int {

	cnt := 0
	for idx := range ap.Buckets {
		if len(ap.Buckets[idx]) > 0 {
			cnt = cnt + 1
		}
	}
	return cnt
}

func (ap *HostProfiler) Fit(stamps []common.Stamp, matrix [][]float64) {

	if len(matrix) == 0 || len(matrix) != len(stamps) {
		return
	}
	numCols := len(matrix[0])
	bucketSize := len(ap.Buckets)

	bucketData := make([][][]float64, bucketSize)
	for b := range bucketData {
		bucketData[b] = make([][]float64, numCols)
	}

	for row := 0; row < len(matrix); row++ {
		t := common.StampToTime(stamps[row])
		b, _ := ap.TimeWindowKey(t, ap.Kind)

		if b < 0 || b >= bucketSize {
			continue
		}

		for col := 0; col < numCols; col++ {
			bucketData[b][col] = append(bucketData[b][col], matrix[row][col])
		}
	}

	for b := 0; b < bucketSize; b++ {

		newVals := bucketData[b][0]
		if len(newVals) == 0 {
			continue
		}

		for col := 0; col < numCols; col++ {

			vals := bucketData[b][col]
			if len(vals) == 0 {
				continue
			}

			if len(ap.Buckets[b]) == 0 {
				ap.Buckets[b] = make([]*HostFeatureStats, numCols)
			}

			sort.Float64s(vals)

			q1 := ap.Percentile(vals, 0.25)
			med := ap.Percentile(vals, 0.50)
			q3 := ap.Percentile(vals, 0.75)
			iqr := q3 - q1

			if iqr == 0 {
				iqr = med * 0.05
				if iqr == 0 {
					iqr = 1.0
				}
			}

			ap.Buckets[b][col] = &HostFeatureStats{
				Median: med,
				Q1:     q1,
				Q3:     q3,
				IQR:    iqr,
			}
		}
	}
}

func (ap *HostProfiler) getLimits(ms float64, col int, val float64) (float64, float64, float64) {

	bucketSize := len(ap.Buckets)
	if bucketSize > 0 {
		floatIdx := ms

		idx1 := int(math.Floor(floatIdx)) % bucketSize
		idx2 := (idx1 + 1) % bucketSize // next bucket

		frac := floatIdx - math.Floor(floatIdx)

		if len(ap.Buckets[idx1]) > col && len(ap.Buckets[idx2]) > col {
			st1 := ap.Buckets[idx1][col]
			st2 := ap.Buckets[idx2][col]

			med := st1.Median*(1-frac) + st2.Median*frac
			q1 := st1.Q1*(1-frac) + st2.Q1*frac
			q3 := st1.Q3*(1-frac) + st2.Q3*frac
			iqr := st1.IQR*(1-frac) + st2.IQR*frac

			upper := q3 + (ap.K * iqr)
			lower := q1 - (ap.K * iqr)

			if lower < 0 {
				lower = 0
			}
			return upper, lower, med
		}
	}

	if val != 0 {
		absVal := math.Abs(val * ap.K)
		return val + absVal, val - absVal, val
	}
	return 1.0, 0.0, 0.5
}

func (ap *HostProfiler) Classify(ms float64, col int, val float64, isAnomaly, isValid bool) common.CaseLevel {

	if !isValid {
		return common.CaseLevelUnknown
	}

	bucketSize := len(ap.Buckets)
	if bucketSize == 0 {
		return common.CaseLevelUnknown
	}

	max, min, med := ap.getLimits(ms, col, val)

	if val > max {
		return common.CaseLevelHigh
	}
	if val < min {
		return common.CaseLevelLow
	}
	if isAnomaly {
		if val > med {
			return common.CaseLevelHigh
		}
		return common.CaseLevelLow
	}
	return common.CaseLevelNormal
}

func NewHostProfiler(k float64, kind HostProfilerBucketsKind) *HostProfiler {

	size := 0
	switch kind {
	case HostProfilerBucketsKindDay:
		size = 7
	case HostProfilerBucketsKindHour:
		size = 7 * 24
	case HostProfilerBucketsKindHalfHour:
		size = 7 * 24 * 2
	case HostProfilerBucketsKind15Minutes:
		size = 7 * 24 * 4
	case HostProfilerBucketsKind10Minutes:
		size = 7 * 24 * 6
	case HostProfilerBucketsKind5Minutes:
		size = 7 * 24 * 12
	case HostProfilerBucketsKindMinute:
		size = 7 * 24 * 60
	}
	return &HostProfiler{
		Buckets: make([][]*HostFeatureStats, size),
		K:       k,
		Kind:    kind,
	}
}

// HostEngine

func (ae *HostEngine) Name() string {
	return "ForestHostEngine"
}

func (ae *HostEngine) getMedians(profiler *HostProfiler, t time.Time, startIdx, count int) []float64 {

	meds := make([]float64, count)
	b, _ := profiler.TimeWindowKey(t, profiler.Kind)
	if len(profiler.Buckets[b]) < startIdx+count {
		return meds
	}
	for i := 0; i < count; i++ {
		meds[i] = profiler.Buckets[b][startIdx+i].Median
	}
	return meds
}

func (ae *HostEngine) extractTimeVectors(t time.Time) []float64 {

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

func (ae *HostEngine) weeklyMinuteKey(t time.Time) int {

	t = t.UTC()

	weekday := int(t.Weekday()) // Sunday = 0, Monday = 1, ..., Saturday = 6
	hour := t.Hour()            // 0 - 23
	minute := t.Minute()        // 0 - 59

	return (weekday * 24 * 60) + (hour * 60) + minute
}

type HostEngineVectors struct {
	time                   time.Time
	bucket                 int
	minutes                float64
	satVec                 []float64
	satValid               []bool
	hostCPUIdx, hostMemIdx int
}

func (ae *HostEngine) extractVectors(t time.Time, frame *HostFrame) (vectors HostEngineVectors) {

	t = t.UTC()
	vectors.time = t
	timeVec := ae.extractTimeVectors(t)

	b, m := ae.satProfiler.TimeWindowKey(t, ae.satProfiler.Kind)
	vectors.bucket = b
	vectors.minutes = m

	vectors.hostCPUIdx = 0
	hostCPUValid := frame.hostCPU != nil
	hostCPU := 0.0
	if hostCPUValid {
		hostCPU = *frame.hostCPU
	}
	if !hostCPUValid && len(ae.satProfiler.Buckets[b]) > vectors.hostCPUIdx {
		hostCPU = ae.satProfiler.Buckets[b][vectors.hostCPUIdx].Median
	}

	vectors.hostMemIdx = vectors.hostCPUIdx + 1
	hostMemValid := frame.hostMem != nil
	hostMem := 0.0
	if hostMemValid {
		hostMem = *frame.hostMem
	}
	if !hostMemValid && len(ae.satProfiler.Buckets[b]) > vectors.hostMemIdx {
		hostMem = ae.satProfiler.Buckets[b][vectors.hostMemIdx].Median
	}

	vectors.satVec = []float64{hostCPU, hostMem}
	vectors.satVec = append(vectors.satVec, timeVec...)

	vectors.satValid = []bool{hostCPUValid, hostMemValid}
	for i := 0; i < len(timeVec); i++ {
		vectors.satValid = append(vectors.satValid, true) // Time is always valid
	}

	return vectors
}

func (ae *HostEngine) splitTimesData(from, first, last common.Stamp) (tL, tR []common.Stamp, satL, satR [][]float64) {

	lstamps := len(ae.stamps)
	if lstamps != len(ae.satData) {
		return tL, tR, satL, satR
	}

	for k, stamp := range ae.stamps {

		// skip outdated data
		if stamp < from {
			continue
		}

		// add only data outside of time limits
		if stamp < first {
			satL = append(satL, ae.satData[k])
			tL = append(tL, stamp)
		} else if stamp > last {
			satR = append(satR, ae.satData[k])
			tR = append(tR, stamp)
		}
	}
	return tL, tR, satL, satR
}

func (ae *HostEngine) Train(frames HostFrames, limits common.DataSourceDataLimits) error {

	if len(frames) == 0 {
		return nil
	}

	// sort frames by time stamp
	slices.SortFunc(frames, func(a *HostFrame, b *HostFrame) int {
		return cmp.Compare(a.Stamp(), b.Stamp())
	})

	// find frames which are new
	newFrames := []*HostFrame{}
	for _, f := range frames {

		index := slices.Index(ae.stamps, f.stamp)
		if index > -1 {
			continue
		}
		newFrames = append(newFrames, f)
	}

	// split data if new frames are inside it, clean up old data
	first := frames[0].stamp
	last := frames[len(frames)-1].stamp
	tL, tR, satL, satR := ae.splitTimesData(limits.From(), first, last)

	// find out vectors from new frames
	stamps := []common.Stamp{}
	satData := [][]float64{}

	for _, f := range newFrames {

		t := common.StampToTime(f.stamp)

		vectors := ae.extractVectors(t, f)
		stamps = append(stamps, f.stamp)
		satData = append(satData, vectors.satVec)

		key := ae.weeklyMinuteKey(t)
		index := slices.Index(ae.minutes, key)
		if index == -1 {
			ae.minutes = append(ae.minutes, key)
		}
	}

	// assign old-splitted and new data
	ae.stamps = append(tL, stamps...)
	ae.stamps = append(ae.stamps, tR...)

	ae.satData = append(satL, satData...)
	ae.satData = append(ae.satData, satR...)

	ae.satForest.Train(ae.satData)
	err := ae.satForest.Test(ae.satData)
	if err != nil {
		return err
	}
	ae.satProfiler.Fit(ae.stamps, ae.satData)

	return nil
}

type HostEngineFileLoadKind = int

const (
	HostEngineFileLoadKindData = iota
	HostEngineFileLoadKindForest
)

var AllHostEngineFileLoadKinds = []HostEngineFileLoadKind{HostEngineFileLoadKindData, HostEngineFileLoadKindForest}

func (ae *HostEngine) decodeV001(decoder *gob.Decoder, kinds []HostEngineFileLoadKind) error {

	engineFile := HostEngineFileV001{}
	err := decoder.Decode(&engineFile)
	if err != nil {
		return err
	}

	ae.stamps = engineFile.Stamps
	ae.minutes = engineFile.Minutes

	if utils.Contains(kinds, HostEngineFileLoadKindData) {

		lstamps := len(engineFile.Stamps)
		if lstamps != len(engineFile.Saturation.Data) {
			return nil
		}
		ae.satData = engineFile.Saturation.Data
	}

	if utils.Contains(kinds, HostEngineFileLoadKindForest) {
		ae.satForest = engineFile.Saturation.Forest
	}

	ae.satProfiler = engineFile.Saturation.Profile
	return nil
}

func (ae *HostEngine) load(kinds []HostEngineFileLoadKind) error {

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
	case HostEngineV001:
		err = ae.decodeV001(decoder, kinds)
	}
	return err
}

func (ae *HostEngine) LoadData() error {
	return ae.load([]HostEngineFileLoadKind{HostEngineFileLoadKindData})
}

func (ae *HostEngine) LoadForest() error {
	return ae.load([]HostEngineFileLoadKind{HostEngineFileLoadKindForest})
}

func (ae *HostEngine) Save() error {

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

	if err := encoder.Encode(HostEngineV001); err != nil {
		return err
	}

	engineFile := HostEngineFileV001{
		Stamps:  ae.stamps,
		Minutes: ae.minutes,
		Saturation: HostEngineFileSaturation{
			Forest:  ae.satForest,
			Profile: ae.satProfiler,
			Data:    ae.satData,
		},
	}
	return encoder.Encode(&engineFile)
}

func (ae *HostEngine) Clone() *HostEngine {

	return &HostEngine{
		id: ae.id,
	}
}

func (ae *HostEngine) extractPoints(vectors HostEngineVectors) HostFramePoints {

	points := HostFramePoints{}
	ms := vectors.minutes

	if len(vectors.satVec) > vectors.hostCPUIdx && vectors.satValid[vectors.hostCPUIdx] {
		val := vectors.satVec[vectors.hostCPUIdx]
		max, min, med := ae.satProfiler.getLimits(ms, vectors.hostCPUIdx, val)
		points.hostCPU = NewHostFramePoint(vectors.time, val, min, max, med)
	}

	if len(vectors.satVec) > vectors.hostMemIdx && vectors.satValid[vectors.hostMemIdx] {
		val := vectors.satVec[vectors.hostMemIdx]
		max, min, med := ae.satProfiler.getLimits(ms, vectors.hostMemIdx, val)
		points.hostMem = NewHostFramePoint(vectors.time, val, min, max, med)
	}
	return points
}

func (ae *HostEngine) predict(frame *HostFrame) (*HostFramePoints, *HostCasePattern) {

	t := common.StampToTime(frame.stamp)

	vectors := ae.extractVectors(t, frame)
	points := ae.extractPoints(vectors)
	if points.IsEmpty() {
		return nil, nil
	}

	satLabels, _, _ := ae.satForest.Predict([][]float64{vectors.satVec})

	// no labels
	if len(satLabels) == 0 {
		return &points, nil
	}

	satAnom := satLabels[0] == 1

	classify := func(profiler *HostProfiler, idx int, vec []float64, valid []bool, anom bool) common.CaseLevel {
		return profiler.Classify(vectors.minutes, idx, vec[idx], anom, valid[idx])
	}

	hostCPULevel := classify(ae.satProfiler, vectors.hostCPUIdx, vectors.satVec, vectors.satValid, satAnom)
	hostMemLevel := classify(ae.satProfiler, vectors.hostMemIdx, vectors.satVec, vectors.satValid, satAnom)

	pattern := &HostCasePattern{
		hostCPU: hostCPULevel,
		hostMem: hostMemLevel,
	}
	return &points, pattern
}

func (ae *HostEngine) aggregateFrames(frames HostFrames) HostFrame {

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

	var maxHostCPU HostSaturationValue
	var maxHostMem HostSaturationValue

	for _, f := range frames {

		maxHostCPU = maxHostSaturation(f.hostCPU, maxHostCPU)
		maxHostMem = maxHostSaturation(f.hostMem, maxHostMem)
	}

	return HostFrame{
		hostCPU: maxHostCPU,
		hostMem: maxHostMem,
	}
}

func (ae *HostEngine) Ready() bool {

	if !ae.satForest.Trained || !ae.satForest.Tested {
		return false
	}
	return true
}

func (ae *HostEngine) Exists(stamp common.Stamp) bool {

	if len(ae.minutes) == 0 {
		return false
	}

	t := common.StampToTime(stamp)
	key := ae.weeklyMinuteKey(t)
	index := slices.Index(ae.minutes, key)

	return index != -1
}

func (ae *HostEngine) Consolidate(frames HostFrames) *HostFrame {

	if len(frames) == 0 {
		return nil
	}

	// sort frames by time stamp
	slices.SortFunc(frames, func(a *HostFrame, b *HostFrame) int {
		return cmp.Compare(a.Stamp(), b.Stamp())
	})

	begin := frames[0].Stamp()
	end := frames[len(frames)-1].Stamp()

	//stamp := begin + (end-begin)/2
	stamp := begin

	frame := ae.aggregateFrames(frames)

	frame.hash = frames[0].Hash()
	frame.stamp = stamp
	frame.begin = begin
	frame.end = end

	return &frame
}

func (ae *HostEngine) Diagnose(frame *HostFrame, cases *HostCases) (*HostFramePoints, *HostCaseVerdict) {

	if frame == nil {
		return nil, nil
	}

	points, pattern := ae.predict(frame)
	cs, score := cases.Match(pattern)
	if cs == nil {
		return points, nil
	}

	verdict := &HostCaseVerdict{
		hcase: cs,
		score: score,
	}
	return points, verdict
}

func NewHostEngine(id common.Hash, options *HostEngineOptions, observability *common.Observability) *HostEngine {

	return &HostEngine{

		logger:  observability.Logs(),
		id:      id,
		options: options,

		satForest:   iforest.NewForest(options.TreesNumber, options.SubsampleSize, options.OutlierRatio),
		satProfiler: NewHostProfiler(options.RangeMultiplier, options.ProfileBucketsKind),
	}
}
