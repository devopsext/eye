package model

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/devopsext/eye/common"
	sreCommon "github.com/devopsext/sre/common"
	"github.com/e-XpertSolutions/go-iforest/iforest"
)

type ForestModelOptions struct {
	FilePath  string
	Schedule  string
	Retention string
}

type ForestModelHistoryItems = map[common.Hash]map[common.Stamp][]float64
type ForestModelHistory struct {
	mu    sync.Mutex
	items ForestModelHistoryItems
}

type ForestModelForestsItems = map[common.Hash]*iforest.Forest
type ForestModelForests struct {
	mu    sync.Mutex
	items map[common.Hash]*iforest.Forest
}

type ForestModel struct {
	mu            sync.Mutex
	options       ForestModelOptions
	observability *common.Observability
	logger        sreCommon.Logger

	history *ForestModelHistory
	forests *ForestModelForests
}

const (
	ForestModelTreesNumber   = 100
	ForestModelSubsampleSize = 256
	ForstModelOutlierRatio   = 0.01
)

// ForestModel

func (f *ForestModel) Name() string {
	return "ForestModel"
}

func (f *ForestModel) Schedule() string {
	return f.options.Schedule
}

func (f *ForestModel) TrainOnDataSource(wg *sync.WaitGroup, ds common.DataSource) {

	if !f.mu.TryLock() {
		return
	}
	defer f.mu.Unlock()

	wg.Add(1)
	defer wg.Done()

	l1, l2 := f.history.Sizes()
	min, max := f.history.Times()
	tmin := time.UnixMilli(int64(min))
	tmax := time.UnixMilli(int64(max))

	f.logger.Debug("Initial history for %d items / %d samples (min: %s, max: %s, diff: %s)", l1, l2, tmin, tmax, tmax.Sub(tmin))
}

func (f *ForestModel) RunOnSchedule(wg *sync.WaitGroup) {

	f.logger.Debug("Running...")

	/*
		m.history.AddOrUpdate(measurements)

		l1, l2 = m.history.Sizes()
		min, max = m.history.Times()
		tmin = time.UnixMilli(int64(min))
		tmax = time.UnixMilli(int64(max))
		m.debug("Updated history for %d items / %d samples (min: %s, max: %s, diff: %s)", l1, l2, tmin, tmax, tmax.Sub(tmin))

		when = time.Now()
		m.info("Training data...")

		m.debug("Initial forest for %d items", len(m.forests.GetItems()))
		m.forests.Train(m.history)
		m.debug("Updated forest for %d items", len(m.forests.GetItems()))

		m.info("Training finished in %s", time.Since(when))

		when = time.Now()
		m.info("Saving data...")
		m.forests.SaveToPath(m.options.FilePath)
		m.info("Saving finished in %s", time.Since(when))

		// time.Sleep(time.Duration(time.Minute * 10))
	*/
}

func NewForestModel(options ForestModelOptions, observability *common.Observability) *ForestModel {

	return &ForestModel{
		options:       options,
		observability: observability,
		logger:        observability.Logs(),
		history:       NewForestModelHistory(),
		forests:       NewForestModelForests(),
	}
}

// ForestModelHistory

func (h *ForestModelHistory) GetItems() ForestModelHistoryItems {

	h.mu.Lock()
	defer h.mu.Unlock()

	return h.items
}

func (h *ForestModelHistory) Sizes() (int, int) {

	h.mu.Lock()
	defer h.mu.Unlock()

	r := 0
	for _, v := range h.items {
		r += len(v)
	}
	return len(h.items), r
}

func (h *ForestModelHistory) Times() (common.Stamp, common.Stamp) {

	h.mu.Lock()
	defer h.mu.Unlock()

	min := common.Stamp(math.MaxUint64)
	max := common.Stamp(0)
	for _, v := range h.items {

		for s := range v {

			if s > max {
				max = s
			}

			if s < min {
				min = s
			}
		}
	}
	return min, max
}

func (h *ForestModelHistory) getApplicationSignalData(signal *common.ApplicationSignal) []float64 {

	inTraffic := 0.0
	inTrafficKind := 0.0
	inErrors := 0.0
	inLatency := 0.0
	outTraffic := 0.0
	outTrafficKind := 0.0
	outErrors := 0.0
	outLatency := 0.0
	saturation := 0.0
	saturationKind := 0.0

	data := []float64{
		inTraffic, inTrafficKind, inErrors, inLatency,
		outTraffic, outTrafficKind, outErrors, outLatency,
		saturation, saturationKind,
	}

	return data
}

func (h *ForestModelHistory) getHostSignalData(signal *common.HostSignal) []float64 {

	cpuSaturation := 0.0
	memorySaturation := 0.0

	data := []float64{
		cpuSaturation, memorySaturation,
	}

	return data
}

func (h *ForestModelHistory) getSignalData(signal common.Signal) []float64 {

	var r []float64

	as, ok := signal.(*common.ApplicationSignal)
	if ok {
		return h.getApplicationSignalData(as)
	}

	hs, ok := signal.(*common.HostSignal)
	if ok {
		return h.getHostSignalData(hs)
	}
	return r
}

func (h *ForestModelHistory) AddOrUpdate(measurements *common.Measurements) {

	h.mu.Lock()
	defer h.mu.Unlock()

	items := measurements.GetItems()

	if h.items == nil {
		h.items = make(ForestModelHistoryItems)
	}

	for stamp, signals := range items {

		for hash, signal := range signals {

			signalData := h.getSignalData(signal)
			if len(signalData) == 0 {
				continue
			}

			historyItem, ok := h.items[hash]
			if !ok {
				historyItem = make(map[common.Stamp][]float64)
			}
			historyItem[stamp] = signalData
			h.items[hash] = historyItem
		}
	}
}

func NewForestModelHistory() *ForestModelHistory {
	return &ForestModelHistory{
		items: make(ForestModelHistoryItems),
	}
}

// ForestModelForests

func (fs *ForestModelForests) GetItems() ForestModelForestsItems {

	fs.mu.Lock()
	defer fs.mu.Unlock()

	return fs.items
}

func (fs *ForestModelForests) SaveToPath(path string) {

	fs.mu.Lock()
	defer fs.mu.Unlock()

	for h, f := range fs.items {

		hpath := filepath.Join(path, fmt.Sprintf("%d.forest", h))

		file, err := os.Create(hpath)
		if err != nil {
			continue
		}
		defer file.Close()

		bw := bufio.NewWriter(file)
		defer bw.Flush()

		encoder := gob.NewEncoder(bw)

		err = encoder.Encode(f)
		if err != nil {
			continue
		}

		//f.Save(hpath)
	}
}

func (fs *ForestModelForests) getStampFeatures(stamp time.Time) (float64, float64, float64, float64) {

	hour := float64(stamp.Hour()) + float64(stamp.Minute())/60.0 + float64(stamp.Second())/3600.0

	timeSin := math.Sin((hour / 24.0) * 2.0 * math.Pi)
	timeCos := math.Cos((hour / 24.0) * 2.0 * math.Pi)

	weekday := float64(stamp.Weekday()) + (hour / 24.0)
	daySin := math.Sin((weekday / 7.0) * 2.0 * math.Pi)
	dayCos := math.Cos((weekday / 7.0) * 2.0 * math.Pi)

	return timeSin, timeCos, daySin, dayCos
}

type timeWindow struct {
	Start time.Time
	End   time.Time
}

func (fs *ForestModelForests) stampIsExcluded(stamp time.Time, exclusions []timeWindow) bool {

	for _, window := range exclusions {
		if (stamp.Equal(window.Start) || stamp.After(window.Start)) && (stamp.Equal(window.End) || stamp.Before(window.End)) {
			return true
		}
	}
	return false
}

func (fs *ForestModelForests) applyWeights(data []float64, weights []int) []float64 {

	var weighted []float64
	for i, val := range data {
		weight := weights[i]
		for w := 0; w < weight; w++ {
			weighted = append(weighted, val)
		}
	}
	return weighted
}

func (fs *ForestModelForests) applicationSignalsWeights() []int {

	weights := []int{
		1, // inTraffic
		1, // inTrafficKind
		1, // inErrors
		1, // inLatency
		1, // outTraffic
		1, // outTrafficKind
		1, // outErrors
		1, // outLatency
		1, // Saturation
		1, // SaturationKind
	}
	return weights
}

func (fs *ForestModelForests) hostSignalsWeights() []int {

	weights := []int{
		1, // cpuSaturation
		1, // memorySaturation
	}
	return weights
}

func (fs *ForestModelForests) timeDayWeights() []int {

	weights := []int{
		1, // timeSin
		1, // timeCos
		1, // daySin
		1, // dayCos
	}
	return weights
}

func (fs *ForestModelForests) Train(history *ForestModelHistory) {

	fs.mu.Lock()
	defer fs.mu.Unlock()

	mf := make(map[*iforest.Forest][][]float64)

	appSignalsWeigths := fs.applicationSignalsWeights()
	appSignalsLen := len(appSignalsWeigths)

	hostSignalsWeigths := fs.hostSignalsWeights()
	hostSignalsLen := len(hostSignalsWeigths)

	timeDayWeights := fs.timeDayWeights()

	for hash, v := range history.GetItems() {

		f, ok := fs.items[hash]
		if !ok {
			f = iforest.NewForest(ForestModelTreesNumber, ForestModelSubsampleSize, ForstModelOutlierRatio)
			fs.items[hash] = f
		}

		for stamp, data := range v {

			t := time.UnixMilli(int64(stamp))
			timeSin, timeCos, daySin, dayCos := fs.getStampFeatures(t)

			if fs.stampIsExcluded(t, []timeWindow{}) {
				continue
			}

			l := len(data)
			if l != appSignalsLen && l != hostSignalsLen {
				l = 0
				continue
			}

			timeDateData := append([]float64{timeSin, timeCos, daySin, dayCos}, data...)

			weighted := []float64{}

			switch l {
			case appSignalsLen:
				weighted = fs.applyWeights(timeDateData, append(timeDayWeights, appSignalsWeigths...))
			case hostSignalsLen:
				weighted = fs.applyWeights(timeDateData, append(timeDayWeights, hostSignalsWeigths...))
			}

			if len(weighted) == 0 {
				continue
			}
			mf[f] = append(mf[f], weighted)
		}
	}

	for f, m := range mf {
		f.Train(m)
		f.Test(m)
	}
}

func NewForestModelForests() *ForestModelForests {
	return &ForestModelForests{
		items: make(ForestModelForestsItems),
	}
}
