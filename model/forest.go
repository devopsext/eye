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
	"github.com/devopsext/eye/datasource"
	sreCommon "github.com/devopsext/sre/common"
	"github.com/e-XpertSolutions/go-iforest/iforest"
)

type ForestModelOptions struct {
	FilePath  string
	Schedule  string
	Retention string
}

type ForestModelDataItems = map[common.Hash]*iforest.Forest
type ForestModelData struct {
	mu    sync.Mutex
	items map[common.Hash]*iforest.Forest
}

type ForestModel struct {
	mu            sync.Mutex
	options       ForestModelOptions
	observability *common.Observability
	logger        sreCommon.Logger

	data *ForestModelData
}

const (
	ForestModelTreesNumber   = 100
	ForestModelSubsampleSize = 256
	ForstModelOutlierRatio   = 0.01
)

// ForestModelData

func (fd *ForestModelData) GetItems() ForestModelDataItems {

	fd.mu.Lock()
	defer fd.mu.Unlock()

	return fd.items
}

func (fd *ForestModelData) saveData(hash common.Hash, data [][]float64, path string) error {

	dpath := filepath.Join(path, fmt.Sprintf("%d.data", hash))

	file, err := os.Create(dpath)
	if err != nil {
		return err
	}
	defer file.Close()

	bw := bufio.NewWriter(file)
	defer bw.Flush()

	encoder := gob.NewEncoder(bw)
	return encoder.Encode(data)
}

func (fd *ForestModelData) saveForest(hash common.Hash, forest *iforest.Forest, path string) error {

	fpath := filepath.Join(path, fmt.Sprintf("%d.forest", hash))

	file, err := os.Create(fpath)
	if err != nil {
		return err
	}
	defer file.Close()

	bw := bufio.NewWriter(file)
	defer bw.Flush()

	encoder := gob.NewEncoder(bw)
	return encoder.Encode(forest)
}

func (fd *ForestModelData) getStampFeatures(stamp time.Time) (float64, float64, float64, float64) {

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

func (fd *ForestModelData) stampIsExcluded(stamp time.Time, exclusions []timeWindow) bool {

	for _, window := range exclusions {
		if (stamp.Equal(window.Start) || stamp.After(window.Start)) && (stamp.Equal(window.End) || stamp.Before(window.End)) {
			return true
		}
	}
	return false
}

func (fd *ForestModelData) applyWeights(data []float64, weights []int) []float64 {

	var weighted []float64
	for i, val := range data {
		weight := weights[i]
		for w := 0; w < weight; w++ {
			weighted = append(weighted, val)
		}
	}
	return weighted
}

func (fd *ForestModelData) applicationSignalsWeights() []int {

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

func (fd *ForestModelData) hostSignalsWeights() []int {

	weights := []int{
		1, // cpuSaturation
		1, // memorySaturation
	}
	return weights
}

func (fd *ForestModelData) timeDayWeights() []int {

	weights := []int{
		1, // timeSin
		1, // timeCos
		1, // daySin
		1, // dayCos
	}
	return weights
}

func (fd *ForestModelData) getApplicationSignalData(signal *common.ApplicationSignal) []float64 {

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

func (fd *ForestModelData) getHostSignalData(signal *common.HostSignal) []float64 {

	cpuSaturation := 0.0
	memorySaturation := 0.0

	data := []float64{
		cpuSaturation, memorySaturation,
	}

	return data
}

func (fd *ForestModelData) getSignalData(signal common.Signal) []float64 {

	var r []float64

	as, ok := signal.(*common.ApplicationSignal)
	if ok {
		return fd.getApplicationSignalData(as)
	}

	hs, ok := signal.(*common.HostSignal)
	if ok {
		return fd.getHostSignalData(hs)
	}
	return r
}

func (fd *ForestModelData) Train(measurements *common.Measurements, path string) {

	fd.mu.Lock()
	defer fd.mu.Unlock()

	md := make(map[common.Hash][][]float64)

	appSignalsWeigths := fd.applicationSignalsWeights()
	appSignalsLen := len(appSignalsWeigths)

	hostSignalsWeigths := fd.hostSignalsWeights()
	hostSignalsLen := len(hostSignalsWeigths)

	timeDayWeights := fd.timeDayWeights()

	for hash, signals := range measurements.GetItems() {

		f, ok := fd.items[hash]
		if !ok {
			f = iforest.NewForest(ForestModelTreesNumber, ForestModelSubsampleSize, ForstModelOutlierRatio)
			fd.items[hash] = f
		}

		// load history

		for stamp, signal := range signals {

			t := time.UnixMilli(int64(stamp))
			timeSin, timeCos, daySin, dayCos := fd.getStampFeatures(t)

			if fd.stampIsExcluded(t, []timeWindow{}) {
				continue
			}

			data := fd.getSignalData(signal)

			l := len(data)
			if l != appSignalsLen && l != hostSignalsLen {
				l = 0
				continue
			}

			timeDateData := append([]float64{timeSin, timeCos, daySin, dayCos}, data...)

			weighted := []float64{}

			switch l {
			case appSignalsLen:
				weighted = fd.applyWeights(timeDateData, append(timeDayWeights, appSignalsWeigths...))
			case hostSignalsLen:
				weighted = fd.applyWeights(timeDateData, append(timeDayWeights, hostSignalsWeigths...))
			}

			if len(weighted) == 0 {
				continue
			}
			md[hash] = append(md[hash], weighted)
		}
	}

	for h, d := range md {

		f, ok := fd.items[h]
		if !ok {
			continue
		}

		err := fd.saveData(h, d, path)
		if err != nil {
			continue
		}

		f.Train(d)
		f.Test(d)

		fd.saveForest(h, f, path)
	}
}

func NewForestModelData() *ForestModelData {
	return &ForestModelData{
		items: make(ForestModelDataItems),
	}
}

// ForestModel

func (f *ForestModel) Name() string {
	return "ForestModel"
}

func (f *ForestModel) Schedule() string {
	return f.options.Schedule
}

func (f *ForestModel) Train(ds common.DataSource) {

	if !f.mu.TryLock() {
		return
	}
	defer f.mu.Unlock()

	f.data.Train(ds.Measurements(), f.options.FilePath)
}

func (f *ForestModel) Start(wg *sync.WaitGroup) {

	f.logger.Debug("Starting...")

	opts := datasource.PrometheusOptions{
		AppQuery: "",
		Schedule: "",
	}

	prom := datasource.NewPrometheus(opts, f.observability, func(ds common.DataSource) {
		//
	})
	prom.Start(wg)

	//prom.RunOnSchedule()

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
		data:          NewForestModelData(),
	}
}
