package model

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/devopsext/eye/common"
	"github.com/devopsext/eye/datasource"
	sreCommon "github.com/devopsext/sre/common"
	"github.com/devopsext/utils"
	"github.com/e-XpertSolutions/go-iforest/iforest"
	"golang.org/x/sync/errgroup"
)

type ForestModelOptions struct {
	FilePath    string
	Schedule    string
	Retention   string
	Concurrency int
	Filter      []string /// add filter by apps and hosts
}

type ForestModelDataItems = map[common.Hash]*iforest.Forest
type ForestModelData struct {
	mu    sync.Mutex
	model *ForestModel
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

func (fd *ForestModelData) AddOrUpdate(hash common.Hash, forest *iforest.Forest) {

	fd.mu.Lock()
	defer fd.mu.Unlock()

	fd.items[hash] = forest
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

func (fd *ForestModelData) prepare(measurements *common.Measurements, hashes []common.Hash) (map[common.Hash][][]float64, map[common.Hash][]common.Stamp) {

	md := make(map[common.Hash][][]float64)
	mt := make(map[common.Hash][]common.Stamp)

	appSignalsWeigths := fd.applicationSignalsWeights()
	appSignalsLen := len(appSignalsWeigths)

	hostSignalsWeigths := fd.hostSignalsWeights()
	hostSignalsLen := len(hostSignalsWeigths)

	timeDayWeights := fd.timeDayWeights()

	for hash, signals := range measurements.GetItems() {

		for stamp, signal := range signals {

			if !signal.ContainsAny(hashes) {
				continue
			}

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
			mt[hash] = append(mt[hash], stamp)
		}
	}
	return md, mt
}

type ForestModelFileForest struct {
	Forest *iforest.Forest
}

func (fd *ForestModelData) loadForest(path string, hash common.Hash) *iforest.Forest {

	fpath := filepath.Join(path, fmt.Sprintf("%d.data", hash))

	f, err := os.Open(fpath)
	if err != nil {
		return nil
	}
	defer f.Close()

	br := bufio.NewReader(f)
	decoder := gob.NewDecoder(br)

	fmff := ForestModelFileForest{}
	err = decoder.Decode(&fmff)
	if err != nil {
		return nil
	}
	return fmff.Forest
}

type ForestModelFileData struct {
	Data  [][]float64
	Times []common.Stamp
}

func (fd *ForestModelData) loadData(path string, hash common.Hash, first, last common.Stamp) ([][]float64, [][]float64) {

	r1 := [][]float64{}
	r2 := [][]float64{}
	fpath := filepath.Join(path, fmt.Sprintf("%d.data", hash))

	f, err := os.Open(fpath)
	if err != nil {
		return r1, r2
	}
	defer f.Close()

	br := bufio.NewReader(f)
	decoder := gob.NewDecoder(br)

	fmfd := ForestModelFileData{}
	err = decoder.Decode(&fmfd)
	if err != nil {
		return r1, r2
	}

	if len(fmfd.Data) != len(fmfd.Times) {
		return r1, r2
	}

	for k, stamp := range fmfd.Times {

		// add only data outside of time limits
		if stamp < first {
			r1 = append(r1, fmfd.Data[k])
		} else if stamp > last {
			r2 = append(r2, fmfd.Data[k])
		}
	}
	return r1, r2
}

type ForestModelFile struct {
	Forest *iforest.Forest
	Data   [][]float64
	Times  []common.Stamp
}

func (fd *ForestModelData) save(path string, hash common.Hash, forest *iforest.Forest, data [][]float64, times []common.Stamp) error {

	fpath := filepath.Join(path, fmt.Sprintf("%d.data", hash))

	file, err := os.Create(fpath)
	if err != nil {
		return err
	}
	defer file.Close()

	bw := bufio.NewWriter(file)
	defer bw.Flush()

	encoder := gob.NewEncoder(bw)

	fmf := ForestModelFile{
		Forest: forest,
		Data:   data,
		Times:  times,
	}
	return encoder.Encode(&fmf)
}

func (fd *ForestModelData) train(measurements *common.Measurements, hashes []common.Hash) {

	first := measurements.GetFirst()
	last := measurements.GetLast()

	data, times := fd.prepare(measurements, hashes)
	if len(data) == 0 {
		return
	}

	path := fd.model.options.FilePath

	gr := &errgroup.Group{}
	gr.SetLimit(fd.model.options.Concurrency)

	for h, d := range data {

		gr.Go(func() error {

			d1, d2 := fd.loadData(path, h, first, last)

			d := append(d1, d...)
			d = append(d, d2...)

			f := iforest.NewForest(ForestModelTreesNumber, ForestModelSubsampleSize, ForstModelOutlierRatio)

			f.Train(d)
			err := f.Test(d)
			if err != nil {
				return err
			}
			fd.AddOrUpdate(h, f)
			fd.save(path, h, f, d, times[h])
			return nil
		})
	}
	gr.Wait()
}

func NewForestModelData() *ForestModelData {
	return &ForestModelData{
		items: make(ForestModelDataItems),
	}
}

// ForestModel

func (fm *ForestModel) Name() string {
	return "ForestModel"
}

func (fm *ForestModel) Schedule() string {
	return fm.options.Schedule
}

func (fm *ForestModel) findHashes(names *common.Names) []common.Hash {

	r := []common.Hash{}

	for _, name := range fm.options.Filter {

		name := strings.TrimSpace(name)
		if utils.IsEmpty(name) {
			continue
		}

		hash := names.FindByName(name)
		if hash == 0 {
			continue
		}
		r = append(r, hash)
	}
	return r
}

func (fm *ForestModel) Train(ds common.DataSource) {

	if !fm.mu.TryLock() {
		return
	}
	defer fm.mu.Unlock()

	hashes := fm.findHashes(ds.Names())

	fm.data.model = fm
	fm.data.train(ds.Measurements(), hashes)
}

func (fm *ForestModel) Start(wg *sync.WaitGroup) {

	fm.logger.Debug("Starting...")

	opts := datasource.PrometheusOptions{
		AppQuery: "",
		Schedule: "",
	}

	prom := datasource.NewPrometheus(opts, fm.observability, func(ds common.DataSource) {
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
