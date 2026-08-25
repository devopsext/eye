package model

import (
	"bufio"
	"encoding/gob"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/devopsext/eye/common"
	sreCommon "github.com/devopsext/sre/common"
	"github.com/devopsext/utils"
	iforest "github.com/e-XpertSolutions/go-iforest/v2/iforest"
	"github.com/jellydator/ttlcache/v3"
	"golang.org/x/sync/errgroup"
)

type ForestModelOptions struct {
	Path        string
	Concurrency int
	Filter      []string /// add filter by apps and hosts
	DataTTL     string
	AnomalyTTL  string
}

type ForestModelData struct {
	path        string
	concurrency int
	items       *ttlcache.Cache[common.Hash, *iforest.Forest]
}

type ForestModelDetection struct {
	id    string
	start common.Stamp
	end   common.Stamp
	bound float64
	score float64
}

type ForestModelDetections struct {
	mu    sync.Mutex
	items *ttlcache.Cache[string, *ForestModelDetection]
}

type ForestModel struct {
	mu            sync.Mutex
	options       ForestModelOptions
	observability *common.Observability
	logger        sreCommon.Logger

	data       *ForestModelData
	detections *ForestModelDetections
}

const (
	ForestModelTreesNumber   = 100
	ForestModelSubsampleSize = 256
	ForestModelOutlierRatio  = 0.01
)

// ForestModelData

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

	lHashes := len(hashes)

	for hash, signals := range measurements.GetItems() {

		for stamp, signal := range signals {

			if lHashes > 0 && !signal.ContainsAny(hashes) {
				continue
			}

			t := common.StampToTime(stamp)
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

type ForestModelFileData struct {
	Data  [][]float64
	Times []common.Stamp
}

func (fd *ForestModelData) loadData(hash common.Hash,
	from, first, last common.Stamp) ([][]float64, []common.Stamp, [][]float64, []common.Stamp) {

	d1 := [][]float64{}
	t1 := []common.Stamp{}
	d2 := [][]float64{}
	t2 := []common.Stamp{}

	fpath := filepath.Join(fd.path, fmt.Sprintf("%d.data", hash))

	f, err := os.Open(fpath)
	if err != nil {
		return d1, t1, d2, t2
	}
	defer f.Close()

	br := bufio.NewReader(f)
	decoder := gob.NewDecoder(br)

	fmfd := ForestModelFileData{}
	err = decoder.Decode(&fmfd)
	if err != nil {
		return d1, t1, d2, t2
	}

	if len(fmfd.Data) != len(fmfd.Times) {
		return d1, t1, d2, t2
	}

	for k, stamp := range fmfd.Times {

		// skip outdated data
		if stamp < from {
			continue
		}

		// add only data outside of time limits
		if stamp < first {
			d1 = append(d1, fmfd.Data[k])
			t1 = append(t1, stamp)
		} else if stamp > last {
			d2 = append(d2, fmfd.Data[k])
			t2 = append(t2, stamp)
		}
	}
	return d1, t1, d2, t2
}

type ForestModelFile struct {
	Forest *iforest.Forest
	Data   [][]float64
	Times  []common.Stamp
}

func (fd *ForestModelData) save(hash common.Hash, forest *iforest.Forest, data [][]float64, times []common.Stamp) error {

	if len(data) != len(times) {
		return nil
	}

	if !utils.DirExists(fd.path) {
		os.MkdirAll(fd.path, os.ModePerm)
	}

	fpath := filepath.Join(fd.path, fmt.Sprintf("%d.data", hash))

	f, err := os.Create(fpath)
	if err != nil {
		return err
	}
	defer f.Close()

	bw := bufio.NewWriter(f)
	defer bw.Flush()

	encoder := gob.NewEncoder(bw)

	fmf := ForestModelFile{
		Forest: forest,
		Data:   data,
		Times:  times,
	}
	return encoder.Encode(&fmf)
}

func (fd *ForestModelData) train(dsd common.DataSourceData, hashes []common.Hash) error {

	measurements := dsd.Measurements()
	from := dsd.From()
	first := dsd.First()
	last := dsd.Last()

	data, times := fd.prepare(measurements, hashes)
	if len(data) == 0 {
		return nil
	}

	gr := &errgroup.Group{}
	gr.SetLimit(fd.concurrency)

	errs := make(chan error, len(data))

	for h, d := range data {

		gr.Go(func() error {

			d1, t1, d2, t2 := fd.loadData(h, from, first, last)

			d := append(d1, d...)
			d = append(d, d2...)

			f := iforest.NewForest(ForestModelTreesNumber, ForestModelSubsampleSize, ForestModelOutlierRatio)

			f.Train(d)

			err := f.Test(d)
			if err != nil {
				errs <- err
				return nil
			}
			fd.items.Set(h, f, ttlcache.DefaultTTL)

			t := times[h]
			t = append(t1, t...)
			t = append(t, t2...)

			return fd.save(h, f, d, t)
		})
	}
	gr.Wait()
	close(errs)

	all := []error{}
	for e := range errs {
		all = append(all, e)
	}
	return errors.Join(all...)
}

func (fd *ForestModelData) detect(dsd common.DataSourceData, hashes []common.Hash, detections *ForestModelDetections) error {

	measurements := dsd.Measurements()

	data, times := fd.prepare(measurements, hashes)
	if len(data) == 0 {
		return nil
	}

	gr := &errgroup.Group{}
	gr.SetLimit(fd.concurrency)

	errs := make(chan error, len(data))

	for h, d := range data {

		gr.Go(func() error {

			item := fd.items.Get(h)
			if item == nil {
				return nil
			}

			f := item.Value()
			if f == nil {
				return nil
			}

			labels, scores, err := f.Predict(d)
			if err != nil {
				errs <- err
				return nil
			}

			for i, label := range labels {

				detected := label == 1
				if !detected {
					continue
				}

				timeExists := len(times[h]) > i
				scoreExists := len(scores) > i

				if !timeExists || !scoreExists {
					continue
				}
				id := "adsasdad" // based on Hash + Dependecies !!!!
				detections.AddOrUpdate(id, times[h][i], f.AnomalyBound, scores[i])
			}
			return nil
		})
	}
	gr.Wait()
	close(errs)

	all := []error{}
	for e := range errs {
		all = append(all, e)
	}
	return errors.Join(all...)
}

type ForestModelFileForest struct {
	Forest *iforest.Forest
}

func (fd *ForestModelData) loadForest(c *ttlcache.Cache[common.Hash, *iforest.Forest], key common.Hash) *ttlcache.Item[common.Hash, *iforest.Forest] {

	fpath := filepath.Join(fd.path, fmt.Sprintf("%d.data", key))

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

	item := c.Set(key, fmff.Forest, ttlcache.DefaultTTL)
	return item
}

func NewForestModelData(path string, ttl time.Duration, concurrency int) *ForestModelData {

	fd := &ForestModelData{
		path:        path,
		concurrency: concurrency,
	}

	loader := ttlcache.LoaderFunc[common.Hash, *iforest.Forest](fd.loadForest)

	fd.items = ttlcache.New(
		ttlcache.WithTTL[common.Hash, *iforest.Forest](ttl),
		ttlcache.WithLoader(ttlcache.NewSuppressedLoader(loader, nil)),
	)
	return fd
}

// ForestModelDetection

func (fd *ForestModelDetection) ID() string {
	return fd.id
}

func (fd *ForestModelDetection) Start() common.Stamp {
	return fd.start
}

func (fd *ForestModelDetection) End() common.Stamp {
	return fd.end
}

func NewForestModelDetection(id string, start common.Stamp, bound, score float64) *ForestModelDetection {
	return &ForestModelDetection{
		id:    id,
		start: start,
		bound: bound,
		score: score,
	}
}

// ForestModelDetections

func (fd *ForestModelDetections) Anomalies() []common.Anomaly {

	fd.mu.Lock()
	defer fd.mu.Unlock()

	r := []common.Anomaly{}
	fd.items.Range(func(item *ttlcache.Item[string, *ForestModelDetection]) bool {

		d := item.Value()
		if d == nil {
			return true
		}
		r = append(r, d)
		return true
	})
	return r
}

func (fd *ForestModelDetections) find(id string) *ForestModelDetection {

	item := fd.items.Get(id)
	if item != nil {
		a := item.Value()
		if a != nil {
			return a
		}
	}
	return nil
}

func (fd *ForestModelDetections) AddOrUpdate(id string, stamp common.Stamp, bound, score float64) {

	fd.mu.Lock()
	defer fd.mu.Unlock()

	d := fd.find(id)
	if d == nil {
		d = NewForestModelDetection(id, stamp, bound, score)
	} else {
		if d.start > stamp {
			d.start = stamp
		}
		if d.end < stamp {
			d.end = stamp
		}
		if d.start > d.end {
			old := d.end
			d.end = d.start
			d.start = old
		}
	}
	fd.items.Set(id, d, ttlcache.DefaultTTL)
}

func (fd *ForestModelDetections) Size() int {
	return fd.items.Len()
}

func NewForestModelDetections(ttl time.Duration) *ForestModelDetections {

	fd := &ForestModelDetections{}
	fd.items = ttlcache.New(
		ttlcache.WithTTL[string, *ForestModelDetection](ttl),
	)
	return fd
}

// ForestModel

func (fm *ForestModel) Name() string {
	return "ForestModel"
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

func (fm *ForestModel) Train(data common.DataSourceData) error {

	when := time.Now()
	name := fm.Name()

	fm.logger.Info("%s: Training...", name)

	filter := fm.findHashes(data.Names())
	err := fm.data.train(data, filter)
	if err != nil {
		fm.logger.Error("%s: Training failed in %s error %s", name, time.Since(when), err)
		return err
	}
	fm.logger.Info("%s: Training successful in %s", name, time.Since(when))
	return nil
}

func (fm *ForestModel) Detect(data common.DataSourceData, after common.ModelAfterDetect) error {

	when := time.Now()
	name := fm.Name()

	fm.logger.Info("%s: Detecting...", name)

	filter := fm.findHashes(data.Names())
	err := fm.data.detect(data, filter, fm.detections)
	if err != nil {
		fm.logger.Error("%s: Detecting failed in %s error %s", name, time.Since(when), err)
		return err
	}

	if after != nil {
		go after(fm.detections.Anomalies())
	}

	fm.logger.Info("%s: Detecting finished found=%d in %s", name, fm.detections.Size(), time.Since(when))
	return nil
}

func NewForestModel(options ForestModelOptions, observability *common.Observability) *ForestModel {

	return &ForestModel{
		options:       options,
		observability: observability,
		logger:        observability.Logs(),
		data:          NewForestModelData(options.Path, common.DefaultTTL(options.DataTTL, time.Hour), options.Concurrency),
		detections:    NewForestModelDetections(common.DefaultTTL(options.AnomalyTTL, 5*time.Minute)),
	}
}
