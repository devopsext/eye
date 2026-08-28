package model

import (
	"bufio"
	"encoding/gob"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/devopsext/eye/common"
	sreCommon "github.com/devopsext/sre/common"
	"github.com/devopsext/utils"
	iforest "github.com/e-XpertSolutions/go-iforest/v2/iforest"
	"github.com/jellydator/ttlcache/v3"
	"golang.org/x/sync/errgroup"
)

type ForestModelOptions struct {
	Path           string
	Concurrency    int
	Filter         []string // filter by apps and hosts
	DataTTL        string
	DetectionTTL   string
	DetectionFalse float64
	DetectionMass  float64
}

type ForestModelData struct {
	logger      sreCommon.Logger
	name        string
	path        string
	concurrency int
	mass        float64
	false       float64
	items       *ttlcache.Cache[common.Hash, *iforest.Forest]
}

type ForestModelDetection struct {
	hash  common.Hash
	begin common.Stamp
	end   common.Stamp
	deps  *common.Dependencies
}

type ForestModelDetections struct {
	mu    sync.Mutex
	items *ttlcache.Cache[common.Hash, *ForestModelDetection]
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

/*

good case: app incoming normal -> app outgoing normal
- app incoming traffic normal (rps, bps)
- app incoming latency normal
- app incoming errors normal
- app saturation normal
- host saturation normal
- app outgoing traffic normal
- app outgoing latency normal
- app outgoing errors normal

good case: app incoming load -> app outgoing load
- app incoming traffic grows (rps, bps)
- app incoming latency normal
- app incoming errors normal
- app saturation normal
- host saturation normal
- app outgoing traffic grows
- app outgoing latency normal
- app outgoing errors normal

good case: app incoming drop -> app outgoing drop
- app incoming traffic drop (rps, bps)
- app incoming latency normal
- app incoming errors normal
- app saturation normal
- host saturation normal
- app outgoing traffic normal
- app outgoing latency normal
- app outgoing errors normal

bad case: app incoming load -> app outgoing drop
reason: bottleneck in app or resources app limitations
- app incoming traffic grows (rps, bps)
- app incoming latency grows
- app incoming errors grows
- app saturation grows
- host saturation normal
- app outgoing traffic drops
- app outgoing latency drops
- app outgoing errors drops

bad case: app incoming load -> app outgoing drop
reason: bottleneck on host, no resources by app load
- app incoming traffic grows (rps, bps)
- app incoming latency grows
- app incoming errors grows
- app saturation grows
- host saturation grows
- app outgoing traffic drops
- app outgoing latency drops
- app outgoing errors drops

bad case: app incoming load -> app outgoing load
reason: bottleneck in backend
- app incoming traffic grows (rps, bps)
- app incoming latency grows
- app incoming errors grows
- app saturation normal
- host saturation normal
- app outgoing traffic drops
- app outgoing latency grows
- app outgoing errors grows

case: app incoming drop -> app outgoing drop
reason: no traffic from client
- app incoming traffic drops (rps, bps)
- app incoming latency drops
- app incoming errors drops
- app saturation drops
- host saturation drops
- app outgoing traffic drops
- app outgoing latency drops
- app outgoing errors drops

case: app incoming normal -> app outgoing drop
reason: bottleneck on host, no resources by different app load
- app incoming traffic normal (rps, bps)
- app incoming latency grows
- app incoming errors grows
- app saturation normal
- host saturation grows
- outgoing traffic drops
- outgoing latency drops
- outgoing errors drops

*/

func (fd *ForestModelData) getApplicationSignalData(stamp common.Stamp, signal *common.ApplicationSignal) []float64 {

	//frontends := signal.Frontends()

	//signal.IncomingTraffic.Value()

	inTraffic := 0.0
	inTrafficKind := 0.0
	inErrors := 0.0 //signal.IncomingErrors.Value()
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

func (fd *ForestModelData) getHostSignalData(stamp common.Stamp, signal *common.HostSignal) []float64 {

	cpuSaturation := 0.0
	memorySaturation := 0.0

	data := []float64{
		cpuSaturation, memorySaturation,
	}

	return data
}

func (fd *ForestModelData) getSignalData(stamp common.Stamp, signal common.Signal) []float64 {

	var r []float64

	as := signal.AsApplicationSignal()
	if as != nil {
		return fd.getApplicationSignalData(stamp, as)
	}

	hs := signal.AsHostSignal()
	if hs != nil {
		return fd.getHostSignalData(stamp, hs)
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

	l := len(hashes)

	for hash, signals := range measurements.GetItems() {

		for stamp, signal := range signals {

			if l > 0 && !signal.ContainsAny(hashes) {
				continue
			}

			t := common.StampToTime(stamp)
			timeSin, timeCos, daySin, dayCos := fd.getStampFeatures(t)

			if fd.stampIsExcluded(t, []timeWindow{}) {
				continue
			}

			data := fd.getSignalData(stamp, signal)

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

func (fd *ForestModelData) findHashes(names *common.Names, filter []string) []common.Hash {

	r := []common.Hash{}

	for _, name := range filter {

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

func (fd *ForestModelData) train(dsd common.DataSourceData, filter []string) error {

	measurements := dsd.Measurements()
	from := dsd.From()
	first := dsd.First()
	last := dsd.Last()

	hashes := fd.findHashes(dsd.Names(), filter)
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

func (fd *ForestModelData) setMassDetections(found []*ForestModelDetection, detections *ForestModelDetections) {

	hash := common.Hash(0)
	min := common.Stamp(math.MaxUint64)
	max := common.Stamp(0)
	deps := common.NewDependencies()

	for _, d := range found {

		h := d.hash
		mi := d.begin
		ma := d.end

		if mi < min {
			hash = h
			min = mi
		}
		if ma > max {
			max = ma
		}
		deps.AddOrUpdate(h, nil)
	}

	if hash == 0 {
		return
	}
	detections.AddOrUpdate(hash, min, max, deps)
}

func (fd *ForestModelData) findSimilar(measurements *common.Measurements, hash common.Hash, stamp common.Stamp) []common.Hash {

	r := []common.Hash{}
	hashes := []common.Hash{}

	// find signal for hash
	signal := measurements.FindSignal(stamp, hash)
	if utils.IsEmpty(signal) {
		return r
	}

	// gather all similar hashes, apps mostly
	as := signal.AsApplicationSignal()
	if as != nil {
		if as.Application() > 0 {
			hashes = append(hashes, as.Application())
		}
	}

	hs := signal.AsHostSignal()
	if hs != nil {
		if hs.Hash() > 0 {
			hashes = append(hashes, hs.Hash())
		}
	}

	if len(hashes) == 0 {
		return r
	}

	// find similar signals based on similar hashes
	signals := measurements.FindSignals(stamp, hashes)

	for _, signal := range signals {
		r = append(r, signal.Hash())
	}
	return r
}

func (fd *ForestModelData) findSimilars(measurements *common.Measurements, found []*ForestModelDetection, hash common.Hash, stamps []common.Stamp) []common.Hash {

	r := []common.Hash{}

	for _, stamp := range stamps {

		similars := fd.findSimilar(measurements, hash, stamp)
		for _, h := range similars {

			if utils.Contains(r, h) {
				continue
			}

			// check if it's in found
			exists := false
			for _, f := range found {
				if f == nil {
					continue
				}
				if f.hash == h {
					exists = true
					break
				}
			}

			// skip if it's not in found
			if !exists {
				continue
			}

			r = append(r, h)
		}
	}
	return r
}

func (fd *ForestModelData) findDependecies(measurements *common.Measurements,
	found map[common.Hash]*common.Dependencies,
	hashes []common.Hash, stamps []common.Stamp) *common.Dependencies {

	if len(hashes) == 0 {
		return nil
	}

	r := common.NewDependencies()

	required := []common.Hash{}
	rest := make(map[common.Hash]*common.Dependencies)

	// find out which are already exists
	for _, h := range hashes {
		ds, ok := found[h]
		if ok {
			rest[h] = ds
			continue
		}
		required = append(required, h)
	}

	// find only required dependecies
	if len(required) > 0 {
		for _, stamp := range stamps {

			deps := measurements.Dependencies(stamp, required)
			if deps == nil {
				continue
			}

			for h, ds := range deps.Items() {
				found[h] = ds
				r.AddOrUpdate(h, ds)
			}
		}
	}

	// add the rest
	for h, ds := range rest {
		r.AddOrUpdate(h, ds)
	}

	return r
}

func (fd *ForestModelData) setHashDetections(found []*ForestModelDetection, detections *ForestModelDetections, dsd common.DataSourceData) {

	measurements := dsd.Measurements()
	names := dsd.Names()
	temp := make(map[common.Hash]*common.Dependencies)

	for _, d := range found {

		hash := d.hash
		min := d.begin
		max := d.end

		t1 := common.StampToTime(min)
		t2 := common.StampToTime(max)

		hn := names.FindByHash(hash)
		similars := fd.findSimilars(measurements, found, hash, []common.Stamp{min, max})

		fd.logger.Debug("%s: Found detection %s (similar %d) duration %s...", fd.name, hn, len(similars), t2.Sub(t1))

		deps := fd.findDependecies(measurements, temp, similars, []common.Stamp{min, max})
		detections.AddOrUpdate(hash, min, max, deps)
	}
}

func (fd *ForestModelData) detect(dsd common.DataSourceData, detections *ForestModelDetections, filter []string) error {

	measurements := dsd.Measurements()

	hashes := fd.findHashes(dsd.Names(), filter)
	data, times := fd.prepare(measurements, hashes)
	if len(data) == 0 {
		return nil
	}

	gr := &errgroup.Group{}
	gr.SetLimit(fd.concurrency)

	errs := make(chan error, len(data))

	found := &sync.Map{}
	var size atomic.Int32

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

			stamps := []common.Stamp{}
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
				stamps = append(stamps, times[h][i])
			}
			if len(stamps) > 0 {
				found.Store(h, stamps)
				size.Add(1)
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
	err := errors.Join(all...)

	l1 := size.Load()
	l2 := int32(len(data))

	// if nothing happened => exit
	if l1 == 0 {
		return err
	}

	// if all triggered => false positive
	if l1 == l2 {
		return err
	}

	p := float64((l1 * 100) / l2)

	// if false triggered => false positive
	if p >= fd.false {
		return err
	}

	temp := []*ForestModelDetection{}
	found.Range(func(key, value any) bool {

		hash := key.(common.Hash)
		stamps := value.([]common.Stamp)
		if len(stamps) == 0 {
			return true
		}
		slices.Sort(stamps)
		min := stamps[0]
		max := stamps[len(stamps)-1]
		temp = append(temp, NewForestModelDetection(hash, min, max, nil))
		return true
	})

	// if mass triggered => only mass detection
	if p >= fd.mass {
		fd.setMassDetections(temp, detections)
		return err
	}

	// if not mass triggered => per hash detection
	fd.setHashDetections(temp, detections, dsd)
	return err
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

func NewForestModelData(logger sreCommon.Logger, name, path string, ttl time.Duration, concurrency int, mass, false float64) *ForestModelData {

	fd := &ForestModelData{
		logger:      logger,
		name:        name,
		path:        path,
		concurrency: concurrency,
		mass:        mass,
		false:       false,
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
	return fmt.Sprintf("%d", fd.hash)
}

func (fd *ForestModelDetection) Begin() common.Stamp {
	return fd.begin
}

func (fd *ForestModelDetection) End() common.Stamp {
	return fd.end
}

func NewForestModelDetection(hash common.Hash, begin, end common.Stamp, deps *common.Dependencies) *ForestModelDetection {
	return &ForestModelDetection{
		hash:  hash,
		begin: begin,
		end:   end,
		deps:  deps,
	}
}

// ForestModelDetections

func (fd *ForestModelDetections) Anomalies() []common.Anomaly {

	fd.mu.Lock()
	defer fd.mu.Unlock()

	r := []common.Anomaly{}
	fd.items.Range(func(item *ttlcache.Item[common.Hash, *ForestModelDetection]) bool {

		d := item.Value()
		if d == nil {
			return true
		}
		r = append(r, d)
		return true
	})
	return r
}

func (fd *ForestModelDetections) unsafeFind(hash common.Hash, min, max common.Stamp) *ForestModelDetection {

	item := fd.items.Get(hash)
	if item != nil {
		d := item.Value()
		if d != nil && d.begin <= min {
			return d
		}
	}

	var found *ForestModelDetection

	fd.items.Range(func(item *ttlcache.Item[common.Hash, *ForestModelDetection]) bool {

		d := item.Value()
		if d == nil {
			return true
		}

		if d.deps.Contains(hash) {
			found = d
			return false
		}
		return true
	})

	return found
}

func (fd *ForestModelDetections) Find(hash common.Hash, min, max common.Stamp) *ForestModelDetection {

	fd.mu.Lock()
	defer fd.mu.Unlock()

	return fd.unsafeFind(hash, min, max)
}

func (fd *ForestModelDetections) AddOrUpdate(hash common.Hash, min, max common.Stamp, deps *common.Dependencies) {

	fd.mu.Lock()
	defer fd.mu.Unlock()

	d := fd.unsafeFind(hash, min, max)
	if d == nil {
		d = NewForestModelDetection(hash, min, max, deps)
	} else {
		if d.begin > min {
			d.begin = min
		}
		if d.end < max {
			d.end = max
		}
		if d.begin > d.end {
			old := d.end
			d.end = d.begin
			d.begin = old
		}
		for h, child := range deps.Items() {
			d.deps.AddOrUpdate(h, child)
		}
	}
	fd.items.Set(d.hash, d, ttlcache.DefaultTTL)
}

func (fd *ForestModelDetections) Size() int {
	return fd.items.Len()
}

func NewForestModelDetections(ttl time.Duration) *ForestModelDetections {

	fd := &ForestModelDetections{}
	fd.items = ttlcache.New(
		ttlcache.WithTTL[common.Hash, *ForestModelDetection](ttl),
	)
	return fd
}

// ForestModel

func (fm *ForestModel) Name() string {
	return "ForestModel"
}

func (fm *ForestModel) Train(data common.DataSourceData) error {

	when := time.Now()
	name := fm.Name()

	fm.logger.Info("%s: Training...", name)

	err := fm.data.train(data, fm.options.Filter)
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

	err := fm.data.detect(data, fm.detections, fm.options.Filter)
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

	logger := observability.Logs()

	model := &ForestModel{
		options:       options,
		observability: observability,
		logger:        logger,
		detections:    NewForestModelDetections(common.DefaultTTL(options.DetectionTTL, 5*time.Minute)),
	}

	model.data = NewForestModelData(logger, model.Name(), options.Path, common.DefaultTTL(options.DataTTL, time.Hour), options.Concurrency, options.DetectionMass, options.DetectionFalse)

	return model
}
