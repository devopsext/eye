package model

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/devopsext/eye/common"
	"github.com/devopsext/eye/forest"
	sreCommon "github.com/devopsext/sre/common"
	"github.com/devopsext/utils"
	iforest "github.com/e-XpertSolutions/go-iforest/v2/iforest"
	"github.com/jellydator/ttlcache/v3"
	"golang.org/x/sync/errgroup"
)

type ForestModelOptions struct {
	//Path           string
	Concurrency int
	Filter      []string // filter by apps and hosts

	EngineTTL string

	DetectionTTL   string
	DetectionFalse float64
	DetectionMass  float64

	ApplicationOptions forest.ApplicationEngineOptions
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

type ForestModelEngine interface {
	Name() string
}

type ForestModel struct {
	mu            sync.Mutex
	options       ForestModelOptions
	observability *common.Observability
	logger        sreCommon.Logger

	engines    *ttlcache.Cache[common.Hash, ForestModelEngine]
	detections *ForestModelDetections
}

const (
	ForestModelTreesNumber                = 100
	ForestModelSubsampleSize              = 256
	ForestModelOutlierRatio               = 0.01
	ForestModelApplicationMaxSlots        = 4
	ForestModelApplicationRangeMultiplier = 1.5
)

type ForestModelFileData struct {
	Data  [][]float64
	Times []common.Stamp
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

	/*measurements := dsd.Measurements()

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
	*/
	return nil
}

type ForestModelFileForest struct {
	Forest *iforest.Forest
}

func (fd *ForestModelData) loadForest(c *ttlcache.Cache[common.Hash, *iforest.Forest], key common.Hash) *ttlcache.Item[common.Hash, *iforest.Forest] {

	/*fpath := filepath.Join(fd.path, fmt.Sprintf("%d.data", key))

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
	return item*/
	return nil
}

func NewForestModelData(logger sreCommon.Logger, name string, cases *common.Cases,
	ttl time.Duration, concurrency int, mass, false float64) *ForestModelData {

	fd := &ForestModelData{
		logger:      logger,
		name:        name,
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

func (fd *ForestModelDetection) Cases() []common.Case {
	return []common.Case{}
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

func (fm *ForestModel) findHashes(names *common.Names, filter []string) []common.Hash {

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

type timeWindow struct {
	Start time.Time
	End   time.Time
}

func (fm *ForestModel) stampIsExcluded(stamp time.Time, exclusions []timeWindow) bool {

	for _, window := range exclusions {
		if (stamp.Equal(window.Start) || stamp.After(window.Start)) && (stamp.Equal(window.End) || stamp.Before(window.End)) {
			return true
		}
	}
	return false
}

func (fm *ForestModel) prepare(measurements *common.Measurements, filter []common.Hash,
	appFrames map[common.Hash]forest.ApplicationFrames, hostFrames map[common.Hash]forest.HostFrames) {

	filterExists := len(filter) > 0

	for hash, signals := range measurements.Items() {
		for stamp, signal := range signals {

			if filterExists && !signal.ContainsAny(filter) {
				continue
			}

			t := common.StampToTime(stamp)
			// exclude invalid window time frames
			if fm.stampIsExcluded(t, []timeWindow{}) {
				continue
			}

			as := signal.AsApplicationSignal()
			if as != nil {
				hs := measurements.FindHostSignal(stamp, as.Host())
				frame := forest.NewApplicationFrame(stamp, as, hs)
				if frame != nil && frame.Valid() {
					appFrames[hash] = append(appFrames[hash], frame)
				}
			}

			hs := signal.AsHostSignal()
			if hs != nil {
				frame := forest.NewHostFrame(stamp, hash, hs)
				if frame != nil && frame.Valid() {
					hostFrames[hash] = append(hostFrames[hash], frame)
				}
			}
		}
	}
}

func (fm *ForestModel) train(data common.DataSourceData) error {

	measurements := data.Measurements()

	hashes := fm.findHashes(data.Names(), fm.options.Filter)
	appFrames := make(map[common.Hash]forest.ApplicationFrames)
	hostFrames := make(map[common.Hash]forest.HostFrames)

	fm.prepare(measurements, hashes, appFrames, hostFrames)

	gr := &errgroup.Group{}
	gr.SetLimit(fm.options.Concurrency)
	errs := make(chan error, len(appFrames)+len(hostFrames))

	appLimits := forest.ApplicationFrameLimits{
		From:  data.From(),
		First: data.First(),
		Last:  data.Last(),
	}

	// run apps engine
	for hash, frames := range appFrames {

		gr.Go(func() error {

			engine := forest.NewApplicationEngine(hash, &fm.options.ApplicationOptions)
			err := engine.Load()
			if err != nil {
				errs <- err
				return nil
			}
			err = engine.Train(frames, appLimits)
			if err != nil {
				errs <- err
				return nil
			}
			err = engine.Save()
			if err != nil {
				errs <- err
				return nil
			}
			fm.engines.Set(hash, engine.Clone(), ttlcache.DefaultTTL)
			return nil
		})
	}

	// run hosts engine where apps are not found
	for _, frames := range hostFrames {

		gr.Go(func() error {

			engine := forest.NewHostEngine()
			err := engine.Train(frames)
			if err != nil {
				errs <- err
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

func (fm *ForestModel) Train(data common.DataSourceData) error {

	when := time.Now()
	name := fm.Name()

	fm.logger.Info("%s: Training...", name)

	err := fm.train(data)
	if err != nil {
		fm.logger.Error("%s: Training failed in %s error %s", name, time.Since(when), err)
		return err
	}
	fm.logger.Info("%s: Training successful in %s", name, time.Since(when))
	return nil
}

func (fm *ForestModel) detect(data common.DataSourceData) error {

	measurements := data.Measurements()

	hashes := fm.findHashes(data.Names(), fm.options.Filter)
	appFrames := make(map[common.Hash]forest.ApplicationFrames)
	hostFrames := make(map[common.Hash]forest.HostFrames)

	fm.prepare(measurements, hashes, appFrames, hostFrames)

	gr := &errgroup.Group{}
	gr.SetLimit(fm.options.Concurrency)
	errs := make(chan error, len(appFrames)+len(hostFrames))

	// run apps engine
	for hash, frames := range appFrames {

		gr.Go(func() error {

			item := fm.engines.Get(hash)
			if item == nil {
				return nil
			}

			var engine *forest.ApplicationEngine
			val := item.Value()
			if utils.IsEmpty(val) {
				engine = forest.LoadApplicationEngine(hash, &fm.options.ApplicationOptions)
			} else {
				e, ok := val.(*forest.ApplicationEngine)
				if !ok {
					return nil
				}
				engine = e
			}

			if engine == nil {
				return nil
			}
			engine.Diagnose(frames)
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

func (fm *ForestModel) Detect(data common.DataSourceData, after common.ModelAfterDetect) error {

	when := time.Now()
	name := fm.Name()

	fm.logger.Info("%s: Detecting...", name)

	err := fm.detect(data)
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

func NewForestModel(options ForestModelOptions, cases *common.Cases, observability *common.Observability) *ForestModel {

	return &ForestModel{
		options:       options,
		observability: observability,
		logger:        observability.Logs(),
		engines: ttlcache.New(
			ttlcache.WithTTL[common.Hash, ForestModelEngine](common.DefaultTTL(options.EngineTTL, 1*time.Hour)),
		),
		detections: NewForestModelDetections(common.DefaultTTL(options.DetectionTTL, 5*time.Minute)),
	}
}
