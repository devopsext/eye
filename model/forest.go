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

type ForestModelVerdict interface {
	Name() string
	Description() string
	RootCause() string
	Category() common.CaseCategory
	Impact() common.CaseImpact
}

type ForestModelDetection struct {
	hash    common.Hash
	verdict ForestModelVerdict
	begin   common.Stamp
	end     common.Stamp
	deps    *common.Dependencies
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

	appCases *forest.ApplicationCases

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

// ForestModelDetection

func (fd *ForestModelDetection) ID() string {
	return fmt.Sprintf("%d", fd.hash)
}

func (fd *ForestModelDetection) Begin() common.Stamp {
	return fd.begin
}

func (fd *ForestModelDetection) End() common.Stamp {
	return fd.begin
}

func NewForestModelDetection(hash common.Hash, min, max common.Stamp, deps *common.Dependencies) *ForestModelDetection {
	return &ForestModelDetection{
		hash:  hash,
		begin: min,
		end:   max,
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
		if d != nil && d.Begin() <= min {
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

func (fd *ForestModelDetections) AddOrUpdate(hash common.Hash, min, max common.Stamp, deps *common.Dependencies) (*ForestModelDetection, bool) {

	fd.mu.Lock()
	defer fd.mu.Unlock()

	exists := true

	d := fd.unsafeFind(hash, min, max)
	if d == nil {
		d = NewForestModelDetection(hash, min, max, deps)
		exists = false
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
	return d, exists
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

			err := engine.LoadData()
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

func (fm *ForestModel) setMassDetections(found []*ForestModelDetection) {

	hash := common.Hash(0)
	min := common.Stamp(math.MaxUint64)
	max := common.Stamp(0)
	deps := common.NewDependencies()

	var first *ForestModelDetection

	for _, d := range found {

		h := d.hash
		mi := d.Begin()
		ma := d.End()

		if mi < min {
			first = d
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
	d, exists := fm.detections.AddOrUpdate(hash, min, max, deps)
	if !exists && d != nil && first != nil {
		d.verdict = first.verdict
	}
}

func (fm *ForestModel) findDependecies(measurements *common.Measurements,
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

func (fm *ForestModel) findSimilar(measurements *common.Measurements, hash common.Hash, stamp common.Stamp) []common.Hash {

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

func (fm *ForestModel) findSimilars(measurements *common.Measurements, found []*ForestModelDetection, hash common.Hash, stamps []common.Stamp) []common.Hash {

	r := []common.Hash{}

	for _, stamp := range stamps {

		similars := fm.findSimilar(measurements, hash, stamp)
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

func (fm *ForestModel) setHashDetections(found []*ForestModelDetection, data common.DataSourceData) {

	measurements := data.Measurements()
	names := data.Names()
	temp := make(map[common.Hash]*common.Dependencies)

	for _, d := range found {

		hash := d.hash
		min := d.begin
		max := d.end

		hn := names.FindByHash(hash)
		similars := fm.findSimilars(measurements, found, hash, []common.Stamp{min, max})

		verdict := "none"
		category := "none"
		impact := "none"
		if !utils.IsEmpty(d.verdict) {
			verdict = d.verdict.Name()
			category = common.CaseCategoryToString(d.verdict.Category())
			impact = common.CaseImpactToString(d.verdict.Impact())
		}
		fm.logger.Debug("%s: Found detection %s (similar %d): %s (%s / %s)", fm.Name(), hn, len(similars), verdict, category, impact)

		deps := fm.findDependecies(measurements, temp, similars, []common.Stamp{min, max})
		n, exists := fm.detections.AddOrUpdate(hash, min, max, deps)
		if !exists && n != nil {
			n.verdict = d.verdict
		}
	}
}

func (fm *ForestModel) detect(data common.DataSourceData) error {

	measurements := data.Measurements()

	hashes := fm.findHashes(data.Names(), fm.options.Filter)
	appFrames := make(map[common.Hash]forest.ApplicationFrames)
	hostFrames := make(map[common.Hash]forest.HostFrames)

	fm.prepare(measurements, hashes, appFrames, hostFrames)

	gr := &errgroup.Group{}
	gr.SetLimit(fm.options.Concurrency)

	length := len(appFrames) + len(hostFrames)
	errs := make(chan error, len(appFrames)+len(hostFrames))

	verdicts := &sync.Map{}

	// run apps engine
	for hash, frames := range appFrames {

		gr.Go(func() error {

			var engine *forest.ApplicationEngine

			item := fm.engines.Get(hash)
			if item != nil {
				val := item.Value()
				if !utils.IsEmpty(val) {
					e, ok := val.(*forest.ApplicationEngine)
					if !ok {
						return nil
					}
					engine = e
				}
			}

			if engine == nil {
				engine = forest.NewApplicationEngine(hash, &fm.options.ApplicationOptions)
				err := engine.LoadForest()
				if err != nil {
					errs <- err
					return nil
				}
				if engine.Ready() {
					fm.engines.Set(hash, engine, ttlcache.DefaultTTL)
				}
			}
			verdict := engine.Diagnose(frames, fm.appCases)
			if verdict != nil {
				verdicts.Store(hash, verdict)
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

	found := []*ForestModelDetection{}
	verdicts.Range(func(key, value any) bool {

		var detection *ForestModelDetection

		hash := key.(common.Hash)
		appVerdict, ok := value.(*forest.ApplicationCaseVerdict)
		if ok {
			detection = NewForestModelDetection(hash, appVerdict.Begin(), appVerdict.End(), nil)
			detection.verdict = appVerdict
		}
		if detection != nil {
			found = append(found, detection)
		}
		return true
	})

	l1 := len(found)
	l2 := length

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
	if p >= fm.options.DetectionFalse {
		return err
	}

	// if mass triggered => only mass detection
	if p >= fm.options.DetectionMass {
		fm.setMassDetections(found)
		return err
	}

	// if not mass triggered => per hash detection
	fm.setHashDetections(found, data)
	return err
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

func NewForestModel(options ForestModelOptions, observability *common.Observability) *ForestModel {

	return &ForestModel{
		options:       options,
		observability: observability,
		logger:        observability.Logs(),
		appCases:      forest.NewApplicationCases(),
		engines: ttlcache.New(
			ttlcache.WithTTL[common.Hash, ForestModelEngine](common.DefaultTTL(options.EngineTTL, 1*time.Hour)),
		),
		detections: NewForestModelDetections(common.DefaultTTL(options.DetectionTTL, 5*time.Minute)),
	}
}
