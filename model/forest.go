package model

import (
	"cmp"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
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
	Enabled     bool
	Concurrency int
	Filter      []string // filter by apps and hosts

	EngineTTL string

	DetectionTTL        string
	DetectionMass       float64
	DetectionCategories string
	DetectionImpacts    string

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

type ForestModelApplicationFrame struct {
	hash    common.Hash
	frame   *forest.ApplicationFrame
	points  *forest.ApplicationFramePoints
	verdict *forest.ApplicationCaseVerdict
	data    common.DataSourceData
}

type ForestModelDetection struct {
	hash    common.Hash
	verdict common.ModelVerdict
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
	Ready() bool
}

type ForestModel struct {
	options       ForestModelOptions
	observability *common.Observability
	logger        sreCommon.Logger

	appCases *forest.ApplicationCases

	engines    *ttlcache.Cache[common.Hash, ForestModelEngine]
	detections *ForestModelDetections
	categories []common.CaseCategory
	impacts    []common.CaseImpact
	trains     sync.Map

	// status
	detected atomic.Int64
}

const (
	ForestModelTreesNumber   = 100
	ForestModelSubsampleSize = 256
	ForestModelOutlierRatio  = 0.01
)

type ForestModelFileData struct {
	Data  [][]float64
	Times []common.Stamp
}

// ForestModelApplicationFrame

func (af *ForestModelApplicationFrame) begin() common.Stamp {
	if af.frame == nil {
		return common.Stamp(0)
	}
	return af.frame.Begin()
}

func (af *ForestModelApplicationFrame) end() common.Stamp {
	if af.frame == nil {
		return common.Stamp(0)
	}
	return af.frame.End()
}

func (af *ForestModelApplicationFrame) IsEmpty() bool {
	if af.frame == nil || af.points == nil {
		return true
	}
	return af.points.IsEmpty()
}

func (af *ForestModelApplicationFrame) Timestamp() time.Time {
	if af.frame == nil {
		return time.Now()
	}
	return common.StampToTime(af.frame.Stamp())
}

func (af *ForestModelApplicationFrame) Ident() string {

	if af.frame == nil || utils.IsEmpty(af.data) {
		return ""
	}

	names := af.data.Names()
	if names == nil {
		return ""
	}

	app := names.FindByHash(af.frame.Hash())
	if utils.IsEmpty(app) {
		return ""
	}
	return app
}

func (af *ForestModelApplicationFrame) Verdict() common.ModelVerdict {
	return af.verdict
}

func (af *ForestModelApplicationFrame) InRequests() common.ModelDataPoint {
	if af.points == nil {
		return nil
	}
	return af.points.InRequets()
}

func (af *ForestModelApplicationFrame) InThroughput() common.ModelDataPoint {
	if af.points == nil {
		return nil
	}
	return af.points.InThroughput()
}

func (af *ForestModelApplicationFrame) InLatency() common.ModelDataPoint {
	if af.points == nil {
		return nil
	}
	return af.points.InLatency()
}

func (af *ForestModelApplicationFrame) InErrors() common.ModelDataPoint {
	if af.points == nil {
		return nil
	}
	return af.points.InErrors()
}

func (af *ForestModelApplicationFrame) OutRequests() common.ModelDataPoint {
	if af.points == nil {
		return nil
	}
	return af.points.OutRequets()
}

func (af *ForestModelApplicationFrame) OutThroughput() common.ModelDataPoint {
	if af.points == nil {
		return nil
	}
	return af.points.OutThroughput()
}

func (af *ForestModelApplicationFrame) OutLatency() common.ModelDataPoint {
	if af.points == nil {
		return nil
	}
	return af.points.OutLatency()
}

func (af *ForestModelApplicationFrame) OutErrors() common.ModelDataPoint {
	if af.points == nil {
		return nil
	}
	return af.points.OutErrors()
}

func (af *ForestModelApplicationFrame) CPU() common.ModelDataPoint {
	if af.points == nil {
		return nil
	}
	return af.points.CPU()
}

func (af *ForestModelApplicationFrame) Memory() common.ModelDataPoint {
	if af.points == nil {
		return nil
	}
	return af.points.Memory()
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

func (fd *ForestModelDetection) score() int {
	if fd.verdict == nil {
		return -1
	}
	return fd.verdict.Score()
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

func (fd *ForestModelDetections) anomalies() []common.ModelAnomaly {

	fd.mu.Lock()
	defer fd.mu.Unlock()

	r := []common.ModelAnomaly{}
	/*fd.items.Range(func(item *ttlcache.Item[common.Hash, *ForestModelDetection]) bool {

		d := item.Value()
		if d == nil {
			return true
		}
		if d.verdict == nil {
			return true
		}

		if !(utils.Contains(categories, d.verdict.Category()) &&
			utils.Contains(impacts, d.verdict.Impact())) {
			return true
		}

		r = append(r, d)
		return true
	})*/
	return r
}

func (fd *ForestModelDetections) Items() map[common.Hash]*ForestModelDetection {

	fd.mu.Lock()
	defer fd.mu.Unlock()

	r := make(map[common.Hash]*ForestModelDetection)
	fd.items.Range(func(item *ttlcache.Item[common.Hash, *ForestModelDetection]) bool {

		d := item.Value()
		if d == nil {
			return true
		}
		r[item.Key()] = d
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
		if deps != nil {
			for h, child := range deps.Items() {
				d.deps.AddOrUpdate(h, child)
			}
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

func (fm *ForestModel) Enabled() bool {
	return fm.options.Enabled
}

func (fm *ForestModel) findHashes(names *common.Names, filter []string) []common.Hash {

	r := []common.Hash{}

	for _, name := range filter {

		name := strings.TrimSpace(name)
		if utils.IsEmpty(name) {
			continue
		}

		hashes := names.FindByRegex(name)
		if len(hashes) == 0 {
			continue
		}
		for _, h := range hashes {
			if utils.Contains(r, h) {
				continue
			}
			r = append(r, h)
		}
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
				frame := forest.NewApplicationFrame(stamp, as)
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

func (fm *ForestModel) train(data common.DataSourceData) ([]common.Hash, error) {

	hs := []common.Hash{}
	measurements := data.Measurements()

	hashes := fm.findHashes(data.Names(), fm.options.Filter)
	appFrames := make(map[common.Hash]forest.ApplicationFrames)
	hostFrames := make(map[common.Hash]forest.HostFrames)

	fm.prepare(measurements, hashes, appFrames, hostFrames)

	length := len(appFrames) + len(hostFrames)
	if length == 0 {
		return hs, nil
	}

	gr := &errgroup.Group{}
	gr.SetLimit(fm.options.Concurrency)
	errs := make(chan error, length)

	appLimits := forest.ApplicationFrameLimits{
		From:  data.From(),
		First: data.First(),
		Last:  data.Last(),
	}

	// run apps engine
	for hash, frames := range appFrames {

		gr.Go(func() error {

			// check existence
			_, exists := fm.trains.LoadOrStore(hash, true)
			if exists {
				return nil
			}

			engine := forest.NewApplicationEngine(hash, &fm.options.ApplicationOptions, fm.observability)

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
			fm.trains.Delete(hash)
			if engine.Ready() {
				fm.engines.Set(hash, engine, ttlcache.DefaultTTL)
			}
			return nil
		})
	}

	// run hosts engine where apps are not found
	for hash, frames := range hostFrames {

		gr.Go(func() error {

			// check existence
			_, exists := fm.trains.LoadOrStore(hash, true)
			if exists {
				return nil
			}

			engine := forest.NewHostEngine()
			err := engine.Train(frames)
			if err != nil {
				errs <- err
			}
			fm.trains.Delete(hash)
			if engine.Ready() {
				fm.engines.Set(hash, engine, ttlcache.DefaultTTL)
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

	fm.engines.Range(func(item *ttlcache.Item[common.Hash, ForestModelEngine]) bool {

		if item == nil {
			return true
		}
		hs = append(hs, item.Key())
		return true
	})

	return hs, errors.Join(all...)
}

func (fm *ForestModel) Train(data common.DataSourceData) ([]common.Hash, error) {

	hs := []common.Hash{}
	if !fm.options.Enabled {
		return hs, nil
	}

	if utils.IsEmpty(data) {
		return hs, nil
	}

	when := time.Now()
	name := fm.Name()

	t1 := common.StampToTime(data.First())
	t2 := common.StampToTime(data.Last())

	fm.logger.Info("%s: Training from %v to %s ...", name, t1, t2)

	hs, err := fm.train(data)
	if err != nil {
		fm.logger.Error("%s: Training failed in %s error %s", name, time.Since(when), err)
		return hs, err
	}
	fm.logger.Info("%s: Training successful in %s", name, time.Since(when))
	return hs, nil
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
		if as.Hash() > 0 {
			hashes = append(hashes, as.Hash())
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

func (fm *ForestModel) eligibleDetection(d *ForestModelDetection) bool {

	if d == nil || d.verdict == nil {
		return false
	}
	category := d.verdict.Category()
	impact := d.verdict.Impact()
	return utils.Contains(fm.categories, category) && utils.Contains(fm.impacts, impact)
}

func (fm *ForestModel) setOneDetections(d *ForestModelDetection) {

	n, exists := fm.detections.AddOrUpdate(d.hash, d.begin, d.end, nil)
	if !exists && n != nil {
		n.verdict = d.verdict
	}
}

func (fm *ForestModel) setMassDetections(eligible, found []*ForestModelDetection) {

	hash := common.Hash(0)
	minStamp := common.Stamp(math.MaxUint64)
	maxStamp := common.Stamp(0)
	deps := common.NewDependencies()

	var first *ForestModelDetection

	for _, d := range eligible {

		h := d.hash
		mi := d.Begin()
		ma := d.End()

		if mi < minStamp {
			first = d
			hash = h
			minStamp = mi
		}
		if ma > maxStamp {
			maxStamp = ma
		}
	}

	if hash == 0 || first == nil {
		return
	}

	for _, d := range found {

		if d.hash == first.hash {
			continue
		}
		deps.AddOrUpdate(d.hash, nil)
	}

	d, exists := fm.detections.AddOrUpdate(hash, minStamp, maxStamp, deps)
	if !exists && d != nil && first != nil {
		d.verdict = first.verdict
	}
}

func (fm *ForestModel) setManyDetections(eligible, found []*ForestModelDetection, data common.DataSourceData) {

	measurements := data.Measurements()
	temp := make(map[common.Hash]*common.Dependencies)

	for _, d := range eligible {

		hash := d.hash
		min := d.begin
		max := d.end

		similars := fm.findSimilars(measurements, found, hash, []common.Stamp{min, max})
		deps := fm.findDependecies(measurements, temp, similars, []common.Stamp{min, max})

		n, exists := fm.detections.AddOrUpdate(hash, min, max, deps)
		if !exists && n != nil {
			n.verdict = d.verdict
		}
	}
}

func (fm *ForestModel) detect(data common.DataSourceData, onFrame common.ModelOnFrame) error {

	measurements := data.Measurements()
	names := data.Names()

	hashes := fm.findHashes(data.Names(), fm.options.Filter)
	appFrames := make(map[common.Hash]forest.ApplicationFrames)
	hostFrames := make(map[common.Hash]forest.HostFrames)

	fm.prepare(measurements, hashes, appFrames, hostFrames)

	length := len(appFrames) + len(hostFrames)
	if length == 0 {
		return nil
	}

	gr := &errgroup.Group{}
	gr.SetLimit(fm.options.Concurrency)
	errs := make(chan error, length)
	verdictFrames := &sync.Map{}

	// run apps engine
	for hash, frames := range appFrames {

		gr.Go(func() error {

			name := names.FindByHash(hash)
			fm.logger.Debug("%s: Detecting %s (%d)", fm.Name(), name, hash)

			// check existence
			if _, ok := fm.trains.Load(hash); ok {
				fm.logger.Debug("%s: Training found %s (%d). Skipped", fm.Name(), name, hash)
				return nil
			}

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
				fm.logger.Debug("%s: Engine not found %s (%d). Initializing...", fm.Name(), name, hash)
				engine = forest.NewApplicationEngine(hash, &fm.options.ApplicationOptions, fm.observability)
				err := engine.LoadForest()
				if err != nil {
					errs <- err
					return nil
				}
				if engine.Ready() {
					fm.engines.Set(hash, engine, ttlcache.DefaultTTL)
				}
			}

			appFrame := engine.Consolidate(frames)
			if appFrame == nil {
				return nil
			}

			if !engine.Ready() {
				fm.logger.Debug("%s: %s not ready %s (%d)", fm.Name(), engine.Name(), name, hash)
				return nil
			}

			stamp := appFrame.Stamp()
			if !engine.Exists(stamp) {
				fm.logger.Debug("%s: %s doesn't exist %s (%d) on %v", fm.Name(), engine.Name(), name, hash, common.StampToTime(stamp))
				return nil
			}

			appPoints, appVerdict := engine.Diagnose(appFrame, fm.appCases)
			if appPoints == nil {
				return nil
			}

			verdictFrame := &ForestModelApplicationFrame{
				hash:    hash,
				frame:   appFrame,
				verdict: appVerdict,
				points:  appPoints,
				data:    data,
			}

			if appVerdict != nil {
				verdictFrames.Store(hash, verdictFrame)
			}

			if onFrame != nil {
				go onFrame(verdictFrame)
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
	eligible := []*ForestModelDetection{}

	verdictFrames.Range(func(key, value any) bool {

		var detection *ForestModelDetection

		hash := key.(common.Hash)
		appFrame, ok := value.(*ForestModelApplicationFrame)
		if ok {
			detection = NewForestModelDetection(hash, appFrame.begin(), appFrame.end(), nil)
			detection.verdict = appFrame.verdict
			found = append(found, detection)
			if fm.eligibleDetection(detection) {
				eligible = append(eligible, detection)
			}
		}
		return true
	})

	l1 := len(eligible)
	l2 := len(found)

	// if not eligible => exit
	if l1 == 0 {
		return err
	}

	// if just one
	if l1 == 1 {
		fm.setOneDetections(eligible[0])
		return err
	}

	// sort by score
	slices.SortFunc(eligible, func(a *ForestModelDetection, b *ForestModelDetection) int {
		return cmp.Compare(b.score(), a.score())
	})

	p := float64((l1 * 100) / l2)

	// if mass triggered => only mass detection
	if p >= fm.options.DetectionMass {
		fm.setMassDetections(eligible, found)
		return err
	}

	// if not mass triggered => per hash detection
	fm.setManyDetections(eligible, found, data)
	return err
}

func (fm *ForestModel) Detect(data common.DataSourceData, onFrame common.ModelOnFrame) ([]common.ModelAnomaly, error) {

	als := []common.ModelAnomaly{}
	if !fm.options.Enabled {
		return als, nil
	}

	if utils.IsEmpty(data) {
		return als, nil
	}

	when := time.Now()
	name := fm.Name()

	t1 := common.StampToTime(data.First())
	t2 := common.StampToTime(data.Last())

	fm.logger.Info("%s: Detecting from %v to %s ...", name, t1, t2)

	err := fm.detect(data, onFrame)
	if err != nil {
		fm.logger.Error("%s: Detecting failed in %s error %s", name, time.Since(when), err)
		return als, err
	}

	ds := fm.detections.Items()
	if len(ds) > 0 {

		names := data.Names()
		for k, v := range ds {

			n := names.FindByHash(k)
			verdict := "none"
			category := "none"
			impact := "none"
			if !utils.IsEmpty(v.verdict) {
				verdict = v.verdict.Name()
				category = common.CaseCategoryToString(v.verdict.Category())
				impact = common.CaseImpactToString(v.verdict.Impact())
			}
			deps := ""
			if v.deps != nil {
				deps = fmt.Sprintf(" (deps %d)", len(v.deps.Items()))
			}
			begin := common.StampToTime(v.begin)
			fm.logger.Info("%s: Found detection on %s %s%s: %s (%s / %s)", name, begin, n, deps, verdict, category, impact)
		}
	}
	fm.detected.Store(int64(len(ds)))
	fm.logger.Info("%s: Detecting finished found=%d in %s", name, fm.detections.Size(), time.Since(when))
	return fm.detections.anomalies(), nil
}

func (fm *ForestModel) Status() *common.ModelStatus {

	return &common.ModelStatus{
		Trained:  fm.engines.Len(),
		Detected: int(fm.detected.Load()),
	}
}

func NewForestModel(options ForestModelOptions, observability *common.Observability) *ForestModel {

	categories := common.StringToCaseCategories(options.DetectionCategories)
	if len(categories) == 0 {
		categories = common.AllCaseCategories()
	}

	impacts := common.StringToCaseImpacts(options.DetectionImpacts)
	if len(impacts) == 0 {
		impacts = common.AllCaseImpacts()
	}

	return &ForestModel{
		options:       options,
		observability: observability,
		logger:        observability.Logs(),
		appCases:      forest.NewApplicationCases(),
		engines: ttlcache.New(
			ttlcache.WithTTL[common.Hash, ForestModelEngine](common.DefaultTTL(options.EngineTTL, 1*time.Hour)),
		),
		detections: NewForestModelDetections(common.DefaultTTL(options.DetectionTTL, 5*time.Minute)),
		categories: categories,
		impacts:    impacts,
	}
}
