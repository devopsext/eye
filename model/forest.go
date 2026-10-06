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
	Enabled     bool
	Concurrency int
	Filter      []string // filter by apps and hosts

	EngineTTL string

	DetectionTTL  string
	DetectionMass float64

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

func (af *ForestModelApplicationFrame) Timestamp() time.Time {
	if af.frame == nil {
		return time.Now()
	}
	return common.StampToTime(af.frame.Stamp())
}

func (af *ForestModelApplicationFrame) Ident() string {

	if af.frame == nil {
		return ""
	}

	if utils.IsEmpty(af.data) {
		return ""
	}

	names := af.data.Names()
	if names == nil {
		return ""
	}

	app := names.FindByHash(af.frame.Application())
	if utils.IsEmpty(app) {
		return ""
	}
	host := names.FindByHash(af.frame.Host())

	if utils.IsEmpty(host) {
		return app
	}

	return fmt.Sprintf("%s/%s", app, host)
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

func (fd *ForestModelDetections) Anomalies() []common.ModelAnomaly {

	fd.mu.Lock()
	defer fd.mu.Unlock()

	r := []common.ModelAnomaly{}
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
	return "Forest"
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

	length := len(appFrames) + len(hostFrames)
	if length == 0 {
		return nil
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

	if !fm.options.Enabled {
		return nil
	}

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

func (fm *ForestModel) setOneDetections(d *ForestModelDetection) {

	n, exists := fm.detections.AddOrUpdate(d.hash, d.begin, d.end, nil)
	if !exists && n != nil {
		n.verdict = d.verdict
	}
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

func (fm *ForestModel) setManyDetections(found []*ForestModelDetection, data common.DataSourceData) {

	measurements := data.Measurements()
	temp := make(map[common.Hash]*common.Dependencies)

	for _, d := range found {

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

			appFrame := engine.Consolidate(frames)
			appPoints, appVerdict := engine.Diagnose(appFrame, fm.appCases)

			verdictFrame := &ForestModelApplicationFrame{
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
	verdictFrames.Range(func(key, value any) bool {

		var detection *ForestModelDetection

		hash := key.(common.Hash)
		appFrame, ok := value.(*ForestModelApplicationFrame)
		if ok {
			detection = NewForestModelDetection(hash, appFrame.begin(), appFrame.end(), nil)
			detection.verdict = appFrame.verdict
		}
		if detection != nil {
			found = append(found, detection)
		}
		return true
	})

	l1 := len(found)
	l2 := length

	// if not found => exit
	if l1 == 0 {
		return err
	}

	// if just one
	if l1 == 1 {
		fm.setOneDetections(found[0])
		return err
	}

	p := float64((l1 * 100) / l2)

	// if mass triggered => only mass detection
	if p >= fm.options.DetectionMass {
		fm.setMassDetections(found)
		return err
	}

	// if not mass triggered => per hash detection
	fm.setManyDetections(found, data)
	return err
}

func (fm *ForestModel) Detect(data common.DataSourceData, onFrame common.ModelOnFrame, onAnomaly common.ModelOnAnomaly) error {

	if !fm.options.Enabled {
		return nil
	}

	when := time.Now()
	name := fm.Name()

	fm.logger.Info("%s: Detecting...", name)

	err := fm.detect(data, onFrame)
	if err != nil {
		fm.logger.Error("%s: Detecting failed in %s error %s", name, time.Since(when), err)
		return err
	}

	items := fm.detections.Items()
	if len(items) > 0 {

		names := data.Names()
		for k, v := range items {

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
			fm.logger.Debug("%s: Found detection %s%s: %s (%s / %s)", name, n, deps, verdict, category, impact)
		}

		anomalies := fm.detections.Anomalies()
		if onAnomaly != nil && len(anomalies) > 0 {
			go func() {
				for _, a := range anomalies {
					onAnomaly(a)
				}
			}()
		}
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
