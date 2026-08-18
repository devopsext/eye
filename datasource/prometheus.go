package datasource

import (
	"cmp"
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/devopsext/eye/common"
	sreCommon "github.com/devopsext/sre/common"
	toolsVendors "github.com/devopsext/tools/vendors"
	"github.com/devopsext/utils"
	"github.com/jinzhu/copier"
	"golang.org/x/sync/errgroup"
	"gopkg.in/yaml.v2"
)

type PrometheusResponseDataVector struct {
	Labels map[string]string `json:"metric"`
	Values [][]any           `json:"values"`
}

type PrometheusResponseData struct {
	ResultType string                          `json:"resultType"`
	Result     []*PrometheusResponseDataVector `json:"result"`
}

type PrometheusResponse struct {
	Status string                  `json:"status"`
	Data   *PrometheusResponseData `json:"data"`
}

type PrometheusSeriesValue struct {
	Hash  common.Hash
	Value float64
}

type PrometheusSeries = map[common.Stamp][]PrometheusSeriesValue

type PrometheusOptions struct {
	AppQuery                 string
	AppTolerance             int
	AppSignalCommonLabels    string
	AppSignalInTrafficQuery  string
	AppSignalInErrorsQuery   string
	AppSignalInLatencyQuery  string
	AppSignalOutTrafficQuery string
	AppSignalOutErrorsQuery  string
	AppSignalOutLatencyQuery string
	AppSignalSaturationQuery string
	AppSignalTolerance       int

	HostQuery                 string
	HostTolerance             int
	HostSignalCommonLabels    string
	HostSignalSaturationQuery string
	HostSignalTolerance       int

	Prometheus  toolsVendors.PrometheusOptions
	Span        string
	Window      string
	Schedule    string
	Concurrency int

	TimeFormat string
	StateYaml  string
}

type PrometheusData struct {
	names        *common.Names
	attributes   *common.Attributes
	hosts        *common.Hosts
	applications *common.Applications
	measurements *common.Measurements
	first        common.Stamp
	last         common.Stamp
}

type PrometheusStateItems = map[common.Stamp]common.Stamp

type PrometheusState struct {
	mu    sync.Mutex
	Items PrometheusStateItems
}

type Prometheus struct {
	mu            sync.Mutex
	options       PrometheusOptions
	observability *common.Observability
	logger        sreCommon.Logger
	onData        common.DataSourceOnData
}

// PrometheusData

func (pd *PrometheusData) Names() *common.Names {
	return pd.names
}

func (pd *PrometheusData) Attributes() *common.Attributes {
	return pd.attributes
}

func (pd *PrometheusData) Hosts() *common.Hosts {
	return pd.hosts
}

func (pd *PrometheusData) Applications() *common.Applications {
	return pd.applications
}

func (pd *PrometheusData) Measurements() *common.Measurements {
	return pd.measurements
}

func (pd *PrometheusData) First() common.Stamp {
	return pd.first
}

func (pd *PrometheusData) Last() common.Stamp {
	return pd.last
}

// PrometheusState

func (ps *PrometheusState) Load(path string) error {

	ps.mu.Lock()
	defer ps.mu.Unlock()

	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	return yaml.NewDecoder(f).Decode(ps.Items)
}

func (ps *PrometheusState) Save(path string) error {

	ps.mu.Lock()
	defer ps.mu.Unlock()

	dir := filepath.Dir(path)
	if !utils.DirExists(dir) {
		os.MkdirAll(dir, os.ModePerm)
	}

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	return yaml.NewEncoder(f).Encode(ps.Items)
}

func (ps *PrometheusState) AddOrUpdateTimes(t1 time.Time, t2 time.Time) {

	ps.mu.Lock()
	defer ps.mu.Unlock()

	s1 := common.TimeToStamp(t1)
	s2 := common.TimeToStamp(t2)
	ps.Items[s1] = s2
}

func (ps *PrometheusState) MinMax(items PrometheusStateItems) (common.Stamp, common.Stamp) {

	min := common.Stamp(math.MaxUint64)
	max := common.Stamp(0)

	for t1, t2 := range items {

		if t1 < min {
			min = t1
		}

		if t2 > max {
			max = t2
		}
	}
	return min, max
}

func (ps *PrometheusState) MinMaxTimes() (time.Time, time.Time) {

	ps.mu.Lock()
	defer ps.mu.Unlock()

	min, max := ps.MinMax(ps.Items)
	return common.StampToTime(min), common.StampToTime(max)
}

func (ps *PrometheusState) FindGaps(items PrometheusStateItems, st1, st2 common.Stamp) PrometheusStateItems {

	keys := slices.SortedFunc(maps.Keys(items), func(a, b common.Stamp) int {
		return cmp.Compare(a, b)
	})

	// find all gaps
	gaps := make(PrometheusStateItems)
	old := common.Stamp(0)
	for _, s1 := range keys {

		s2 := items[s1]

		if s1 > old && old > 0 {
			gaps[old] = s1
		}
		old = s2
	}

	gapsKeys := slices.SortedFunc(maps.Keys(gaps), func(a, b common.Stamp) int {
		return cmp.Compare(a, b)
	})

	found := make(PrometheusStateItems)
	for _, s1 := range gapsKeys {

		s2 := gaps[s1]

		if s1 >= st1 && s1 < st2 &&
			s2 > st1 && s2 <= st2 {
			found[s1] = s2
		}
	}
	return found
}

func (ps *PrometheusState) RebuildIntervals(from, to time.Time, window time.Duration) PrometheusStateItems {

	ps.mu.Lock()
	defer ps.mu.Unlock()

	min, max := ps.MinMax(ps.Items)

	r := make(PrometheusStateItems)
	t1 := from

	// add windows
	for t1.Unix() < to.Unix() {

		st1 := common.TimeToStamp(t1)

		t2 := t1.Add(window)
		st2 := common.TimeToStamp(t2)

		// all from left side
		if st1 < min && st2 <= min {
			r[st1] = st2
			t1 = common.StampToTime(st2)
			continue
		}

		// all from right side
		if st1 >= max && st2 > max {
			r[st1] = st2
			t1 = common.StampToTime(st2)
			continue
		}

		// part from left side
		if st1 < min && st2 > min {
			r[st1] = min
			t1 = common.StampToTime(min)
			continue
		}

		// part from right side
		if st1 < max && st2 > max {
			r[max] = st2
			t1 = common.StampToTime(st2)
			continue
		}
		t1 = common.StampToTime(st2)
	}

	// in case there are gaps
	gaps := ps.FindGaps(ps.Items, min, max)
	for s1, s2 := range gaps {
		r[s1] = s2
	}

	return r
}

func NewPrometheusState() *PrometheusState {
	return &PrometheusState{
		Items: make(PrometheusStateItems),
	}
}

// Prometheus

func (p *Prometheus) Name() string {
	return "Prometheus"
}

func (p *Prometheus) Schedule() string {
	return p.options.Schedule
}

func (p *Prometheus) setTimeFormat(args ...any) []any {

	arr := args

	for k, v := range arr {

		if v == nil {
			continue
		}

		t, ok := v.(time.Time)
		if ok {
			arr[k] = t.Format(p.options.TimeFormat)
			continue
		}

		d, ok := v.(time.Duration)
		if ok {
			arr[k] = common.DurationToString(d)
			continue
		}
	}
	return arr
}

func (p *Prometheus) info(msg any, args ...any) {
	gid := utils.GoRoutineID()
	args = p.setTimeFormat(args...)
	p.logger.Info(fmt.Sprintf("%v: [%d] %v", p.Name(), gid, msg), args...)
}

func (p *Prometheus) error(msg any, args ...any) {
	gid := utils.GoRoutineID()
	args = p.setTimeFormat(args...)
	p.logger.Error(fmt.Sprintf("%v: [%d] %v", p.Name(), gid, msg), args...)
}

func (p *Prometheus) debug(msg any, args ...any) {
	gid := utils.GoRoutineID()
	args = p.setTimeFormat(args...)
	p.logger.Debug(fmt.Sprintf("%v: [%d] %v", p.Name(), gid, msg), args...)
}

func (p *Prometheus) addUniqueKeys(keys []string, ma []map[string]string) []string {

	r := keys
	for _, v := range ma {
		mKeys := common.GetStringKeys(v)
		for _, v2 := range mKeys {
			if utils.Contains(keys, v2) {
				continue
			}
			r = append(r, v2)
		}
	}
	return r
}

func (p *Prometheus) sliceCommonLabels(parent, labels map[string]string) []map[string]string {

	r := []map[string]string{}
	pKeys := common.GetStringKeys(parent)
	lKeys := common.GetStringKeys(labels)
	if len(pKeys) == len(lKeys) {
		r = append(r, parent)
		return r
	}
	for k, v := range labels {
		if utils.Contains(pKeys, k) {
			continue
		}
		vv := common.RemoveEmptyStrings(strings.Split(v, ","))
		for _, v2 := range vv {
			lbs := make(map[string]string)
			maps.Copy(lbs, parent)
			lbs[k] = v2
			r2 := p.sliceCommonLabels(lbs, labels)
			r = append(r, r2...)
		}
		pKeys = p.addUniqueKeys(pKeys, r)
	}
	return r
}

func (p *Prometheus) loadData(q string, from, to time.Time) (*common.PrometheusResponseData, error) {

	opts := toolsVendors.PrometheusOptions{}
	copier.Copy(&opts, &p.options.Prometheus)

	opts.Query = q
	opts.From = strconv.Itoa(int(from.UTC().Unix()))
	opts.To = strconv.Itoa(int(to.UTC().Unix()))

	prom := toolsVendors.NewPrometheus(opts)
	if prom == nil {
		return nil, fmt.Errorf("prometheus cannot create client")
	}

	d, err := prom.Get()
	if err != nil {
		return nil, err
	}

	var res common.PrometheusResponse
	if err := json.Unmarshal(d, &res); err != nil {
		return nil, err
	}

	if res.Status != "success" {
		return nil, fmt.Errorf("prometheus got wrong status %s", res.Status)
	}

	if !utils.Contains([]string{"vector", "matrix"}, res.Data.ResultType) {
		return nil, fmt.Errorf("prometheus supports only vector and matrix data")
	}

	if (res.Data == nil) || (len(res.Data.Result) == 0) {
		return nil, nil
	}
	return res.Data, nil
}

func (p *Prometheus) getStampedValue(values []any) (bool, common.Stamp, float64) {

	if len(values) < 2 {
		return false, 0, 0
	}

	// 1. Extract Timestamp using a type switch
	var rawStamp int64
	switch v := values[0].(type) {
	case float64:
		rawStamp = int64(v)
	case int64:
		rawStamp = v
	case int:
		rawStamp = int64(v)
	case string:
		// Fallback in case it actually arrived as a string
		parsed, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return false, 0, 0
		}
		rawStamp = int64(parsed)
	default:
		return false, 0, 0
	}

	// 2. Validate digit length mathematically instead of using strings
	var stamp common.Stamp
	if rawStamp >= 1_000_000_000_000 && rawStamp <= 9_999_999_999_999 {
		// 13 digits: already in milliseconds
		stamp = common.Stamp(rawStamp)
	} else if rawStamp >= 1_000_000_000 && rawStamp <= 9_999_999_999 {
		// 10 digits: convert seconds to milliseconds
		stamp = common.Stamp(rawStamp * 1000)
	} else {
		// Handles the original stamp == 0 check and invalid lengths
		return false, 0, 0
	}

	// 3. Extract Value using a type switch
	var val float64
	switch v := values[1].(type) {
	case float64:
		val = v
	case int64:
		val = float64(v)
	case int:
		val = float64(v)
	case string:
		parsed, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return false, 0, 0
		}
		val = parsed
	default:
		return false, 0, 0
	}

	return true, stamp, val
}

func (p *Prometheus) loadHosts(data *PrometheusData, q string, from, to time.Time) (*common.Hosts, error) {

	promData, err := p.loadData(q, from, to)
	if err != nil {
		return nil, err
	}

	hosts := common.NewHosts()
	if promData == nil {
		return hosts, nil
	}

	for _, dr := range promData.Result {

		name := dr.Labels[common.HostName]
		if utils.IsEmpty(name) {
			continue
		}
		nameOn := dr.Labels[common.HostOn]
		findOn := !utils.IsEmpty(nameOn)

		for _, v := range dr.Values {

			found, stamp, _ := p.getStampedValue(v)
			if !found {
				continue
			}

			hostLbs := maps.Clone(dr.Labels)

			var on *common.Host
			if findOn {
				nameHash := data.names.AddOrUpdate(nameOn)
				on = hosts.Find(stamp, nameHash)
				if on == nil {

					lbs := maps.Clone(hostLbs)
					lbs[common.HostName] = nameOn
					delete(lbs, common.HostOn)

					// delete all key apart from name
					for k := range maps.Keys(hostLbs) {
						if k == common.HostName {
							continue
						}
						delete(hostLbs, k)
					}
					labelsHash := data.attributes.AddOrUpdate(lbs)

					on = common.NewHost(nameHash, labelsHash, nil)
					hosts.AddOrUpdate(stamp, on)
				}
			}

			namehash := data.names.AddOrUpdate(name)
			host := hosts.Find(stamp, namehash)
			if host == nil {
				labelsHash := data.attributes.AddOrUpdate(hostLbs)
				host = common.NewHost(namehash, labelsHash, on)
			}
			hosts.AddOrUpdate(stamp, host)
		}
	}
	return hosts, nil
}

func (p *Prometheus) gatherHostsByQuery(data *PrometheusData, query string, from, to time.Time) (*common.Hosts, error) {

	when := time.Now()

	p.debug("Hosts gathering (%s / %s)...", from, to)
	p.debug("Hosts gathering started => %s", query)

	hosts, err := p.loadHosts(data, query, from, to)
	if err != nil {
		return nil, err
	}

	ts, hs := hosts.Sizes()
	if ts > 0 {
		p.debug("Hosts gathering finished (%d over %d timeseries) in %s", hs, ts, time.Since(when))
	} else {
		p.debug("Hosts gathering finished (no data) in %s", time.Since(when))
	}
	return hosts, nil
}

func (p *Prometheus) gatherHostsBySpan(data *PrometheusData, query string, from, to time.Time, span time.Duration) error {

	tt := make(map[time.Time]time.Time)
	t1 := from

	for t1.Unix() < to.Unix() {
		t2 := t1.Add(span)
		tt[t1] = t2
		t1 = t2
	}

	keys := slices.SortedFunc(maps.Keys(tt), func(a, b time.Time) int {
		return a.Compare(b)
	})

	gr := &errgroup.Group{}
	gr.SetLimit(p.options.Concurrency)

	mp := &sync.Map{}

	for _, t1 := range keys {

		t2 := tt[t1]
		gr.Go(func() error {

			hsts, err := p.gatherHostsByQuery(data, query, t1, t2)
			if err != nil {
				return err
			}
			mp.Store(t1, hsts)
			return nil
		})
	}

	err := gr.Wait()
	if err != nil {
		return err
	}

	for t1 := range tt {

		v, ok := mp.Load(t1)
		if ok {
			d, ok := v.(*common.Hosts)
			if !ok {
				continue
			}
			data.hosts.Merge(d)
		}
	}
	return nil
}

func (p *Prometheus) loadApplications(data *PrometheusData, q string, from, to time.Time) (*common.Applications, error) {

	promData, err := p.loadData(q, from, to)
	if err != nil {
		return nil, err
	}

	apps := common.NewApplications()
	if promData == nil {
		return apps, nil
	}

	for _, dr := range promData.Result {

		name := dr.Labels[common.ApplicationName]
		if utils.IsEmpty(name) {
			continue
		}

		labelsHash := data.attributes.AddOrUpdate(dr.Labels)

		for _, v := range dr.Values {

			found, stamp, _ := p.getStampedValue(v)
			if !found {
				continue
			}

			nameHash := data.names.AddOrUpdate(name)
			app := apps.Find(stamp, nameHash)
			if app == nil {
				app = common.NewApplication(nameHash, labelsHash)
			}
			apps.AddOrUpdate(stamp, app)
		}
	}
	return apps, nil
}

func (p *Prometheus) gatherApplicationsByQuery(data *PrometheusData, query string, from, to time.Time) (*common.Applications, error) {

	when := time.Now()

	p.debug("Applications gathering (%s / %s)...", from, to)
	p.debug("Applications gathering started => %s", query)

	apps, err := p.loadApplications(data, query, from, to)
	if err != nil {
		return nil, err
	}

	ts, hs := apps.Sizes()
	if ts > 0 {
		p.debug("Applications gathering finished (%d over %d timeseries) in %s", hs, ts, time.Since(when))
	} else {
		p.debug("Applications gathering finished (no data) in %s", time.Since(when))
	}
	return apps, nil
}

func (p *Prometheus) gatherApplicationsBySpan(data *PrometheusData, query string, from, to time.Time, span time.Duration) error {

	tt := make(map[time.Time]time.Time)
	t1 := from

	for t1.Unix() < to.Unix() {
		t2 := t1.Add(span)
		tt[t1] = t2
		t1 = t2
	}

	keys := slices.SortedFunc(maps.Keys(tt), func(a, b time.Time) int {
		return a.Compare(b)
	})

	gr := &errgroup.Group{}
	gr.SetLimit(p.options.Concurrency)

	mp := &sync.Map{}

	for _, t1 := range keys {

		t2 := tt[t1]

		gr.Go(func() error {

			apps, err := p.gatherApplicationsByQuery(data, query, t1, t2)
			if err != nil {
				return err
			}
			mp.Store(t1, apps)
			return nil
		})
	}

	err := gr.Wait()
	if err != nil {
		return err
	}

	for t1 := range tt {

		v, ok := mp.Load(t1)
		if ok {
			d, ok := v.(*common.Applications)
			if !ok {
				continue
			}
			data.applications.Merge(d)
		}
	}
	return nil
}

func (p *Prometheus) loadModelData(data *PrometheusData, q string, from, to time.Time) (PrometheusSeries, error) {

	promData, err := p.loadData(q, from, to)
	if err != nil {
		return nil, err
	}

	modelData := PrometheusSeries{}
	if promData == nil {
		return modelData, nil
	}

	for _, dr := range promData.Result {

		for _, v := range dr.Values {

			found, stamp, value := p.getStampedValue(v)
			if !found {
				continue
			}

			v := PrometheusSeriesValue{
				Hash:  data.attributes.AddOrUpdate(dr.Labels),
				Value: value,
			}
			modelData[stamp] = append(modelData[stamp], v)
		}
	}
	return modelData, nil
}

func (p *Prometheus) gatherSignals(data *PrometheusData, queries map[common.SignalKind]string, from, to time.Time) (map[common.SignalKind]PrometheusSeries, error) {

	gr := &errgroup.Group{}
	gr.SetLimit(p.options.Concurrency)

	mp := &sync.Map{}

	// gather signals
	for k, q := range queries {

		gr.Go(func() error {

			data, err := p.loadModelData(data, q, from, to)
			if err != nil {
				return err
			}

			mp.Store(k, data)
			return nil
		})
	}

	err := gr.Wait()
	if err != nil {
		return nil, err
	}

	r := make(map[common.SignalKind]PrometheusSeries)

	for k := range queries {

		v, ok := mp.Load(k)
		if ok {
			d, ok := v.(PrometheusSeries)
			if ok {
				r[k] = d
			}
		}
	}
	return r, nil
}

func (p *Prometheus) prepareQuery(q string, labels map[string]string) string {

	s := q
	for k, v := range labels {
		s = strings.ReplaceAll(s, fmt.Sprintf(".%s", k), v)
	}
	return s
}

func (p *Prometheus) prepareQueries(queries map[common.SignalKind]string, labels map[string]string) map[common.SignalKind]string {

	r := make(map[common.SignalKind]string)

	for k, q := range queries {
		v := p.prepareQuery(q, labels)
		if !utils.IsEmpty(v) {
			r[k] = v
		}
	}
	return r
}

func (p *Prometheus) gatherSignalsByQueries(name string, data *PrometheusData,
	queries map[common.SignalKind]string, labels map[string]string, from, to time.Time) ([]map[common.SignalKind]PrometheusSeries, error) {

	when := time.Now()

	signals := []map[common.SignalKind]PrometheusSeries{}

	p.debug("Signals gathering %s (%s / %s)...", name, from, to)

	if len(labels) > 0 {

		gr := &errgroup.Group{}
		gr.SetLimit(p.options.Concurrency)

		mp := &sync.Map{}
		arr := p.sliceCommonLabels(nil, labels)

		for k, lbs := range arr {

			gr.Go(func() error {

				qrs := p.prepareQueries(queries, lbs)
				for k, q := range qrs {
					p.debug("Signals gathering %s %s started => %s", name, common.SignalKindToString(k), q)
				}

				r, err := p.gatherSignals(data, qrs, from, to)
				if err != nil {
					return err
				}

				infos := []string{}
				for k, v := range r {
					infos = append(infos, fmt.Sprintf("%s: %d", common.SignalKindToString(k), len(v)))
				}
				if len(infos) > 0 {
					p.debug("Signals gathering %s finished%s in %s", name, fmt.Sprintf(" (%s)", strings.Join(infos, ", ")), time.Since(when))
				} else {
					p.debug("Signals gathering %s finished (no data) in %s", name, time.Since(when))
				}

				mp.Store(k, r)
				return nil
			})
		}

		err := gr.Wait()
		if err != nil {
			return nil, err
		}

		for k := range arr {

			v, ok := mp.Load(k)
			if ok {
				r, ok := v.(map[common.SignalKind]PrometheusSeries)
				if ok {
					signals = append(signals, r)
				}
			}
		}

	} else {

		queries := p.prepareQueries(queries, nil)
		for k, q := range queries {
			p.debug("Signals gathering %s %s started => %s", name, common.SignalKindToString(k), q)
		}

		r, err := p.gatherSignals(data, queries, from, to)
		if err != nil {
			return nil, err
		}

		infos := []string{}
		for k, v := range r {
			infos = append(infos, fmt.Sprintf("%s: %d", common.SignalKindToString(k), len(v)))
		}
		if len(infos) > 0 {
			p.debug("Signals gathering %s finished%s in %s", name, fmt.Sprintf(" (%s)", strings.Join(infos, ", ")), time.Since(when))
		} else {
			p.debug("Signals gathering %s finished (no data) in %s", name, time.Since(when))
		}

		signals = append(signals, r)
	}
	return signals, nil
}

func (p *Prometheus) reduceSignals(arr []map[common.SignalKind]PrometheusSeries) map[common.SignalKind]PrometheusSeries {

	r := make(map[common.SignalKind]PrometheusSeries)

	for _, v := range arr {

		for k, d := range v {

			rk := r[k]
			if rk == nil {
				rk = d
			} else {

				for dk, dd := range d {

					rkt := rk[dk]
					if rkt == nil {
						rkt = dd
					} else {
						rkt = append(rkt, dd...)
					}
					rk[dk] = rkt
				}
			}
			r[k] = rk
		}
	}
	return r
}

func (p *Prometheus) gatherSignalsBySpan(name string, data *PrometheusData,
	queries map[common.SignalKind]string, labels map[string]string,
	from, to time.Time, span time.Duration) (map[common.SignalKind]PrometheusSeries, error) {

	qs := make(map[common.SignalKind]string)
	for i, v := range queries {
		if utils.IsEmpty(v) {
			continue
		}
		qs[i] = v
	}

	if len(qs) == 0 {
		return nil, nil
	}

	tt := make(map[time.Time]time.Time)
	t1 := from

	for t1.Unix() < to.Unix() {
		t2 := t1.Add(span)
		tt[t1] = t2
		t1 = t2
	}

	keys := slices.SortedFunc(maps.Keys(tt), func(a, b time.Time) int {
		return a.Compare(b)
	})

	gr := &errgroup.Group{}
	gr.SetLimit(p.options.Concurrency)

	mp := &sync.Map{}

	for _, t1 := range keys {

		t2 := tt[t1]

		gr.Go(func() error {

			sqd, err := p.gatherSignalsByQueries(name, data, qs, labels, t1, t2)
			if err != nil {
				return err
			}
			d := p.reduceSignals(sqd)
			mp.Store(t1, d)
			return nil
		})
	}

	err := gr.Wait()
	if err != nil {
		return nil, err
	}

	dd := []map[common.SignalKind]PrometheusSeries{}

	for t1 := range tt {

		v, ok := mp.Load(t1)
		if ok {
			d, ok := v.(map[common.SignalKind]PrometheusSeries)
			if !ok {
				continue
			}
			dd = append(dd, d)
		}
	}

	r := p.reduceSignals(dd)

	return r, nil
}

func (p *Prometheus) string2Time(ts string) time.Time {

	t, err := time.Parse(time.RFC3339Nano, ts)
	if err == nil {
		return t
	} else {

		d, err := time.ParseDuration(ts)
		if err == nil {
			td := time.Now().Add(d)
			return td
		}
	}

	i, err := strconv.Atoi(ts)
	if err != nil {
		return time.Now()
	}

	t = time.Unix(int64(i), 0)
	return t
}

func (p *Prometheus) getApplicationHost(
	data *PrometheusData, stamp common.Stamp,
	appName, hostName string, appHostHash common.Hash) (bool, common.Hash, common.Hash) {

	if utils.IsEmpty(appName) {
		return false, 0, 0
	}

	// find application & hosts in the dictionaries
	appHash := data.names.AddOrUpdate(appName)
	app := data.applications.FindWithTolerance(stamp, appHash, p.options.AppTolerance)

	hostHash := data.names.AddOrUpdate(hostName)
	host := data.hosts.FindWithTolerance(stamp, hostHash, p.options.HostTolerance)

	// find application signal in measurements
	if app == nil || host == nil {
		signal := data.measurements.FindApplicationSignalWithTolerance(stamp, appHostHash, p.options.AppSignalTolerance)
		if app == nil && signal != nil {
			appHash = signal.GetApplication()
			app = data.applications.Find(stamp, appHash)
		}
		if host == nil && signal != nil {
			hostHash = signal.GetHost()
			host = data.hosts.Find(stamp, hostHash)
		}
	}

	// create application if there are no
	if app == nil {
		appLbs := make(map[string]string)
		appLbs[common.ApplicationName] = appName
		app = common.NewApplication(appHash, data.attributes.AddOrUpdate(appLbs))
		data.applications.AddOrUpdate(stamp, app)
	}

	// create host if there are no
	if host == nil && !utils.IsEmpty(hostName) {
		hostLbs := make(map[string]string)
		hostLbs[common.HostName] = hostName
		host = common.NewHost(hostHash, data.attributes.AddOrUpdate(hostLbs), nil)
		data.hosts.AddOrUpdate(stamp, host)
	}

	return true, appHash, hostHash
}

type applicationSignalsOverDataCallback = func(as *common.ApplicationSignal, kind common.SignalKind, value float64, hash common.Hash)

func (p *Prometheus) applicationSignalsOverData(
	series map[common.SignalKind]PrometheusSeries,
	data *PrometheusData,
	callback applicationSignalsOverDataCallback) {

	for kind, iv := range series {
		for stamp, d := range iv {
			for _, v := range d {

				lbs := data.attributes.Find(v.Hash)
				if lbs == nil {
					continue
				}

				appName := lbs[common.ApplicationSignalName]
				hostName := lbs[common.ApplicationSignalHost]

				appHost := data.measurements.BuildApplicationSignalName(appName, hostName)
				appHostHash := data.names.AddOrUpdate(appHost)

				as := data.measurements.FindApplicationSignal(stamp, appHostHash)
				if as == nil {
					found, appHash, hostHash := p.getApplicationHost(data, stamp, appName, hostName, appHostHash)
					if !found {
						continue
					}
					as = common.NewApplicationSignal(appHostHash, appHash, hostHash)
				}
				data.measurements.AddOrUpdate(stamp, as)
				callback(as, kind, v.Value, v.Hash)
			}
		}
	}

}

func (p *Prometheus) getHost(
	data *PrometheusData, stamp common.Stamp,
	hostName string, hostHash common.Hash) (bool, common.Hash) {

	if utils.IsEmpty(hostName) {
		return false, 0
	}

	// find hosts in the dictionaries
	host := data.hosts.FindWithTolerance(stamp, hostHash, p.options.HostTolerance)

	// find host signal in measurements

	if host == nil {
		signal := data.measurements.FindHostSignalWithTolerance(stamp, hostHash, p.options.HostSignalTolerance)
		if signal != nil {
			hostHash = signal.GetName()
			host = data.hosts.Find(stamp, hostHash)
		}
	}

	// create host if there are no

	if host == nil {
		hostLbs := make(map[string]string)
		hostLbs[common.HostName] = hostName
		host = common.NewHost(hostHash, data.attributes.AddOrUpdate(hostLbs), nil)
		data.hosts.AddOrUpdate(stamp, host)
	}
	return true, hostHash
}

type hostSignalsOverDataCallback = func(hs *common.HostSignal, kind common.SignalKind, value float64, hash common.Hash)

func (p *Prometheus) hostSignalsOverData(
	series map[common.SignalKind]PrometheusSeries,
	data *PrometheusData,
	callback hostSignalsOverDataCallback) {

	for kind, iv := range series {
		for stamp, d := range iv {
			for _, v := range d {

				lbs := data.attributes.Find(v.Hash)
				if lbs == nil {
					continue
				}

				hostName := lbs[common.HostSignalHost]
				hostHash := data.names.AddOrUpdate(hostName)

				hs := data.measurements.FindHostSignal(stamp, hostHash)
				if hs == nil {
					found, hostHash := p.getHost(data, stamp, hostName, hostHash)
					if !found {
						continue
					}
					hs = common.NewHostSignal(hostHash)
				}
				data.measurements.AddOrUpdate(stamp, hs)
				callback(hs, kind, v.Value, v.Hash)
			}
		}
	}

}

func (p *Prometheus) gatherSpans(data *PrometheusData, from, to time.Time, span time.Duration) error {

	p.info("Gathering hosts (%s / %s) span=%s...", from, to, span)

	err := p.gatherHostsBySpan(data, p.options.HostQuery, from, to, span)
	if err != nil {
		return err
	}

	p.info("Gathering applications (%s / %s) span=%s...", from, to, span)

	err = p.gatherApplicationsBySpan(data, p.options.AppQuery, from, to, span)
	if err != nil {
		return err
	}

	p.info("Gathering signals (%s / %s) span=%s...", from, to, span)

	appLabels := utils.MapGetKeyValuesEx(p.options.AppSignalCommonLabels, ";", "=")

	queries := make(map[common.SignalKind]string)
	queries[common.SignalTraffic] = p.options.AppSignalInTrafficQuery
	queries[common.SignalErrors] = p.options.AppSignalInErrorsQuery
	queries[common.SignalLatency] = p.options.AppSignalInLatencyQuery

	series, err := p.gatherSignalsBySpan("apps incoming", data, queries, appLabels, from, to, span)
	if err != nil {
		return err
	}

	// fill up app incomings
	p.applicationSignalsOverData(series, data,
		func(as *common.ApplicationSignal, kind common.SignalKind, value float64, hash common.Hash) {

			switch kind {
			case common.SignalTraffic:
				as.IncomingTraffic.AddOrUpdate(value, hash)
			case common.SignalErrors:
				as.IncomingErrors.AddOrUpdate(value, hash)
			case common.SignalLatency:
				as.IncomingLatency.AddOrUpdate(value, hash)
			}
		})

	queries = make(map[common.SignalKind]string)
	queries[common.SignalTraffic] = p.options.AppSignalOutTrafficQuery
	queries[common.SignalErrors] = p.options.AppSignalOutErrorsQuery
	queries[common.SignalLatency] = p.options.AppSignalOutLatencyQuery
	queries[common.SignalSaturation] = p.options.AppSignalSaturationQuery

	series, err = p.gatherSignalsBySpan("apps outgoing & saturation", data, queries, appLabels, from, to, span)
	if err != nil {
		return err
	}

	// fill up app outgoings and saturations
	p.applicationSignalsOverData(series, data,
		func(as *common.ApplicationSignal, kind common.SignalKind, value float64, hash common.Hash) {

			switch kind {
			case common.SignalTraffic:
				as.OutgoingTraffic.AddOrUpdate(value, hash)
			case common.SignalErrors:
				as.OutgoingErrors.AddOrUpdate(value, hash)
			case common.SignalLatency:
				as.OutgoingLatency.AddOrUpdate(value, hash)
			case common.SignalSaturation:
				as.Saturation.AddOrUpdate(value, hash)
			}
		})

	hostLabels := utils.MapGetKeyValuesEx(p.options.HostSignalCommonLabels, ";", "=")

	queries = make(map[common.SignalKind]string)
	queries[common.SignalSaturation] = p.options.HostSignalSaturationQuery

	series, err = p.gatherSignalsBySpan("hosts", data, queries, hostLabels, from, to, span)
	if err != nil {
		return err
	}

	// fill up host saturations
	p.hostSignalsOverData(series, data,
		func(hs *common.HostSignal, kind common.SignalKind, value float64, hash common.Hash) {

			switch kind {
			case common.SignalSaturation:
				hs.Saturation.AddOrUpdate(value, hash)
			}
		})

	return nil
}

func (p *Prometheus) gatherWindows(from, to time.Time, span, window time.Duration, state *PrometheusState, yaml string) {

	items := state.RebuildIntervals(from, to, window)

	keys := slices.SortedFunc(maps.Keys(items), func(a, b common.Stamp) int {
		return cmp.Compare(a, b)
	})

	for _, s1 := range keys {

		s2 := items[s1]

		t1 := common.StampToTime(s1)
		t2 := common.StampToTime(s2)

		p.info("Gathering span (%s / %s) started duration=%s left=%s...", t1, t2, t2.Sub(t1), to.Sub(t2))

		when := time.Now()

		data := &PrometheusData{
			attributes:   common.NewAttributes(),
			names:        common.NewNames(),
			hosts:        common.NewHosts(),
			applications: common.NewApplications(),
			measurements: common.NewMeasurements(),
			first:        s1,
			last:         s2,
		}
		err := p.gatherSpans(data, t1, t2, span)
		if err != nil {
			p.error("Gathering span (%s / %s) finished with error: %s", t1, t2, err)
			continue
		}
		p.info("Gathering span (%s / %s) finished in %s left=%s", t1, t2, time.Since(when), to.Sub(t2))

		if p.onData != nil {
			go func() {

				err := p.onData(data)
				if err != nil {
					return
				}

				state.AddOrUpdateTimes(t1, t2)

				if !utils.IsEmpty(yaml) {
					p.info("Saving state to %s...", yaml)
					err := state.Save(yaml)
					if err != nil {
						p.error("Saving state to %s failed with error: %s", yaml, err)
					} else {
						t1, t2 := state.MinMaxTimes()
						p.info("Saving state to %s was successful items=%d (%s / %s) duration=%s", yaml, len(state.Items), t1, t2, t2.Sub(t1))
					}
				}

			}()
		}
	}
}

func (p *Prometheus) Start(wg *sync.WaitGroup) {

	if !p.mu.TryLock() {
		return
	}
	defer p.mu.Unlock()

	wg.Add(1)
	defer wg.Done()

	if utils.IsEmpty(p.options.Prometheus.From) {
		p.error("Cannot train due to from is not defined")
		return
	}

	from := p.string2Time(p.options.Prometheus.From)

	to := time.Now()
	if !utils.IsEmpty(p.options.Prometheus.To) {
		to = p.string2Time(p.options.Prometheus.To)
	}

	span := to.Sub(from)
	if !utils.IsEmpty(p.options.Span) {
		s, err := time.ParseDuration(p.options.Span)
		if err == nil {
			span = s
		}
	}

	win := to.Sub(from)
	if !utils.IsEmpty(p.options.Window) {
		s, err := time.ParseDuration(p.options.Window)
		if err == nil {
			win = s
		}
	}

	state := NewPrometheusState()
	yaml := p.options.StateYaml

	if utils.FileExists(yaml) {
		p.info("Loading state from %s...", yaml)
		err := state.Load(yaml)
		if err != nil {
			p.error("Loading state from %s failed with error: %s", yaml, err)
		} else {
			t1, t2 := state.MinMaxTimes()
			p.info("Loading state from %s was successful items=%d (%s / %s) duration=%s", yaml, len(state.Items), t1, t2, t2.Sub(t1))
		}
	}

	when := time.Now()
	p.info("Gathering window (%s / %s) duration=%s...", from, to, to.Sub(from))
	p.gatherWindows(from, to, span, win, state, yaml)
	p.info("Gathering window (%s / %s) finished in %s", from, to, time.Since(when))
}

func NewPrometheus(options PrometheusOptions, observability *common.Observability, onData common.DataSourceOnData) *Prometheus {

	return &Prometheus{
		options:       options,
		observability: observability,
		logger:        observability.Logs(),
		onData:        onData,
	}
}
