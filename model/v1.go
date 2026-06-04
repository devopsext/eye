package model

import (
	"bufio"
	"compress/gzip"
	"encoding/gob"
	"fmt"
	"maps"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/devopsext/eye/common"
	sreCommon "github.com/devopsext/sre/common"
	toolsVendors "github.com/devopsext/tools/vendors"
	"github.com/devopsext/utils"
	"github.com/jinzhu/copier"

	jsoniter "github.com/json-iterator/go"
	"golang.org/x/sync/errgroup"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

type V1ModelSeriesValue struct {
	Hash  common.Hash
	Value float64
}

type V1ModelSeries = map[common.Stamp][]V1ModelSeriesValue

type V1ModelOptions struct {
	File string

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

	Prometheus toolsVendors.PrometheusOptions
	Span       string
}

type V1Model struct {
	options       V1ModelOptions
	observability *common.Observability
	logger        sreCommon.Logger
}

type V1ModelData struct {
	Hashes       *common.Hashes
	Hosts        *common.Hosts
	Applications *common.Applications
	Measurements *common.Measurements
}

type V1ModelFileHeader struct {
	Model   string
	Version uint32
}

type V1ModelFileHashes struct {
	Items map[common.Hash]common.Labels
}

type V1ModelFileHosts struct {
	Items map[common.Stamp]map[string]*common.Host
}

type V1ModelFileApplications struct {
	Items map[common.Stamp]map[string]*common.Application
}

type V1ModelFileMeasurements struct {
	Items map[common.Stamp]*common.Signals
}

const (
	V1ModelFileVersion0 = iota
	V1ModelFileVersion1
)

func (m *V1Model) Name() string {
	return "V1Model"
}

func (m *V1Model) info(msg any, args ...any) {
	gid := utils.GoRoutineID()
	m.logger.Info(fmt.Sprintf("%v: [%d] %v", m.Name(), gid, msg), args...)
}

func (m *V1Model) error(msg any, args ...any) {
	gid := utils.GoRoutineID()
	m.logger.Error(fmt.Sprintf("%v: [%d] %v", m.Name(), gid, msg), args...)
}

func (m *V1Model) debug(msg any, args ...any) {
	gid := utils.GoRoutineID()
	m.logger.Debug(fmt.Sprintf("%v: [%d] %v", m.Name(), gid, msg), args...)
}

func (m *V1Model) addUniqueKeys(keys []string, ma []map[string]string) []string {

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

func (m *V1Model) sliceCommonLabels(parent, labels map[string]string) []map[string]string {

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
			r2 := m.sliceCommonLabels(lbs, labels)
			r = append(r, r2...)
		}
		pKeys = m.addUniqueKeys(pKeys, r)
	}
	return r
}

func (m *V1Model) loadData(q string, from, to time.Time) (*common.PrometheusResponseData, error) {

	opts := toolsVendors.PrometheusOptions{}
	copier.Copy(&opts, &m.options.Prometheus)

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

func (m *V1Model) getStampedValue(values []any) (bool, common.Stamp, float64) {

	var stamp common.Stamp = 0
	var value float64 = 0.0

	if len(values) < 2 {
		return false, stamp, value
	}

	// get stamp
	s := fmt.Sprintf("%.0f", values[0])
	s = strings.ReplaceAll(s, ".", "")
	if len(s) == 13 { // unix millisec
		i, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return false, stamp, value
		}
		stamp = common.Stamp(i)
	} else if len(s) == 10 { // unix sec
		i, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return false, stamp, value
		}
		stamp = common.Stamp(i * 1000) // unix millisec
	}

	// get value
	s = fmt.Sprintf("%s", values[1])
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return false, stamp, value
	}
	value = f

	if stamp == 0 {
		return false, stamp, value
	}
	return true, stamp, value
}

func (m *V1Model) loadHosts(hashes *common.Hashes, q string, from, to time.Time) (*common.Hosts, error) {

	promData, err := m.loadData(q, from, to)
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

			found, stamp, _ := m.getStampedValue(v)
			if !found {
				continue
			}

			hostLbs := maps.Clone(dr.Labels)

			var on *common.Host
			if findOn {
				on = hosts.Find(stamp, nameOn)
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
					hash := hashes.AddOrUpdate(lbs)

					on = common.NewHost(hashes, hash, nil)
					hosts.AddOrUpdate(stamp, on)
				}
			}

			host := hosts.Find(stamp, name)
			if host == nil {
				hash := hashes.AddOrUpdate(hostLbs)
				host = common.NewHost(hashes, hash, on)
			}
			hosts.AddOrUpdate(stamp, host)
		}
	}
	return hosts, nil
}

func (m *V1Model) gatherHostsByQuery(hashes *common.Hashes, query string, from, to time.Time) (*common.Hosts, error) {

	when := time.Now()

	m.debug("Hosts gathering (%s / %s) ...", from, to)
	m.debug("Hosts gathering started => %s", query)

	hosts, err := m.loadHosts(hashes, query, from, to)
	if err != nil {
		return nil, err
	}

	ts, hs := hosts.Sizes()
	if ts > 0 {
		m.debug("Hosts gathering finished (%d over %d timeseries) in %s", hs, ts, time.Since(when))
	} else {
		m.debug("Hosts gathering finished (no data) in %s", time.Since(when))
	}
	return hosts, nil
}

func (m *V1Model) gatherHostsBySpan(hashes *common.Hashes, hosts *common.Hosts, query string, from, to time.Time, span time.Duration) error {

	tt := make(map[time.Time]time.Time)
	t1 := from

	for t1.Unix() < to.Unix() {

		t2 := t1.Add(span)
		tt[t1] = t2
		t1 = t2
	}

	gr := &errgroup.Group{}
	mp := &sync.Map{}

	for t1, t2 := range tt {

		gr.Go(func() error {

			hsts, err := m.gatherHostsByQuery(hashes, query, t1, t2)
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
			hosts.Merge(d)
		}
	}
	return nil
}

func (m *V1Model) loadApplications(hashes *common.Hashes, q string, from, to time.Time) (*common.Applications, error) {

	promData, err := m.loadData(q, from, to)
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

		hash := hashes.AddOrUpdate(dr.Labels)

		for _, v := range dr.Values {

			found, stamp, _ := m.getStampedValue(v)
			if !found {
				continue
			}

			app := apps.Find(stamp, name)
			if app == nil {
				app = common.NewApplication(hashes, hash)
			}
			apps.AddOrUpdate(stamp, app)
		}
	}
	return apps, nil
}

func (m *V1Model) gatherApplicationsByQuery(hashes *common.Hashes, query string, from, to time.Time) (*common.Applications, error) {

	when := time.Now()

	m.debug("Applications gathering (%s / %s) ...", from, to)
	m.debug("Applications gathering started => %s", query)

	apps, err := m.loadApplications(hashes, query, from, to)
	if err != nil {
		return nil, err
	}

	ts, hs := apps.Sizes()
	if ts > 0 {
		m.debug("Applications gathering finished (%d over %d timeseries) in %s", hs, ts, time.Since(when))
	} else {
		m.debug("Applications gathering finished (no data) in %s", time.Since(when))
	}
	return apps, nil
}

func (m *V1Model) gatherApplicationsBySpan(hashes *common.Hashes, applications *common.Applications, query string, from, to time.Time, span time.Duration) error {

	tt := make(map[time.Time]time.Time)
	t1 := from

	for t1.Unix() < to.Unix() {

		t2 := t1.Add(span)
		tt[t1] = t2
		t1 = t2
	}

	gr := &errgroup.Group{}
	mp := &sync.Map{}

	for t1, t2 := range tt {

		gr.Go(func() error {

			apps, err := m.gatherApplicationsByQuery(hashes, query, t1, t2)
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
			applications.Merge(d)
		}
	}
	return nil
}

func (m *V1Model) loadModelData(hashes *common.Hashes, q string, from, to time.Time) (V1ModelSeries, error) {

	promData, err := m.loadData(q, from, to)
	if err != nil {
		return nil, err
	}

	modelData := V1ModelSeries{}
	if promData == nil {
		return modelData, nil
	}

	for _, dr := range promData.Result {

		for _, v := range dr.Values {

			found, stamp, value := m.getStampedValue(v)
			if !found {
				continue
			}

			v := V1ModelSeriesValue{
				Hash:  hashes.AddOrUpdate(dr.Labels),
				Value: value,
			}
			modelData[stamp] = append(modelData[stamp], v)
		}
	}
	return modelData, nil
}

func (m *V1Model) gatherSignals(hashes *common.Hashes, queries map[common.SignalKind]string, from, to time.Time) (map[common.SignalKind]V1ModelSeries, error) {

	gr := &errgroup.Group{}
	mp := &sync.Map{}

	// gather signals
	for k, q := range queries {

		gr.Go(func() error {

			data, err := m.loadModelData(hashes, q, from, to)
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

	r := make(map[common.SignalKind]V1ModelSeries)

	for k := range queries {

		v, ok := mp.Load(k)
		if ok {
			d, ok := v.(V1ModelSeries)
			if ok {
				r[k] = d
			}
		}
	}
	return r, nil
}

func (m *V1Model) prepareQuery(q string, labels map[string]string) string {

	s := q
	for k, v := range labels {
		s = strings.ReplaceAll(s, fmt.Sprintf(".%s", k), v)
	}
	return s
}

func (m *V1Model) prepareQueries(queries map[common.SignalKind]string, labels map[string]string) map[common.SignalKind]string {

	r := make(map[common.SignalKind]string)

	for k, q := range queries {
		v := m.prepareQuery(q, labels)
		if !utils.IsEmpty(v) {
			r[k] = v
		}
	}
	return r
}

func (m *V1Model) gatherSignalsByQueries(hashes *common.Hashes, name string, queries map[common.SignalKind]string, labels map[string]string, from, to time.Time) ([]map[common.SignalKind]V1ModelSeries, error) {

	when := time.Now()

	signals := []map[common.SignalKind]V1ModelSeries{}

	m.debug("Signals gathering %s (%s / %s)...", name, from, to)

	if len(labels) > 0 {

		gr := &errgroup.Group{}
		mp := &sync.Map{}
		arr := m.sliceCommonLabels(nil, labels)

		for k, lbs := range arr {

			gr.Go(func() error {

				qrs := m.prepareQueries(queries, lbs)
				for k, q := range qrs {
					m.debug("Signals gathering %s %s started => %s", name, common.SignalKindToString(k), q)
				}

				r, err := m.gatherSignals(hashes, qrs, from, to)
				if err != nil {
					return err
				}

				infos := []string{}
				for k, v := range r {
					infos = append(infos, fmt.Sprintf("%s: %d", common.SignalKindToString(k), len(v)))
				}
				if len(infos) > 0 {
					m.debug("Signals gathering %s finished%s in %s", name, fmt.Sprintf(" (%s)", strings.Join(infos, ", ")), time.Since(when))
				} else {
					m.debug("Signals gathering %s finished (no data) in %s", name, time.Since(when))
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
				r, ok := v.(map[common.SignalKind]V1ModelSeries)
				if ok {
					signals = append(signals, r)
				}
			}
		}

	} else {

		queries := m.prepareQueries(queries, nil)
		for k, q := range queries {
			m.debug("Signals gathering %s %s started => %s", name, common.SignalKindToString(k), q)
		}

		r, err := m.gatherSignals(hashes, queries, from, to)
		if err != nil {
			return nil, err
		}

		infos := []string{}
		for k, v := range r {
			infos = append(infos, fmt.Sprintf("%s: %d", common.SignalKindToString(k), len(v)))
		}
		if len(infos) > 0 {
			m.debug("Signals gathering %s finished%s in %s", name, fmt.Sprintf(" (%s)", strings.Join(infos, ", ")), time.Since(when))
		} else {
			m.debug("Signals gathering %s finished (no data) in %s", name, time.Since(when))
		}

		signals = append(signals, r)
	}
	return signals, nil
}

func (m *V1Model) reduceSignals(arr []map[common.SignalKind]V1ModelSeries) map[common.SignalKind]V1ModelSeries {

	r := make(map[common.SignalKind]V1ModelSeries)

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

func (m *V1Model) gatherSignalsBySpan(name string, hashes *common.Hashes,
	queries map[common.SignalKind]string, labels map[string]string,
	from, to time.Time, span time.Duration) (map[common.SignalKind]V1ModelSeries, error) {

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

	gr := &errgroup.Group{}
	mp := &sync.Map{}

	for t1, t2 := range tt {

		gr.Go(func() error {

			sqd, err := m.gatherSignalsByQueries(hashes, name, qs, labels, t1, t2)
			if err != nil {
				return err
			}
			d := m.reduceSignals(sqd)
			mp.Store(t1, d)
			return nil
		})
	}

	err := gr.Wait()
	if err != nil {
		return nil, err
	}

	dd := []map[common.SignalKind]V1ModelSeries{}

	for t1 := range tt {

		v, ok := mp.Load(t1)
		if ok {
			d, ok := v.(map[common.SignalKind]V1ModelSeries)
			if !ok {
				continue
			}
			dd = append(dd, d)
		}
	}

	r := m.reduceSignals(dd)

	return r, nil
}

func (m *V1Model) string2Time(ts string) time.Time {

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

func (m *V1Model) getApplicationHost(
	hashes *common.Hashes, measurements *common.Measurements,
	hosts *common.Hosts, applications *common.Applications,
	stamp common.Stamp, appName, hostName, appHost string) (*common.Application, *common.Host) {

	if utils.IsEmpty(appName) {
		return nil, nil
	}

	// find application & hosts in the dictionaries
	app := applications.FindWithTolerance(stamp, appName, m.options.AppTolerance)
	host := hosts.FindWithTolerance(stamp, hostName, m.options.HostTolerance)

	// find application signal in measurements
	if app == nil || host == nil {
		signal := measurements.FindApplicationSignalWithTolerance(stamp, appHost, m.options.AppSignalTolerance)
		if app == nil && signal != nil {
			app = signal.Application
		}
		if host == nil && signal != nil {
			host = signal.Host
		}
	}

	// create application if there are no
	if app == nil {
		appLbs := make(map[string]string)
		appLbs[common.ApplicationName] = appName
		app = common.NewApplication(hashes, hashes.AddOrUpdate(appLbs))
		applications.AddOrUpdate(stamp, app)
	}

	// create host if there are no
	if host == nil && !utils.IsEmpty(hostName) {
		hostLbs := make(map[string]string)
		hostLbs[common.HostName] = hostName
		host = common.NewHost(hashes, hashes.AddOrUpdate(hostLbs), nil)
		hosts.AddOrUpdate(stamp, host)
	}

	return app, host
}

type applicationSignalsOverDataCallback = func(as *common.ApplicationSignal, kind common.SignalKind, value float64, hash common.Hash)

func (m *V1Model) applicationSignalsOverData(
	data map[common.SignalKind]V1ModelSeries,
	measurements *common.Measurements,
	hashes *common.Hashes,
	hosts *common.Hosts, applications *common.Applications,
	callback applicationSignalsOverDataCallback) {

	for kind, iv := range data {
		for stamp, data := range iv {
			for _, v := range data {

				lbs := hashes.Find(v.Hash)
				if lbs == nil {
					continue
				}

				appName := lbs[common.ApplicationSignalName]
				hostName := lbs[common.ApplicationSignalHost]
				appHost := common.BuildApplicationSignalName(appName, hostName)

				as := measurements.FindApplicationSignal(stamp, appHost)
				if as == nil {
					app, host := m.getApplicationHost(hashes, measurements, hosts, applications, stamp, appName, hostName, appHost)
					if app == nil {
						continue
					}
					as = common.NewApplicationSignal(hashes, applications, app, host)
				}
				measurements.AddOrUpdate(stamp, as)
				callback(as, kind, v.Value, v.Hash)
			}
		}
	}
}

func (m *V1Model) getHost(
	hashes *common.Hashes, measurements *common.Measurements,
	hosts *common.Hosts, stamp common.Stamp, hostName string) *common.Host {

	if utils.IsEmpty(hostName) {
		return nil
	}

	// find hosts in the dictionaries
	host := hosts.FindWithTolerance(stamp, hostName, m.options.HostTolerance)

	// find host signal in measurements
	if host == nil {
		signal := measurements.FindHostSignalWithTolerance(stamp, hostName, m.options.HostSignalTolerance)
		if signal != nil {
			host = signal.Host()
		}
	}

	// create host if there are no
	if host == nil {
		hostLbs := make(map[string]string)
		hostLbs[common.HostName] = hostName
		host = common.NewHost(hashes, hashes.AddOrUpdate(hostLbs), nil)
		hosts.AddOrUpdate(stamp, host)
	}

	return host
}

type hostSignalsOverDataCallback = func(hs *common.HostSignal, kind common.SignalKind, value float64, hash common.Hash)

func (m *V1Model) hostSignalsOverData(
	data map[common.SignalKind]V1ModelSeries,
	measurements *common.Measurements,
	hashes *common.Hashes,
	hosts *common.Hosts,
	callback hostSignalsOverDataCallback) {

	for kind, iv := range data {
		for stamp, data := range iv {
			for _, v := range data {

				lbs := hashes.Find(v.Hash)
				if lbs == nil {
					continue
				}

				host := lbs[common.HostSignalHost]

				hs := measurements.FindHostSignal(stamp, host)
				if hs == nil {
					host := m.getHost(hashes, measurements, hosts, stamp, host)
					if host == nil {
						continue
					}
					hs = common.NewHostSignal(hashes, host)
				}
				measurements.AddOrUpdate(stamp, hs)
				callback(hs, kind, v.Value, v.Hash)
			}
		}
	}
}

func (m *V1Model) train(data *V1ModelData) error {

	// 0. gather application and host infos +++
	// 1. gather incoming traffic, errors, latency per application +++
	// 2. gather outgoing traffic, errors, latency per application +++
	// 3. gather saturation per application +++
	// 4. make application signals (set application and host) based on incomings +++
	// 5. add application signals (set application and host) based on outgoings & saturation +++
	// 6. add saturation to host signals +++
	// 7. add frontends & backends to application signals +++

	if utils.IsEmpty(m.options.Prometheus.From) {
		return fmt.Errorf("Cannot train due to from time is not defined")
	}

	from := m.string2Time(m.options.Prometheus.From)

	to := time.Now()
	if !utils.IsEmpty(m.options.Prometheus.To) {
		to = m.string2Time(m.options.Prometheus.To)
	}

	span := to.Sub(from)
	if !utils.IsEmpty(m.options.Span) {
		s, err := time.ParseDuration(m.options.Span)
		if err == nil {
			span = s
		}
	}

	m.info("Gathering hosts (%s / %s, span: %s)...", from, to, span)

	err := m.gatherHostsBySpan(data.Hashes, data.Hosts, m.options.HostQuery, from, to, span)
	if err != nil {
		return err
	}

	m.info("Gathering applications (%s / %s, span: %s)...", from, to, span)

	err = m.gatherApplicationsBySpan(data.Hashes, data.Applications, m.options.AppQuery, from, to, span)
	if err != nil {
		return err
	}

	m.info("Gathering signals (%s / %s, span: %s)...", from, to, span)

	appLabels := utils.MapGetKeyValuesEx(m.options.AppSignalCommonLabels, ";", "=")

	queries := make(map[common.SignalKind]string)
	queries[common.SignalTraffic] = m.options.AppSignalInTrafficQuery
	queries[common.SignalErrors] = m.options.AppSignalInErrorsQuery
	queries[common.SignalLatency] = m.options.AppSignalInLatencyQuery

	mData, err := m.gatherSignalsBySpan("apps incoming", data.Hashes, queries, appLabels, from, to, span)
	if err != nil {
		return err
	}

	// fill up app incomings
	m.applicationSignalsOverData(mData, data.Measurements, data.Hashes, data.Hosts, data.Applications,
		func(as *common.ApplicationSignal, kind common.SignalKind, value float64, hash common.Hash) {

			switch kind {
			case common.SignalTraffic:
				as.IncomingTraffic().AddOrUpdate(value, hash)
			case common.SignalErrors:
				as.IncomingErrors().AddOrUpdate(value, hash)
			case common.SignalLatency:
				as.IncomingLatency().AddOrUpdate(value, hash)
			}
		})

	queries = make(map[common.SignalKind]string)
	queries[common.SignalTraffic] = m.options.AppSignalOutTrafficQuery
	queries[common.SignalErrors] = m.options.AppSignalOutErrorsQuery
	queries[common.SignalLatency] = m.options.AppSignalOutLatencyQuery
	queries[common.SignalSaturation] = m.options.AppSignalSaturationQuery

	mData, err = m.gatherSignalsBySpan("apps outgoing & saturation", data.Hashes, queries, appLabels, from, to, span)
	if err != nil {
		return err
	}

	// fill up app outgoings and saturations
	m.applicationSignalsOverData(mData, data.Measurements, data.Hashes, data.Hosts, data.Applications,
		func(as *common.ApplicationSignal, kind common.SignalKind, value float64, hash common.Hash) {

			switch kind {
			case common.SignalTraffic:
				as.OutgoingTraffic().AddOrUpdate(value, hash)
			case common.SignalErrors:
				as.OutgoingErrors().AddOrUpdate(value, hash)
			case common.SignalLatency:
				as.OutgoingLatency().AddOrUpdate(value, hash)
			case common.SignalSaturation:
				as.Saturation().AddOrUpdate(value, hash)
			}
		})

	hostLabels := utils.MapGetKeyValuesEx(m.options.HostSignalCommonLabels, ";", "=")

	queries = make(map[common.SignalKind]string)
	queries[common.SignalSaturation] = m.options.HostSignalSaturationQuery

	mData, err = m.gatherSignalsBySpan("hosts", data.Hashes, queries, hostLabels, from, to, span)
	if err != nil {
		return err
	}

	// fill up host saturations
	m.hostSignalsOverData(mData, data.Measurements, data.Hashes, data.Hosts,
		func(hs *common.HostSignal, kind common.SignalKind, value float64, hash common.Hash) {

			switch kind {
			case common.SignalSaturation:
				hs.Saturation().AddOrUpdate(value, hash)
			}
		})

	//last := measurements.LastStamp()

	/*
		// all last application signals
		appSignals := measurements.ApplicationSignalsByNames(last, nil)
		if !utils.IsEmpty(appSignals) {

			for _, as := range appSignals {

				n := as.Name()
				// show frontends
				frontedns := as.Frontends(last)
				for _, f := range frontedns {
					m.debug("Frontend %s => %s", f.Name(), n)
				}

				// show backends
				backends := as.Backends(last)
				for _, b := range backends {
					m.debug("Backend %s => %s", n, b.Name())
				}
			}
		}
	*/

	/*
			// all last host signals
			hostSignals := measurements.HostSignals(last, nil)
			if !utils.IsEmpty(hostSignals) {

				for _, hs := range hostSignals {
					m.debug(hs.Name())
				}
			}

			// all last applications
			apps := measurements.Applications(last, nil)
			if !utils.IsEmpty(apps) {

				for _, a := range apps {
					m.debug("Application %s", a.Name())
				}
			}

			// all last hosts
			hsts := measurements.Hosts(last, nil)
			if !utils.IsEmpty(hsts) {

				for _, h := range hsts {
					m.debug("Host %s", h.Name())
				}
			}



		deps := measurements.DependenciesByNames(last, []string{"other"})
		if !utils.IsEmpty(deps) {

			for a, arr := range deps.Items() {
				hosts := m.hostNames(measurements, last, a)
				m.debugDependecies(measurements, last, a.Name(), "", arr, hosts)
			}
		}*/

	return nil
}

func (m *V1Model) hostNames(measurements *common.Measurements, stamp common.Stamp, app *common.Application) []string {

	hosts := measurements.Hosts(stamp, []*common.Application{app})

	arr := []string{}
	for _, h := range hosts {
		hName := h.Name()
		if !utils.Contains(arr, hName) {
			arr = append(arr, hName)
		}
	}
	if len(arr) > 0 {
		return arr
	}

	return []string{}
}

func (m *V1Model) debugDependecies(measurements *common.Measurements, stamp common.Stamp, name, parent string, deps *common.Dependencies, hosts []string) {

	if deps == nil {
		m.debug("Dependency graph %s => %s (%s)", name, parent, strings.Join(hosts, ","))
		return
	}
	for a, arr := range deps.Items() {

		aName := a.Name()
		if parent != "" {
			aName = fmt.Sprintf("%s/%s", parent, aName)
		}
		hosts := m.hostNames(measurements, stamp, a)
		m.debugDependecies(measurements, stamp, name, aName, arr, hosts)
	}
}

func (m *V1Model) LoadFromFile(path string, data *V1ModelData) error {

	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	br := bufio.NewReader(f)

	gr, err := gzip.NewReader(br)
	if err != nil {
		return err
	}
	defer gr.Close()

	decoder := gob.NewDecoder(gr)

	// read header
	header := V1ModelFileHeader{}
	err = decoder.Decode(&header)
	if err != nil {
		return err
	}

	if header.Model != m.Name() {
		return fmt.Errorf("Load from file doesn't support model %s", header.Model)
	}

	switch header.Version {
	case V1ModelFileVersion1:

		// read hashes
		hashes := V1ModelFileHashes{}
		err = decoder.Decode(&hashes)
		if err != nil {
			return err
		}
		data.Hashes.SetItems(hashes.Items)

		// read hosts
		hosts := V1ModelFileHosts{}
		err = decoder.Decode(&hosts)
		if err != nil {
			return err
		}
		data.Hosts.SetItems(hosts.Items)

		// read applications
		apps := V1ModelFileApplications{}
		err = decoder.Decode(&apps)
		if err != nil {
			return err
		}
		data.Applications.SetItems(apps.Items)

		// read measurements
		measurements := V1ModelFileMeasurements{}
		err = decoder.Decode(&measurements)
		if err != nil {
			return err
		}
		data.Measurements.SetItems(measurements.Items)

	default:
		return fmt.Errorf("Load from file doesn't support version %d", header.Version)
	}

	return nil
}

func (m *V1Model) SaveToFile(path string, data *V1ModelData) error {

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	bw := bufio.NewWriter(f)
	defer bw.Flush()

	gw := gzip.NewWriter(bw)
	defer gw.Close()

	encoder := gob.NewEncoder(gw)

	// write header
	err = encoder.Encode(V1ModelFileHeader{
		Model:   m.Name(),
		Version: V1ModelFileVersion1,
	})
	if err != nil {
		return err
	}

	// write hashes
	err = encoder.Encode(V1ModelFileHashes{
		Items: data.Hashes.GetItems(),
	})
	if err != nil {
		return err
	}

	// write hosts
	err = encoder.Encode(V1ModelFileHosts{
		Items: data.Hosts.GetItems(),
	})
	if err != nil {
		return err
	}

	// write applications
	err = encoder.Encode(V1ModelFileApplications{
		Items: data.Applications.GetItems(),
	})
	if err != nil {
		return err
	}

	// write measurements
	err = encoder.Encode(V1ModelFileMeasurements{
		Items: data.Measurements.GetItems(),
	})
	if err != nil {
		return err
	}

	return nil
}

func (m *V1Model) Train(wg *sync.WaitGroup) {

	wg.Add(1)
	go func(swg *sync.WaitGroup) {

		defer swg.Done()

		m.info("Training...")

		hosts := common.NewHosts()
		apps := common.NewApplications()

		data := &V1ModelData{
			Hashes:       common.NewHashes(),
			Hosts:        hosts,
			Applications: apps,
			Measurements: common.NewMeasurements(hosts, apps),
		}

		// load from file if there is file
		if utils.FileExists(m.options.File) {
			m.info("Loading from file %s...", m.options.File)
			when := time.Now()
			err := m.LoadFromFile(m.options.File, data)
			if err != nil {
				m.error("Loading from file has error: %s", err)
			} else {
				m.info("Loading from file finished in %s", time.Since(when))
			}
		}

		when := time.Now()
		err := m.train(data)
		if err != nil {
			m.error("Training finished with error: %s", err)
			return
		}
		m.info("Training finished in %s", time.Since(when))

		// save to file if it's needed
		if !utils.IsEmpty(m.options.File) {
			m.info("Saving to file %s...", m.options.File)
			when := time.Now()
			err := m.SaveToFile(m.options.File, data)
			if err != nil {
				m.error("Saving to file has error: %s", err)
			} else {
				m.info("Saving to file finished in %s", time.Since(when))
			}
		}

	}(wg)
}

func NewV1Model(options V1ModelOptions, observability *common.Observability) *V1Model {

	return &V1Model{
		options:       options,
		observability: observability,
		logger:        observability.Logs(),
	}
}

func init() {
	//gob.Register(common.ApplicationSignal{})
	gob.RegisterName("common.Signal", &common.ApplicationSignal{})
}
