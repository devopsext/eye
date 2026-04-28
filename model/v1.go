package model

import (
	"encoding/json"
	"fmt"
	"maps"
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
)

type V1ModelDataValue struct {
	Labels *map[string]string
	Value  float64
}

type V1ModelData = map[int64][]V1ModelDataValue

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

	HostQuery                 string
	HostTolerance             int
	HostSignalCommonLabels    string
	HostSignalSaturationQuery string

	Prometheus toolsVendors.PrometheusOptions
	Span       string
}

type V1Model struct {
	options       V1ModelOptions
	observability *common.Observability
	logger        sreCommon.Logger
}

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

func (m *V1Model) getStampedValue(values []any) (bool, int64, float64) {

	var stamp int64 = 0
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
		stamp = i
	} else if len(s) == 10 { // unix sec
		i, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return false, stamp, value
		}
		stamp = i * 1000 // unix millisec
	}

	// get value
	s = fmt.Sprintf("%s", values[1])
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return false, stamp, value
	}
	value = f

	if stamp <= 0 {
		return false, stamp, value
	}
	return true, stamp, value
}

func (m *V1Model) loadHosts(q string, from, to time.Time) (*common.Hosts, error) {

	promData, err := m.loadData(q, from, to)
	if err != nil {
		return nil, err
	}

	hosts := common.NewHosts()

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
				on = hosts.Find(stamp, nameOn, 0)
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

					on = common.NewHost(lbs, nil)
				}
			}

			host := hosts.Find(stamp, name, 0)
			if host == nil {
				host = common.NewHost(hostLbs, on)
			}
			if host.On == nil {
				host.On = on
			}
			hosts.AddOrUpdate(stamp, host)
		}
	}
	return hosts, nil
}

func (m *V1Model) gatherHostsByQuery(query string, from, to time.Time) (*common.Hosts, error) {

	when := time.Now()

	m.debug("Hosts gathering (%s / %s) ...", from, to)
	m.debug("Hosts gathering started => %s", query)

	hosts, err := m.loadHosts(query, from, to)
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

func (m *V1Model) gatherHostsBySpan(query string, from, to time.Time, span time.Duration) (*common.Hosts, error) {

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

			hosts, err := m.gatherHostsByQuery(query, t1, t2)
			if err != nil {
				return err
			}
			mp.Store(t1, hosts)
			return nil
		})
	}

	err := gr.Wait()
	if err != nil {
		return nil, err
	}

	hosts := &common.Hosts{}

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
	return hosts, nil
}

func (m *V1Model) loadApplications(q string, from, to time.Time) (*common.Applications, error) {

	promData, err := m.loadData(q, from, to)
	if err != nil {
		return nil, err
	}

	apps := common.NewApplications()

	for _, dr := range promData.Result {

		name := dr.Labels[common.ApplicationName]
		if utils.IsEmpty(name) {
			continue
		}

		for _, v := range dr.Values {

			found, stamp, _ := m.getStampedValue(v)
			if !found {
				continue
			}

			app := apps.Find(stamp, name, 0)
			if app == nil {
				app = common.NewApplication(dr.Labels)
			}
			apps.AddOrUpdate(stamp, app)
		}
	}
	return apps, nil
}

func (m *V1Model) gatherApplicationsByQuery(query string, from, to time.Time) (*common.Applications, error) {

	when := time.Now()

	m.debug("Applications gathering (%s / %s) ...", from, to)
	m.debug("Applications gathering started => %s", query)

	apps, err := m.loadApplications(query, from, to)
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

func (m *V1Model) gatherApplicationsBySpan(query string, from, to time.Time, span time.Duration) (*common.Applications, error) {

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

			apps, err := m.gatherApplicationsByQuery(query, t1, t2)
			if err != nil {
				return err
			}
			mp.Store(t1, apps)
			return nil
		})
	}

	err := gr.Wait()
	if err != nil {
		return nil, err
	}

	apps := &common.Applications{}

	for t1 := range tt {

		v, ok := mp.Load(t1)
		if ok {
			d, ok := v.(*common.Applications)
			if !ok {
				continue
			}
			apps.Merge(d)
		}
	}
	return apps, nil
}

func (m *V1Model) loadModelData(q string, from, to time.Time) (V1ModelData, error) {

	promData, err := m.loadData(q, from, to)
	if err != nil {
		return nil, err
	}

	modelData := V1ModelData{}

	for _, dr := range promData.Result {

		for _, v := range dr.Values {

			found, stamp, value := m.getStampedValue(v)
			if !found {
				continue
			}

			v := V1ModelDataValue{
				Labels: &dr.Labels,
				Value:  value,
			}
			modelData[stamp] = append(modelData[stamp], v)
		}
	}
	return modelData, nil
}

func (m *V1Model) gatherSignals(queries map[common.SignalKind]string, from, to time.Time) (map[common.SignalKind]V1ModelData, error) {

	gr := &errgroup.Group{}
	mp := &sync.Map{}

	// gather signals
	for k, q := range queries {

		gr.Go(func() error {

			data, err := m.loadModelData(q, from, to)
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

	r := make(map[common.SignalKind]V1ModelData)

	for k := range queries {

		v, ok := mp.Load(k)
		if ok {
			d, ok := v.(V1ModelData)
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

func (m *V1Model) gatherSignalsByQueries(name string, queries map[common.SignalKind]string, labels map[string]string, from, to time.Time) ([]map[common.SignalKind]V1ModelData, error) {

	when := time.Now()

	signals := []map[common.SignalKind]V1ModelData{}

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

				r, err := m.gatherSignals(qrs, from, to)
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
				r, ok := v.(map[common.SignalKind]V1ModelData)
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

		r, err := m.gatherSignals(queries, from, to)
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

func (m *V1Model) reduceSignals(arr []map[common.SignalKind]V1ModelData) map[common.SignalKind]V1ModelData {

	r := make(map[common.SignalKind]V1ModelData)

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

func (m *V1Model) gatherSignalsBySpan(name string,
	queries map[common.SignalKind]string, labels map[string]string,
	from, to time.Time, span time.Duration) (map[common.SignalKind]V1ModelData, error) {

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

			sqd, err := m.gatherSignalsByQueries(name, qs, labels, t1, t2)
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

	dd := []map[common.SignalKind]V1ModelData{}

	for t1 := range tt {

		v, ok := mp.Load(t1)
		if ok {
			d, ok := v.(map[common.SignalKind]V1ModelData)
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

func (m *V1Model) getSignalApplicationHost(hosts *common.Hosts, applications *common.Applications,
	stamp int64, labels map[string]string) (*common.Application, *common.Host) {

	appName := labels[common.ApplicationSignalName]
	if utils.IsEmpty(appName) {
		return nil, nil
	}

	app := applications.Find(stamp, appName, m.options.AppTolerance)
	if app == nil {
		lbs := make(map[string]string)
		lbs[common.ApplicationName] = appName
		app = common.NewApplication(lbs)
		applications.AddOrUpdate(stamp, app)
	}

	var host *common.Host
	hostName := labels[common.ApplicationSignalHost]
	if !utils.IsEmpty(hostName) {
		host = hosts.Find(stamp, hostName, m.options.HostTolerance)
		if host == nil {
			lbs := make(map[string]string)
			lbs[common.HostName] = hostName
			host = common.NewHost(lbs, nil)
		}
	}
	return app, host
}

func (m *V1Model) createIncomingMeasurements(hosts *common.Hosts,
	applications *common.Applications,
	incomings map[common.SignalKind]V1ModelData) *common.Measurements {

	measurements := common.NewMeasurements()

	for _, iv := range incomings {
		for stamp, data := range iv {
			for _, v := range data {

				mp := v.Labels
				if m == nil {
					continue
				}
				mv := *mp

				app, host := m.getSignalApplicationHost(hosts, applications, stamp, mv)
				if app == nil {
					continue
				}

				s := common.NewApplicationSignal(app, host)
				measurements.AddOrUpdate(stamp, s)

				/*switch ik {
				case common.SignalTraffic:
					// need to find out
					tKind := common.TrafficKindRps
					values := make(map[common.TrafficKind]*common.Traffic)
					values[tKind] = &v.Value

					s.IncomingTraffic = common.NewIncomingTraffic(values, mv)
				case common.SignalLatency:
					s.IncomingLatency = common.NewIncomingLatency(&v.Value, mv)
				case common.SignalErrors:
					s.IncomingErrors = common.NewIncomingErrors(&v.Value, mv)
				}*/
			}
		}
	}
	return measurements
}

func (m *V1Model) fillOutgoingMeasurements(measurements *common.Measurements,
	hosts *common.Hosts,
	applications *common.Applications,
	outgoings map[common.SignalKind]V1ModelData) {

	/*
		for ok, ov := range outgoings {

			for stamp, data := range ov {
			}
		}
	*/
}

func (m *V1Model) train() error {

	// 0. gather application and host infos +++
	// 1. gather incoming traffic, errors, latency per application +++
	// 2. gather outgoing traffic, errors, latency per application +++
	// 3. gather saturation per application +++
	// 4. make application signals (set application and host)

	// 7. add saturation to host signals

	if utils.IsEmpty(m.options.Prometheus.From) {
		return fmt.Errorf("Prometheus from time is not defined")
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

	hosts, err := m.gatherHostsBySpan(m.options.HostQuery, from, to, span)
	if err != nil {
		return err
	}

	m.info("Gathering applications (%s / %s, span: %s)...", from, to, span)

	applications, err := m.gatherApplicationsBySpan(m.options.AppQuery, from, to, span)
	if err != nil {
		return err
	}

	m.info("Gathering signals (%s / %s, span: %s)...", from, to, span)

	appLabels := utils.MapGetKeyValuesEx(m.options.AppSignalCommonLabels, ";", "=")

	inQueries := make(map[common.SignalKind]string)
	inQueries[common.SignalTraffic] = m.options.AppSignalInTrafficQuery
	inQueries[common.SignalErrors] = m.options.AppSignalInErrorsQuery
	inQueries[common.SignalLatency] = m.options.AppSignalInLatencyQuery

	ins, err := m.gatherSignalsBySpan("apps incoming", inQueries, appLabels, from, to, span)
	if err != nil {
		return err
	}

	measurements := m.createIncomingMeasurements(hosts, applications, ins)

	outQueries := make(map[common.SignalKind]string)
	outQueries[common.SignalTraffic] = m.options.AppSignalOutTrafficQuery
	outQueries[common.SignalErrors] = m.options.AppSignalOutErrorsQuery
	outQueries[common.SignalLatency] = m.options.AppSignalOutLatencyQuery

	outs, err := m.gatherSignalsBySpan("apps outgoing", outQueries, appLabels, from, to, span)
	if err != nil {
		return err
	}

	m.fillOutgoingMeasurements(measurements, hosts, applications, outs)

	appQueries := make(map[common.SignalKind]string)
	appQueries[common.SignalSaturation] = m.options.AppSignalSaturationQuery

	apps, err := m.gatherSignalsBySpan("apps", appQueries, appLabels, from, to, span)
	if err != nil {
		return err
	}
	m.debug(len(apps))

	hostLabels := utils.MapGetKeyValuesEx(m.options.HostSignalCommonLabels, ";", "=")

	hostQueries := make(map[common.SignalKind]string)
	hostQueries[common.SignalSaturation] = m.options.HostSignalSaturationQuery

	hsts, err := m.gatherSignalsBySpan("hosts", hostQueries, hostLabels, from, to, span)
	if err != nil {
		return err
	}
	m.debug(len(hsts))

	return nil
}

func (m *V1Model) Train(wg *sync.WaitGroup) {

	wg.Add(1)
	go func(swg *sync.WaitGroup) {

		defer swg.Done()

		m.debug("Training...")

		when := time.Now()

		err := m.train()
		if err != nil {
			m.error("Training finished with error: %s", err)
			return
		}

		m.debug("Training successfully finished in %s", time.Since(when))
	}(wg)
}

func NewV1Model(options V1ModelOptions, observability *common.Observability) *V1Model {

	return &V1Model{
		options:       options,
		observability: observability,
		logger:        observability.Logs(),
	}
}
