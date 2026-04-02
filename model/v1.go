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

	AppCommonLabels string

	AppInTrafficQuery string
	AppInErrorsQuery  string
	AppInLatencyQuery string

	AppOutTrafficQuery string
	AppOutErrorsQuery  string
	AppOutLatencyQuery string

	AppSaturationQuery string

	HostCommonLabels    string
	HostSaturationQuery string

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

func (m *V1Model) loadData(q string, from, to time.Time) (V1ModelData, error) {

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

	data := V1ModelData{}

	for _, dr := range res.Data.Result {

		var stamp int64 = 0
		var value float64 = 0.0

		for _, rv := range dr.Values {

			if len(rv) < 2 {
				continue
			}

			// get stamp
			s := fmt.Sprintf("%.0f", rv[0])
			s = strings.ReplaceAll(s, ".", "")
			if len(s) == 13 { // unix millisec
				i, err := strconv.ParseInt(s, 10, 64)
				if err != nil {
					continue
				}
				stamp = i
			} else if len(s) == 10 { // unix sec
				i, err := strconv.ParseInt(s, 10, 64)
				if err != nil {
					continue
				}
				stamp = i * 1000 // unix millisec
			}

			// get value
			s = fmt.Sprintf("%s", rv[1])
			f, err := strconv.ParseFloat(s, 64)
			if err != nil {
				continue
			}
			value = f

			if stamp <= 0 {
				continue
			}

			v := V1ModelDataValue{
				Labels: &dr.Labels,
				Value:  value,
			}

			data[stamp] = append(data[stamp], v)
		}
	}
	return data, nil
}

func (m *V1Model) gatherSignals(queries map[common.SignalKind]string, from, to time.Time) (map[common.SignalKind]V1ModelData, error) {

	gr := &errgroup.Group{}
	mp := &sync.Map{}

	// gather signals
	for k, q := range queries {

		gr.Go(func() error {

			data, err := m.loadData(q, from, to)
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

	m.info("Gathering %s (%s / %s)...", name, from, to)

	if len(labels) > 0 {

		gr := &errgroup.Group{}
		mp := &sync.Map{}
		arr := m.sliceCommonLabels(nil, labels)

		for k, lbs := range arr {

			gr.Go(func() error {

				qrs := m.prepareQueries(queries, lbs)
				for k, q := range qrs {
					m.info("Gathering %s %s started => %s", name, common.SignalKindToString(k), q)
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
					m.debug("Gathering %s finished%s in %s", name, fmt.Sprintf(" (%s)", strings.Join(infos, ", ")), time.Since(when))
				} else {
					m.debug("Gathering %s finished (no data) in %s", name, time.Since(when))
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
			m.info("Gathering %s %s started => %s", name, common.SignalKindToString(k), q)
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
			m.debug("Gathering %s finished%s in %s", name, fmt.Sprintf(" (%s)", strings.Join(infos, ", ")), time.Since(when))
		} else {
			m.debug("Gathering %s finished (no data) in %s", name, time.Since(when))
		}

		signals = append(signals, r)
	}
	return signals, nil
}

func (m *V1Model) gatherSignalsBySpan(ms *common.Measurements, name string,
	queries map[common.SignalKind]string, labels map[string]string,
	from, to time.Time, span time.Duration) error {

	qs := make(map[common.SignalKind]string)
	for i, v := range queries {
		if utils.IsEmpty(v) {
			continue
		}
		qs[i] = v
	}

	if len(qs) == 0 {
		return nil
	}

	tt := make(map[time.Time]time.Time)
	t1 := from

	for t1.Unix() < to.Unix() {

		t2 := t1.Add(span)
		tt[t1] = t2
		t1 = t2
	}

	gr := &errgroup.Group{}
	for t1, t2 := range tt {

		gr.Go(func() error {

			_, err := m.gatherSignalsByQueries(name, qs, labels, t1, t2)
			if err != nil {
				return err
			}

			/*ms.Add()
			for k, v := range qs {

				md
			}*/

			return nil
		})
	}

	err := gr.Wait()
	if err != nil {
		return err
	}
	return nil
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

func (m *V1Model) train() error {

	// 1. gather incoming traffic, errors, latency per application +++
	// 2. create initial application signals based on incoming
	// 3. gather outgoing traffic, errors, latency per application +++
	// 4. add outgoing to application signals, with dependencies
	// 5. add saturation to application signals
	// 6. add hosts to applications
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

	m.info("Gathering (%s / %s, span: %s)...", from, to, span)

	appLabels := utils.MapGetKeyValuesEx(m.options.AppCommonLabels, ";", "=")
	hostLabels := utils.MapGetKeyValuesEx(m.options.HostCommonLabels, ";", "=")

	measurements := &common.Measurements{}

	incomings := make(map[common.SignalKind]string)
	incomings[common.SignalTraffic] = m.options.AppInTrafficQuery
	incomings[common.SignalErrors] = m.options.AppInErrorsQuery
	incomings[common.SignalLatency] = m.options.AppInLatencyQuery

	err := m.gatherSignalsBySpan(measurements, "apps incoming", incomings, appLabels, from, to, span)
	if err != nil {
		return err
	}

	outgoings := make(map[common.SignalKind]string)
	outgoings[common.SignalTraffic] = m.options.AppOutTrafficQuery
	outgoings[common.SignalErrors] = m.options.AppOutErrorsQuery
	outgoings[common.SignalLatency] = m.options.AppOutLatencyQuery

	err = m.gatherSignalsBySpan(measurements, "apps outgoing", outgoings, appLabels, from, to, span)
	if err != nil {
		return err
	}

	apps := make(map[common.SignalKind]string)
	apps[common.SignalSaturation] = m.options.AppSaturationQuery

	err = m.gatherSignalsBySpan(measurements, "apps", apps, appLabels, from, to, span)
	if err != nil {
		return err
	}

	hosts := make(map[common.SignalKind]string)
	hosts[common.SignalSaturation] = m.options.HostSaturationQuery

	err = m.gatherSignalsBySpan(measurements, "hosts", hosts, hostLabels, from, to, span)
	if err != nil {
		return err
	}
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
