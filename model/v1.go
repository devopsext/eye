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

	Prometheus toolsVendors.PrometheusOptions
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
	m.logger.Info(fmt.Sprintf("%v: %v", m.Name(), msg), args...)
}

func (m *V1Model) error(msg any, args ...any) {
	m.logger.Error(fmt.Sprintf("%v: %v", m.Name(), msg), args...)
}

func (m *V1Model) debug(msg any, args ...any) {
	m.logger.Debug(fmt.Sprintf("%v: %v", m.Name(), msg), args...)
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

func (m *V1Model) loadData(q string) (V1ModelData, error) {

	opts := toolsVendors.PrometheusOptions{}
	copier.Copy(&opts, &m.options.Prometheus)
	opts.Query = q

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

func (m *V1Model) gatherSignals(queries map[common.SignalKind]string) (map[common.SignalKind]V1ModelData, error) {

	gr := &errgroup.Group{}
	mp := &sync.Map{}

	// gather signals
	for k, q := range queries {

		gr.Go(func() error {

			data, err := m.loadData(q)
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

func (m *V1Model) gatherSignalsByQueries(name string, queries map[common.SignalKind]string, labels map[string]string) ([]map[common.SignalKind]V1ModelData, error) {

	when := time.Now()

	signals := []map[common.SignalKind]V1ModelData{}

	m.info("Gathering %s...", name)

	if len(labels) > 0 {

		gr := &errgroup.Group{}
		mp := &sync.Map{}
		arr := m.sliceCommonLabels(nil, labels)

		for k, lbs := range arr {

			gr.Go(func() error {

				gid := utils.GoRoutineID()

				qrs := m.prepareQueries(queries, lbs)
				for k, q := range qrs {
					m.info("Gathering #%d %s started %s => %s", gid, name, common.SignalKindToString(k), q)
				}

				r, err := m.gatherSignals(qrs)
				if err != nil {
					return err
				}

				infos := []string{}
				for k, v := range r {
					infos = append(infos, fmt.Sprintf("%s: %d", common.SignalKindToString(k), len(v)))
				}
				if len(infos) > 0 {
					m.debug("Gathering #%d %s finished in %s%s", gid, name, time.Since(when), fmt.Sprintf(" %s", strings.Join(infos, ", ")))
				} else {
					m.debug("Gathering #%d %s finished in %s (no data)", gid, name, time.Since(when))
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
			m.info("Gathering %s started %s => %s", name, common.SignalKindToString(k), q)
		}

		r, err := m.gatherSignals(queries)
		if err != nil {
			return nil, err
		}

		infos := []string{}
		for k, v := range r {
			infos = append(infos, fmt.Sprintf("%s: %d", common.SignalKindToString(k), len(v)))
		}
		if len(infos) > 0 {
			m.debug("Gathering %s finished in %s%s", name, time.Since(when), fmt.Sprintf(" %s", strings.Join(infos, ", ")))
		} else {
			m.debug("Gathering %s finished in %s (no data)", name, time.Since(when))
		}

		signals = append(signals, r)
	}
	return signals, nil
}

func (m *V1Model) train() error {

	// 1. gather incoming traffic, errors, latency per application +++
	// 2. create initial application signals based on incoming
	// 3. gather outgoing traffic, errors, latency per application
	// 4. add outgoing to application signals, with dependencies
	// 5. add saturation to application signals
	// 6. add hosts to applications
	// 7. add saturation to host signals

	labels := utils.MapGetKeyValuesEx(m.options.AppCommonLabels, ";", "=")

	incomings := make(map[common.SignalKind]string)
	incomings[common.SignalTraffic] = m.options.AppInTrafficQuery
	incomings[common.SignalErrors] = m.options.AppInErrorsQuery
	incomings[common.SignalLatency] = m.options.AppInLatencyQuery

	outgoings := make(map[common.SignalKind]string)
	outgoings[common.SignalTraffic] = m.options.AppOutTrafficQuery
	outgoings[common.SignalErrors] = m.options.AppOutErrorsQuery
	outgoings[common.SignalLatency] = m.options.AppOutLatencyQuery

	//apps := make(map[common.SignalKind]string)
	//saturations[common.Sat]

	m.gatherSignalsByQueries("incoming", incomings, labels)
	m.gatherSignalsByQueries("outgoing", outgoings, labels)

	//measurements := &common.Measurements{}

	//m.mergePrometheusData(traffic, errors, latency)

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
