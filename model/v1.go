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

type V1ModelPrometheusDataValue struct {
	Labels *map[string]string
	Value  float64
}

type V1ModelPrometheusData = map[int64][]V1ModelPrometheusDataValue

type V1ModelOptions struct {
	File                        string
	PrometheusAppCommonLabels   string
	PrometheusAppInTrafficQuery string
	PrometheusAppInErrorsQuery  string
	PrometheusAppInLatencyQuery string
	Prometheus                  toolsVendors.PrometheusOptions
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

func (m *V1Model) preparePrometheusQuery(q string, labels map[string]string) string {

	s := q
	for k, v := range labels {
		s = strings.ReplaceAll(s, fmt.Sprintf(".%s", k), v)
	}
	return s
}

func (m *V1Model) loadPrometheusData(q string) (V1ModelPrometheusData, error) {

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

	data := V1ModelPrometheusData{}

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

			v := V1ModelPrometheusDataValue{
				Labels: &dr.Labels,
				Value:  value,
			}

			data[stamp] = append(data[stamp], v)
		}
	}
	return data, nil
}

func (m *V1Model) gatherSignals(queries map[common.SignalKind]string) (map[common.SignalKind]V1ModelPrometheusData, error) {

	gr := &errgroup.Group{}
	mp := &sync.Map{}

	// gather signals
	for k, q := range queries {

		gr.Go(func() error {

			data, err := m.loadPrometheusData(q)
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

	r := make(map[common.SignalKind]V1ModelPrometheusData)

	for k, _ := range queries {

		v, ok := mp.Load(k)
		if ok {
			d, ok := v.(V1ModelPrometheusData)
			if ok {
				r[k] = d
			}
		}
	}
	return r, nil
}

func (m *V1Model) prepareIncomingQueries(labels map[string]string) map[common.SignalKind]string {

	queries := make(map[common.SignalKind]string)

	qTraffic := m.preparePrometheusQuery(m.options.PrometheusAppInTrafficQuery, labels)
	if !utils.IsEmpty(qTraffic) {
		queries[common.SignalTraffic] = qTraffic
	}

	qErrors := m.preparePrometheusQuery(m.options.PrometheusAppInErrorsQuery, labels)
	if !utils.IsEmpty(qErrors) {
		queries[common.SignalErrors] = qErrors
	}

	qLatency := m.preparePrometheusQuery(m.options.PrometheusAppInLatencyQuery, labels)
	if !utils.IsEmpty(qLatency) {
		queries[common.SignalLatency] = qLatency
	}
	return queries
}

func (m *V1Model) gatherIncomingSignals() ([]map[common.SignalKind]V1ModelPrometheusData, error) {

	when := time.Now()

	signals := []map[common.SignalKind]V1ModelPrometheusData{}

	m.info("Gathering incoming...")

	labels := utils.MapGetKeyValuesEx(m.options.PrometheusAppCommonLabels, ";", "=")
	if len(labels) > 0 {

		arr := m.sliceCommonLabels(nil, labels)

		for n, lbs := range arr {

			queries := m.prepareIncomingQueries(lbs)
			for k, q := range queries {
				m.info("Gathering #%d incoming started %s => %s", n, common.SignalKindToString(k), q)
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
				m.debug("Gathering #%d incoming finished in %s%s", n, time.Since(when), fmt.Sprintf(" %s", strings.Join(infos, ", ")))
			} else {
				m.debug("Gathering #%d incoming finished in %s (no data)", n, time.Since(when))
			}

			signals = append(signals, r)
		}

	} else {

		queries := m.prepareIncomingQueries(nil)
		for k, q := range queries {
			m.info("Gathering incoming started %s => %s", common.SignalKindToString(k), q)
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
			m.debug("Gathering incoming finished in %s%s", time.Since(when), fmt.Sprintf(" %s", strings.Join(infos, ", ")))
		} else {
			m.debug("Gathering incoming finished in %s (no data)", time.Since(when))
		}

		signals = append(signals, r)
	}
	return signals, nil
}

func (m *V1Model) mergePrometheusData(traffic, errors, latency V1ModelPrometheusData) {
	//
}

func (m *V1Model) train() error {

	// 1. gather incoming traffic, errors, latency per application
	// 2. create initial application signals based on incoming
	// 3. gather outgoing traffic, errors, latency per application
	// 4. add outgoing to application signals, with dependencies
	// 5. add saturation to application signals
	// 6. add hosts to applications
	// 7. add saturation to host signals

	m.gatherIncomingSignals()

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
