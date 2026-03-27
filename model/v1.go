package model

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/devopsext/eye/common"
	sreCommon "github.com/devopsext/sre/common"
	toolsVendors "github.com/devopsext/tools/vendors"
	"github.com/devopsext/utils"
	"github.com/jinzhu/copier"
)

type V1ModelPrometheusDataValue struct {
	Labels *map[string]string
	Value  float64
}

type V1ModelPrometheusData = map[int64][]V1ModelPrometheusDataValue

type V1ModelOptions struct {
	File                        string
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

	if (res.Data == nil) || (len(res.Data.Result) == 0) {
		return nil, fmt.Errorf("prometheus got no data")
	}

	if !utils.Contains([]string{"vector", "matrix"}, res.Data.ResultType) {
		return nil, fmt.Errorf("prometheus supports only vector and matrix data")
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

	// gather incoming traffic
	traffic, err := m.loadPrometheusData(m.options.PrometheusAppInTrafficQuery)
	if err != nil {
		return err
	}

	// gather incoming errors
	errors, err := m.loadPrometheusData(m.options.PrometheusAppInErrorsQuery)
	if err != nil {
		return err
	}

	// gather incoming latency
	latency, err := m.loadPrometheusData(m.options.PrometheusAppInLatencyQuery)
	if err != nil {
		return err
	}

	m.mergePrometheusData(traffic, errors, latency)

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
