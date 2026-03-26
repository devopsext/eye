package model

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/devopsext/eye/common"
	sreCommon "github.com/devopsext/sre/common"
	toolsVendors "github.com/devopsext/tools/vendors"
	"github.com/devopsext/utils"
	"github.com/jinzhu/copier"
)

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

func (m *V1Model) getPrometheusVector(q string) ([]*common.PrometheusResponseDataVector, error) {

	opts := toolsVendors.PrometheusOptions{}
	copier.Copy(&opts, &m.options.Prometheus)
	opts.Query = q

	prom := toolsVendors.NewPrometheus(opts)
	if prom == nil {
		return nil, fmt.Errorf("prometheus cannot create client")
	}

	data, err := prom.Get()
	if err != nil {
		return nil, err
	}

	var res common.PrometheusResponse
	if err := json.Unmarshal(data, &res); err != nil {
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

	return res.Data.Result, nil
}

func (m *V1Model) train() error {

	// 1. gather incoming traffic, errors, latency per application
	// 2. gather outgoing traffic, errors, latency per application

	vector, err := m.getPrometheusVector(m.options.PrometheusAppInTrafficQuery)
	if err != nil {
		return err
	}

	for _, v := range vector {

		m.debug("Vector %d : %v %s", v.Stamp(), v.Value(), v.Labels)
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
