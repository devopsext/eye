package model

import (
	"sync"
	"time"

	"github.com/devopsext/eye/common"
	sreCommon "github.com/devopsext/sre/common"
)

type TestModelOptions struct {
	WaitInterval string
}

type TestModel struct {
	mu            sync.Mutex
	options       TestModelOptions
	observability *common.Observability
	logger        sreCommon.Logger
}

func (t *TestModel) Name() string {
	return "TestModel"
}

func (t *TestModel) Train(data common.DataSourceData) error {

	name := t.Name()
	t.logger.Info("%s: Training...", name)

	when := time.Now()
	d, _ := time.ParseDuration(t.options.WaitInterval)
	time.Sleep(d)

	t.logger.Info("%s: Training finished in %s", name, time.Since(when))
	return nil
}

func (t *TestModel) Detect(data common.DataSourceData, onFrame common.ModelOnFrame, onAnomaly common.ModelOnAnomaly) error {

	name := t.Name()
	t.logger.Info("%s: Detecting...", name)

	when := time.Now()
	d, _ := time.ParseDuration(t.options.WaitInterval)
	time.Sleep(d)

	t.logger.Info("%s: Detecting finished in %s", name, time.Since(when))
	return nil
}

func NewTestModel(options TestModelOptions, observability *common.Observability) *TestModel {

	return &TestModel{
		options:       options,
		observability: observability,
		logger:        observability.Logs(),
	}
}
