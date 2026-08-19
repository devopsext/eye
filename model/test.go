package model

import (
	"sync"
	"time"

	"github.com/devopsext/eye/common"
	sreCommon "github.com/devopsext/sre/common"
)

type TestModelOptions struct {
	Schedule     string
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

func (t *TestModel) Schedule() string {
	return t.options.Schedule
}

func (t *TestModel) Train(data common.DataSourceData) error {

	if !t.mu.TryLock() {
		return nil
	}
	defer t.mu.Unlock()

	name := t.Name()
	t.logger.Info("%s: Training...", name)

	when := time.Now()
	d, _ := time.ParseDuration(t.options.WaitInterval)
	time.Sleep(d)

	t.logger.Info("%s: Training finished in %s", name, time.Since(when))
	return nil
}

func (t *TestModel) Start(wg *sync.WaitGroup) {

	name := t.Name()
	t.logger.Info("%s: Starting...", name)

	when := time.Now()
	d, _ := time.ParseDuration(t.options.WaitInterval)
	time.Sleep(d)

	t.logger.Info("%s: Started in %s", name, time.Since(when))
}

func NewTestModel(options TestModelOptions, observability *common.Observability) *TestModel {

	return &TestModel{
		options:       options,
		observability: observability,
		logger:        observability.Logs(),
	}
}
