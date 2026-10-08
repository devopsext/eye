package model

import (
	"sync"
	"time"

	"github.com/devopsext/eye/common"
	sreCommon "github.com/devopsext/sre/common"
)

type TestModelOptions struct {
	Enabled      bool
	WaitInterval string
}

type TestModel struct {
	mu            sync.Mutex
	options       TestModelOptions
	observability *common.Observability
	logger        sreCommon.Logger
}

func (t *TestModel) Name() string {
	return "Test"
}

func (t *TestModel) Enabled() bool {
	return t.options.Enabled
}

func (t *TestModel) Train(data common.DataSourceData) ([]common.Hash, error) {

	hs := []common.Hash{}
	if !t.options.Enabled {
		return hs, nil
	}

	name := t.Name()
	t.logger.Info("%s: Training...", name)

	when := time.Now()
	d, _ := time.ParseDuration(t.options.WaitInterval)
	time.Sleep(d)

	t.logger.Info("%s: Training finished in %s", name, time.Since(when))
	return hs, nil
}

func (t *TestModel) Detect(data common.DataSourceData, onFrame common.ModelOnFrame, onAnomaly common.ModelOnAnomaly) error {

	if !t.options.Enabled {
		return nil
	}

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
