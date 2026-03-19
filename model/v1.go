package model

import (
	"sync"
	"time"

	"github.com/devopsext/eye/common"
	sreCommon "github.com/devopsext/sre/common"
)

type V1ModelOptions struct {
	File string
}

type V1Model struct {
	options       V1ModelOptions
	observability *common.Observability
	logger        sreCommon.Logger
}

func (m *V1Model) Name() string {
	return "V1"
}

func (m *V1Model) train() error {

	return nil
}

func (m *V1Model) Train(wg *sync.WaitGroup) {

	wg.Add(1)
	go func(swg *sync.WaitGroup) {

		defer swg.Done()

		m.logger.Info("Training V1 model...")

		when := time.Now()

		err := m.train()
		if err != nil {
			m.logger.Error("Training V1 model finished with error: %s", err)
		}

		m.logger.Info("Training V1 model successfully finished in %s", time.Since(when))
	}(wg)
}

func NewV1Model(options V1ModelOptions, observability *common.Observability) *V1Model {

	return &V1Model{
		options:       options,
		observability: observability,
		logger:        observability.Logs(),
	}
}
