package common

import (
	"errors"
	"reflect"

	"github.com/devopsext/utils"
)

type ModelAnomaly interface {
	ID() string
	Begin() Stamp
	End() Stamp
}

type ModelAnomalySubscriber interface {
	Anomaly(anomaly ModelAnomaly)
}

type ModelOnAnomaly = func(anomaly ModelAnomaly)

type ModelVerdict interface {
	Name() string
	Description() string
	RootCause() string
	Category() CaseCategory
	Impact() CaseImpact
}

type ModelFrame interface {
	Stamp() Stamp
	Begin() Stamp
	End() Stamp
	Verdict() ModelVerdict
}

type ModelFrameSubscriber interface {
	Frame(frame ModelFrame)
}

type ModelOnFrame = func(frame ModelFrame)

type ModelState = int

const (
	ModelStateUnknown = iota
	ModelStateReady
	ModelStateTraining
	ModelStateTrainError
	ModelStateDetecting
	ModelStateDetectError
)

type ModelStateSubscriber interface {
	State(model Model, state ModelState)
}

type Model interface {
	Name() string
	Enabled() bool
	Train(data DataSourceData) error
	Detect(data DataSourceData, onFrame ModelOnFrame, onAnomaly ModelOnAnomaly) error
}

type Models struct {
	list               []Model
	anomalySubscribers []ModelAnomalySubscriber
	frameSubscribers   []ModelFrameSubscriber
	stateSubscribers   []ModelStateSubscriber
}

func ModelStateToString(state ModelState) string {

	switch state {
	case ModelStateUnknown:
		return "unknown"
	case ModelStateReady:
		return "ready"
	case ModelStateTraining:
		return "training"
	case ModelStateTrainError:
		return "trainerror"
	case ModelStateDetecting:
		return "detecting"
	case ModelStateDetectError:
		return "detecterror"
	default:
		return "unknown"
	}
}

// Models

func (ms *Models) Items() []Model {
	return ms.list
}

func (ms *Models) Add(m Model) {

	if reflect.ValueOf(m).IsNil() {
		return
	}
	ms.list = append(ms.list, m)
}

func (ms *Models) Find(name string) Model {
	for _, m := range ms.list {
		if m.Name() == name {
			return m
		}
	}
	return nil
}

func (ms *Models) state(model Model, state ModelState) {

	for _, s := range ms.stateSubscribers {
		if utils.IsEmpty(s) {
			continue
		}
		s.State(model, state)
	}
}

func (ms *Models) Train(data DataSourceData) error {

	all := []error{}
	for _, m := range ms.list {

		if !m.Enabled() {
			continue
		}

		ms.state(m, ModelStateTraining)
		e := m.Train(data)
		if e != nil {
			all = append(all, e)
			ms.state(m, ModelStateTrainError)
			continue
		}
		ms.state(m, ModelStateReady)
	}
	return errors.Join(all...)
}

func (ms *Models) Detect(data DataSourceData) error {

	all := []error{}
	for _, m := range ms.list {

		if !m.Enabled() {
			continue
		}

		ms.state(m, ModelStateDetecting)
		e := m.Detect(data, ms.frame, ms.anomaly)
		if e != nil {
			all = append(all, e)
			ms.state(m, ModelStateDetectError)
			continue
		}
		ms.state(m, ModelStateReady)
	}
	return errors.Join(all...)
}

func (ms *Models) frame(frame ModelFrame) {

	for _, s := range ms.frameSubscribers {
		if utils.IsEmpty(s) {
			continue
		}
		s.Frame(frame)
	}
}

func (ms *Models) anomaly(anomaly ModelAnomaly) {

	for _, s := range ms.anomalySubscribers {
		if utils.IsEmpty(s) {
			continue
		}
		s.Anomaly(anomaly)
	}
}

func NewModels(
	anomalySubscribers []ModelAnomalySubscriber,
	frameSubscribers []ModelFrameSubscriber,
	stateSubscribers []ModelStateSubscriber) *Models {

	return &Models{
		anomalySubscribers: anomalySubscribers,
		frameSubscribers:   frameSubscribers,
		stateSubscribers:   stateSubscribers,
	}
}
