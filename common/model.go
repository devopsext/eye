package common

import (
	"errors"
	"reflect"
	"time"

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
	Score() int
}

type ModelDataPoint interface {
	Timestamp() time.Time
	Value() float64
	Max() float64
	Min() float64
}

type ModelFrame interface {
	Ident() string
	Timestamp() time.Time
	Verdict() ModelVerdict
	IsEmpty() bool
}

type ModelApplicationFrame interface {
	ModelFrame
	InRequests() ModelDataPoint
	InThroughput() ModelDataPoint
	InLatency() ModelDataPoint
	InErrors() ModelDataPoint
	OutRequests() ModelDataPoint
	OutThroughput() ModelDataPoint
	OutLatency() ModelDataPoint
	OutErrors() ModelDataPoint
	CPU() ModelDataPoint
	Memory() ModelDataPoint
	HostCPU() ModelDataPoint
	HostMemory() ModelDataPoint
}

type ModelFrameSubscriber interface {
	Frame(frame ModelFrame)
}

type ModelOnFrame = func(frame ModelFrame)

type ModelState = int

const (
	ModelStateUnknown = iota
	ModelStateIdle
	ModelStateTraining
	ModelStateDetecting
)

type ModelStateSubscriber interface {
	State(model Model, state ModelState)
}

type ModelSubscribers struct {
	Anomalies []ModelAnomalySubscriber
	Frames    []ModelFrameSubscriber
	States    []ModelStateSubscriber
}

type Model interface {
	Name() string
	Enabled() bool
	Train(data DataSourceData) ([]Hash, error)
	Detect(data DataSourceData, onFrame ModelOnFrame, onAnomaly ModelOnAnomaly) error
}

type Models struct {
	list        []Model
	subscribers *ModelSubscribers
}

func ModelStateToString(state ModelState) string {

	switch state {
	case ModelStateUnknown:
		return "Unknown"
	case ModelStateIdle:
		return "Idle"
	case ModelStateTraining:
		return "Training"
	case ModelStateDetecting:
		return "Detecting"
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

func (ms *Models) state(m Model, state ModelState) {

	if !m.Enabled() {
		return
	}

	for _, s := range ms.subscribers.States {
		if utils.IsEmpty(s) {
			continue
		}
		s.State(m, state)
	}
}

func (ms *Models) states(state ModelState) {

	for _, m := range ms.list {
		ms.state(m, state)
	}
}

func (ms *Models) Train(data DataSourceData) error {

	ms.states(ModelStateTraining)
	defer ms.states(ModelStateIdle)

	all := []error{}

	for _, m := range ms.list {

		if !m.Enabled() {
			continue
		}
		_, e := m.Train(data)
		if e != nil {
			all = append(all, e)
			continue
		}
	}

	return errors.Join(all...)
}

func (ms *Models) Detect(data DataSourceData) error {

	ms.states(ModelStateDetecting)
	defer ms.states(ModelStateIdle)

	all := []error{}
	for _, m := range ms.list {

		if !m.Enabled() {
			continue
		}

		e := m.Detect(data, ms.detectOnFrame, ms.detectOnAnomaly)
		if e != nil {
			all = append(all, e)
			continue
		}
	}
	return errors.Join(all...)
}

func (ms *Models) detectOnFrame(frame ModelFrame) {

	for _, s := range ms.subscribers.Frames {
		if utils.IsEmpty(s) {
			continue
		}
		if frame.IsEmpty() {
			continue
		}
		s.Frame(frame)
	}
}

func (ms *Models) detectOnAnomaly(anomaly ModelAnomaly) {

	for _, s := range ms.subscribers.Anomalies {
		if utils.IsEmpty(s) {
			continue
		}
		s.Anomaly(anomaly)
	}
}

func NewModels(subsrcibers *ModelSubscribers) *Models {

	return &Models{
		subscribers: subsrcibers,
	}
}
