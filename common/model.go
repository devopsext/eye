package common

import (
	"reflect"

	"github.com/devopsext/utils"
)

type ModelAnomaly interface {
	ID() string
	Begin() Stamp
	End() Stamp
}

type ModelFrame interface {
}

type ModelAnomalySubscriber interface {
	Anomaly(anomaly ModelAnomaly)
}

type ModelFrameSubscriber interface {
	Frame(frame ModelFrame)
}

type ModelOnAnomaly = func(anomaly ModelAnomaly)
type ModelOnFrame = func(frame ModelFrame)

type Model interface {
	Name() string
	Train(data DataSourceData) error
	Detect(data DataSourceData, onFrame ModelOnFrame, onAnomaly ModelOnAnomaly) error
}

type Models struct {
	list               []Model
	anomalySubscribers []ModelAnomalySubscriber
	frameSubscribers   []ModelFrameSubscriber
}

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

func (ms *Models) Train(data DataSourceData) error {

	var err error
	for _, m := range ms.list {
		e := m.Train(data)
		if e != nil {
			err = e
		}
	}
	return err
}

func (ms *Models) Detect(data DataSourceData) error {

	var err error
	for _, m := range ms.list {
		e := m.Detect(data, ms.frame, ms.anomaly)
		if e != nil {
			err = e
		}
	}
	return err
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

func NewModels(anomalySubscribers []ModelAnomalySubscriber, frameSubscribers []ModelFrameSubscriber) *Models {
	return &Models{
		anomalySubscribers: anomalySubscribers,
		frameSubscribers:   frameSubscribers,
	}
}
