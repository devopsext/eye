package common

import (
	"reflect"
)

type ModelAfterDetect = func(hashes []Hash)

type Model interface {
	Name() string
	Train(data DataSourceData) error
	Detect(data DataSourceData, after ModelAfterDetect) error
}

type Models struct {
	list      []Model
	notifiers *Notifiers
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

func (ms *Models) OnTrain(data DataSourceData) error {

	var err error
	for _, m := range ms.list {
		e := m.Train(data)
		if e != nil {
			err = e
		}
	}
	return err
}

func (ms *Models) afterDetect(hashes []Hash) {

	list := ms.notifiers.Items()
	for _, n := range list {
		n.Notify(hashes)
	}
}

func (ms *Models) OnDetect(data DataSourceData) error {

	var err error
	for _, m := range ms.list {
		e := m.Detect(data, ms.afterDetect)
		if e != nil {
			err = e
		}
	}
	return err
}

func NewModels(notifiers *Notifiers) *Models {
	return &Models{
		notifiers: notifiers,
	}
}
