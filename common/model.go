package common

import (
	"reflect"
	"sync"
)

type Model interface {
	Name() string
	Start(wg *sync.WaitGroup)
	Train(data DataSourceData) error
}

type Models struct {
	list []Model
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

func (ms *Models) OnData(data DataSourceData) error {

	var err error
	for _, m := range ms.list {
		e := m.Train(data)
		if e != nil {
			err = e
		}
	}
	return err
}

func NewModels() *Models {
	return &Models{}
}
