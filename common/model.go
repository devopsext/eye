package common

import (
	"reflect"
	"sync"
)

type Model interface {
	Name() string
	Start(wg *sync.WaitGroup)
	Train(ds DataSource)
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

func (ms *Models) OnData(ds DataSource) {

	for _, m := range ms.list {
		m.Train(ds)
	}
}

func NewModels() *Models {
	return &Models{}
}
