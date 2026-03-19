package common

import (
	"reflect"
	"sync"
)

type Model interface {
	Name() string
	Train(wg *sync.WaitGroup)
	Start(wg *sync.WaitGroup)
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

func (ms *Models) Start(wg *sync.WaitGroup) {

	for _, i := range ms.list {

		if i != nil {
			(i).Start(wg)
		}
	}
}

func NewModels() *Models {
	return &Models{}
}
