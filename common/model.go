package common

import "reflect"

type Model interface {
	Name() string
	Save()
	Load()
	Train()
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

func NewModels() *Models {
	return &Models{}
}
