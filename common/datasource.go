package common

import (
	"reflect"
	"sync"
)

type DataSource interface {
	Name() string
	Schedule() string
	RunOnSchedule(wg *sync.WaitGroup)
}

type DataSources struct {
	list []DataSource
}

func (ms *DataSources) Items() []DataSource {
	return ms.list
}

func (ms *DataSources) Add(m DataSource) {

	if reflect.ValueOf(m).IsNil() {
		return
	}
	ms.list = append(ms.list, m)
}

func (ms *DataSources) Find(name string) DataSource {
	for _, m := range ms.list {
		if m.Name() == name {
			return m
		}
	}
	return nil
}

func NewDataSources() *DataSources {
	return &DataSources{}
}
