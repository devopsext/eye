package common

import (
	"reflect"
	"sync"
)

type DataSource interface {
	Name() string
	Schedule() string
	Start(wg *sync.WaitGroup)
	Names() *Names
	Attributes() *Attributes
	Hosts() *Hosts
	Applications() *Applications
	Measurements() *Measurements
}

type DataSources struct {
	list []DataSource
}

type DataSourceOnData = func(ds DataSource)

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
