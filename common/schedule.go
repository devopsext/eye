package common

import (
	"reflect"
	"sync"
)

type Schedule interface {
	Name() string
	Schedule() string
	Start(wg *sync.WaitGroup)
}

type Schedules struct {
	list []Schedule
}

func (ss *Schedules) Items() []Schedule {
	return ss.list
}

func (ss *Schedules) Add(s ...Schedule) {

	if len(s) == 0 {
		return
	}

	for _, v := range s {

		if reflect.ValueOf(v).IsNil() {
			continue
		}
		ss.list = append(ss.list, v)
	}
}

func (ss *Schedules) AddDatasources(ds *DataSources) {

	if ds == nil {
		return
	}

	for _, v := range ds.Items() {

		if reflect.ValueOf(v).IsNil() {
			continue
		}
		ss.Add(v)
	}
}

func (ss *Schedules) AddGenerators(gs *Generators) {

	if gs == nil {
		return
	}

	for _, v := range gs.Items() {

		if reflect.ValueOf(v).IsNil() {
			continue
		}
		ss.Add(v)
	}
}

func (ss *Schedules) Find(name string) Schedule {

	for _, s := range ss.list {
		if s.Name() == name {
			return s
		}
	}
	return nil
}

func NewSchedules() *Schedules {
	return &Schedules{}
}
