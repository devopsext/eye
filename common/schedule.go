package common

import (
	"reflect"
	"sync"
)

type Schedule interface {
	Name() string
	Schedule() string
	RunOnSchedule(wg *sync.WaitGroup)
}

type Schedules struct {
	list []Schedule
}

func (ss *Schedules) Items() []Schedule {
	return ss.list
}

func (ss *Schedules) Add(s Schedule) {

	if reflect.ValueOf(s).IsNil() {
		return
	}
	ss.list = append(ss.list, s)
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
