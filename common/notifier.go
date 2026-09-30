package common

import (
	"reflect"

	"github.com/devopsext/utils"
)

type Notifier interface {
	Name() string
}

type Notifiers struct {
	list []Notifier
}

func (ns *Notifiers) Items() []Notifier {
	return ns.list
}

func (ns *Notifiers) Add(n Notifier) {

	if reflect.ValueOf(n).IsNil() {
		return
	}
	ns.list = append(ns.list, n)
}

func (ns *Notifiers) Find(name string) Notifier {
	for _, n := range ns.list {
		if utils.IsEmpty(n) {
			continue
		}
		if n.Name() == name {
			return n
		}
	}
	return nil
}

func (ns *Notifiers) Subscribers() []ModelAnomalySubscriber {

	r := []ModelAnomalySubscriber{}
	for _, n := range ns.list {
		if utils.IsEmpty(n) {
			continue
		}
		s, ok := n.(ModelAnomalySubscriber)
		if ok {
			r = append(r, s)
		}
	}
	return r
}

func NewNotifiers() *Notifiers {
	return &Notifiers{}
}
