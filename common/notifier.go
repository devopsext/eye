package common

import (
	"reflect"
)

type Notifier interface {
	Name() string
	Notify(hashes []Hash)
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
		if n.Name() == name {
			return n
		}
	}
	return nil
}

func NewNotifiers() *Notifiers {
	return &Notifiers{}
}
