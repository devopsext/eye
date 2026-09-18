package common

import (
	"reflect"
	"sync"
)

type GeneratorValue struct {
	Metric string
	Labels map[string]string
	Value  struct {
		Default *float64
		Min     *float64
		Max     *float64
	}
}

type GeneratorValues = map[string]*GeneratorValue

type Generator interface {
	Name() string
	Schedule() string
	Start(wg *sync.WaitGroup)
	Generate(values GeneratorValues) error
}

type Generators struct {
	list []Generator
}

func (gs *Generators) Items() []Generator {
	return gs.list
}

func (gs *Generators) Add(g Generator) {

	if reflect.ValueOf(g).IsNil() {
		return
	}
	gs.list = append(gs.list, g)
}

func (gs *Generators) Find(name string) Generator {
	for _, g := range gs.list {
		if g.Name() == name {
			return g
		}
	}
	return nil
}

func NewGenerators() *Generators {
	return &Generators{}
}
