package common

import (
	"reflect"
	"sync"
)

type GeneratorValueKind = string

const (
	GeneratorValueKindMin     = "min"
	GeneratorValueKindMax     = "max"
	GeneratorValueKindAverage = "average"
)

type GeneratorValue struct {
	Metric   string
	Labels   map[string]string
	Kind     GeneratorValueKind `yaml:"kind,omitempty"`
	Devation *float64           `yaml:"deviation,omitempty"`
	Value    struct {
		Min *float64 `yaml:",omitempty"`
		Max *float64 `yaml:",omitempty"`
	}
	Disabled bool
	Only     bool
}

type GeneratorValues = map[string]*GeneratorValue

type Generator interface {
	Name() string
	Schedule() string
	Start(wg *sync.WaitGroup)
	SetData(data DataSourceData) error
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

func (gs *Generators) SetData(data DataSourceData) error {

	var err error
	for _, g := range gs.list {
		e := g.SetData(data)
		if e != nil {
			err = e
		}
	}
	return err
}

func NewGenerators() *Generators {
	return &Generators{}
}
