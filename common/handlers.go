package common

import (
	"net/http"
	"reflect"
)

type Handler interface {
	Type() string
}

type HttpHandler interface {
	Handler
	Path() string
	HandleHttpRequest(w http.ResponseWriter, r *http.Request) error
}

type Handlers struct {
	list []Handler
}

func (ps *Handlers) Items() []Handler {
	return ps.list
}

func (ps *Handlers) Add(p Handler) {

	if reflect.ValueOf(p).IsNil() {
		return
	}
	ps.list = append(ps.list, p)
}

func (ps *Handlers) Find(typ string) Handler {
	for _, p := range ps.list {
		if p.Type() == typ {
			return p
		}
	}
	return nil
}

func (ps *Handlers) FindHttpHandler(typ string) HttpHandler {
	for _, p := range ps.list {
		hp, ok := p.(HttpHandler)
		if ok && hp.Type() == typ {
			return hp
		}
	}
	return nil
}

func NewHandlers() *Handlers {
	return &Handlers{}
}
