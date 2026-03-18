package common

import (
	"net/http"
	"reflect"
)

type Handler interface {
	Name() string
}

type HttpHandler interface {
	Handler
	URL() string
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

func (ps *Handlers) Find(name string) Handler {
	for _, p := range ps.list {
		if p.Name() == name {
			return p
		}
	}
	return nil
}

func (ps *Handlers) FindHttpHandler(name string) HttpHandler {
	for _, p := range ps.list {
		hp, ok := p.(HttpHandler)
		if ok && hp.Name() == name {
			return hp
		}
	}
	return nil
}

func NewHandlers() *Handlers {
	return &Handlers{}
}
