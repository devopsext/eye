package common

import (
	"net/http"
	"reflect"

	"github.com/devopsext/utils"
)

type Handler interface {
	Name() string
	Start()
}

type HttpHandler interface {
	Handler
	Path() string
	HandleHttpRequest(path string, w http.ResponseWriter, r *http.Request) error
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
	for _, h := range ps.list {
		if utils.IsEmpty(h) {
			continue
		}
		hp, ok := h.(HttpHandler)
		if ok && hp.Name() == name {
			return hp
		}
	}
	return nil
}

func (ps *Handlers) FrameSubscribers() []ModelFrameSubscriber {

	r := []ModelFrameSubscriber{}
	for _, h := range ps.list {
		if utils.IsEmpty(h) {
			continue
		}
		s, ok := h.(ModelFrameSubscriber)
		if ok {
			r = append(r, s)
		}
	}
	return r
}

func (ps *Handlers) StateSubscribers() []ModelStateSubscriber {

	r := []ModelStateSubscriber{}
	for _, h := range ps.list {
		if utils.IsEmpty(h) {
			continue
		}
		s, ok := h.(ModelStateSubscriber)
		if ok {
			r = append(r, s)
		}
	}
	return r
}

func NewHandlers() *Handlers {
	return &Handlers{}
}
