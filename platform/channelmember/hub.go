package channelmember

import (
	"context"
	"errors"
	"sync"

	"github.com/wanpengxie/atoll/protocol/channel"
)

var ErrUnreachable = errors.New("channelmember: opposite organ unavailable")

type Pair struct {
	Host channel.ID
	Body channel.ID
}

func (p Pair) valid() bool { return p.Host != "" && p.Body != "" && p.Host != p.Body }

type Request struct {
	Target          string
	Type            string
	Payload         []byte
	CallerChannel   channel.ID
	CallerActor     string
	CallerRequestID string
	Deadline        int64
	OnProgress      func(Progress)
}

type Progress struct {
	Status  string
	Payload []byte
}

type Response struct{ Payload []byte }
type Endpoint func(context.Context, Request) (Response, error)

type endpoint struct {
	generation uint64
	call       Endpoint
}
type link struct {
	seat     endpoint
	handle   endpoint
	watchers map[uint64]func(bool)
}

// Hub contains only current port bindings. Generation-checked release makes a
// stale incarnation unable to detach its successor.
type Hub struct {
	mu    sync.RWMutex
	next  uint64
	links map[Pair]*link
}

func NewHub() *Hub { return &Hub{links: make(map[Pair]*link)} }

func (h *Hub) AttachSeat(pair Pair, call Endpoint) (func(), error) {
	return h.attach(pair, true, call)
}

func (h *Hub) AttachHandle(pair Pair, call Endpoint) (func(), error) {
	return h.attach(pair, false, call)
}

func (h *Hub) attach(pair Pair, seat bool, call Endpoint) (func(), error) {
	if h == nil || !pair.valid() || call == nil {
		return nil, errors.New("channelmember: invalid attachment")
	}
	h.mu.Lock()
	h.next++
	generation := h.next
	l := h.links[pair]
	if l == nil {
		l = &link{}
		h.links[pair] = l
	}
	if seat {
		l.seat = endpoint{generation: generation, call: call}
	} else {
		l.handle = endpoint{generation: generation, call: call}
	}
	watchers := livenessWatchers(l)
	online := l.handle.call != nil
	h.mu.Unlock()
	if !seat {
		notify(watchers, online)
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			h.mu.Lock()
			l := h.links[pair]
			if l == nil {
				h.mu.Unlock()
				return
			}
			changed := false
			if seat && l.seat.generation == generation {
				l.seat = endpoint{}
			}
			if !seat && l.handle.generation == generation {
				l.handle = endpoint{}
				changed = true
			}
			watchers := livenessWatchers(l)
			if l.seat.call == nil && l.handle.call == nil && len(l.watchers) == 0 {
				delete(h.links, pair)
			}
			h.mu.Unlock()
			if changed {
				notify(watchers, false)
			}
		})
	}, nil
}

// WatchHandle reports whether the body-side Handle is currently attached.
// The returned release is generation-fenced in the same way as port releases.
func (h *Hub) WatchHandle(pair Pair, observe func(bool)) (func(), error) {
	if h == nil || !pair.valid() || observe == nil {
		return nil, errors.New("channelmember: invalid liveness watcher")
	}
	h.mu.Lock()
	h.next++
	generation := h.next
	l := h.links[pair]
	if l == nil {
		l = &link{}
		h.links[pair] = l
	}
	if l.watchers == nil {
		l.watchers = make(map[uint64]func(bool))
	}
	l.watchers[generation] = observe
	online := l.handle.call != nil
	h.mu.Unlock()
	observe(online)
	var once sync.Once
	return func() {
		once.Do(func() {
			h.mu.Lock()
			l := h.links[pair]
			if l != nil {
				delete(l.watchers, generation)
				if l.seat.call == nil && l.handle.call == nil && len(l.watchers) == 0 {
					delete(h.links, pair)
				}
			}
			h.mu.Unlock()
		})
	}, nil
}

func livenessWatchers(l *link) []func(bool) {
	if l == nil || len(l.watchers) == 0 {
		return nil
	}
	out := make([]func(bool), 0, len(l.watchers))
	for _, watcher := range l.watchers {
		out = append(out, watcher)
	}
	return out
}

func notify(watchers []func(bool), online bool) {
	for _, watcher := range watchers {
		watcher(online)
	}
}

// Drive transports a request from A's Handle to H's Seat.
func (h *Hub) Drive(ctx context.Context, pair Pair, req Request) (Response, error) {
	return h.call(ctx, pair, true, req)
}

// Deliver transports a request addressed to H's Seat into A's Handle.
func (h *Hub) Deliver(ctx context.Context, pair Pair, req Request) (Response, error) {
	return h.call(ctx, pair, false, req)
}

func (h *Hub) call(ctx context.Context, pair Pair, seat bool, req Request) (Response, error) {
	if h == nil {
		return Response{}, ErrUnreachable
	}
	h.mu.RLock()
	l := h.links[pair]
	var fn Endpoint
	if l != nil {
		if seat {
			fn = l.seat.call
		} else {
			fn = l.handle.call
		}
	}
	h.mu.RUnlock()
	if fn == nil {
		return Response{}, ErrUnreachable
	}
	return fn(ctx, req)
}
