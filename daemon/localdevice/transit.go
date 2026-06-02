// Package localdevice is the v2 device transit on an attached compute: it bridges
// a relay adapter cell (proxyfacade-class) to a local external device (e.g. an
// xhs browser driven out-of-process, or a kimi local worker). The cell→device
// half is the Forward seam (ExternalRequestFunc the adapter calls); the
// device→cell half is Callback, which routes a device's final/lifecycle frame
// back to the owning adapter cell on its goroutine (via the host). See doc.go.
package localdevice

import (
	"context"
	"fmt"
	"sync"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/lib/behavior"
)

// CallbackDeliverer routes a device callback frame to the owning adapter cell
// (daemon/host.Host.DeliverCallbackFrame implements it).
type CallbackDeliverer interface {
	DeliverCallbackFrame(frame behavior.ExternalCallbackFrame) error
}

// Forwarded is one request the relay adapter handed to the device, awaiting the
// device's pickup + eventual callback.
type Forwarded struct {
	FrameID  string
	Self     actor.ActorID
	Envelope *message.Envelope
	Payload  []byte
}

// Transit is the in-process device transit. The external device side (HTTP/ws on
// 127.0.0.1, or an embedded driver) calls Take to pull forwarded requests and
// Callback to return results; the relay adapter calls the Forward seam.
type Transit struct {
	deliver CallbackDeliverer

	mu      sync.Mutex
	pending []Forwarded
	nextID  int
}

// New constructs a transit that routes callbacks through deliver (the host).
func New(deliver CallbackDeliverer) *Transit {
	return &Transit{deliver: deliver}
}

// ForwardFunc returns the ExternalRequestFunc the daemon injects into a relay
// adapter's InstallDeps.Forward (self = the relay actor). It records the request
// for the device and returns a FrameID — the cell→device half of the relay.
func (t *Transit) ForwardFunc(self actor.ActorID) behavior.ExternalRequestFunc {
	return func(_ context.Context, env *message.Envelope, payload behavior.ExternalRequestPayload) (behavior.ExternalRequestResult, error) {
		t.mu.Lock()
		t.nextID++
		id := fmt.Sprintf("frame-%d", t.nextID)
		t.pending = append(t.pending, Forwarded{FrameID: id, Self: self, Envelope: env, Payload: []byte(payload)})
		t.mu.Unlock()
		return behavior.ExternalRequestResult{FrameID: id}, nil
	}
}

// Take drains and returns the forwarded requests awaiting the device (the device
// polls this; a real 127.0.0.1 HTTP endpoint wraps it).
func (t *Transit) Take() []Forwarded {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := t.pending
	t.pending = nil
	return out
}

// Callback routes a device's final/lifecycle frame back to the owning adapter
// cell (device→cell half). The adapter applies it on its goroutine and (for a
// final) calls ctx.Resolve to write the terminal. Returns the cell's verdict.
func (t *Transit) Callback(frame behavior.ExternalCallbackFrame) error {
	if t.deliver == nil {
		return fmt.Errorf("localdevice: no callback deliverer wired")
	}
	return t.deliver.DeliverCallbackFrame(frame)
}
