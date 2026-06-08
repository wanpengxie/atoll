package localdevice

import (
	"context"
	"fmt"
	"sync"

	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
)

// Forwarded is one request the relay adapter handed to the local device,
// awaiting the device's pickup and eventual callback.
type Forwarded struct {
	FrameID  string
	Self     actor.ActorID
	Envelope *message.Envelope
	Payload  []byte
}

// Transit is the in-process local device transit. The external device side
// (HTTP/WS on 127.0.0.1) calls Take to pull forwarded requests and Callback to
// return results; the relay adapter cell calls the Forward seam.
type Transit struct {
	deliverer actorrt.Deliverer

	mu      sync.Mutex
	pending []Forwarded
	nextID  int
}

// New constructs a transit that routes dispatch through deliverer.
func New(deliverer actorrt.Deliverer) *Transit {
	return &Transit{deliverer: deliverer}
}

// ForwardFunc returns a function the daemon injects into a relay adapter so the
// adapter can forward requests to the local device. It records the request for
// the device and returns a frame ID.
func (t *Transit) ForwardFunc(self actor.ActorID) func(ctx context.Context, env *message.Envelope, payload []byte) (string, error) {
	return func(_ context.Context, env *message.Envelope, payload []byte) (string, error) {
		t.mu.Lock()
		t.nextID++
		id := fmt.Sprintf("frame-%d", t.nextID)
		t.pending = append(t.pending, Forwarded{
			FrameID:  id,
			Self:     self,
			Envelope: env,
			Payload:  payload,
		})
		t.mu.Unlock()
		return id, nil
	}
}

// Take drains and returns the forwarded requests awaiting the device.
func (t *Transit) Take() []Forwarded {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := t.pending
	t.pending = nil
	return out
}

// Callback routes a device's result back to the owning actor cell by delivering
// an envelope into its mailbox via the Deliverer.
func (t *Transit) Callback(target actor.ActorID, env *message.Envelope) error {
	if t.deliverer == nil {
		return fmt.Errorf("localdevice: no deliverer wired")
	}
	_, err := t.deliverer.Deliver([]actor.ActorID{target}, env)
	return err
}
