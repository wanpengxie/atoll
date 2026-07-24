package actorrt

import (
	"context"
	"errors"
	"testing"

	"github.com/wanpengxie/atoll/protocol/message"
)

type stopFirstProbe struct {
	order    *[]string
	dying    chan error
	cancelID message.ID
}

func (p *stopFirstProbe) Receive(context.Context, *message.Envelope) error { return nil }
func (p *stopFirstProbe) Start(context.Context, ActorContext) error {
	*p.order = append(*p.order, "start")
	return nil
}
func (p *stopFirstProbe) Stop(context.Context) error {
	*p.order = append(*p.order, "stop")
	return errors.New("stop result")
}
func (p *stopFirstProbe) Dying() <-chan error { return p.dying }
func (p *stopFirstProbe) CancelRequest(id message.ID) {
	p.cancelID = id
}

func TestWithStopFirstPreservesOptionalActorBehavior(t *testing.T) {
	var order []string
	impl := &stopFirstProbe{order: &order, dying: make(chan error)}
	wrapped := WithStopFirst(impl, func() { order = append(order, "before") })

	starter, ok := wrapped.(Starter)
	if !ok {
		t.Fatal("wrapper lost Starter")
	}
	if err := starter.Start(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
	if wrapped.(DownReporter).Dying() != impl.dying {
		t.Fatal("wrapper lost exact Dying channel")
	}
	wrapped.(RequestCanceller).CancelRequest("request")
	if impl.cancelID != "request" {
		t.Fatal("wrapper lost CancelRequest")
	}
	err := wrapped.(Stopper).Stop(t.Context())
	if err == nil || err.Error() != "stop result" {
		t.Fatalf("Stop error = %v", err)
	}
	if len(order) != 3 || order[0] != "start" || order[1] != "before" || order[2] != "stop" {
		t.Fatalf("order = %v", order)
	}
}
