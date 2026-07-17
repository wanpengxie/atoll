package link

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/actorrt"
)

type nonceRetryLifecycle struct {
	mu     sync.Mutex
	nonces []string
	calls  int
}

func (h *nonceRetryLifecycle) Fork(context.Context, actorrt.ForkSpec) (actor.ActorID, error) {
	return "", errors.New("unexpected non-nonce Fork path")
}
func (h *nonceRetryLifecycle) forkWithNonce(_ context.Context, _ actorrt.ForkSpec, nonce string) (actor.ActorID, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.calls++
	h.nonces = append(h.nonces, nonce)
	if h.calls == 1 {
		return "", ErrLifecycleTransient
	}
	return "parent/child-fixed", nil
}
func (*nonceRetryLifecycle) DespawnChild(context.Context, actor.ActorID, string) error { return nil }
func (*nonceRetryLifecycle) EndSelf(context.Context) error                             { return nil }

func TestRebindLifecycleHidesTransientAndReusesNonce(t *testing.T) {
	raw := &nonceRetryLifecycle{}
	rb := NewRebindableArms(CellArms{Lifecycle: raw})
	child, err := rb.Lifecycle().Fork(context.Background(), actorrt.ForkSpec{Kind: actor.KindAgent, Class: "worker"})
	if err != nil || child != "parent/child-fixed" {
		t.Fatalf("Fork=(%q,%v)", child, err)
	}
	raw.mu.Lock()
	defer raw.mu.Unlock()
	if raw.calls != 2 || len(raw.nonces) != 2 || raw.nonces[0] == "" || raw.nonces[0] != raw.nonces[1] {
		t.Fatalf("calls=%d nonces=%v", raw.calls, raw.nonces)
	}
}
