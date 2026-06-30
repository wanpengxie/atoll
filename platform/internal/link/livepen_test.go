package link_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/wanpengxie/ActOS/platform/internal/link"
	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
	"github.com/wanpengxie/ActOS/runtime/harness"
)

// recordPen is a minimal raw harness.Pen: it counts the writes that reach it and
// always accepts. Wrapping it in a livePen lets a test assert which writes the
// incarnation gate let THROUGH to the raw pen versus fenced before it.
type recordPen struct {
	mu sync.Mutex
	n  int
}

func (p *recordPen) Write(_ context.Context, env *message.Envelope) (harness.WriteResult, error) {
	p.mu.Lock()
	p.n++
	seq := p.n
	p.mu.Unlock()
	return harness.WriteResult{MessageID: env.ID, Seq: int64(seq)}, nil
}

func (p *recordPen) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.n
}

// noopLiveActor is a trivial cell — it exists only to be hosted (and despawned).
type noopLiveActor struct{}

func (noopLiveActor) Receive(context.Context, *message.Envelope) error { return nil }

// TestLivePenFencesPostDeathWrite is the death-after-write收口 (B1, cell path): a
// pen welded to an incarnation writes while the incarnation is live, is
// structurally rejected during construction (before go-live), and is rejected
// with ErrWriterNotLive after the incarnation is despawned — proving a capability
// that outlives its incarnation cannot author truth on its behalf.
func TestLivePenFencesPostDeathWrite(t *testing.T) {
	t.Parallel()
	rt, _ := actorrt.New(actorrt.Config{Parent: context.Background()})
	defer rt.StopAll()

	raw := &recordPen{}
	var pen harness.Pen
	var ctorErr error

	// The build closure runs inside Spawn, BEFORE go-live: IsLive(inc)==false, so a
	// write attempted during construction is fenced (the "factory must not write"
	// rule is structural, not a soft convention).
	inc := rt.Spawn("w", func(i actorrt.Incarnation) actorrt.Actor {
		pen = link.NewLivePen(raw, i, rt)
		_, ctorErr = pen.Write(context.Background(), &message.Envelope{ID: "during-ctor"})
		return noopLiveActor{}
	})

	if !errors.Is(ctorErr, link.ErrWriterNotLive) {
		t.Fatalf("construction-time write err = %v, want ErrWriterNotLive (pre-go-live)", ctorErr)
	}
	if raw.count() != 0 {
		t.Fatalf("raw pen saw %d writes during construction, want 0 (fenced before go-live)", raw.count())
	}

	// Now live: the write passes the gate and reaches the raw pen.
	if _, err := pen.Write(context.Background(), &message.Envelope{ID: "while-live"}); err != nil {
		t.Fatalf("live write err = %v, want nil", err)
	}
	if raw.count() != 1 {
		t.Fatalf("raw pen saw %d writes while live, want 1", raw.count())
	}

	// Despawn the incarnation; the welded pen must now fence every write.
	rt.Despawn(inc)
	_, err := pen.Write(context.Background(), &message.Envelope{ID: "after-death"})
	if !errors.Is(err, link.ErrWriterNotLive) {
		t.Fatalf("post-death write err = %v, want ErrWriterNotLive", err)
	}
	if raw.count() != 1 {
		t.Fatalf("raw pen saw %d writes total, want 1 (post-death write fenced before raw)", raw.count())
	}
}
