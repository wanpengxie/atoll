package link_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"sync"
	"testing"

	"github.com/wanpengxie/ActOS/platform/internal/link"
	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
	"github.com/wanpengxie/ActOS/runtime/harness"
	"github.com/wanpengxie/ActOS/runtime/ipc"
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

// attachTestPort connects a port INTO rt over an in-memory net.Pipe (running the
// remote handshake concurrently with Attach) and returns the bound Incarnation.
// It is the port-path analogue of Spawn in the cell test above — the only
// difference is the embodiment is an out-of-process `port`, not an in-process
// cell.
func attachTestPort(t *testing.T, rt *actorrt.Runtime, id actor.ActorID) (actorrt.Incarnation, net.Conn) {
	t.Helper()
	hostConn, remoteConn := net.Pipe()
	codec := ipc.NewCodec(remoteConn, remoteConn)
	hsErr := make(chan error, 1)
	go func() {
		payload, _ := json.Marshal(ipc.HandshakePayload{LeaseID: string(id)})
		if err := codec.Write(ipc.Frame{Kind: ipc.KindHandshake, Payload: payload}); err != nil {
			hsErr <- err
			return
		}
		ack, err := codec.Read()
		if err != nil {
			hsErr <- err
			return
		}
		if ack.Kind != ipc.KindHandshakeAck {
			hsErr <- io.ErrUnexpectedEOF
			return
		}
		hsErr <- nil
	}()
	inc, err := rt.Attach(context.Background(),
		hostConn,
		func(context.Context, actorrt.Incarnation, *message.Envelope) (ipc.EmitResult, error) {
			return ipc.EmitResult{}, nil
		},
		func(string) (actor.ActorID, error) { return id, nil },
	)
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	if e := <-hsErr; e != nil {
		t.Fatalf("remote handshake: %v", e)
	}
	return inc, remoteConn
}

// TestLivePenFencesPostDeathWrite_PortPath is the death-after-write收口 on the
// PORT path (§3.C1): a livePen welded to an out-of-process port's Incarnation
// writes while the port is the live embodiment, then — once a same-id reattach
// REPLACES that port (the predecessor incarnation is stopped) — fences every
// write with ErrWriterNotLive. It is the message-plane parity of the cell test:
// the same membrane, gated by pointer identity (ABA-safe), on the wire transport.
func TestLivePenFencesPostDeathWrite_PortPath(t *testing.T) {
	t.Parallel()
	rt, _ := actorrt.New(actorrt.Config{Parent: context.Background()})
	defer rt.StopAll()

	const id = actor.ActorID("remote-1")
	inc1, remote1 := attachTestPort(t, rt, id)
	defer remote1.Close()

	raw := &recordPen{}
	pen := link.NewLivePen(raw, inc1, rt)

	// While inc1 is the live embodiment, the welded pen writes through to raw.
	if _, err := pen.Write(context.Background(), &message.Envelope{ID: "while-live"}); err != nil {
		t.Fatalf("live write err = %v, want nil", err)
	}
	if raw.count() != 1 {
		t.Fatalf("raw pen saw %d writes while live, want 1", raw.count())
	}

	// A second attach for the SAME id replaces the port: Attach stops the
	// predecessor (inc1) before returning, so a pen welded to inc1 is no longer
	// the live incarnation and must fence every emit — an in-flight cross-wire
	// emit from the replaced port cannot author truth on the successor's id.
	_, remote2 := attachTestPort(t, rt, id)
	defer remote2.Close()

	_, err := pen.Write(context.Background(), &message.Envelope{ID: "after-replace"})
	if !errors.Is(err, link.ErrWriterNotLive) {
		t.Fatalf("post-replace write err = %v, want ErrWriterNotLive", err)
	}
	if raw.count() != 1 {
		t.Fatalf("raw pen saw %d writes total, want 1 (post-replace write fenced before raw)", raw.count())
	}
}
