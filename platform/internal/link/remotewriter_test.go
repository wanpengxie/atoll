package link

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/harness"
	"github.com/wanpengxie/atoll/runtime/ipc"
)

// hostEnd is a minimal stand-in for the substrate-side port in remote-writer
// tests: it reads KindEmit frames and writes back a KindEmitAck produced by the
// injected sink — exactly the contract actorrt.port.readLoop implements (call
// EmitSink, then ack in receipt order). It lets the remote writer be tested
// against a faithful host without pulling in actorrt.
type hostEnd struct {
	codec *ipc.Codec
	sink  func(env message.Envelope) ipc.EmitAckPayload
}

func (h *hostEnd) serve(t *testing.T) {
	t.Helper()
	for {
		f, err := h.codec.Read()
		if err != nil {
			return // EOF / closed: stop serving
		}
		if f.Kind != ipc.KindEmit {
			continue
		}
		var ep ipc.EmitPayload
		if err := json.Unmarshal(f.Payload, &ep); err != nil {
			return
		}
		ack := h.sink(ep.Envelope)
		raw, err := json.Marshal(ack)
		if err != nil {
			return
		}
		if err := h.codec.Write(ipc.Frame{Kind: ipc.KindEmitAck, Payload: raw}); err != nil {
			return
		}
	}
}

// remotePair wires a RemoteWriter to a hostEnd over an in-memory net.Pipe and
// starts the host serve loop + the remote ack-reader (which feeds KindEmitAck
// frames into the writer's FIFO queue, as the real remote read loop does).
func remotePair(t *testing.T, sink func(env message.Envelope) ipc.EmitAckPayload) (*RemoteWriter, func()) {
	t.Helper()
	hostConn, remoteConn := net.Pipe()
	host := &hostEnd{codec: ipc.NewCodec(hostConn, hostConn), sink: sink}
	go host.serve(t)

	rcodec := ipc.NewCodec(remoteConn, remoteConn)
	rw := NewRemoteWriter(rcodec)
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		for {
			f, err := rcodec.Read()
			if err != nil {
				return
			}
			if f.Kind != ipc.KindEmitAck {
				continue
			}
			var ap ipc.EmitAckPayload
			if err := json.Unmarshal(f.Payload, &ap); err != nil {
				return
			}
			rw.DeliverAck(ap)
		}
	}()
	cleanup := func() {
		_ = hostConn.Close()
		_ = remoteConn.Close()
		<-readerDone
	}
	return rw, cleanup
}

// TestRemoteWriterSeesVerdict: a remote actor's emit observes the host's
// authoritative WriteResult on the same connection — the writer contract is not
// downgraded across the wire (an accepted write).
func TestRemoteWriterSeesVerdict(t *testing.T) {
	t.Parallel()
	rw, cleanup := remotePair(t, func(env message.Envelope) ipc.EmitAckPayload {
		return ipc.EmitAckPayload{EmitResult: ipc.EmitResult{MessageID: env.ID + "-durable", Seq: 42}}
	})
	defer cleanup()

	res, err := rw.Write(context.Background(), &message.Envelope{ID: "e1"})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if res.MessageID != message.ID("e1-durable") {
		t.Fatalf("MessageID = %q, want e1-durable", res.MessageID)
	}
	// Seq is part of the authoritative verdict — it must not be dropped on the
	// wire (a local writer returns the store-allocated seq; so must the remote).
	if res.Seq != 42 {
		t.Fatalf("Seq = %d, want 42 (verdict downgraded across the wire)", res.Seq)
	}
	if !res.Accepted() {
		t.Fatalf("want accepted, got reject %q", res.RejectReason)
	}
}

// TestRemoteWriterSeesReject: a rejected write surfaces the same RejectReason a
// local cell would see (verdict, not transport error).
func TestRemoteWriterSeesReject(t *testing.T) {
	t.Parallel()
	rw, cleanup := remotePair(t, func(env message.Envelope) ipc.EmitAckPayload {
		return ipc.EmitAckPayload{EmitResult: ipc.EmitResult{
			MessageID:    env.ID,
			RejectReason: string(harness.HarnessKindInvalid),
			RejectDetail: "kind \"\" not in closed set",
		}}
	})
	defer cleanup()

	res, err := rw.Write(context.Background(), &message.Envelope{ID: "e2"})
	if err != nil {
		t.Fatalf("Write returned transport error for a reject verdict: %v", err)
	}
	if res.Accepted() {
		t.Fatal("want rejected verdict")
	}
	if res.RejectReason != harness.HarnessKindInvalid {
		t.Fatalf("RejectReason = %q, want %q", res.RejectReason, harness.HarnessKindInvalid)
	}
	// RejectDetail is part of the verdict too — the human-readable reason a local
	// cell would see must survive the wire intact.
	if res.RejectDetail != "kind \"\" not in closed set" {
		t.Fatalf("RejectDetail = %q, want the full detail (verdict downgraded)", res.RejectDetail)
	}
}

// TestRemoteWriterSeesErr: the host's transport/write error string is rebuilt as
// a Go error on the remote side.
func TestRemoteWriterSeesErr(t *testing.T) {
	t.Parallel()
	rw, cleanup := remotePair(t, func(env message.Envelope) ipc.EmitAckPayload {
		return ipc.EmitAckPayload{ErrorCode: "unknown", ErrorMessage: "boom"}
	})
	defer cleanup()

	_, err := rw.Write(context.Background(), &message.Envelope{ID: "e3"})
	if err == nil || err.Error() != "boom" {
		t.Fatalf("err = %v, want boom", err)
	}
}

// TestRemoteWriterFIFOOrder pins the wire contract: with no per-emit id, acks
// are correlated strictly by receipt order. The host here acks in receipt order
// (the contract), and each concurrent Write must receive ITS OWN verdict — the
// MessageID the host minted from that emit's envelope id. A desynced FIFO queue
// would cross the verdicts.
func TestRemoteWriterFIFOOrder(t *testing.T) {
	t.Parallel()
	rw, cleanup := remotePair(t, func(env message.Envelope) ipc.EmitAckPayload {
		// Verdict echoes the envelope id, so a crossed pairing is detectable.
		return ipc.EmitAckPayload{EmitResult: ipc.EmitResult{MessageID: env.ID + "-ack"}}
	})
	defer cleanup()

	const n = 50
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := message.ID(string(rune('A')+rune(i%26))) + message.ID(itoa(i))
			res, err := rw.Write(context.Background(), &message.Envelope{ID: id})
			if err != nil {
				errs <- err
				return
			}
			if res.MessageID != id+"-ack" {
				errs <- &mismatchErr{want: string(id) + "-ack", got: string(res.MessageID)}
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Fatalf("FIFO verdict crossed: %v", e)
	}
}

// TestRemoteWriterCloseFailsPending: closing the writer (connection died) fails
// every in-flight emit with a transport error rather than blocking forever.
func TestRemoteWriterCloseFailsPending(t *testing.T) {
	t.Parallel()
	// A host that never acks: the emit stays pending until Close.
	hostConn, remoteConn := net.Pipe()
	go func() {
		c := ipc.NewCodec(hostConn, hostConn)
		for {
			if _, err := c.Read(); err != nil {
				return
			}
			// deliberately never ack
		}
	}()
	rcodec := ipc.NewCodec(remoteConn, remoteConn)
	rw := NewRemoteWriter(rcodec)
	defer func() { _ = hostConn.Close(); _ = remoteConn.Close() }()

	done := make(chan error, 1)
	go func() {
		_, err := rw.Write(context.Background(), &message.Envelope{ID: "stuck"})
		done <- err
	}()
	// Let the emit reach the host and the waiter enqueue.
	time.Sleep(20 * time.Millisecond)
	rw.Close()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("pending Write returned nil after Close, want a transport error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("pending Write never unblocked after Close")
	}

	// A Write after Close is rejected immediately.
	if _, err := rw.Write(context.Background(), &message.Envelope{ID: "after"}); err == nil {
		t.Fatal("Write after Close returned nil, want closed error")
	}
}

// TestRemoteWriterCtxCancel: a cancelled context unblocks Write without waiting
// for an ack.
func TestRemoteWriterCtxCancel(t *testing.T) {
	t.Parallel()
	hostConn, remoteConn := net.Pipe()
	go func() {
		c := ipc.NewCodec(hostConn, hostConn)
		for {
			if _, err := c.Read(); err != nil {
				return
			}
		}
	}()
	rcodec := ipc.NewCodec(remoteConn, remoteConn)
	rw := NewRemoteWriter(rcodec)
	defer func() { _ = hostConn.Close(); _ = remoteConn.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := rw.Write(ctx, &message.Envelope{ID: "c"})
		done <- err
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != context.Canceled {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ctx cancel did not unblock Write")
	}
}

// TestRemoteWriterPreSendCancelDoesNotEmit pins behaviour-correction #1: a ctx
// already cancelled BEFORE Write means the emit frame provably never reaches the
// host — the pre-send ctx check short-circuits before codec.Write. The old
// hand-copied writer had NO pre-send check: it wrote the emit onto the wire and
// only then observed ctx.Done(), lying an already-executed emit down as a plain
// cancellation. Now the frame stays off the wire (host sees zero emits) and Write
// returns ctx.Err().
func TestRemoteWriterPreSendCancelDoesNotEmit(t *testing.T) {
	t.Parallel()
	hostConn, remoteConn := net.Pipe()
	var mu sync.Mutex
	emits := 0
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		c := ipc.NewCodec(hostConn, hostConn)
		for {
			f, err := c.Read()
			if err != nil {
				return
			}
			if f.Kind == ipc.KindEmit {
				mu.Lock()
				emits++
				mu.Unlock()
			}
		}
	}()
	rcodec := ipc.NewCodec(remoteConn, remoteConn)
	rw := NewRemoteWriter(rcodec)
	defer func() { _ = hostConn.Close(); _ = remoteConn.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled before Write

	_, err := rw.Write(ctx, &message.Envelope{ID: "pre"})
	if err != context.Canceled {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	// Deterministic zero-frame assertion (codex 终审 P2 hardening): close the
	// remote end and wait for the host reader to drain to EOF — every frame that
	// was ever written is counted before we assert none was.
	_ = remoteConn.Close()
	select {
	case <-readerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("host reader never drained after remote close")
	}
	mu.Lock()
	got := emits
	mu.Unlock()
	if got != 0 {
		t.Fatalf("host received %d emit frames, want 0 (pre-send cancel must not put the emit on the wire)", got)
	}
}

// TestRemoteWriterCloseSettlesInFlightAsClosed pins behaviour-correction #2: a
// teardown with an emit in flight settles that Write with the errRemoteWriterClosed
// sentinel identity (relayCore boxes it as an unconfirmed transport settlement, and
// the adapter surfaces that identity unchanged), and a Write after Close is
// rejected with the same sentinel. The consumer-visible contract is still an error
// carrying errRemoteWriterClosed — the boxing is internal.
func TestRemoteWriterCloseSettlesInFlightAsClosed(t *testing.T) {
	t.Parallel()
	hostConn, remoteConn := net.Pipe()
	// hostGotEmit makes the interleaving DETERMINISTIC (codex 终审 P2): Close
	// fires only after the host has provably READ the emit off the wire, so the
	// Write is genuinely in flight — never a lost race where Close wins before
	// the frame was even enqueued.
	hostGotEmit := make(chan struct{}, 1)
	go func() {
		c := ipc.NewCodec(hostConn, hostConn)
		for {
			f, err := c.Read()
			if err != nil {
				return
			}
			if f.Kind == ipc.KindEmit {
				select {
				case hostGotEmit <- struct{}{}:
				default:
				}
			}
			// read the emit, never ack
		}
	}()
	rcodec := ipc.NewCodec(remoteConn, remoteConn)
	rw := NewRemoteWriter(rcodec)
	defer func() { _ = hostConn.Close(); _ = remoteConn.Close() }()

	done := make(chan error, 1)
	go func() {
		_, err := rw.Write(context.Background(), &message.Envelope{ID: "inflight"})
		done <- err
	}()
	select {
	case <-hostGotEmit:
	case <-time.After(2 * time.Second):
		t.Fatal("host never received the in-flight emit")
	}
	rw.Close()
	select {
	case err := <-done:
		if !errors.Is(err, errRemoteWriterClosed) {
			t.Fatalf("in-flight Write err = %v, want errRemoteWriterClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("in-flight Write never unblocked after Close")
	}
	if _, err := rw.Write(context.Background(), &message.Envelope{ID: "after"}); !errors.Is(err, errRemoteWriterClosed) {
		t.Fatalf("Write after Close err = %v, want errRemoteWriterClosed", err)
	}
}

// --- tiny local helpers (avoid strconv import churn in the FIFO test) -------

type mismatchErr struct{ want, got string }

func (e *mismatchErr) Error() string { return "want " + e.want + " got " + e.got }

func itoa(n int) message.ID {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return message.ID(b)
}
