package ipc

import (
	"context"
	"encoding/json"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/runtime/harness"
)

// hostEnd is a minimal stand-in for the substrate-side port in remote-writer
// tests: it reads KindEmit frames and writes back a KindEmitAck produced by the
// injected sink — exactly the contract actorrt.port.readLoop implements (call
// EmitSink, then ack in receipt order). It lets the remote writer be tested
// against a faithful host without pulling in actorrt.
type hostEnd struct {
	codec *Codec
	sink  func(env message.Envelope) EmitAckPayload
}

func (h *hostEnd) serve(t *testing.T) {
	t.Helper()
	for {
		f, err := h.codec.Read()
		if err != nil {
			return // EOF / closed: stop serving
		}
		if f.Kind != KindEmit {
			continue
		}
		var ep EmitPayload
		if err := json.Unmarshal(f.Payload, &ep); err != nil {
			return
		}
		ack := h.sink(ep.Envelope)
		raw, err := json.Marshal(ack)
		if err != nil {
			return
		}
		if err := h.codec.Write(Frame{Kind: KindEmitAck, Payload: raw}); err != nil {
			return
		}
	}
}

// remotePair wires a RemoteWriter to a hostEnd over an in-memory net.Pipe and
// starts the host serve loop + the remote ack-reader (which feeds KindEmitAck
// frames into the writer's FIFO queue, as the real remote read loop does).
func remotePair(t *testing.T, sink func(env message.Envelope) EmitAckPayload) (*RemoteWriter, func()) {
	t.Helper()
	hostConn, remoteConn := net.Pipe()
	host := &hostEnd{codec: NewCodec(hostConn, hostConn), sink: sink}
	go host.serve(t)

	rcodec := NewCodec(remoteConn, remoteConn)
	rw := NewRemoteWriter(rcodec)
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		for {
			f, err := rcodec.Read()
			if err != nil {
				return
			}
			if f.Kind != KindEmitAck {
				continue
			}
			var ap EmitAckPayload
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
	rw, cleanup := remotePair(t, func(env message.Envelope) EmitAckPayload {
		return EmitAckPayload{EmitResult: EmitResult{MessageID: env.ID + "-durable"}}
	})
	defer cleanup()

	res, err := rw.Write(context.Background(), &message.Envelope{ID: "e1"})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if res.MessageID != message.ID("e1-durable") {
		t.Fatalf("MessageID = %q, want e1-durable", res.MessageID)
	}
	if !res.Accepted() {
		t.Fatalf("want accepted, got reject %q", res.RejectReason)
	}
}

// TestRemoteWriterSeesReject: a rejected write surfaces the same RejectReason a
// local cell would see (verdict, not transport error).
func TestRemoteWriterSeesReject(t *testing.T) {
	t.Parallel()
	rw, cleanup := remotePair(t, func(env message.Envelope) EmitAckPayload {
		return EmitAckPayload{EmitResult: EmitResult{
			MessageID:    env.ID,
			RejectReason: string(harness.HarnessKindInvalid),
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
}

// TestRemoteWriterSeesErr: the host's transport/write error string is rebuilt as
// a Go error on the remote side.
func TestRemoteWriterSeesErr(t *testing.T) {
	t.Parallel()
	rw, cleanup := remotePair(t, func(env message.Envelope) EmitAckPayload {
		return EmitAckPayload{Err: "boom"}
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
	rw, cleanup := remotePair(t, func(env message.Envelope) EmitAckPayload {
		// Verdict echoes the envelope id, so a crossed pairing is detectable.
		return EmitAckPayload{EmitResult: EmitResult{MessageID: env.ID + "-ack"}}
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
		c := NewCodec(hostConn, hostConn)
		for {
			if _, err := c.Read(); err != nil {
				return
			}
			// deliberately never ack
		}
	}()
	rcodec := NewCodec(remoteConn, remoteConn)
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
		c := NewCodec(hostConn, hostConn)
		for {
			if _, err := c.Read(); err != nil {
				return
			}
		}
	}()
	rcodec := NewCodec(remoteConn, remoteConn)
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
