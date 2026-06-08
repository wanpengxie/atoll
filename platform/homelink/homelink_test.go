package homelink

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/platform/computebus"
)

// testServer is a minimal WS server that accepts attach, echoes EmitAck for
// each EmitFrame, and can send DispatchFrames.
type testServer struct {
	srv  *httptest.Server
	conn *websocket.Conn // server-side conn
	done chan struct{}
}

func newTestServer(t *testing.T, onAttach func(computebus.AttachRequest) computebus.AttachReply) *testServer {
	t.Helper()
	ts := &testServer{done: make(chan struct{})}
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	ts.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade: %v", err)
		}
		ts.conn = ws

		// Read AttachRequest.
		_, raw, err := ws.ReadMessage()
		if err != nil {
			return
		}
		fr, err := computebus.Decode(raw)
		if err != nil || fr.Attach == nil {
			return
		}
		reply := onAttach(*fr.Attach)
		replyFrame := computebus.Frame{
			Type:  computebus.FrameAttachReply,
			Reply: &reply,
		}
		b, _ := computebus.Encode(replyFrame)
		_ = ws.WriteMessage(websocket.TextMessage, b)

		// Signal that handshake is done.
		close(ts.done)
	}))
	return ts
}

func (ts *testServer) wsURL() string {
	return "ws" + strings.TrimPrefix(ts.srv.URL, "http")
}

func (ts *testServer) close() {
	if ts.conn != nil {
		_ = ts.conn.Close()
	}
	ts.srv.Close()
}

// TestConnect_AttachHandshake proves Connect sends AttachRequest and processes
// AttachReply.
func TestConnect_AttachHandshake(t *testing.T) {
	var gotReq computebus.AttachRequest
	ts := newTestServer(t, func(req computebus.AttachRequest) computebus.AttachReply {
		gotReq = req
		return computebus.AttachReply{ChannelID: "ch-1", Accepted: true}
	})
	defer ts.close()

	decls := []computebus.AttachDeclaration{
		{ActorID: "tool1", Kind: actor.KindTool, Binding: actor.BindingRuntimeOutbound},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	hl, err := Connect(ctx, ts.wsURL(), "key-abc", "compute-1", decls, nil)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer func() { _ = hl.Close() }()

	<-ts.done // wait for handshake

	if gotReq.APIKey != "key-abc" {
		t.Fatalf("APIKey = %q, want key-abc", gotReq.APIKey)
	}
	if gotReq.ComputeID != "compute-1" {
		t.Fatalf("ComputeID = %q, want compute-1", gotReq.ComputeID)
	}
	if len(gotReq.Declarations) != 1 || gotReq.Declarations[0].ActorID != "tool1" {
		t.Fatalf("Declarations = %+v, want [{tool1 ...}]", gotReq.Declarations)
	}
	if hl.ChannelID() != "ch-1" {
		t.Fatalf("ChannelID = %q, want ch-1", hl.ChannelID())
	}
}

// TestConnect_AttachRejected proves Connect returns an error when the home
// rejects the attach.
func TestConnect_AttachRejected(t *testing.T) {
	ts := newTestServer(t, func(_ computebus.AttachRequest) computebus.AttachReply {
		return computebus.AttachReply{Accepted: false, Reason: "bad key"}
	})
	defer ts.close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := Connect(ctx, ts.wsURL(), "bad", "c1", nil, nil)
	if err == nil {
		t.Fatal("expected error on rejected attach")
	}
}

// TestEmit_EmitAckCorrelation proves Emit sends EmitFrame and wakes on the
// matching EmitAck.
func TestEmit_EmitAckCorrelation(t *testing.T) {
	ts := newTestServer(t, func(_ computebus.AttachRequest) computebus.AttachReply {
		return computebus.AttachReply{ChannelID: "ch-1", Accepted: true}
	})
	defer ts.close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	hl, err := Connect(ctx, ts.wsURL(), "k", "c", nil, nil)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer func() { _ = hl.Close() }()

	<-ts.done

	// Start a goroutine on the server side that reads EmitFrame and replies.
	go func() {
		for {
			_, raw, err := ts.conn.ReadMessage()
			if err != nil {
				return
			}
			fr, err := computebus.Decode(raw)
			if err != nil {
				continue
			}
			if fr.Type == computebus.FrameEmit && fr.Emit != nil {
				ack := computebus.Frame{
					Type: computebus.FrameEmitAck,
					Ack: &computebus.EmitAck{
						EmitID:    fr.EmitID,
						MessageID: "msg-written",
					},
				}
				b, _ := computebus.Encode(ack)
				_ = ts.conn.WriteMessage(websocket.TextMessage, b)
			}
		}
	}()

	ack, err := hl.Emit(ctx, computebus.EmitFrame{
		Source:   "tool1",
		Envelope: &message.Envelope{ID: "env-1", Kind: message.KindResponse},
	})
	if err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if ack.MessageID != "msg-written" {
		t.Fatalf("MessageID = %q, want msg-written", ack.MessageID)
	}
}

// TestDispatchFrame_RoutesToHandler proves inbound DispatchFrame triggers the
// onDispatch callback.
func TestDispatchFrame_RoutesToHandler(t *testing.T) {
	ts := newTestServer(t, func(_ computebus.AttachRequest) computebus.AttachReply {
		return computebus.AttachReply{ChannelID: "ch-1", Accepted: true}
	})
	defer ts.close()

	dispatched := make(chan computebus.DispatchFrame, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	hl, err := Connect(ctx, ts.wsURL(), "k", "c", nil, func(df computebus.DispatchFrame) {
		dispatched <- df
	})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer func() { _ = hl.Close() }()

	<-ts.done

	// Server sends a DispatchFrame.
	df := computebus.Frame{
		Type: computebus.FrameDispatch,
		Dispatch: &computebus.DispatchFrame{
			Target:   "tool1",
			Envelope: &message.Envelope{ID: "d-1", Kind: message.KindRequest, Type: "test.do"},
		},
	}
	b, _ := computebus.Encode(df)
	_ = ts.conn.WriteMessage(websocket.TextMessage, b)

	select {
	case got := <-dispatched:
		if got.Target != "tool1" {
			t.Fatalf("target = %q, want tool1", got.Target)
		}
		if got.Envelope.ID != "d-1" {
			t.Fatalf("envelope ID = %q, want d-1", got.Envelope.ID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("DispatchFrame never reached handler")
	}
}

// TestSendDeath proves SendDeath sends a FrameDeath that the server can read.
func TestSendDeath(t *testing.T) {
	ts := newTestServer(t, func(_ computebus.AttachRequest) computebus.AttachReply {
		return computebus.AttachReply{ChannelID: "ch-1", Accepted: true}
	})
	defer ts.close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	hl, err := Connect(ctx, ts.wsURL(), "k", "c", nil, nil)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer func() { _ = hl.Close() }()

	<-ts.done

	// Server reads in background.
	got := make(chan computebus.DeathFrame, 1)
	go func() {
		for {
			_, raw, err := ts.conn.ReadMessage()
			if err != nil {
				return
			}
			fr, err := computebus.Decode(raw)
			if err != nil {
				continue
			}
			if fr.Type == computebus.FrameDeath && fr.Death != nil {
				got <- *fr.Death
				return
			}
		}
	}()

	hl.SendDeath("tool1", "panic: boom")

	select {
	case df := <-got:
		if df.Actor != "tool1" {
			t.Fatalf("Actor = %q, want tool1", df.Actor)
		}
		if df.Cause != "panic: boom" {
			t.Fatalf("Cause = %q, want 'panic: boom'", df.Cause)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("DeathFrame never received by server")
	}
}
