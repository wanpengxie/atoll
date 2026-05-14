package coagent

import (
	"context"
	"strings"
	"testing"

	"github.com/coagent-ai/daemon-go/pkg/v4types"
)

// TestDaemonRPC_HappyPath drives the HTTP binding against a hand-rolled
// mock daemon that asserts the wire shape: path, Authorization header,
// JSON body fields, then replies with the L2 §3.6.1 success envelope.
func TestDaemonRPC_HappyPath(t *testing.T) {
	md := newMockDaemon(t, []mockResponse{{
		Status: 200,
		Body: messageSendSuccess{
			ID:            "id-1",
			CorrelationID: "trig-1",
			Kind:          v4types.KindEvent,
		},
	}})
	binding := NewDaemonRPCBinding(DaemonRPCOptions{
		BaseURL:   md.URL(),
		AuthToken: "alice-token",
	})

	exit, stdout, stderr := runWithBinding([]string{"emit", "hello"}, binding)
	if exit != 0 {
		t.Fatalf("expected exit 0, got %d (stderr=%s)", exit, stderr)
	}
	out := decodeSuccess(t, stdout)
	if out.ID != "id-1" || out.Kind != "event" {
		t.Fatalf("unexpected success output: %+v", out)
	}

	rec := md.lastRequest(t)
	if rec.Path != "/api/rpc/message.send" {
		t.Fatalf("unexpected path: %q", rec.Path)
	}
	if rec.Auth != "Bearer alice-token" {
		t.Fatalf("unexpected auth header: %q", rec.Auth)
	}
	if rec.Body.Params.Sender.ID != "alice" {
		t.Fatalf("expected envelope.sender.id=alice, got %q", rec.Body.Params.Sender.ID)
	}
	if rec.Body.Params.Kind != v4types.KindEvent {
		t.Fatalf("expected wire kind=event, got %q", rec.Body.Params.Kind)
	}
	if rec.Body.TriggerCorrelationID != "trig-1" {
		t.Fatalf("expected trigger_correlation_id=trig-1, got %q", rec.Body.TriggerCorrelationID)
	}
}

// TestDaemonRPC_RejectMappedToRejectError verifies HTTP 4xx error
// bodies map cleanly into *RejectError surfaces.
func TestDaemonRPC_RejectMappedToRejectError(t *testing.T) {
	md := newMockDaemon(t, []mockResponse{{
		Status: 401,
		Body: messageSendError{Error: messageSendErrorBody{
			Reason: v4types.HarnessAuthFailed,
			Detail: "token invalid",
		}},
	}})
	binding := NewDaemonRPCBinding(DaemonRPCOptions{
		BaseURL:   md.URL(),
		AuthToken: "wrong-token",
	})

	exit, _, stderr := runWithBinding([]string{"emit", "hello"}, binding)
	if exit != exitReject {
		t.Fatalf("expected exit %d, got %d (stderr=%s)", exitReject, exit, stderr)
	}
	if !strings.Contains(stderr, string(v4types.HarnessAuthFailed)) {
		t.Fatalf("expected auth_failed in stderr, got %q", stderr)
	}
	if !strings.Contains(stderr, "token invalid") {
		t.Fatalf("expected detail in stderr, got %q", stderr)
	}
}

// TestDaemonRPC_SendNoAuthToken verifies the binding omits the
// Authorization header when no token is configured (daemon will then
// reject with auth_failed — the harness's job, not the binding's).
func TestDaemonRPC_SendNoAuthToken(t *testing.T) {
	md := newMockDaemon(t, []mockResponse{{
		Status: 401,
		Body: messageSendError{Error: messageSendErrorBody{
			Reason: v4types.HarnessAuthFailed,
			Detail: "missing Authorization header",
		}},
	}})
	binding := NewDaemonRPCBinding(DaemonRPCOptions{BaseURL: md.URL()})

	_, _, _ = runWithBinding([]string{"emit", "hi"}, binding)

	rec := md.lastRequest(t)
	if rec.Auth != "" {
		t.Fatalf("expected no Authorization header, got %q", rec.Auth)
	}
}

// TestDaemonRPC_DedupeFieldPropagates checks the binding surfaces
// the dedupe flag from the 200 body so callers can observe replay.
func TestDaemonRPC_DedupeFieldPropagates(t *testing.T) {
	md := newMockDaemon(t, []mockResponse{{
		Status: 200,
		Body: messageSendSuccess{
			ID:            "id-1",
			CorrelationID: "trig-1",
			Kind:          v4types.KindEvent,
			Dedupe:        true,
		},
	}})
	binding := NewDaemonRPCBinding(DaemonRPCOptions{BaseURL: md.URL(), AuthToken: "tok"})

	_, stdout, _ := runWithBinding([]string{"emit", "hi"}, binding)
	out := decodeSuccess(t, stdout)
	if !out.Dedupe {
		t.Fatalf("expected dedupe=true, got false")
	}
}

// TestDaemonRPC_UnsupportedLookup confirms LookupRequest /
// ResolveHandlerActorID return (false, nil) so the CLI falls back to
// caller-supplied flags.
func TestDaemonRPC_UnsupportedLookup(t *testing.T) {
	binding := NewDaemonRPCBinding(DaemonRPCOptions{BaseURL: "http://unused"})
	env, ok, err := binding.LookupRequest(context.Background(), "id")
	if err != nil || ok || env != nil {
		t.Fatalf("expected (nil,false,nil), got (%+v,%v,%v)", env, ok, err)
	}
	actor, ok, err := binding.ResolveHandlerActorID(context.Background(), "biz.foo")
	if err != nil || ok || actor != "" {
		t.Fatalf("expected (\"\",false,nil), got (%q,%v,%v)", actor, ok, err)
	}
}
