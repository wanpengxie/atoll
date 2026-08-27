package e2e

import (
	"testing"
	"time"
)

// Breaking a wedged channel by hand means being able to close a request you
// did not send — which no member can do directly, because closure authorship
// is a closed set (the receiver, the caller itself, the substrate). The word
// under test is how a member asks the substrate to observe the closure
// instead, and this walks it against a real server.
func TestAMemberCanCloseARequestItDidNotSend(t *testing.T) {
	h := newHarness(t)
	api, ws := rootClient(t, h, map[string]int64{c0ChannelID: 0})
	registrar := findRegistrar(t, ws)

	// An echo tool that waits long enough to still be open when we close it.
	const decl = "e2e-cancel-echo"
	registrarRequest(t, ws, c0ChannelID, registrar, "system.actor.template.create", map[string]any{
		"id": decl, "name": decl, "class": "echo", "config": map[string]any{}, "visibility": "private",
	})
	seated := ws.request(c0ChannelID, "system.member.create", systemActor, map[string]any{"decl_id": decl})
	echoID := stringField(t, seated, "member")

	// Someone ELSE's open request: the system actor sends it, so the human
	// pressing cancel is neither its caller nor its receiver — exactly the
	// shape that used to have no button and no mechanism behind one.
	// submit, not request: this one must still be OPEN when it is closed, so
	// waiting for its terminal here would defeat the whole test.
	requestID := ws.submit(c0ChannelID, "countdown.start", "request", []string{echoID}, map[string]any{"seconds": 60})
	if requestID == "" {
		t.Fatal("the countdown request was not accepted")
	}

	closed := ws.request(c0ChannelID, "system.request.cancel", systemActor, map[string]any{
		"request_id": requestID,
	})
	if got := stringField(t, closed, "request_id"); got != requestID {
		t.Fatalf("cancel answered for %q, want %q", got, requestID)
	}

	// The request really ends, and it ends the way a passed deadline ends it:
	// same word, substrate as author, provenance in the payload rather than in
	// a fourth vocabulary term nobody downstream would know.
	audit := dialWS(t, api.base, api.cookieHeader(), map[string]int64{c0ChannelID: 0})
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case item := <-audit.feed:
			envelope, _ := item["envelope"].(map[string]any)
			if envelope == nil || envelope["kind"] != "response" || envelope["parent_id"] != requestID {
				continue
			}
			body, _ := envelope["payload"].(map[string]any)
			if body["status"] != "failed" {
				continue
			}
			sender, _ := envelope["sender"].(map[string]any)
			if sender["id"] != systemActor {
				t.Fatalf("the terminal was authored by %v, want the substrate", sender["id"])
			}
			if body["reason"] != "unanswered_timeout" {
				t.Fatalf("terminal=%v, want the same word a passed deadline uses", body)
			}
			// cancelled:true separates "somebody stopped this" from "a deadline
			// simply passed", which read identically otherwise.
			if body["cancelled"] != true {
				t.Fatalf("terminal=%v, want it marked a deliberate close", body)
			}
			if body["closed_by"] != "system" || body["requested_by"] == "" {
				t.Fatalf("terminal=%v, want provenance for who observed and who asked", body)
			}
			return
		case <-time.After(2 * time.Second):
		}
	}
	t.Fatal("the request never closed")
}

// Asking twice is not an error to reason about: the second ask meets a
// request that already has its one terminal, and the uniqueness index makes
// that a benign loss rather than a failure the caller must handle.
func TestClosingAnAlreadyClosedRequestIsNotAnError(t *testing.T) {
	h := newHarness(t)
	_, ws := rootClient(t, h, map[string]int64{c0ChannelID: 0})
	registrar := findRegistrar(t, ws)

	const decl = "e2e-cancel-twice"
	registrarRequest(t, ws, c0ChannelID, registrar, "system.actor.template.create", map[string]any{
		"id": decl, "name": decl, "class": "echo", "config": map[string]any{}, "visibility": "private",
	})
	seated := ws.request(c0ChannelID, "system.member.create", systemActor, map[string]any{"decl_id": decl})
	echoID := stringField(t, seated, "member")

	requestID := ws.submit(c0ChannelID, "countdown.start", "request", []string{echoID}, map[string]any{"seconds": 60})
	ws.request(c0ChannelID, "system.request.cancel", systemActor, map[string]any{"request_id": requestID})
	ws.request(c0ChannelID, "system.request.cancel", systemActor, map[string]any{"request_id": requestID})
}

// A request id that is not in this channel is a caller mistake, answered as
// one rather than silently succeeding.
func TestClosingAnUnknownRequestIsRefused(t *testing.T) {
	h := newHarness(t)
	_, ws := rootClient(t, h, map[string]int64{c0ChannelID: 0})

	_, failure, err := ws.tryRequest(c0ChannelID, "system.request.cancel", systemActor, map[string]any{
		"request_id": "no-such-request",
	})
	if err == nil {
		t.Fatal("closing an unknown request was accepted")
	}
	if failure["error_code"] != "bad_payload" {
		t.Fatalf("terminal=%v, want bad_payload", failure)
	}
}
