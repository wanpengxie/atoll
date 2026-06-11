package platform_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/platform/internal/link"
	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/runtime/harness"
)

// TestEmitIdentity_WireSelfReportCannotImpersonate pins the substrate identity
// axiom across the wire: the author of an emit is stamped by the basis from the
// connection's AUTHENTICATED bound id, never from the envelope's self-reported
// sender. A stream authenticated as tool:x that emits an envelope claiming
// sender=user:alice falls on harness_sender_mismatch and is NOT committed —
// exactly as a local cell self-reporting a foreign sender would. The local path
// already obeyed this; the port path now obeys it too (the identity axiom does
// not downgrade across the wire).
func TestEmitIdentity_WireSelfReportCannotImpersonate(t *testing.T) {
	ch := newClosureHome(t)

	const toolID = actor.ActorID("tool:x")
	const victimID = actor.ActorID("user:alice")
	registerActor(t, ch, toolID, actor.KindTool)
	registerActor(t, ch, victimID, actor.KindHuman)

	// Real home↔daemon link over httptest: the daemon attaches tool:x and gets a
	// real port presence bound to that authenticated id at the handshake.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ch.Links().Serve(w, r, "daemon-1")
	}))
	defer srv.Close()
	wsURL := "ws" + srv.URL[4:]

	d, err := link.Dial(context.Background(), wsURL, "daemon-1",
		[]link.Declaration{{ActorID: toolID, Kind: actor.KindTool, Binding: actor.BindingEmbedded}}, nil)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = d.Close() }()

	pen, _, err := d.OpenStream(toolID, func(*message.Envelope) error { return nil }, nil)
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	d.Start()

	// 1. Honest emit: the stream self-reports its own authenticated id. Commits.
	honest := &message.Envelope{
		ID:         "honest-1",
		TS:         time.Now().UnixMilli(),
		ChannelID:  closureTestChannelID,
		Kind:       message.KindEvent,
		Type:       "tool.note",
		Sender:     message.Sender{ID: toolID, Kind: actor.KindTool},
		Visibility: message.VisibilityPublic,
		Audience:   message.Audience{victimID},
		Payload:    json.RawMessage(`{}`),
	}
	hres, err := pen.Write(context.Background(), honest)
	if err != nil {
		t.Fatalf("honest emit: %v", err)
	}
	if hres.RejectReason != "" {
		t.Fatalf("honest emit rejected %q, want commit", hres.RejectReason)
	}

	// 2. Impersonation: the SAME tool:x stream emits an envelope self-reporting
	//    sender=user:alice. The basis stamps the caller from the authenticated id
	//    (tool:x), so the harness sender-consistency step sees caller != sender →
	//    harness_sender_mismatch. The self-reported sender carries no authority.
	forged := &message.Envelope{
		ID:         "forged-1",
		TS:         time.Now().UnixMilli(),
		ChannelID:  closureTestChannelID,
		Kind:       message.KindEvent,
		Type:       "tool.note",
		Sender:     message.Sender{ID: victimID, Kind: actor.KindHuman},
		Visibility: message.VisibilityPublic,
		Audience:   message.Audience{toolID},
		Payload:    json.RawMessage(`{}`),
	}
	fres, err := pen.Write(context.Background(), forged)
	if err != nil {
		t.Fatalf("forged emit transport error: %v", err)
	}
	if fres.RejectReason != harness.HarnessSenderMismatch {
		t.Fatalf("forged emit verdict = %q, want %q", fres.RejectReason, harness.HarnessSenderMismatch)
	}

	// 3. Truth check: only the honest emit committed; the forged envelope never
	//    landed in the channel log (rejected, not committed).
	deadline := time.Now().Add(2 * time.Second)
	for {
		rows, err := ch.View().ReadAfterSeq(context.Background(), 0, 1000)
		if err != nil {
			t.Fatalf("ReadAfterSeq: %v", err)
		}
		var sawHonest bool
		for _, row := range rows {
			if row.Envelope.ID == "forged-1" {
				t.Fatalf("forged envelope committed to truth — identity axiom downgraded across the wire")
			}
			if row.Envelope.ID == "honest-1" {
				sawHonest = true
			}
		}
		if sawHonest {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("honest emit never committed")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
