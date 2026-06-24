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

// TestEmitIdentity_HostWeldsAuthorFromBoundID pins the substrate identity axiom
// across the wire under sealed-pen. The daemon side holds a RELAY-ONLY proxy pen
// (no Minter, no identity injection): a daemon cell's behavior leaves
// Sender.ID/ChannelID empty, the proxy relays that empty envelope up, and the
// HOST emitSink Mints a Pen welded to the connection's AUTHENTICATED bound id and
// stamps authorship there. The author is never read from the wire's self-report.
//
// Two cases:
//  1. The daemon relays an envelope with EMPTY identity (the production path now
//     that behavior no longer fills sender/chID). The host Mint(bound_id) welds
//     tool:x and the write commits.
//  2. The daemon self-reports a FOREIGN sender (a hand-stuffed attack). The host
//     pen rejects it FAIL-FAST with HarnessIdentityNotCallerSettable — the
//     substrate-injected identity fields are not caller-settable, so the forged
//     self-report is stopped BEFORE step 4 (it never reaches sender_mismatch).
//
// There is no "honest self-report of the correct identity" case anymore: a daemon
// cell does not author its own identity; it relays empty and the host welds.
func TestEmitIdentity_HostWeldsAuthorFromBoundID(t *testing.T) {
	ch := newClosureHome(t)

	const toolID = actor.ActorID("tool:x")
	const victimID = actor.ActorID("user:alice")
	registerActor(t, ch, toolID, actor.KindTool)
	registerActor(t, ch, victimID, actor.KindHuman)

	// Real home↔daemon link over httptest: the daemon attaches tool:x and gets a
	// real port presence bound to that authenticated id at the handshake.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ch.ServeAttach(w, r, "daemon-1")
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

	// 1. Production path: the daemon relays an envelope with EMPTY identity. The
	//    host emitSink Mints a Pen for the bound id (tool:x) and welds it. Commits.
	relayed := &message.Envelope{
		ID:         "relayed-1",
		TS:         time.Now().UnixMilli(),
		Kind:       message.KindEvent,
		Type:       "tool.note",
		Visibility: message.VisibilityPublic,
		Audience:   message.Audience{victimID},
		Payload:    json.RawMessage(`{}`),
		// Sender.ID + ChannelID intentionally EMPTY — the host pen welds them.
	}
	rres, err := pen.Write(context.Background(), relayed)
	if err != nil {
		t.Fatalf("relayed emit: %v", err)
	}
	if rres.RejectReason != "" {
		t.Fatalf("relayed emit rejected %q, want commit", rres.RejectReason)
	}

	// 2. Attack: the SAME tool:x stream relays an envelope that self-reports a
	//    foreign sender (user:alice). The host pen sees a non-empty
	//    Sender.ID/ChannelID and rejects FAIL-FAST — identity is substrate-
	//    injected, not caller-settable. The forged self-report is stopped before
	//    step 4 (NOT a sender_mismatch — the fail-fast is earlier).
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
	if fres.RejectReason != harness.HarnessIdentityNotCallerSettable {
		t.Fatalf("forged emit verdict = %q, want %q", fres.RejectReason, harness.HarnessIdentityNotCallerSettable)
	}

	// 3. Truth check: only the relayed (host-welded) emit committed; the forged
	//    self-report never landed in the channel log (rejected fail-fast). The
	//    committed envelope carries the HOST-WELDED sender (tool:x), proving the
	//    author came from the authenticated bound id, not the wire.
	deadline := time.Now().Add(2 * time.Second)
	for {
		rows, err := ch.View().ReadAfterSeq(context.Background(), 0, 1000)
		if err != nil {
			t.Fatalf("ReadAfterSeq: %v", err)
		}
		var sawRelayed bool
		for _, row := range rows {
			if row.Envelope.ID == "forged-1" {
				t.Fatalf("forged envelope committed to truth — identity axiom downgraded across the wire")
			}
			if row.Envelope.ID == "relayed-1" {
				sawRelayed = true
				if row.Envelope.Sender.ID != toolID {
					t.Fatalf("relayed emit committed with sender %q, want host-welded %q",
						row.Envelope.Sender.ID, toolID)
				}
				if row.Envelope.ChannelID != closureTestChannelID {
					t.Fatalf("relayed emit committed with channel %q, want host-welded %q",
						row.Envelope.ChannelID, closureTestChannelID)
				}
			}
		}
		if sawRelayed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("relayed emit never committed")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
