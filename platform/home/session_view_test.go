package home

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorcaps"
	"github.com/wanpengxie/atoll/runtime/harness"
)

func sessionRow(seq int64, id, session, word, body string) actorcaps.LedgerRow {
	raw, _ := harness.WrapPayload(harness.Context{Session: session}, json.RawMessage(body))
	return actorcaps.LedgerRow{Seq: seq, Envelope: message.Envelope{ID: message.ID(id), Type: word, Payload: raw}}
}

func TestSessionBuildExpandsNestedBasesAtTheirOwnBoundaries(t *testing.T) {
	s := actorcaps.LedgerSnapshot{HeadSeq: 7, Rows: []actorcaps.LedgerRow{
		sessionRow(1, "main-open", "main", "session.opened", `{}`),
		sessionRow(2, "main-merge", "main", "session.merge", `{}`),
		sessionRow(3, "a-open", "a", "session.opened", `{"base":{"session":"main","at":"main-merge"}}`),
		sessionRow(4, "a-report", "a", "loop.report", `{}`),
		sessionRow(5, "b-open", "b", "session.opened", `{"base":{"session":"a","at":"a-report"}}`),
		sessionRow(6, "main-late", "main", "session.merge", `{}`),
		sessionRow(7, "a-late", "a", "loop.report", `{}`),
	}}
	rows, err := (View{}).BuildSession(t.Context(), s, "b", "b-open")
	if err != nil {
		t.Fatal(err)
	}
	var ids []message.ID
	for _, r := range rows {
		ids = append(ids, r.Envelope.ID)
	}
	if !reflect.DeepEqual(ids, []message.ID{"main-open", "main-merge", "a-open", "a-report", "b-open"}) {
		t.Fatalf("wrong prefix: %v", ids)
	}
	// Later messages cannot change a fixed checkout, even on the target session.
	s.HeadSeq = 8
	s.Rows = append(s.Rows, sessionRow(8, "b-late", "b", "session.reset", `{}`))
	again, err := (View{}).BuildSession(t.Context(), s, "b", "b-open")
	if err != nil || !reflect.DeepEqual(rows, again) {
		t.Fatalf("fixed checkout changed: %v %v", again, err)
	}
}

func TestSessionBuildRejectsInvalidEdges(t *testing.T) {
	for _, tc := range []struct{ name, base string }{
		{"missing", `{"session":"main","at":"missing"}`},
		{"wrong-session", `{"session":"other","at":"main-open"}`},
		{"future", `{"session":"main","at":"late"}`},
		{"cycle", `{"session":"branch"}`},
		{"empty", `{"session":""}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := actorcaps.LedgerSnapshot{HeadSeq: 3, Rows: []actorcaps.LedgerRow{
				sessionRow(1, "main-open", "main", "session.opened", `{}`),
				sessionRow(2, "branch-open", "branch", "session.opened", `{"base":`+tc.base+`}`),
				sessionRow(3, "late", "main", "session.merge", `{}`),
			}}
			if rows, err := (View{}).BuildSession(t.Context(), s, "branch", ""); err == nil || rows != nil {
				t.Fatalf("invalid edge accepted: rows=%v err=%v", rows, err)
			}
		})
	}
}

func TestUnpinnedBaseStopsBeforeOpening(t *testing.T) {
	s := actorcaps.LedgerSnapshot{HeadSeq: 3, Rows: []actorcaps.LedgerRow{
		sessionRow(1, "main-open", "main", "session.opened", `{}`),
		sessionRow(2, "branch-open", "branch", "session.opened", `{"base":{"session":"main"}}`),
		sessionRow(3, "late", "main", "session.merge", `{}`),
	}}
	rows, err := (View{}).BuildSession(t.Context(), s, "branch", "")
	if err != nil || len(rows) != 2 || rows[0].Envelope.ID != "main-open" {
		t.Fatalf("inherited future: rows=%v err=%v", rows, err)
	}
}
