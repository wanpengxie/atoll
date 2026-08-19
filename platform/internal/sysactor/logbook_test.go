package sysactor

import (
	"context"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

type fakeLogbook struct{ rows []storespec.StoredRow }

func (f fakeLogbook) MaxSeq(context.Context) (int64, error) {
	if len(f.rows) == 0 {
		return 0, nil
	}
	return f.rows[len(f.rows)-1].Seq, nil
}
func (f fakeLogbook) ReadAfterSeq(_ context.Context, after int64, limit int) ([]storespec.StoredRow, error) {
	out := []storespec.StoredRow{}
	for _, row := range f.rows {
		if row.Seq > after {
			out = append(out, row)
			if len(out) == limit {
				break
			}
		}
	}
	return out, nil
}
func logRow(seq int64, kind message.Kind, sender actor.ActorID, typ string) storespec.StoredRow {
	return storespec.StoredRow{Seq: seq, Envelope: message.Envelope{ID: message.ID(typ), Kind: kind, Type: typ, Sender: message.Sender{ID: sender}, Payload: []byte(`{"text":"x"}`)}}
}

func TestLogbookRecentFiltersBeforeLimitAndOrdersAscending(t *testing.T) {
	rows := []storespec.StoredRow{}
	for i := int64(1); i <= 8; i++ {
		rows = append(rows, logRow(i, message.KindRequest, "other", "user.text"))
	}
	sys := &fakeSys{}
	New(Deps{Logbook: fakeLogbook{rows}}).handle(sys, requestMsg("q", message.TypeSystemLogRecent, []byte(`{"limit":5}`)))
	got := sys.replies[0].v.(LogbookRecentResponse).Messages
	if len(got) != 5 || got[0].Seq != 4 || got[4].Seq != 8 {
		t.Fatalf("messages=%+v", got)
	}
}
func TestLogbookRecentIncludesCallerAndProtocolTrafficInsideTheGate(t *testing.T) {
	rows := []storespec.StoredRow{logRow(1, message.KindRequest, "agent:caller:1", "user.text"), logRow(2, message.KindRequest, "other", message.TypeSystemLogRecent), logRow(3, message.KindResponse, "system", message.TypeSystemLogRecent), logRow(4, message.KindResponse, "other", "answer")}
	sys := &fakeSys{}
	New(Deps{Logbook: fakeLogbook{rows}}).handle(sys, requestMsg("q", message.TypeSystemLogRecent, []byte(`{"limit":5}`)))
	got := sys.replies[0].v.(LogbookRecentResponse).Messages
	if len(got) != 4 || got[0].Seq != 1 || got[3].Seq != 4 {
		t.Fatalf("messages=%+v", got)
	}
}
func TestLogbookRecentNeverReturnsEvents(t *testing.T) {
	rows := []storespec.StoredRow{logRow(1, message.KindEvent, "other", "agent.turn.started"), logRow(2, message.KindRequest, "other", "user.text")}
	sys := &fakeSys{}
	New(Deps{Logbook: fakeLogbook{rows}}).handle(sys, requestMsg("q", message.TypeSystemLogRecent, []byte(`{"limit":5}`)))
	got := sys.replies[0].v.(LogbookRecentResponse).Messages
	if len(got) != 1 || got[0].Envelope.Kind != message.KindRequest {
		t.Fatalf("messages=%+v", got)
	}
}
func TestDescribeIncludesLogbookRecent(t *testing.T) {
	if _, ok := systemManifest().Words[message.TypeSystemLogRecent]; !ok {
		t.Fatal("system.log.recent missing")
	}
}
