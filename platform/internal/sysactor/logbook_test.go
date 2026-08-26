package sysactor

import (
	"context"
	"errors"
	"testing"

	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
)

func logRow(seq int64, kind message.Kind, sender actor.ActorID, typ string) channelspec.VisibleMessageRow {
	return channelspec.VisibleMessageRow{Seq: seq, Envelope: message.Envelope{ID: message.ID(typ), Kind: kind, Type: typ, Sender: message.Sender{ID: sender}, Payload: []byte(`{"text":"x"}`)}}
}

func fakeRecent(rows []channelspec.VisibleMessageRow) func(context.Context, int) (channelspec.HistoryWindow, error) {
	return func(_ context.Context, turns int) (channelspec.HistoryWindow, error) {
		return channelspec.HistoryWindow{Rows: rows}, nil
	}
}

// The system actor is a thin face: it validates limit as a turn count and
// hands the projected window through unchanged, in ledger order. The turn
// selection itself is platform/home's readVisibleTurnWindow (history_test.go).
func TestLogbookRecentPassesProjectedTurnsThroughInLedgerOrder(t *testing.T) {
	rows := []channelspec.VisibleMessageRow{
		logRow(4, message.KindRequest, "agent:caller:1", "agent.ask"),
		logRow(9, message.KindResponse, "agent:steward:1", "agent.ask"),
		logRow(10, message.KindRequest, "human:root:1", "agent.ask"),
	}
	var asked int
	recent := func(_ context.Context, turns int) (channelspec.HistoryWindow, error) {
		asked = turns
		return channelspec.HistoryWindow{Rows: rows}, nil
	}
	sys := &fakeSys{}
	New(Deps{RecentTurns: recent}).handle(sys, requestMsg("q", message.TypeSystemLogRecent, []byte(`{"limit":7}`)))
	if asked != 7 {
		t.Fatalf("turns asked = %d, want 7 (limit is a turn count)", asked)
	}
	got := sys.replies[0].v.(LogbookRecentResponse).Messages
	if len(got) != 3 || got[0].Seq != 4 || got[2].Seq != 10 {
		t.Fatalf("messages=%+v", got)
	}
}

func TestLogbookRecentLimitIsOneToTwentyTurns(t *testing.T) {
	for _, body := range []string{`{"limit":0}`, `{"limit":21}`, `{}`} {
		sys := &failSys{}
		New(Deps{RecentTurns: fakeRecent(nil)}).handle(sys, requestMsg("q", message.TypeSystemLogRecent, []byte(body)))
		if len(sys.fails) != 1 || sys.fails[0].code != "invalid_args" {
			t.Fatalf("%s: fails=%+v", body, sys.fails)
		}
	}
	sys := &failSys{}
	New(Deps{RecentTurns: fakeRecent(nil)}).handle(sys, requestMsg("q", message.TypeSystemLogRecent, []byte(`{"limit":20}`)))
	if len(sys.replies) != 1 {
		t.Fatalf("limit 20 must be accepted: replies=%d fails=%+v", len(sys.replies), sys.fails)
	}
}

func TestLogbookRecentReportsReaderFailureAsProviderFailed(t *testing.T) {
	sys := &failSys{}
	failing := func(context.Context, int) (channelspec.HistoryWindow, error) {
		return channelspec.HistoryWindow{}, errors.New("boom")
	}
	New(Deps{RecentTurns: failing}).handle(sys, requestMsg("q", message.TypeSystemLogRecent, []byte(`{"limit":5}`)))
	if len(sys.fails) != 1 || sys.fails[0].code != "provider_failed" {
		t.Fatalf("fails=%+v", sys.fails)
	}
}

func TestDescribeIncludesLogbookRecent(t *testing.T) {
	if _, ok := systemManifest().Words[message.TypeSystemLogRecent]; !ok {
		t.Fatal("system.log.recent missing")
	}
}
