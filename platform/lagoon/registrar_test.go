package lagoon

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/protocol"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	_ "modernc.org/sqlite"
)

type registrarFactsStub struct {
	facts channelspec.ActorFacts
	found bool
	err   error
}

func (s registrarFactsStub) ActorFacts(context.Context, actor.ActorID) (channelspec.ActorFacts, bool, error) {
	return s.facts, s.found, s.err
}

type registrarSysStub struct {
	actorbase.Sys
	code   string
	detail string
}

type principalScanner struct{ kind string }

func (s principalScanner) Scan(dest ...any) error {
	*dest[0].(*string) = "principal"
	*dest[1].(*string) = s.kind
	*dest[2].(*sql.NullString) = sql.NullString{}
	*dest[3].(*sql.NullString) = sql.NullString{}
	*dest[4].(*PrincipalStatus) = PrincipalPresent
	*dest[5].(*int64) = 1
	return nil
}

func (s *registrarSysStub) Fail(_ actorbase.Msg, code, detail string) (message.ID, error) {
	s.code, s.detail = code, detail
	return "failed", nil
}

func registrarMessage(word Word, payload string) actorbase.Msg {
	return actorbase.NewMsg(actorbase.OriginMailbox, context.Background(), message.Envelope{
		ID: "request", ChannelID: channel.ID("c0"), Type: string(word), Kind: message.KindRequest,
		Sender: message.Sender{Kind: actor.KindHuman, ID: "human:root"}, Payload: json.RawMessage(payload),
	})
}

func TestEditDeclPropagatesQueryErrorBeforeInspectingRow(t *testing.T) {
	db, err := sql.Open("sqlite", "file:"+t.TempDir()+"/registry.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE decls (
		id TEXT PRIMARY KEY, name TEXT, owner TEXT, default_class TEXT, config_json TEXT,
		status TEXT, visibility TEXT, created_at INTEGER, updated_at INTEGER)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DROP TABLE decls`); err != nil {
		t.Fatal(err)
	}
	commits := 0
	r := NewRegistrar(&Registry{db: db, onCommit: func(Change) { commits++ }}, nil, nil)
	_, err = r.editDecl(context.Background(), "root", DeclEdit{ID: "decl"})
	if err == nil {
		t.Fatal("decl.edit succeeded after its table was removed")
	}
	var contractErr *Error
	if errors.As(err, &contractErr) && contractErr.Code == CodeNotFound {
		t.Fatalf("query error was disguised as not_found: %v", err)
	}
	if !strings.Contains(err.Error(), "no such table") {
		t.Fatalf("query error was not propagated: %v", err)
	}
	if commits != 0 {
		t.Fatalf("failed write emitted %d onCommit callbacks", commits)
	}
}

func TestDetachAuthorizesBeforeRevealingLocalDeviceReservation(t *testing.T) {
	r := NewRegistrar(&Registry{}, nil, nil)
	_, err := r.detachDevice(context.Background(), "alice", "source", DeviceBinding{ChannelID: "other", DeviceID: protocol.LocalDeviceID})
	var lagoonErr *Error
	if !errors.As(err, &lagoonErr) || lagoonErr.Code != CodePermissionDenied {
		t.Fatalf("cross-channel local detach error=%v, want permission_denied", err)
	}
}

func TestScanPrincipalAcceptsOnlyHumanAndAgentKinds(t *testing.T) {
	for _, kind := range []actor.Kind{actor.KindHuman, actor.KindAgent} {
		row, err := scanPrincipal(principalScanner{kind: string(kind)})
		if err != nil || row.Kind != kind {
			t.Fatalf("kind %q scanned as (%q,%v)", kind, row.Kind, err)
		}
	}
	for _, kind := range []actor.Kind{actor.KindTool, actor.KindSystem} {
		if _, err := scanPrincipal(principalScanner{kind: string(kind)}); err == nil {
			t.Fatalf("principal kind %q was accepted", kind)
		}
	}
}

func TestRegistrarFactsFailureUsesClosedResultUnknownCode(t *testing.T) {
	want := errors.New("facts backend failed")
	r := NewRegistrar(&Registry{}, registrarFactsStub{err: want}, nil)
	sys := &registrarSysStub{}
	r.handle(sys, registrarMessage(WordChannelGet, `{"channel_id":"c0"}`))
	if sys.code != string(CodeResultUnknown) || sys.detail != want.Error() {
		t.Fatalf("failure=(%q,%q), want (%q,%q)", sys.code, sys.detail, CodeResultUnknown, want)
	}
}

func TestRegistrarExecutionFailureUsesClosedResultUnknownCode(t *testing.T) {
	db, err := sql.Open("sqlite", "file:"+t.TempDir()+"/empty.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	r := NewRegistrar(&Registry{db: db}, registrarFactsStub{
		facts: channelspec.ActorFacts{Active: true, Principal: "root", Kind: actor.KindHuman}, found: true,
	}, nil)
	sys := &registrarSysStub{}
	r.handle(sys, registrarMessage(WordChannelGet, `{"channel_id":"c0"}`))
	if sys.code != string(CodeResultUnknown) || !strings.Contains(sys.detail, "no such table") {
		t.Fatalf("failure=(%q,%q), want result_unknown with query detail", sys.code, sys.detail)
	}
}
