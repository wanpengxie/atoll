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
	regstore "github.com/wanpengxie/atoll/platform/lagoon/internal/store"
	"github.com/wanpengxie/atoll/platform/lagoon/regspec"
	"github.com/wanpengxie/atoll/platform/peerproto"
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

type registrarFactsFunc func(context.Context, channel.ID, actor.ActorID) (channelspec.ActorFacts, bool, error)

func (f registrarFactsFunc) ActorFacts(ctx context.Context, ch channel.ID, id actor.ActorID) (channelspec.ActorFacts, bool, error) {
	return f(ctx, ch, id)
}

type registrarClassStub struct{}

func (registrarClassStub) ValidateConfig(string, json.RawMessage) error { return nil }
func (registrarClassStub) LookupClassKind(string) (actor.Kind, bool) {
	return actor.KindTool, true
}
func (registrarClassStub) LookupClassPlacement(string) (channel.PlacementKind, bool) {
	return channel.PlacementServer, true
}

func (s registrarFactsStub) ActorFacts(context.Context, channel.ID, actor.ActorID) (channelspec.ActorFacts, bool, error) {
	return s.facts, s.found, s.err
}

type registrarSysStub struct {
	actorbase.Sys
	code   string
	detail string
	value  any
}

func (s *registrarSysStub) Reply(_ actorbase.Msg, value any) (message.ID, error) {
	s.value = value
	return "reply", nil
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
	dbPath := t.TempDir() + "/registry.db"
	db, err := sql.Open("sqlite", "file:"+dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE decls (
		id TEXT PRIMARY KEY, name TEXT, description TEXT, owner TEXT, default_class TEXT, config_json TEXT,
		status TEXT, visibility TEXT, created_at INTEGER, updated_at INTEGER)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DROP TABLE decls`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	storage, err := regstore.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	commits := 0
	r := NewRegistrar(&Registry{store: storage, onCommit: func(Change) { commits++ }}, nil, nil)
	_, err = r.editDecl(context.Background(), "root", protocol.C0ChannelID, DeclEdit{ID: "decl"})
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

func TestDeclDescriptionPersistsAndCanBeClearedByEdit(t *testing.T) {
	dbPath := t.TempDir() + "/registry.db"
	db, err := sql.Open("sqlite", "file:"+dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE decls (
		id TEXT PRIMARY KEY, name TEXT, description TEXT, owner TEXT, default_class TEXT, config_json TEXT,
		status TEXT, visibility TEXT, created_at INTEGER, updated_at INTEGER)`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	storage, err := regstore.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	r := NewRegistrar(&Registry{store: storage}, nil, registrarClassStub{})
	created, err := r.registerDecl(context.Background(), "alice", DeclRegister{
		ID: "orders", Name: "Orders", Description: "Create and inspect orders.",
		Class: "mcp", Config: json.RawMessage(`{}`), Visibility: "private",
	})
	if err != nil || created.Description != "Create and inspect orders." {
		t.Fatalf("created=%+v err=%v", created, err)
	}
	empty := ""
	edited, err := r.editDecl(context.Background(), "alice", "source", DeclEdit{ID: "orders", Description: &empty})
	if err != nil || edited.Description != "" {
		t.Fatalf("edited=%+v err=%v", edited, err)
	}
	raw, err := json.Marshal(edited)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "description") {
		t.Fatalf("empty description did not omit field: %s", raw)
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
	dbPath := t.TempDir() + "/empty.db"
	db, err := sql.Open("sqlite", "file:"+dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`PRAGMA user_version`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	storage, err := regstore.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	r := NewRegistrar(&Registry{store: storage}, registrarFactsStub{
		facts: channelspec.ActorFacts{Active: true, Principal: "root", Kind: actor.KindHuman}, found: true,
	}, nil)
	sys := &registrarSysStub{}
	r.handle(sys, registrarMessage(WordChannelGet, `{"channel_id":"c0"}`))
	if sys.code != string(CodeResultUnknown) || !strings.Contains(sys.detail, "no such table") {
		t.Fatalf("failure=(%q,%q), want result_unknown with query detail", sys.code, sys.detail)
	}
}

func TestSvcactorWrappedAgentAttributionIsResolvedByRegistrar(t *testing.T) {
	dbPath := t.TempDir() + "/registry.db"
	db, err := sql.Open("sqlite", "file:"+dbPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		`CREATE TABLE decls (id TEXT PRIMARY KEY, name TEXT, description TEXT, owner TEXT, default_class TEXT, config_json TEXT, status TEXT, visibility TEXT, created_at INTEGER, updated_at INTEGER)`,
		`CREATE TABLE principals (id TEXT PRIMARY KEY, kind TEXT, email TEXT, display_name TEXT, status TEXT, created_at INTEGER)`,
		`INSERT INTO decls VALUES ('agent-decl','agent',NULL,'root','codex','{}','present','private',1,1)`,
		`INSERT INTO principals VALUES ('root','human','root@example.test','Root','present',1)`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	storage, err := regstore.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	r := NewRegistrar(&Registry{store: storage}, registrarFactsFunc(func(_ context.Context, ch channel.ID, id actor.ActorID) (channelspec.ActorFacts, bool, error) {
		switch {
		case ch == protocol.C0ChannelID && id == "svc":
			return channelspec.ActorFacts{Active: true, SourceDeclID: SvcActorDeclID, Kind: actor.KindTool}, true, nil
		case ch == "ordinary" && id == "agent:member":
			return channelspec.ActorFacts{Active: true, SourceDeclID: "agent-decl", Kind: actor.KindAgent}, true, nil
		default:
			return channelspec.ActorFacts{}, false, nil
		}
	}), nil)
	wrapped, err := json.Marshal(map[string]any{
		"origin": peerproto.Origin{Channel: "ordinary", Actor: "agent:member", RequestID: "source-request"},
		"args":   json.RawMessage(`{"unknown_field":true}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	msg := actorbase.NewMsg(actorbase.OriginMailbox, context.Background(), message.Envelope{
		ID: "c0-request", ChannelID: protocol.C0ChannelID, Sender: message.Sender{Kind: actor.KindTool, ID: "svc"},
		Kind: message.KindRequest, Type: string(WordPrincipalMe), Payload: wrapped,
	})
	sys := &registrarSysStub{}
	r.handle(sys, msg)
	reply, ok := sys.value.(Reply)
	if !ok {
		t.Fatalf("registrar reply=%T failure=(%q,%q)", sys.value, sys.code, sys.detail)
	}
	var principal regspec.PrincipalRow
	if err := reply.DecodeValue(&principal); err != nil || principal.ID != "root" {
		t.Fatalf("receiver attribution principal=%+v err=%v", principal, err)
	}
	if reply.Source.ChannelID != "ordinary" || reply.Source.RequestID != "source-request" {
		t.Fatalf("source=%+v", reply.Source)
	}
}
