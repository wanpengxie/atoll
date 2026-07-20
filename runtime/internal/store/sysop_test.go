package store

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

func openSysOpTestStore(t *testing.T) *ChannelStores {
	t.Helper()
	cs, err := OpenChannel(context.Background(), "sysop-test", filepath.Join(t.TempDir(), "channel.sqlite"), OpenOptions{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	return cs
}

func sysMeta(anchor, digest string) storespec.SysOpMeta {
	return storespec.SysOpMeta{Anchor: anchor, RequestDigest: digest, Source: storespec.SysOpSourceSystem}
}

func TestSysOpAdmitReplayAndRefConflict(t *testing.T) {
	cs := openSysOpTestStore(t)
	ctx := context.Background()
	first, err := cs.SysOps.Admit(ctx, storespec.AdmitTx{SysOpMeta: sysMeta("op:ref:v1:one", "v1:a"), Principal: "alice"})
	if err != nil || !first.Created || first.ActorID == "" {
		t.Fatalf("first admit=(%+v,%v)", first, err)
	}
	replay, err := cs.SysOps.Admit(ctx, storespec.AdmitTx{SysOpMeta: sysMeta("op:ref:v1:one", "v1:a"), Principal: "alice"})
	if err != nil || replay.ActorID != first.ActorID || replay.Created != first.Created {
		t.Fatalf("replay=(%+v,%v), first=%+v", replay, err, first)
	}
	_, err = cs.SysOps.Admit(ctx, storespec.AdmitTx{SysOpMeta: sysMeta("op:ref:v1:one", "v1:different"), Principal: "alice"})
	var operationErr *channel.OperationError
	if !errors.As(err, &operationErr) || operationErr.Code != channel.ErrCodeRefConflict {
		t.Fatalf("different digest error=%v", err)
	}
	var completed int
	if err := cs.db.QueryRow(`SELECT COUNT(*) FROM messages WHERE correlation_id='op:ref:v1:one' AND type='sysop_completed'`).Scan(&completed); err != nil {
		t.Fatal(err)
	}
	if completed != 1 {
		t.Fatalf("completed events=%d want 1", completed)
	}
}

func TestSysOpDecisiveRefusalIsTerminalAndReplayable(t *testing.T) {
	cs := openSysOpTestStore(t)
	ctx := context.Background()
	in := storespec.AdmitTx{SysOpMeta: storespec.SysOpMeta{
		Anchor: "op:msg:v1:member", RequestDigest: "v1:member", Source: storespec.SysOpSourceMember, Sender: "human:alice",
	}, Principal: "bob"}
	for range 2 {
		_, err := cs.SysOps.Admit(ctx, in)
		var operationErr *channel.OperationError
		if !errors.As(err, &operationErr) || operationErr.Code != channel.ErrCodeNotAcceptedSource || operationErr.Retryable {
			t.Fatalf("refusal=%v", err)
		}
	}
	var started, completed int
	if err := cs.db.QueryRow(`SELECT SUM(type='sysop_started'),SUM(type='sysop_completed') FROM messages WHERE correlation_id=?`, in.Anchor).Scan(&started, &completed); err != nil {
		t.Fatal(err)
	}
	if started != 1 || completed != 1 {
		t.Fatalf("event pair=(%d,%d), want (1,1)", started, completed)
	}
}

func TestSysOpTransientStoreFailureRollsBackPairAndSameRefRetries(t *testing.T) {
	cs := openSysOpTestStore(t)
	ctx := context.Background()
	if _, err := cs.db.Exec(`CREATE TRIGGER fail_alice BEFORE INSERT ON actor_registry WHEN NEW.principal='alice' BEGIN SELECT RAISE(ABORT,'injected'); END`); err != nil {
		t.Fatal(err)
	}
	in := storespec.AdmitTx{SysOpMeta: sysMeta("op:ref:v1:retry", "v1:retry"), Principal: "alice"}
	if _, err := cs.SysOps.Admit(ctx, in); err == nil {
		t.Fatal("injected store failure unexpectedly succeeded")
	}
	var events int
	if err := cs.db.QueryRow(`SELECT COUNT(*) FROM messages WHERE correlation_id=?`, in.Anchor).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if events != 0 {
		t.Fatalf("transient failure left %d event rows", events)
	}
	if _, err := cs.db.Exec(`DROP TRIGGER fail_alice`); err != nil {
		t.Fatal(err)
	}
	result, err := cs.SysOps.Admit(ctx, in)
	if err != nil || !result.Created {
		t.Fatalf("same-ref retry=(%+v,%v)", result, err)
	}
	completed, found, err := cs.SysOps.LookupCompleted(ctx, in.Anchor, in.RequestDigest)
	if err != nil || !found || completed.Operation != "admit" || len(completed.Result) == 0 {
		t.Fatalf("completed lookup=(%+v,%v,%v)", completed, found, err)
	}
}

func TestSysOpAuditEventsAreSystemAuthoredAndHidden(t *testing.T) {
	cs := openSysOpTestStore(t)
	in := storespec.AdmitTx{SysOpMeta: sysMeta("op:ref:v1:audit", "v1:audit"), Principal: "alice"}
	if _, err := cs.SysOps.Admit(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	rows, err := cs.db.Query(`SELECT sender_kind,sender_id,visibility,type FROM messages WHERE correlation_id=? ORDER BY seq`, in.Anchor)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var kind, id, visibility, typ string
		if err := rows.Scan(&kind, &id, &visibility, &typ); err != nil {
			t.Fatal(err)
		}
		if kind != "system" || id != "system" || visibility != "system" || (typ != sysOpStarted && typ != sysOpCompleted) {
			t.Fatalf("audit row=(%s,%s,%s,%s)", kind, id, visibility, typ)
		}
		count++
	}
	if count != 2 {
		t.Fatalf("audit event count=%d", count)
	}
}

func TestSysOpApplyRejectsUnresolvedDaemonPlacement(t *testing.T) {
	cs := openSysOpTestStore(t)
	rendered, err := (channel.RenderedSnapshot{
		Class: "agent", Placement: channel.Placement{Kind: channel.PlacementDaemon}, RenderSeq: 1,
	}).Seal()
	if err != nil {
		t.Fatal(err)
	}
	in := storespec.ApplyTx{
		SysOpMeta: sysMeta("op:ref:v1:invalid-host", "v1:invalid-host"),
		DeclID:    "decl", Rendered: rendered, Authority: channel.AuthorityRealm,
	}
	_, err = cs.SysOps.ApplyDeclVersion(context.Background(), in)
	var operationErr *channel.OperationError
	if !errors.As(err, &operationErr) || operationErr.Code != channel.ErrCodeInvalidDesiredHost || operationErr.Retryable {
		t.Fatalf("apply error=%v", err)
	}
	var started, completed int
	if err := cs.db.QueryRow(`SELECT SUM(type='sysop_started'),SUM(type='sysop_completed') FROM messages WHERE correlation_id=?`, in.Anchor).Scan(&started, &completed); err != nil {
		t.Fatal(err)
	}
	if started != 1 || completed != 1 {
		t.Fatalf("event pair=(%d,%d), want (1,1)", started, completed)
	}
}
