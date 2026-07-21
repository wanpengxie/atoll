package store

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

func admitDurableAgent(t *testing.T, cs *ChannelStores, principal string) actor.ActorID {
	t.Helper()
	res, err := cs.DeclAdmission.AdmitDeclared(context.Background(), storespec.AdmitBundle{
		Kind: actor.KindAgent, Principal: principal, Class: "agent",
		Placement: storespec.NewServerPlacement(), CreatedAt: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("admit durable agent %q: %v", principal, err)
	}
	return res.ID
}

func assertEventPair(t *testing.T, cs *ChannelStores, anchor string) {
	t.Helper()
	var started, completed int
	if err := cs.db.QueryRow(`SELECT SUM(type='sysop_started'),SUM(type='sysop_completed') FROM messages WHERE correlation_id=?`, anchor).Scan(&started, &completed); err != nil {
		t.Fatal(err)
	}
	if started != 1 || completed != 1 {
		t.Fatalf("event pair for %q=(%d,%d), want (1,1)", anchor, started, completed)
	}
}

func memberMetaFor(anchor, digest string) storespec.SysOpMeta {
	return storespec.SysOpMeta{Anchor: anchor, RequestDigest: digest, Source: storespec.SysOpSourceMember, Sender: "human:alice"}
}

func TestSysOpRemoveActorEndsClosureWithEventPairAndReplays(t *testing.T) {
	cs := openSysOpTestStore(t)
	ctx := context.Background()
	target := admitDurableAgent(t, cs, "decl:worker")
	tx := storespec.RemoveTx{
		SysOpMeta:  memberMetaFor("op:msg:remove:1", "d1"),
		Target:     target,
		Reason:     "member_remove",
		DurableIDs: []actor.ActorID{target},
		Envelopes:  []storespec.CascadeEnvelope{{Target: target, Reason: "member_remove", EndedBy: actor.SystemActorID}},
	}
	res, err := cs.SysOps.RemoveActor(ctx, tx)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Removed) != 1 || res.Removed[0] != target {
		t.Fatalf("removed=%v want [%s]", res.Removed, target)
	}
	// Cascade death row + ended event + the operation event pair all in one tx.
	var active, ended int
	if err := cs.db.QueryRow(`SELECT COUNT(*) FROM actor_registry WHERE actor_id=? AND deregistered_at IS NULL`, string(target)).Scan(&active); err != nil {
		t.Fatal(err)
	}
	if active != 0 {
		t.Fatal("target still active after remove")
	}
	if err := cs.db.QueryRow(`SELECT COUNT(*) FROM messages WHERE id=?`, "actor-ended:"+string(target)).Scan(&ended); err != nil {
		t.Fatal(err)
	}
	if ended != 1 {
		t.Fatalf("ended events=%d want 1", ended)
	}
	assertEventPair(t, cs, tx.Anchor)

	// Same-anchor replay is idempotent: no second execution, still exactly one
	// completed terminal, cached result returned.
	replay, err := cs.SysOps.RemoveActor(ctx, tx)
	if err != nil {
		t.Fatal(err)
	}
	if len(replay.Removed) != 1 || replay.Removed[0] != target {
		t.Fatalf("replay removed=%v", replay.Removed)
	}
	assertEventPair(t, cs, tx.Anchor)
}

func TestSysOpRemoveActorAlreadyGoneReturnsSuccessEmptySet(t *testing.T) {
	cs := openSysOpTestStore(t)
	ctx := context.Background()
	// Fresh anchor, empty closure (Home found nothing to end): idempotent
	// success with an empty removed set, event pair still committed.
	res, err := cs.SysOps.RemoveActor(ctx, storespec.RemoveTx{
		SysOpMeta: memberMetaFor("op:msg:remove:gone", "dg"),
		Target:    "agent:vanished:1",
		Reason:    "member_remove",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Removed) != 0 {
		t.Fatalf("removed=%v want empty", res.Removed)
	}
	assertEventPair(t, cs, "op:msg:remove:gone")
}

func TestSysOpRestartActorBumpsEpochMonotonicallyWithEventPair(t *testing.T) {
	cs := openSysOpTestStore(t)
	ctx := context.Background()
	target := admitDurableAgent(t, cs, "decl:restartable")
	first, err := cs.SysOps.RestartActor(ctx, storespec.RestartTx{SysOpMeta: memberMetaFor("op:msg:restart:1", "r1"), Target: target})
	if err != nil || first.Epoch != 1 {
		t.Fatalf("first restart=(%+v,%v) want epoch 1", first, err)
	}
	assertEventPair(t, cs, "op:msg:restart:1")
	second, err := cs.SysOps.RestartActor(ctx, storespec.RestartTx{SysOpMeta: memberMetaFor("op:msg:restart:2", "r2"), Target: target})
	if err != nil || second.Epoch != 2 {
		t.Fatalf("second restart=(%+v,%v) want epoch 2", second, err)
	}
	// Same-anchor replay returns the cached epoch, no further bump, one completed.
	replay, err := cs.SysOps.RestartActor(ctx, storespec.RestartTx{SysOpMeta: memberMetaFor("op:msg:restart:2", "r2"), Target: target})
	if err != nil || replay.Epoch != 2 {
		t.Fatalf("replay restart=(%+v,%v) want cached epoch 2", replay, err)
	}
	assertEventPair(t, cs, "op:msg:restart:2")
	var epoch int64
	if err := cs.db.QueryRow(`SELECT restart_epoch FROM actor_registry WHERE actor_id=?`, string(target)).Scan(&epoch); err != nil {
		t.Fatal(err)
	}
	if epoch != 2 {
		t.Fatalf("durable restart_epoch=%d want 2", epoch)
	}
}

func TestSysOpRestartActorMissingTargetIsDecisive(t *testing.T) {
	cs := openSysOpTestStore(t)
	_, err := cs.SysOps.RestartActor(context.Background(), storespec.RestartTx{SysOpMeta: memberMetaFor("op:msg:restart:absent", "ra"), Target: "agent:absent:1"})
	var operationErr *channel.OperationError
	if !errors.As(err, &operationErr) || operationErr.Code != channel.ErrCodeNotInComposition || operationErr.Retryable {
		t.Fatalf("absent restart error=%v", err)
	}
	assertEventPair(t, cs, "op:msg:restart:absent")
}

func TestSysOpSetDefaultAgentWritesValueRowWithEventPair(t *testing.T) {
	cs := openSysOpTestStore(t)
	ctx := context.Background()
	target := admitDurableAgent(t, cs, "decl:router")
	if _, err := cs.SysOps.SetDefaultAgent(ctx, storespec.SetDefaultTx{SysOpMeta: memberMetaFor("op:msg:default:1", "sd1"), Target: target}); err != nil {
		t.Fatal(err)
	}
	got, ok, err := cs.Routing.DefaultAgent(ctx)
	if err != nil || !ok || got != target {
		t.Fatalf("default agent=(%q,%v,%v) want %s", got, ok, err, target)
	}
	assertEventPair(t, cs, "op:msg:default:1")
	// Same-anchor replay: idempotent, one completed.
	if _, err := cs.SysOps.SetDefaultAgent(ctx, storespec.SetDefaultTx{SysOpMeta: memberMetaFor("op:msg:default:1", "sd1"), Target: target}); err != nil {
		t.Fatal(err)
	}
	assertEventPair(t, cs, "op:msg:default:1")
}

func TestSysOpRestartTransientFailureRollsBackPairLeavingNoResidue(t *testing.T) {
	cs := openSysOpTestStore(t)
	ctx := context.Background()
	target := admitDurableAgent(t, cs, "decl:transient")
	if _, err := cs.db.Exec(`CREATE TRIGGER fail_restart BEFORE UPDATE ON actor_registry WHEN NEW.restart_epoch>0 BEGIN SELECT RAISE(ABORT,'injected'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := cs.SysOps.RestartActor(ctx, storespec.RestartTx{SysOpMeta: memberMetaFor("op:msg:restart:transient", "rt"), Target: target}); err == nil {
		t.Fatal("injected restart failure unexpectedly succeeded")
	}
	var events int
	if err := cs.db.QueryRow(`SELECT COUNT(*) FROM messages WHERE correlation_id=?`, "op:msg:restart:transient").Scan(&events); err != nil {
		t.Fatal(err)
	}
	if events != 0 {
		t.Fatalf("transient restart failure left %d event rows (started not rolled back)", events)
	}
	if _, err := cs.db.Exec(`DROP TRIGGER fail_restart`); err != nil {
		t.Fatal(err)
	}
	// Same anchor retries cleanly to a durable bump after the fault clears.
	res, err := cs.SysOps.RestartActor(ctx, storespec.RestartTx{SysOpMeta: memberMetaFor("op:msg:restart:transient", "rt"), Target: target})
	if err != nil || res.Epoch != 1 {
		t.Fatalf("retry restart=(%+v,%v)", res, err)
	}
}

// Member-source permission is judged inside the value transaction: an unknown
// or deregistered sender is a decisive unauthorized_sender terminal with its
// event pair — no outside pre-read can go stale against the value write.
func TestSysOpMemberWordsJudgeSenderInTransaction(t *testing.T) {
	cs := openSysOpTestStore(t)
	ctx := context.Background()
	target := admitDurableAgent(t, cs, "decl:tx-judged")
	ghost := storespec.SysOpMeta{Anchor: "op:msg:ghost:restart", RequestDigest: "g1", Source: storespec.SysOpSourceMember, Sender: "human:ghost"}
	_, err := cs.SysOps.RestartActor(ctx, storespec.RestartTx{SysOpMeta: ghost, Target: target})
	var operationErr *channel.OperationError
	if !errors.As(err, &operationErr) || operationErr.Code != channel.ErrCodeUnauthorizedSender || operationErr.Retryable {
		t.Fatalf("ghost-sender restart error=%v, want decisive unauthorized_sender", err)
	}
	assertEventPair(t, cs, "op:msg:ghost:restart")
	var epoch int64
	if err := cs.db.QueryRow(`SELECT restart_epoch FROM actor_registry WHERE actor_id=?`, string(target)).Scan(&epoch); err != nil {
		t.Fatal(err)
	}
	if epoch != 0 {
		t.Fatalf("ghost-sender restart mutated the value row (epoch=%d)", epoch)
	}
}

// The member words execute the operation×source admission table inside the
// transaction: a system-source frame is a decisive not_accepted_source terminal
// with its event pair, never a structural write.
func TestSysOpMemberWordsRejectSystemSource(t *testing.T) {
	cs := openSysOpTestStore(t)
	ctx := context.Background()
	target := admitDurableAgent(t, cs, "decl:member-only")
	assertSourceRejected := func(anchor string, err error) {
		t.Helper()
		var operationErr *channel.OperationError
		if !errors.As(err, &operationErr) || operationErr.Code != channel.ErrCodeNotAcceptedSource || operationErr.Retryable {
			t.Fatalf("system-source member word error=%v, want decisive not_accepted_source", err)
		}
		assertEventPair(t, cs, anchor)
	}
	_, err := cs.SysOps.RemoveActor(ctx, storespec.RemoveTx{SysOpMeta: sysMeta("op:ref:src:remove", "sr1"), Target: target})
	assertSourceRejected("op:ref:src:remove", err)
	_, err = cs.SysOps.RestartActor(ctx, storespec.RestartTx{SysOpMeta: sysMeta("op:ref:src:restart", "sr2"), Target: target})
	assertSourceRejected("op:ref:src:restart", err)
	_, err = cs.SysOps.SetDefaultAgent(ctx, storespec.SetDefaultTx{SysOpMeta: sysMeta("op:ref:src:default", "sr3"), Target: target})
	assertSourceRejected("op:ref:src:default", err)
	var epoch int64
	if err := cs.db.QueryRow(`SELECT restart_epoch FROM actor_registry WHERE actor_id=?`, string(target)).Scan(&epoch); err != nil {
		t.Fatal(err)
	}
	if epoch != 0 {
		t.Fatalf("system-source restart mutated the value row (epoch=%d)", epoch)
	}
}

func openSysOpTestStore(t *testing.T) *ChannelStores {
	t.Helper()
	cs, err := OpenChannel(context.Background(), "sysop-test", filepath.Join(t.TempDir(), "channel.sqlite"), OpenOptions{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	// Member-source permission is judged in-transaction against the sender row,
	// so the fixture registers memberMetaFor's sender as an active member.
	if _, err := cs.DeclAdmission.AdmitDeclared(context.Background(), storespec.AdmitBundle{
		ID: "human:alice", Kind: actor.KindHuman, Principal: "alice-op", Class: "human",
		Placement: storespec.NewServerPlacement(), CreatedAt: time.Now().UnixMilli(),
	}); err != nil {
		t.Fatalf("seed member sender: %v", err)
	}
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
