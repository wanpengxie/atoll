package store_test

import (
	"context"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// closedChannel opens a fresh channel sqlite, then Close()s it so every
// subsequent query/exec/begin surfaces the driver "database is closed" error.
// Close() is an already-available injection point on the public assembly: it
// lets us drive the defensive DB-error branches without touching production
// code or constructing a deliberately-broken DB.
func closedChannel(t *testing.T) *storeHandles {
	t.Helper()
	cs := openTestChannel(t)
	h := &storeHandles{
		Log:        cs.Log,
		Query:      cs.Query,
		Requests:   cs.Requests,
		Registry:   cs.Registry,
		Membership: cs.Membership,
	}
	if err := cs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return h
}

type storeHandles struct {
	Log        storespec.MessageLog
	Query      storespec.MessageQuery
	Requests   storespec.RequestLookup
	Registry   storespec.Registry
	Membership storespec.MembershipControlPlane
}

// Every read/write surface must propagate the DB error rather than swallow it.
func TestClosedDB_RegistryReadsError(t *testing.T) {
	ctx := context.Background()
	h := closedChannel(t)

	if _, _, err := h.Registry.Lookup(ctx, "x"); err == nil {
		t.Error("Lookup on closed DB must error")
	}
	if _, err := h.Registry.Exists(ctx, "x"); err == nil {
		t.Error("Exists on closed DB must error")
	}
	if _, err := h.Registry.ListActive(ctx); err == nil {
		t.Error("ListActive on closed DB must error")
	}
}

func TestClosedDB_MembershipWritesError(t *testing.T) {
	ctx := context.Background()
	h := closedChannel(t)

	if err := h.Membership.Insert(ctx, storespec.Record{ID: "a", Kind: actor.KindAgent, CreatedAt: 1}); err == nil {
		t.Error("Insert on closed DB must error (BeginTx fails)")
	}
	if err := h.Membership.Deregister(ctx, "a", 1); err == nil {
		t.Error("Deregister on closed DB must error")
	}
	if err := h.Membership.ApplyMemberTransitions(ctx,
		[]storespec.MemberActorAdd{{ID: "a", Kind: actor.KindAgent, At: 1}}, nil); err == nil {
		t.Error("ApplyMemberTransitions on closed DB must error (BeginTx fails)")
	}
}

func TestClosedDB_MessageReadsError(t *testing.T) {
	ctx := context.Background()
	h := closedChannel(t)

	if _, err := h.Query.MaxSeq(ctx); err == nil {
		t.Error("MaxSeq on closed DB must error")
	}
	if _, err := h.Query.ReadAfterSeq(ctx, 0, 10); err == nil {
		t.Error("ReadAfterSeq on closed DB must error")
	}
	if _, err := h.Query.OpenRequestsForActor(ctx, "a"); err == nil {
		t.Error("OpenRequestsForActor on closed DB must error")
	}
	if _, err := h.Log.HasFinalResponse(ctx, "p"); err == nil {
		t.Error("HasFinalResponse on closed DB must error")
	}
	if _, _, err := h.Requests.FindByID(ctx, "p"); err == nil {
		t.Error("RequestLookup.FindByID on closed DB must error")
	}
}

func TestClosedDB_AppendError(t *testing.T) {
	ctx := context.Background()
	h := closedChannel(t)
	env := newEnv("m1", message.KindEvent, message.Audience{"x"})
	if _, err := h.Log.Append(ctx, env, false); err == nil {
		t.Error("Append on closed DB must error (BeginTx fails)")
	}
}
