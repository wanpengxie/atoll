package effectcap

import (
	"testing"

	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/harness"
)

func TestResolveOpenRacesWithCuts(t *testing.T) {
	vault := NewVault()
	scope := vault.Mint(message.Anchored("parent", "correlation"), harness.Context{Session: "S", Caller: &harness.Caller{Channel: "c", Actor: "human:a:1"}})
	if got, ok := vault.ResolveOpen(scope); !ok || got.ParentID != "parent" || got.CorrelationID != "correlation" {
		t.Fatalf("open resolve=%+v ok=%v", got, ok)
	}
	snapshot, _ := vault.ResolveOpen(scope)
	app := snapshot.Context
	if app.Session != "S" || app.Caller == nil || app.Caller.Actor != "human:a:1" {
		t.Fatalf("tool scope lost request context: %+v", app)
	}
	vault.Revoke(scope)
	if len(vault.rows) != 0 {
		t.Fatal("revoked request metadata was retained")
	}
	if _, ok := vault.ResolveOpen(scope); ok {
		t.Fatal("revoked scope admitted")
	}
	other := vault.Mint(message.Anchored(message.ID("other"), message.ID("other")), harness.Context{})
	vault.Seal()
	if _, ok := vault.ResolveOpen(other); ok {
		t.Fatal("sealed vault admitted")
	}
	if got := vault.Mint(message.Anchored(message.ID("late"), message.ID("late")), harness.Context{}); got.id != 0 {
		t.Fatal("mint after seal succeeded")
	}
}

func TestScopeContextIsCopiedAtBothBoundaries(t *testing.T) {
	vault := NewVault()
	app := harness.Context{Session: "S", Caller: &harness.Caller{Channel: "c", Actor: "human:a:1"}}
	scope := vault.Mint(message.Anchored("parent", "tree"), app)
	app.Caller.Actor = "human:changed:1"
	first, ok := vault.ResolveOpen(scope)
	if !ok || first.Context.Caller.Actor != "human:a:1" {
		t.Fatal("mint retained caller pointer")
	}
	first.Context.Caller.Actor = "human:changed:2"
	second, ok := vault.ResolveOpen(scope)
	if !ok || second.Context.Caller.Actor != "human:a:1" {
		t.Fatal("resolve exposed retained caller pointer")
	}
}
