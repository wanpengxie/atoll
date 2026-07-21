package app

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform/channelhost"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

// seedConvergeDecl inserts a realm declaration and provisions one open channel
// whose genesis carries the given rendered snapshot (the channel's running
// value may therefore lag the registry — exactly the drift the patrol closes).
func seedConvergeDecl(t *testing.T, a *App, declID, globalConfig string) {
	t.Helper()
	if _, err := a.db.Exec(`INSERT INTO actor_decls(id,name,owner,default_class,config_json,created_at,updated_at,visibility) VALUES (?,?,?,?,?,?,?,?)`,
		declID, "Converge", "owner", "go-kimi", globalConfig, 1, 2, "public"); err != nil {
		t.Fatal(err)
	}
}

// directoryRow registers a channel in the app directory — the patrol walks the
// directory, so a bare-fixture channel (host.Provision only) is invisible to
// it until this row exists.
func directoryRow(t *testing.T, a *App, chID channel.ID) {
	t.Helper()
	if _, err := a.db.Exec(`INSERT INTO channels(id,name,type,created_at,parent_id) VALUES (?,?,'group',1,NULL)`, string(chID), string(chID)); err != nil {
		t.Fatal(err)
	}
}

func genesisChannel(t *testing.T, a *App, chID channel.ID, declID, config string) channelhost.Bundle {
	t.Helper()
	rendered, err := (channel.RenderedSnapshot{
		Class: "go-kimi", Config: json.RawMessage(config),
		Placement: channel.Placement{Kind: channel.PlacementServer}, RenderSeq: 1,
	}).Seal()
	if err != nil {
		t.Fatal(err)
	}
	directoryRow(t, a, chID)
	return openTestChannelForTest(t, a, chID, []channelhost.GenesisDeclaration{
		{DeclID: declID, Kind: actor.KindAgent, Rendered: rendered},
	})
}

func instanceConfig(t *testing.T, bundle channelhost.Bundle, declID string) (string, int) {
	t.Helper()
	rows, err := bundle.View().DeclaredBySource(context.Background(), declID)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) == 0 {
		return "", 0
	}
	return string(rows[0].Config), len(rows)
}

// TestConvergeAppliesGlobalEditAndKeepsOverlayMask is the two-arm happy path:
// a realm registry write alone (no job, no ledger) converges every serving
// channel on the next pass, while a channel-local effective overlay masks the
// global value for its channel.
func TestConvergeAppliesGlobalEditAndKeepsOverlayMask(t *testing.T) {
	a := newBareAppForTest(t)
	const declID = "decl-conv"
	seedConvergeDecl(t, a, declID, `{"model":"v2"}`)
	plain := genesisChannel(t, a, "conv-plain", declID, `{"model":"v1"}`)
	masked := genesisChannel(t, a, "conv-masked", declID, `{"model":"v1"}`)
	if _, err := a.db.Exec(`INSERT INTO channel_decl_overlays(channel_id,decl_id,config_json,updated_at) VALUES ('conv-masked',?,?,3)`,
		declID, `{"model":"custom"}`); err != nil {
		t.Fatal(err)
	}
	w := newFanoutWorker(a)
	w.converge()
	if got, n := instanceConfig(t, plain, declID); n != 1 || got != `{"model":"v2"}` {
		t.Fatalf("plain channel config=%q rows=%d, want global v2", got, n)
	}
	if got, n := instanceConfig(t, masked, declID); n != 1 || got != `{"model":"custom"}` {
		t.Fatalf("masked channel config=%q rows=%d, want overlay value", got, n)
	}
	// Observation gate: a second pass on a converged world delivers nothing —
	// the same current version stays in place (result-unknown replays are
	// resolved the same way: observe, see convergence, do nothing).
	before, _ := currentVersion(t, plain, declID)
	w.converge()
	after, _ := currentVersion(t, plain, declID)
	if before != after {
		t.Fatalf("converged channel re-delivered: version %d -> %d", before, after)
	}
}

func currentVersion(t *testing.T, bundle channelhost.Bundle, declID string) (int64, string) {
	t.Helper()
	rows, err := bundle.View().DeclaredBySource(context.Background(), declID)
	if err != nil || len(rows) != 1 {
		t.Fatalf("rows=%v err=%v", rows, err)
	}
	return rows[0].CurrentDeclVersion, string(rows[0].Config)
}

// TestConvergeABAContentReturnsWithoutRefConflict pins the attempt-ref rule:
// content moving A→B→A must keep converging (a content-derived ref would hit
// the old anchor and stall the channel on B forever).
func TestConvergeABAContentReturnsWithoutRefConflict(t *testing.T) {
	a := newBareAppForTest(t)
	const declID = "decl-aba"
	seedConvergeDecl(t, a, declID, `{"model":"A"}`)
	bundle := genesisChannel(t, a, "conv-aba", declID, `{"model":"A"}`)
	w := newFanoutWorker(a)
	for _, target := range []string{`{"model":"B"}`, `{"model":"A"}`} {
		if _, err := a.db.Exec(`UPDATE actor_decls SET config_json=?, updated_at=9 WHERE id=?`, target, declID); err != nil {
			t.Fatal(err)
		}
		w.converge()
		if _, got := currentVersion(t, bundle, declID); got != target {
			t.Fatalf("after converge config=%q, want %q", got, target)
		}
	}
}

// TestConvergeRevokesDeletedDeclButFailsClosedOnReadError pins the definitive-
// absence rule: a soft-deleted declaration revokes its instance, while a realm
// READ FAULT must not be folded into absence — the instance survives the pass.
func TestConvergeRevokesDeletedDeclButFailsClosedOnReadError(t *testing.T) {
	a := newBareAppForTest(t)
	const declID = "decl-gone"
	seedConvergeDecl(t, a, declID, `{"model":"v1"}`)
	bundle := genesisChannel(t, a, "conv-gone", declID, `{"model":"v1"}`)
	w := newFanoutWorker(a)

	// Fault injection: the registry table is unreachable — the patrol must
	// skip, never revoke.
	if _, err := a.db.Exec(`ALTER TABLE actor_decls RENAME TO actor_decls_faulted`); err != nil {
		t.Fatal(err)
	}
	w.converge()
	if _, n := instanceConfig(t, bundle, declID); n != 1 {
		t.Fatalf("read fault folded into absence: instance revoked (rows=%d)", n)
	}
	if _, err := a.db.Exec(`ALTER TABLE actor_decls_faulted RENAME TO actor_decls`); err != nil {
		t.Fatal(err)
	}

	// Definitive absence: the soft-delete row is the authority's own answer.
	if _, err := a.db.Exec(`UPDATE actor_decls SET deleted_at=5 WHERE id=?`, declID); err != nil {
		t.Fatal(err)
	}
	w.converge()
	if _, n := instanceConfig(t, bundle, declID); n != 0 {
		t.Fatalf("soft-deleted decl still has %d active instances after pass", n)
	}
}

// TestConvergeRevokesOrphanDaemonBinding covers the daemon arm: a channel
// binding whose daemon no longer exists in the realm registry is revoked;
// a still-registered daemon's binding survives.
func TestConvergeRevokesOrphanDaemonBinding(t *testing.T) {
	a := newBareAppForTest(t)
	ctx := context.Background()
	directoryRow(t, a, "conv-daemon")
	bundle := openTestChannelForTest(t, a, "conv-daemon", nil)
	if _, err := a.db.Exec(`INSERT INTO users(id,email,password,created_at) VALUES ('u','u@x.test','p',1)`); err != nil {
		t.Fatal(err)
	}
	if _, err := a.db.Exec(`INSERT INTO daemons(id,owner_id,name,api_key_hash,created_at) VALUES ('kept','u','k','h',1)`); err != nil {
		t.Fatal(err)
	}
	for _, daemonID := range []string{"kept", "orphan"} {
		if _, err := bundle.SysOp().AttachDaemon(ctx, channel.DaemonRequest{Ref: "adm:attach-" + daemonID, DaemonID: daemonID}); err != nil {
			t.Fatalf("attach %s: %v", daemonID, err)
		}
	}
	w := newFanoutWorker(a)
	w.converge()
	bound, err := bundle.View().ListBound(ctx)
	if err != nil || len(bound) != 1 || bound[0] != "kept" {
		t.Fatalf("bound=%v err=%v, want [kept]", bound, err)
	}
}

// TestConvergeMintedSeqCrashSkipsNumberAndConverges pins the minted-but-
// undelivered recovery: a seq claimed right before a crash is a harmless gap;
// the next pass mints past it and converges.
func TestConvergeMintedSeqCrashSkipsNumberAndConverges(t *testing.T) {
	a := newBareAppForTest(t)
	const declID = "decl-crash"
	seedConvergeDecl(t, a, declID, `{"model":"v2"}`)
	bundle := genesisChannel(t, a, "conv-crash", declID, `{"model":"v1"}`)
	w := newFanoutWorker(a)
	// Simulate the crash: a seq was minted but the SysOp call never happened.
	if _, err := w.mintRenderSeq(context.Background(), "conv-crash", declID, 1); err != nil {
		t.Fatal(err)
	}
	w.converge()
	if _, got := currentVersion(t, bundle, declID); got != `{"model":"v2"}` {
		t.Fatalf("config=%q after minted-seq crash recovery, want v2", got)
	}
}

// TestConvergeUnavailableChannelDoesNotBlockLaterChannel pins pass-level error
// isolation: a directory channel that is not serving contributes nothing and
// never prevents later channels from converging in the same pass.
func TestConvergeUnavailableChannelDoesNotBlockLaterChannel(t *testing.T) {
	a := newBareAppForTest(t)
	const declID = "decl-iso"
	seedConvergeDecl(t, a, declID, `{"model":"v2"}`)
	// "a-closed" sorts before the open channel and is present in the directory
	// only — never provisioned, so Acquire fails for it.
	if _, err := a.db.Exec(`INSERT INTO channels(id,name,type,created_at,parent_id) VALUES ('a-closed','x','group',1,NULL)`); err != nil {
		t.Fatal(err)
	}
	bundle := genesisChannel(t, a, "b-open", declID, `{"model":"v1"}`)
	w := newFanoutWorker(a)
	w.converge()
	if _, got := currentVersion(t, bundle, declID); got != `{"model":"v2"}` {
		t.Fatalf("later channel did not converge past unavailable one: %q", got)
	}
}

// TestConvergeWorkerStartupPassRecoversLostPoke pins the startup pass: an
// authority write whose poke was lost (crash before notify) converges once the
// worker starts, without any poke.
func TestConvergeWorkerStartupPassRecoversLostPoke(t *testing.T) {
	a := newBareAppForTest(t)
	const declID = "decl-boot"
	seedConvergeDecl(t, a, declID, `{"model":"v2"}`)
	bundle := genesisChannel(t, a, "conv-boot", declID, `{"model":"v1"}`)
	w := newFanoutWorker(a)
	w.start()
	defer w.close()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if got, _ := instanceConfig(t, bundle, declID); got == `{"model":"v2"}` {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("startup pass did not converge the channel")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
