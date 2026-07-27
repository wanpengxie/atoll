package home

import (
	"context"
	"database/sql"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"
	_ "modernc.org/sqlite" // the test opens the channel file directly to corrupt one column

	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// Open's purity contract, in one place: a rejected or exploding Open leaves
// NOTHING behind. Every failure path in this file is driven through the real
// production Open over a real channel sqlite, and every one of them is checked
// twice — once for the verdict the caller sees, once for the residue it must
// not leave (live goroutines, a locked database file, a half-built Home).
//
// Why the goroutine check is part of the contract and not test hygiene: by the
// time Open reaches its late failure points it has already started the schedule
// engine, the delivery pump, the reconcile loop, the actor host and the system
// kernel. A rollback that forgets one of them is invisible to the caller (it
// still gets its error) and fatal to the process (the abandoned loop keeps
// writing into a store the next Open reopens).

// lifecycleOpenConfig is the one-owner bootstrap config these tests fail out
// of. Each caller gets its own temp database, so a config can be reopened as a
// restart without touching another test's file.
func lifecycleOpenConfig(t *testing.T, name string) Config {
	t.Helper()
	id := channel.ID("lifecycle-" + name)
	return Config{
		ChannelID:            id,
		DBPath:               filepath.Join(t.TempDir(), "channel.sqlite"),
		CompositionResolver:  emptyCompositionResolver{},
		IntroductionResolver: inertIntroductionResolver{},
		ReconcileInterval:    time.Hour,
		Bootstrap:            true,
		Genesis: &storespec.ChannelGenesis{
			ChannelID: string(id), Type: "channel",
			OwnerPrincipal: "lifecycle-owner", CreatedAt: time.Now().UnixMilli(),
		},
		BootstrapOwnerPrincipal: "lifecycle-owner",
	}
}

// lifecycleReopenConfig turns a bootstrap config into the ordinary restart
// open over the same file: no seeding, and the file must already be there.
func lifecycleReopenConfig(cfg Config) Config {
	cfg.Bootstrap = false
	cfg.Genesis = nil
	cfg.BootstrapOwnerPrincipal = ""
	cfg.BootstrapDeclarations = nil
	cfg.MustExistDB = true
	cfg.Logger = nil
	return cfg
}

// lifecycleLogProbe is a slog handler that panics on one named production
// event and counts what it saw. It injects a fault at a REAL production
// boundary (Open's own logging call) without any test hook in production code.
// The park half of the same idea lives in lifecycle_close_teardown_test.go;
// this one only needs the panic.
type lifecycleLogProbe struct {
	mu      sync.Mutex
	seen    map[string]int
	panicOn string
	value   any
}

func newLifecycleLogProbe(panicOn string, value any) *lifecycleLogProbe {
	return &lifecycleLogProbe{seen: make(map[string]int), panicOn: panicOn, value: value}
}

func (h *lifecycleLogProbe) Enabled(context.Context, slog.Level) bool { return true }

func (h *lifecycleLogProbe) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	h.seen[r.Message]++
	h.mu.Unlock()
	if r.Message == h.panicOn {
		panic(h.value)
	}
	return nil
}

func (h *lifecycleLogProbe) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *lifecycleLogProbe) WithGroup(string) slog.Handler      { return h }

func (h *lifecycleLogProbe) count(message string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.seen[message]
}

// blankGenesisOwnerPrincipal empties the owner column of an ALREADY CLOSED
// channel file. The store's own CreateGenesis refuses to write an ownerless
// genesis, so this is the only way to produce the state the open-time check
// exists for: a database whose genesis row is there but names nobody.
func blankGenesisOwnerPrincipal(t *testing.T, dbPath string) {
	t.Helper()
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open channel file directly: %v", err)
	}
	defer func() { _ = db.Close() }()
	result, err := db.ExecContext(context.Background(),
		`UPDATE channel_genesis SET owner_principal = ''`)
	if err != nil {
		t.Fatalf("blank genesis owner: %v", err)
	}
	affected, err := result.RowsAffected()
	if err != nil || affected != 1 {
		t.Fatalf("blank genesis owner touched %d rows (err=%v), want exactly 1", affected, err)
	}
}

// T11. Owner is channel self-truth living on the genesis pointer alone, so the
// one thing a normal open can check is that the pointer names someone. The
// control is the same file opening fine one line earlier: only the blanked
// owner column separates the accepted open from the refused one.
func TestNormalOpenRejectsGenesisWithoutAnOwnerPrincipal(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	cfg := lifecycleOpenConfig(t, "zero-owner")

	seeded, err := Open(cfg)
	if err != nil {
		t.Fatalf("bootstrap Open: %v", err)
	}
	if seeded.ownerPrincipal != "lifecycle-owner" {
		t.Fatalf("bootstrapped owner pointer = %q", seeded.ownerPrincipal)
	}
	if err := seeded.closeInternal("test"); err != nil {
		t.Fatalf("close the seeded home: %v", err)
	}

	reopen := lifecycleReopenConfig(cfg)
	control, err := Open(reopen)
	if err != nil {
		t.Fatalf("control restart over the owned channel: %v", err)
	}
	if err := control.closeInternal("test"); err != nil {
		t.Fatalf("close the control home: %v", err)
	}

	blankGenesisOwnerPrincipal(t, cfg.DBPath)

	h, err := Open(reopen)
	if h != nil {
		_ = h.closeInternal("test")
		t.Fatal("an ownerless channel opened normally")
	}
	if err == nil || !strings.Contains(err.Error(), "carries no owner principal") {
		t.Fatalf("Open over an ownerless genesis = %v, want the owner-pointer rejection", err)
	}
}

// T12. The late Open panic is the maximal rollback: by the time Open logs
// "ready" the schedule engine, the delivery pump, the reconcile loop, the link
// acceptor, the actor host and the system kernel are all live and the durable
// bootstrap seed is committed. Three things must hold at once — the caller
// sees ITS OWN panic (not a cleanup artefact), nothing keeps running, and the
// database is left reopenable.
func TestLateOpenPanicPreservesTheOriginalPanicAndRollsEverythingBack(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	cfg := lifecycleOpenConfig(t, "late-open-panic")
	handler := newLifecycleLogProbe("platform.home.ready", "ready-panic")
	cfg.Logger = slog.New(handler)

	var recovered any
	func() {
		defer func() { recovered = recover() }()
		_, _ = Open(cfg)
	}()
	if recovered != "ready-panic" {
		t.Fatalf("panic seen by the caller = %v, want the original ready-panic", recovered)
	}
	// The rollback ran the ordinary teardown, so it logged the ordinary closed
	// event exactly once. This is what says "rollback", not "abandonment".
	if got := handler.count("platform.home.closed"); got != 1 {
		t.Fatalf("closed events during the panic rollback = %d, want exactly 1", got)
	}

	// Reopening the same file proves the rollback gave the database back and
	// left the committed bootstrap seed replayable.
	reopened, err := Open(lifecycleReopenConfig(cfg))
	if err != nil {
		t.Fatalf("reopen after the panic rollback: %v", err)
	}
	if reopened.ownerPrincipal != "lifecycle-owner" {
		t.Fatalf("owner pointer after reopen = %q", reopened.ownerPrincipal)
	}
	roster, err := reopened.controller.ActiveIdentities()
	if err != nil || len(roster) == 0 {
		t.Fatalf("durable seed after the panic rollback: roster=%v err=%v", roster, err)
	}
	if err := reopened.closeInternal("test"); err != nil {
		t.Fatalf("close the reopened home: %v", err)
	}
}

// The two structural refusals happen before any resource is taken, and their
// wording is the whole diagnosis a caller gets.
func TestOpenStructuralRefusalsAreExactAndTakeNothing(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	t.Run("introduction resolver is required", func(t *testing.T) {
		cfg := lifecycleOpenConfig(t, "no-introduction-resolver")
		cfg.IntroductionResolver = nil
		h, err := Open(cfg)
		if h != nil || err == nil || err.Error() != "platform: IntroductionResolver required" {
			t.Fatalf("Open = (%v, %v), want the exact resolver rejection", h, err)
		}
	})
	t.Run("bootstrap and must-exist are mutually exclusive", func(t *testing.T) {
		cfg := lifecycleOpenConfig(t, "bootstrap-and-must-exist")
		cfg.MustExistDB = true
		h, err := Open(cfg)
		if h != nil || err == nil ||
			err.Error() != "platform: Bootstrap and MustExistDB are mutually exclusive" {
			t.Fatalf("Open = (%v, %v), want the exact exclusivity rejection", h, err)
		}
	})
}

// A required-but-absent database fails at the very first resource Open takes.
// Nothing above the store exists yet, so this is the cheapest possible proof
// that the rollback defer is armed before the first acquisition — and the
// goroutine check is the assertion that carries it.
func TestOpenMissingRequiredDatabaseRollsBackWithoutResidue(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	cfg := lifecycleOpenConfig(t, "missing-db")
	cfg.Bootstrap = false
	cfg.Genesis = nil
	cfg.BootstrapOwnerPrincipal = ""
	cfg.MustExistDB = true

	h, err := Open(cfg)
	if h != nil || err == nil {
		t.Fatalf("Open = (%v, %v), want nil home and an error", h, err)
	}
	if !strings.Contains(err.Error(), "open channel store") {
		t.Fatalf("Open error = %v, want the store-open failure", err)
	}
}
