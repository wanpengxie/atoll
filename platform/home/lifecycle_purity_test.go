package home

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/schedule"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

func lifecycleConfig(t *testing.T, name string) Config {
	t.Helper()
	return Config{CompositionResolver: emptyCompositionResolver{}, DaemonAuthority: allowTestDaemonAuthority{}, ChannelID: channel.ID("lifecycle-" + name), DBPath: filepath.Join(t.TempDir(), name+".sqlite")}
}

func closeEvents(events []string) []string {
	var out []string
	for _, event := range events {
		if len(event) >= 6 && event[:6] == "close." {
			out = append(out, event)
		}
	}
	return out
}

func TestHomeRollbackFailureArmsUseOneCloseCore(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	steps := []string{
		"construct.open_channel", "construct.ensure_system", "construct.max_seq",
		"construct.open_scheduler", "activate.channel_start",
	}
	for _, step := range steps {
		t.Run(step, func(t *testing.T) {
			var events []string
			var logs bytes.Buffer
			fault := errors.New("injected " + step)
			cfg := lifecycleConfig(t, step)
			cfg.Logger = slog.New(slog.NewTextHandler(&logs, nil))
			h, err := openHome(cfg, &homeFaults{
				fail: map[string]error{step: fault}, record: func(s string) { events = append(events, s) },
			})
			if h != nil || !errors.Is(err, fault) {
				t.Fatalf("Open = (%v, %v)", h, err)
			}
			got := closeEvents(events)
			if len(got) == 0 || got[0] != "close.seal" || got[len(got)-1] != "close.end" {
				t.Fatalf("rollback did not traverse close core: %v", got)
			}
			if text := logs.String(); !strings.Contains(text, "platform.home.rollback") || !strings.Contains(text, "platform.home.closed") {
				t.Fatalf("rollback observability incomplete: %s", text)
			}
		})
	}
}

func TestHomePublishPanicsRollbackAndPreserveOriginal(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	for _, step := range []string{"publish.bind", "publish.sweep", "publish.goroutine_started", "publish.published"} {
		t.Run(step, func(t *testing.T) {
			var events []string
			original := "panic:" + step
			func() {
				defer func() {
					if got := recover(); got != original {
						t.Fatalf("panic = %v", got)
					}
				}()
				_, _ = openHome(lifecycleConfig(t, step), &homeFaults{
					panicAt: map[string]any{step: original}, record: func(s string) { events = append(events, s) },
				})
			}()
			got := closeEvents(events)
			if len(got) == 0 || got[len(got)-1] != "close.end" {
				t.Fatalf("cleanup incomplete: %v", got)
			}
		})
	}
}

type lifecycleActor struct{}

func (lifecycleActor) Receive(context.Context, *message.Envelope) error { return nil }

func TestHomeActivateInvariantPanicBeatsCleanupPanic(t *testing.T) {
	if testing.Short() {
		t.Skip("short: ~5s real-interleaving/goleak-settle test — full gate (make test-full) runs it")
	}
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	var h *Home
	var logs bytes.Buffer
	original := any(nil)
	func() {
		defer func() { original = recover() }()
		cfg := lifecycleConfig(t, "dual-panic")
		cfg.Logger = slog.New(slog.NewTextHandler(&logs, nil))
		_, _ = openHome(cfg, &homeFaults{
			created: func(got *Home) { h = got },
			action: map[string]func(){"activate.channel_start": func() {
				_, _, err := h.channel.Cells().SpawnIfAbsent(actor.SystemActorID, actor.KindSystem, func(actorrt.Incarnation) actorrt.Actor { return lifecycleActor{} })
				if err != nil {
					panic(err)
				}
			}},
			panicAt: map[string]any{"close.delivery": "cleanup-panic"},
		})
	}()
	if original == nil || original == "cleanup-panic" {
		t.Fatalf("original invariant panic lost: %v", original)
	}
	if text := logs.String(); !strings.Contains(text, "home.teardown.panic") || !strings.Contains(text, "home.rollback.panic") {
		t.Fatalf("cleanup panic not attached to logs: %s", text)
	}
	select {
	case <-h.closeDone:
	case <-time.After(time.Second):
		t.Fatal("cleanup panic wedged closeDone")
	}
}

func TestHomeRollbackAndNormalCloseHaveSameOrder(t *testing.T) {
	var normalEvents []string
	h, err := openHome(lifecycleConfig(t, "normal-order"), &homeFaults{record: func(s string) { normalEvents = append(normalEvents, s) }})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	var rollbackEvents []string
	_, err = openHome(lifecycleConfig(t, "rollback-order"), &homeFaults{
		fail:   map[string]error{"publish.published": errors.New("rollback")},
		record: func(s string) { rollbackEvents = append(rollbackEvents, s) },
	})
	if err == nil {
		t.Fatal("faulted Open succeeded")
	}
	if got, want := closeEvents(rollbackEvents), closeEvents(normalEvents); !reflect.DeepEqual(got, want) {
		t.Fatalf("rollback order %v != normal %v", got, want)
	}
}

func TestHomeCursorWindowInitialDrainDoesNotLoseRow(t *testing.T) {
	var h *Home
	delivered := make(chan storespec.StoredRow, 1)
	cfg := lifecycleConfig(t, "cursor-window")
	home, err := openHome(cfg, &homeFaults{
		created:  func(got *Home) { h = got },
		delivery: func(row storespec.StoredRow) error { delivered <- row; return nil },
		action: map[string]func(){"activate.before_pump": func() {
			_, appendErr := h.cs.Log.Append(context.Background(), &message.Envelope{
				ID: "window-row", ChannelID: cfg.ChannelID, Kind: message.KindEvent,
				Type: "test.window", Payload: json.RawMessage(`{}`),
				Sender:     message.Sender{ID: actor.SystemActorID, Kind: actor.KindSystem},
				Audience:   message.Audience{actor.SystemActorID},
				Visibility: message.VisibilitySystem,
			}, false)
			if appendErr != nil {
				panic(appendErr)
			}
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer home.Close()
	rows, qerr := home.cs.Query.ReadAfterSeq(context.Background(), 0, 100)
	if qerr != nil || len(rows) == 0 {
		t.Fatalf("window row not in store: rows=%d err=%v", len(rows), qerr)
	}
	select {
	case row := <-delivered:
		if row.Envelope.ID != "window-row" {
			t.Fatalf("delivered %s", row.Envelope.ID)
		}
	case <-time.After(time.Second):
		t.Fatal("row appended after MaxSeq and before OpenPump was lost")
	}
}

func TestHomeSeedSurvivesLaterAssemblyFailureAndReplays(t *testing.T) {
	db := filepath.Join(t.TempDir(), "seed-replay.sqlite")
	cfg := Config{CompositionResolver: emptyCompositionResolver{}, DaemonAuthority: allowTestDaemonAuthority{}, ChannelID: channel.ID("seed-replay"), DBPath: db}
	fault := errors.New("after seed")
	if _, err := openHome(cfg, &homeFaults{fail: map[string]error{"construct.max_seq": fault}}); !errors.Is(err, fault) {
		t.Fatalf("faulted Open err = %v", err)
	}
	h, err := Open(cfg)
	if err != nil {
		t.Fatalf("replay Open: %v", err)
	}
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestHomeConcurrentCloseShareOneCloseErr locks Close's completion semantics
// on the NORMAL path (no panic): N concurrent callers all wait for the single
// teardown run and every one of them receives the same closeErr.
// TestCloseWindowDueTimerNeitherRevivesNorPoisons is C10's real path: a due
// identity timer fires INSIDE the close window (runtime sealed, cells already
// stopped, engine still running — the exact revival window the Seal axiom
// closes). Two assertions, both ends of the disaster: the fire must NOT
// revive a new cell into the closing station, and the sealed rejection must
// stay transient — the timer row survives the shutdown and lands as truth on
// the next boot (a poison would have deleted a live author's wake forever).
func TestCloseWindowDueTimerNeitherRevivesNorPoisons(t *testing.T) {
	if testing.Short() {
		t.Skip("short: ~1.2s shutdown-window interleaving — full gate (make test-full) runs it")
	}
	db := filepath.Join(t.TempDir(), "c10-window.sqlite")
	cfg := Config{CompositionResolver: emptyCompositionResolver{}, DaemonAuthority: allowTestDaemonAuthority{}, ChannelID: channel.ID("lifecycle-c10"), DBPath: db}
	var logs bytes.Buffer
	cfg.Logger = slog.New(slog.NewTextHandler(&logs, nil))
	parked, release := make(chan struct{}), make(chan struct{})
	var h *Home
	h1, err := openHome(cfg, &homeFaults{
		created: func(got *Home) { h = got },
		action:  map[string]func(){"close.engine": func() { close(parked); <-release }},
	})
	if err != nil {
		t.Fatal(err)
	}
	id, err := h1.Admit(context.Background(), actor.KindHuman, "c10-user")
	if err != nil {
		t.Fatal(err)
	}
	closeErr := make(chan error, 1)
	go func() { closeErr <- h1.Close() }()
	<-parked
	// The window: sealed + StopAll done (author absent) + engine alive. A timer
	// due right now fires here — Revive → EnsureLive → SpawnIfAbsent → sealed.
	if _, err := h.schedMinter.Mint(id).Schedule(context.Background(), schedule.ScheduleReq{
		Bind: schedule.BindIdentity, FireAt: h.nowMs() + 20,
		Type: "test.c10.wake", Payload: []byte(`{}`),
	}); err != nil {
		t.Fatalf("schedule in close window: %v", err)
	}
	deadline := time.Now().Add(900 * time.Millisecond)
	for time.Now().Before(deadline) {
		if _, ok := h.channel.Cells().Stat(id); ok {
			t.Fatal("due timer fire revived a cell inside the close window")
		}
		time.Sleep(20 * time.Millisecond)
	}
	close(release)
	if err := <-closeErr; err != nil {
		t.Fatal(err)
	}
	// The fire must have actually HAPPENED in the window and hit the seal —
	// otherwise the absence poll above proves nothing (a vacuous pass).
	if !strings.Contains(logs.String(), "platform.revive.runtime_sealed") {
		t.Fatal("no revive attempt hit the seal inside the close window — the test never exercised the revival path")
	}
	// Reboot the same station: a transient (non-poisoned) row is still there
	// and the SAME wake must now fire normally and land as truth.
	h2, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer h2.Close()
	fireDeadline := time.Now().Add(10 * time.Second)
	for {
		rows, err := h2.cs.Query.ReadAfterSeq(context.Background(), 0, 1000)
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, row := range rows {
			if row.Envelope.Type == "test.c10.wake" {
				found = true
			}
		}
		if found {
			break
		}
		if time.Now().After(fireDeadline) {
			t.Fatal("retained timer never fired after reboot — the sealed rejection was not transient")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestHomeConcurrentCloseShareOneCloseErr(t *testing.T) {
	fault := errors.New("injected close.stores")
	h, err := openHome(lifecycleConfig(t, "concurrent-close"), &homeFaults{
		fail: map[string]error{"close.stores": fault},
	})
	if err != nil {
		t.Fatal(err)
	}
	const callers = 8
	results := make(chan error, callers)
	for range callers {
		go func() { results <- h.Close() }()
	}
	for range callers {
		select {
		case got := <-results:
			if !errors.Is(got, fault) {
				t.Fatalf("concurrent Close err = %v, want the one closeErr", got)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("concurrent Close wedged")
		}
	}
}

func TestHomeClosePanicDoesNotWedgeWaiters(t *testing.T) {
	parked, release := make(chan struct{}), make(chan struct{})
	h, err := openHome(lifecycleConfig(t, "close-panic"), &homeFaults{
		action:  map[string]func(){"close.delivery": func() { close(parked); <-release }},
		panicAt: map[string]any{"close.delivery": "teardown-panic"},
	})
	if err != nil {
		t.Fatal(err)
	}
	first := make(chan any, 1)
	go func() { defer func() { first <- recover() }(); _ = h.Close() }()
	<-parked
	waiter := make(chan error, 1)
	go func() { waiter <- h.Close() }()
	close(release)
	if got := <-first; got != "teardown-panic" {
		t.Fatalf("panic = %v", got)
	}
	select {
	case <-waiter:
	case <-time.After(time.Second):
		t.Fatal("concurrent Close wedged")
	}
	select {
	case <-h.closeDone:
	default:
		t.Fatal("closeDone not closed")
	}
}

func TestHomeStateTransitionsAndUnpublishIssuedHandles(t *testing.T) {
	var events []string
	h, err := openHome(lifecycleConfig(t, "state-unpublish"), &homeFaults{record: func(s string) { events = append(events, s) }})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	wantStates := []string{"state.constructing", "state.activating", "state.published", "state.closing", "state.closed"}
	var gotStates []string
	for _, event := range events {
		if len(event) >= 6 && event[:6] == "state." {
			gotStates = append(gotStates, event)
		}
	}
	if !reflect.DeepEqual(gotStates, wantStates) {
		t.Fatalf("states = %v, want %v", gotStates, wantStates)
	}
	// The former "issued HumanHandle refuses after Close" assertion retired with
	// the door (gateway 期 S5): a subject's post-Close write now fails at the cell
	// caps' live membrane / a torn-down slot's ErrNoOccupant, and the mutating
	// entry-point refusals below (Admit/Remove/Restart = ErrClosed) carry the
	// unpublish 对账 for Home itself. Gateway close-order frame behaviour is
	// covered in drivers/gateway (TestGatewayCloseSealsArms).
	if got := h.KickDaemon("none"); got != 0 {
		t.Fatalf("KickDaemon after Close = %d", got)
	}
	if _, _, err := h.PrincipalOf(context.Background(), "issued-human"); err == nil {
		t.Fatal("read after stores close did not surface an error")
	}
	// Unpublish 对账, per-entry (H9): every mutating entry point refuses with
	// ErrClosed; Subscribe hands back an already-closed channel (a waiter must
	// wake, not hang); ServeAttach answers 503 through the closed Acceptor.
	ctx := context.Background()
	if _, err := h.Admit(ctx, actor.KindHuman, "late-admit"); !errors.Is(err, ErrClosed) {
		t.Fatalf("Admit after Close = %v, want ErrClosed", err)
	}
	if err := h.Remove(ctx, "issued-human"); !errors.Is(err, ErrClosed) {
		t.Fatalf("Remove after Close = %v, want ErrClosed", err)
	}
	if err := h.Restart(ctx, "issued-human"); !errors.Is(err, ErrClosed) {
		t.Fatalf("Restart after Close = %v, want ErrClosed", err)
	}
	sub, cancelSub := h.Subscribe()
	select {
	case <-sub:
	default:
		t.Fatal("Subscribe after Close did not return a closed channel")
	}
	cancelSub()
	rec := httptest.NewRecorder()
	h.ServeAttach(rec, httptest.NewRequest("GET", "/attach", nil), "daemon-late")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("ServeAttach after Close = %d, want 503", rec.Code)
	}
}
