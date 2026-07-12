package platform

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

func lifecycleConfig(t *testing.T, name string) HomeConfig {
	t.Helper()
	return HomeConfig{ChannelID: channel.ID("lifecycle-" + name), DBPath: filepath.Join(t.TempDir(), name+".sqlite")}
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
	cfg := HomeConfig{ChannelID: channel.ID("seed-replay"), DBPath: db}
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

type blockingDesired struct {
	calls   atomic.Int64
	entered chan struct{}
	release chan struct{}
}

func (d *blockingDesired) Members(context.Context) ([]actorrt.DesiredMember, error) {
	if d.calls.Add(1) == 1 {
		return nil, nil
	}
	select {
	case <-d.entered:
	default:
		close(d.entered)
	}
	<-d.release
	return []actorrt.DesiredMember{{ID: "must-not-build", Kind: actor.KindAgent, Lifecycle: actorrt.LifecycleAlwaysOn}}, nil
}

func TestHomeCloseBoundsDesiredAndSealPrecedesAbandon(t *testing.T) {
	d := &blockingDesired{entered: make(chan struct{}), release: make(chan struct{})}
	var mu sync.Mutex
	var events []string
	h, err := openHome(HomeConfig{
		ChannelID: channel.ID("lifecycle-blocking"), DBPath: filepath.Join(t.TempDir(), "blocking.sqlite"),
		Desired: d, ReconcileInterval: time.Millisecond,
	}, &homeFaults{record: func(s string) { mu.Lock(); events = append(events, s); mu.Unlock() }})
	if err != nil {
		t.Fatal(err)
	}
	<-d.entered
	if err := h.closeInternalWithin("test", 25*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if h.reconcileLeaked.Load() != 1 {
		t.Fatalf("reconcile leaks = %d", h.reconcileLeaked.Load())
	}
	close(d.release)
	select {
	case <-h.reconcileDone:
	case <-time.After(time.Second):
		t.Fatal("released reconcile did not exit")
	}
	if _, ok := h.channel.Cells().Stat("must-not-build"); ok {
		t.Fatal("abandoned reconcile built after Seal")
	}
	mu.Lock()
	got := closeEvents(events)
	mu.Unlock()
	seal, reconcile := -1, -1
	for i, event := range got {
		if event == "close.seal" {
			seal = i
		}
		if event == "close.reconcile" {
			reconcile = i
		}
	}
	if seal < 0 || reconcile < 0 || seal >= reconcile {
		t.Fatalf("Seal not first: %v", got)
	}
}

// TestHomeConcurrentCloseShareOneCloseErr locks Close's completion semantics
// on the NORMAL path (no panic): N concurrent callers all wait for the single
// teardown run and every one of them receives the same closeErr.
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
	issued := HumanHandle{home: h, userID: "issued-human"}
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
	if token := issued.PresenceConnect(); token != "" {
		t.Fatalf("PresenceConnect after Close minted token %q", token)
	}
	if _, _, err := issued.Submit(context.Background(), SubmitSpec{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("issued handle Submit after Close = %v", err)
	}
	if got := h.KickDaemon("none"); got != 0 {
		t.Fatalf("KickDaemon after Close = %d", got)
	}
	if _, _, err := h.PrincipalOf(context.Background(), "issued-human"); err == nil {
		t.Fatal("read after stores close did not surface an error")
	}
}
