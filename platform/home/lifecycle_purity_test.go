package home

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/schedule"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

func lifecycleConfig(t *testing.T, name string) Config {
	t.Helper()
	return Config{
		CompositionResolver: emptyCompositionResolver{},
		DaemonAuthority:     allowTestDaemonAuthority{},
		ChannelID:           channel.ID("lifecycle-" + name),
		DBPath:              filepath.Join(t.TempDir(), name+".sqlite"),
	}
}

// lifecycleLogHandler exercises slog's real callback boundary. It can hold a
// lifecycle operation at an observable production event and, separately, model
// a downstream logger panic. Neither capability is visible to production code.
type lifecycleLogHandler struct {
	mu       sync.Mutex
	seen     map[string]int
	parkOn   string
	entered  chan struct{}
	release  <-chan struct{}
	parkOnce sync.Once
	panicOn  map[string]any
}

func newLifecycleLogHandler() *lifecycleLogHandler {
	return &lifecycleLogHandler{seen: make(map[string]int)}
}

func (h *lifecycleLogHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *lifecycleLogHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	h.seen[r.Message]++
	h.mu.Unlock()
	if r.Message == h.parkOn {
		h.parkOnce.Do(func() { close(h.entered) })
		<-h.release
	}
	if p, ok := h.panicOn[r.Message]; ok {
		panic(p)
	}
	return nil
}

func (h *lifecycleLogHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *lifecycleLogHandler) WithGroup(string) slog.Handler      { return h }

func (h *lifecycleLogHandler) saw(message string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.seen[message] > 0
}

func TestHomeOpenMissingRequiredDBRollsBack(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	cfg := lifecycleConfig(t, "missing-db")
	cfg.MustExistDB = true
	h, err := Open(cfg)
	if h != nil || err == nil {
		t.Fatalf("Open = (%v, %v), want nil home and an error", h, err)
	}
}

func TestHomeLateOpenPanicRollsBackAndPreservesOriginal(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	cfg := lifecycleConfig(t, "late-open-panic")
	handler := newLifecycleLogHandler()
	handler.panicOn = map[string]any{
		"platform.home.ready":  "ready-panic",
		"link.acceptor_closed": "cleanup-panic",
	}
	cfg.Logger = slog.New(handler)

	var got any
	func() {
		defer func() { got = recover() }()
		_, _ = Open(cfg)
	}()
	if got != "ready-panic" {
		t.Fatalf("panic = %v, want the original ready panic", got)
	}
	if !handler.saw("home.teardown.panic") || !handler.saw("home.rollback.panic") {
		t.Fatal("cleanup panic was not observed while preserving the original panic")
	}

	// Reopening the same database proves the late rollback released every owned
	// resource and left the durable genesis seed replayable.
	cfg.Logger = nil
	h, err := Open(cfg)
	if err != nil {
		t.Fatalf("reopen after rollback: %v", err)
	}
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCloseWindowDueTimerNeitherRevivesNorPoisons(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	db := filepath.Join(t.TempDir(), "close-window.sqlite")
	clock := newFakeClock(time.Unix(1_700_000_000, 0))
	release := make(chan struct{})
	handler := newLifecycleLogHandler()
	handler.parkOn = "link.acceptor_closed"
	handler.entered = make(chan struct{})
	handler.release = release
	cfg := Config{
		CompositionResolver: emptyCompositionResolver{},
		DaemonAuthority:     allowTestDaemonAuthority{},
		ChannelID:           channel.ID("lifecycle-close-window"),
		DBPath:              db,
		Clock:               clock,
		Logger:              slog.New(handler),
		ReconcileInterval:   time.Hour,
	}
	h, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	id, err := h.Admit(context.Background(), actor.KindHuman, "close-window-user")
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		if _, ok := h.channel.Cells().Stat(id); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("admitted human was not embodied")
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := h.schedMinter.Mint(id).Schedule(context.Background(), schedule.ScheduleReq{
		Bind: schedule.BindIdentity, FireAt: clock.Now().Add(time.Second).UnixMilli(),
		Type: "test.close-window.wake", Payload: []byte(`{}`),
	}); err != nil {
		t.Fatal(err)
	}
	if !h.channel.Cells().DespawnID(id) {
		t.Fatal("scheduled author was not live before the close-window setup")
	}

	closeErr := make(chan error, 1)
	go func() { closeErr <- h.Close() }()
	select {
	case <-handler.entered:
	case <-time.After(time.Second):
		t.Fatal("Close did not reach the real link teardown boundary")
	}
	// closed + Runtime.Seal are already published, while the schedule engine and
	// stores are still alive. The absent author therefore reaches the real sealed
	// revival verdict without requiring a source-line hook after StopAll.
	clock.Advance(time.Second)
	deadline = time.Now().Add(time.Second)
	for !handler.saw("platform.revive.runtime_sealed") {
		if time.Now().After(deadline) {
			t.Fatal("due timer never exercised sealed revival")
		}
		time.Sleep(time.Millisecond)
	}
	if _, ok := h.channel.Cells().Stat(id); ok {
		t.Fatal("due timer revived an actor after Home began closing")
	}
	close(release)
	if err := <-closeErr; err != nil {
		t.Fatal(err)
	}

	// A sealed rejection is transient: the durable timer remains and fires after
	// the station is reopened.
	cfg.Logger = nil
	h2, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer h2.Close()
	deadline = time.Now().Add(2 * time.Second)
	for {
		rows, err := h2.cs.Query.ReadAfterSeq(context.Background(), 0, 1000)
		if err != nil {
			t.Fatal(err)
		}
		for _, row := range rows {
			if row.Envelope.Type == "test.close-window.wake" {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("transiently rejected timer did not fire after reopen")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestHomeConcurrentCloseWaitsForOneTeardown(t *testing.T) {
	release := make(chan struct{})
	handler := newLifecycleLogHandler()
	handler.parkOn = "link.acceptor_closed"
	handler.entered = make(chan struct{})
	handler.release = release
	cfg := lifecycleConfig(t, "concurrent-close")
	cfg.Logger = slog.New(handler)
	h, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	const callers = 8
	results := make(chan error, callers)
	go func() { results <- h.Close() }()
	<-handler.entered
	for range callers - 1 {
		go func() { results <- h.Close() }()
	}
	select {
	case err := <-results:
		t.Fatalf("Close returned before the one teardown completed: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	for range callers {
		select {
		case err := <-results:
			if err != nil {
				t.Fatalf("Close: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("concurrent Close caller wedged")
		}
	}
}

func TestHomeClosePanicDoesNotWedgeWaiters(t *testing.T) {
	release := make(chan struct{})
	handler := newLifecycleLogHandler()
	handler.parkOn = "link.acceptor_closed"
	handler.entered = make(chan struct{})
	handler.release = release
	handler.panicOn = map[string]any{"link.acceptor_closed": "teardown-panic"}
	cfg := lifecycleConfig(t, "close-panic")
	cfg.Logger = slog.New(handler)
	h, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	first := make(chan any, 1)
	go func() {
		defer func() { first <- recover() }()
		_ = h.Close()
	}()
	<-handler.entered
	waiter := make(chan error, 1)
	go func() { waiter <- h.Close() }()
	close(release)
	if got := <-first; got != "teardown-panic" {
		t.Fatalf("panic = %v", got)
	}
	select {
	case err := <-waiter:
		if err != nil {
			t.Fatalf("waiting Close: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("waiting Close wedged after teardown panic")
	}
	select {
	case <-h.closeDone:
	default:
		t.Fatal("closeDone was not closed")
	}
}

func TestHomeCloseUnpublishesEveryEntryPoint(t *testing.T) {
	h, err := Open(lifecycleConfig(t, "unpublish"))
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	if got := h.KickDaemon("none"); got != 0 {
		t.Fatalf("KickDaemon after Close = %d", got)
	}
	if _, _, err := h.PrincipalOf(context.Background(), "issued-human"); err == nil {
		t.Fatal("read after stores close did not surface an error")
	}
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
	h.ServeAttach(rec, httptest.NewRequest(http.MethodGet, "/attach", nil), "daemon-late")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("ServeAttach after Close = %d, want 503", rec.Code)
	}
}

func TestHomeClosePublishesMutationFenceBeforeTeardown(t *testing.T) {
	release := make(chan struct{})
	handler := newLifecycleLogHandler()
	handler.parkOn = "link.acceptor_closed"
	handler.entered = make(chan struct{})
	handler.release = release
	cfg := lifecycleConfig(t, "mutation-fence")
	cfg.Logger = slog.New(handler)
	h, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	closed := make(chan error, 1)
	go func() { closed <- h.Close() }()
	<-handler.entered

	if _, _, _, err := h.IntroduceComposition(context.Background(), storespec.CompositionIntroduce{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("IntroduceComposition during Close = %v, want ErrClosed", err)
	}
	if err := h.Restart(context.Background(), "agent:closing"); !errors.Is(err, ErrClosed) {
		t.Fatalf("Restart during Close = %v, want ErrClosed", err)
	}
	if err := h.Remove(context.Background(), "agent:closing"); !errors.Is(err, ErrClosed) {
		t.Fatalf("Remove during Close = %v, want ErrClosed", err)
	}
	rows, err := h.cs.Composition.ListComposition(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("close-window mutations persisted rows: %+v", rows)
	}
	close(release)
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
}
