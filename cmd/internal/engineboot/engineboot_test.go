package engineboot

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// recorder captures the order of teardown calls (and log step messages) so the
// SIGTERM ordering — the load-bearing semantics of gracefulShutdown — is
// asserted, not assumed.
type recorder struct {
	steps []string
}

type fakeApp struct {
	r         *recorder
	shutErr   error
	closeErr  error
	shutdowns int
}

func (f *fakeApp) Shutdown(context.Context) error {
	f.shutdowns++
	f.r.steps = append(f.r.steps, "http-drain")
	return f.shutErr
}

func (f *fakeApp) Close(context.Context) error {
	f.r.steps = append(f.r.steps, "homes-close")
	return f.closeErr
}

type fakeGateway struct {
	r      *recorder
	err    error
	closed int
}

func (g *fakeGateway) Close() error {
	g.closed++
	g.r.steps = append(g.r.steps, "gateway-silence")
	return g.err
}

type fakeDB struct {
	r      *recorder
	err    error
	closed int
}

func (d *fakeDB) Close() error {
	d.closed++
	d.r.steps = append(d.r.steps, "db-close")
	return d.err
}

// TestGracefulShutdownOrder pins the ordered teardown: ① http drain → ② silence
// gateway (seal every频道臂 — gateway先静默 before Home, DoD-9) → ③ close homes →
// ④ close db. The order is the semantics (stop the entry, then silence ingress,
// then dismantle the substrate behind it).
func TestGracefulShutdownOrder(t *testing.T) {
	r := &recorder{}
	a := &fakeApp{r: r}
	gw := &fakeGateway{r: r}
	db := &fakeDB{r: r}

	if err := gracefulShutdown(context.Background(), slog.New(slog.DiscardHandler), a, gw, db); err != nil {
		t.Fatalf("gracefulShutdown: %v", err)
	}

	want := []string{"http-drain", "gateway-silence", "homes-close", "db-close"}
	if len(r.steps) != len(want) {
		t.Fatalf("steps = %v, want %v", r.steps, want)
	}
	for i := range want {
		if r.steps[i] != want[i] {
			t.Fatalf("step %d = %q, want %q (full: %v)", i, r.steps[i], want[i], r.steps)
		}
	}
	if a.shutdowns != 1 || gw.closed != 1 || db.closed != 1 {
		t.Fatalf("each step must run exactly once: shutdowns=%d gw.closed=%d db.closed=%d", a.shutdowns, gw.closed, db.closed)
	}
}

// TestGracefulShutdownRunsAllStepsOnError: an error in an earlier step must not
// short-circuit the later ones (a failed drain still needs homes + db closed);
// all errors are joined.
func TestGracefulShutdownRunsAllStepsOnError(t *testing.T) {
	r := &recorder{}
	drainErr := errors.New("drain boom")
	a := &fakeApp{r: r, shutErr: drainErr}
	gw := &fakeGateway{r: r}
	db := &fakeDB{r: r}

	err := gracefulShutdown(context.Background(), slog.New(slog.DiscardHandler), a, gw, db)
	if !errors.Is(err, drainErr) {
		t.Fatalf("joined error must carry drain err, got %v", err)
	}
	if len(r.steps) != 4 || r.steps[3] != "db-close" {
		t.Fatalf("all four steps must run despite step-1 error: %v", r.steps)
	}
}

// TestServeLifecycleInvariant pins "Serve returned ⇒ nothing is left running"
// on the REAL narrow face, not the teardown helper: a booted engine serves on
// :0, Ready closes with a dialable BoundAddr, and after a signal-shaped ctx
// cancel Serve has (a) returned, (b) joined its serve goroutine, and (c) left
// the port closed — a dial after return must fail.
func TestServeLifecycleInvariant(t *testing.T) {
	dir := t.TempDir()
	eng, err := Boot(Config{
		DBPath:       filepath.Join(dir, "app.db"),
		ChannelDBDir: filepath.Join(dir, "channels"),
		Addr:         "127.0.0.1:0",
		InitDB:       true,
	}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("boot: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	serveDone := make(chan error, 1)
	go func() { serveDone <- eng.Serve(ctx) }()

	select {
	case <-eng.Ready():
	case <-time.After(10 * time.Second):
		t.Fatal("Ready never closed")
	}
	addr := eng.BoundAddr()
	if addr == "" || strings.HasSuffix(addr, ":0") {
		t.Fatalf("BoundAddr must be the resolved address, got %q", addr)
	}
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("engine not dialable at BoundAddr %s: %v", addr, err)
	}
	conn.Close()

	cancel()
	select {
	case err := <-serveDone:
		if err != nil {
			t.Fatalf("clean shutdown must return nil, got %v", err)
		}
	case <-time.After(35 * time.Second):
		t.Fatal("Serve did not return after ctx cancel")
	}
	if _, err := net.DialTimeout("tcp", addr, 500*time.Millisecond); err == nil {
		t.Fatalf("port %s still accepting after Serve returned — something is left running", addr)
	}
}
