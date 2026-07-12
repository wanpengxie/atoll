package main

import (
	"context"
	"errors"
	"log/slog"
	"testing"
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

func (f *fakeApp) Close() error {
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
