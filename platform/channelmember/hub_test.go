package channelmember

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
)

func TestOccupiedPortLogsConflict(t *testing.T) {
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	hub := NewHub()
	pair := Pair{Host: "h", Body: "a"}
	fn := func(context.Context, Request) (Response, error) { return Response{}, nil }
	release, err := hub.AttachHandle(pair, fn)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if _, err := hub.AttachHandle(pair, fn); !errors.Is(err, ErrPortBusy) {
		t.Fatalf("error=%v", err)
	}
	for _, field := range []string{`"level":"ERROR"`, `"msg":"channelmember.attach_rejected"`, `"host":"h"`, `"body":"a"`, `"side":"handle"`, "port_busy"} {
		if !strings.Contains(logs.String(), field) {
			t.Fatalf("missing %s in log: %s", field, logs.String())
		}
	}
}

func TestHandleLivenessAndGenerationFencing(t *testing.T) {
	hub := NewHub()
	pair := Pair{Host: "host", Body: "body"}
	var edges []bool
	releaseWatch, err := hub.WatchHandle(pair, func(online bool) { edges = append(edges, online) })
	if err != nil {
		t.Fatal(err)
	}
	defer releaseWatch()

	oldRelease, err := hub.AttachHandle(pair, func(context.Context, Request) (Response, error) { return Response{}, nil })
	if err != nil {
		t.Fatal(err)
	}
	if release, err := hub.AttachHandle(pair, func(context.Context, Request) (Response, error) { return Response{}, nil }); !errors.Is(err, ErrPortBusy) || release != nil {
		t.Fatalf("duplicate attachment: release=%v err=%v", release != nil, err)
	}
	if len(edges) != 2 {
		t.Fatalf("rejected attachment changed liveness: %v", edges)
	}
	oldRelease()
	newRelease, err := hub.AttachHandle(pair, func(context.Context, Request) (Response, error) { return Response{}, nil })
	if err != nil {
		t.Fatal(err)
	}
	oldRelease()
	if _, err := hub.Deliver(context.Background(), pair, Request{}); err != nil {
		t.Fatalf("stale release detached successor: %v", err)
	}
	newRelease()
	if _, err := hub.Deliver(context.Background(), pair, Request{}); err != ErrUnreachable {
		t.Fatalf("deliver after current release=%v want %v", err, ErrUnreachable)
	}
	if len(edges) != 5 || edges[0] || !edges[1] || edges[2] || !edges[3] || edges[4] {
		t.Fatalf("liveness edges=%v want [false true false true false]", edges)
	}
}

func TestHubOccupiedSlotsRejectConcurrentAttachments(t *testing.T) {
	for _, seat := range []bool{false, true} {
		name := "handle"
		if seat {
			name = "seat"
		}
		t.Run(name, func(t *testing.T) {
			hub := NewHub()
			pair := Pair{Host: "h", Body: "a"}
			attach, call, opposite := hub.AttachHandle, hub.Deliver, hub.AttachSeat
			if seat {
				attach, call, opposite = hub.AttachSeat, hub.Drive, hub.AttachHandle
			}
			fn := func(context.Context, Request) (Response, error) { return Response{Payload: []byte("winner")}, nil }
			var wg sync.WaitGroup
			releases := make(chan func(), 32)
			start := make(chan struct{})
			for i := 0; i < 32; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					release, err := attach(pair, fn)
					if err == nil {
						releases <- release
						return
					}
					if !errors.Is(err, ErrPortBusy) || release != nil {
						t.Errorf("rejected attachment: %v", err)
					}
				}()
			}
			close(start)
			wg.Wait()
			close(releases)
			if len(releases) != 1 {
				t.Fatalf("successful attachments=%d want 1", len(releases))
			}
			release := <-releases
			defer release()
			if got, err := call(context.Background(), pair, Request{}); err != nil || string(got.Payload) != "winner" {
				t.Fatalf("winner lost: %+v %v", got, err)
			}
			other, err := opposite(pair, fn)
			if err != nil {
				t.Fatal(err)
			}
			defer other()
			independent, err := attach(Pair{Host: "other", Body: "a"}, fn)
			if err != nil {
				t.Fatal(err)
			}
			defer independent()
			release()
			next, err := attach(pair, fn)
			if err != nil {
				t.Fatalf("reattach after release: %v", err)
			}
			defer next()
			release()
			if _, err := call(context.Background(), pair, Request{}); err != nil {
				t.Fatalf("old release removed successor: %v", err)
			}
		})
	}
}
