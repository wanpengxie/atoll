package channelmember

import (
	"context"
	"testing"
)

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
	if len(edges) != 4 || edges[0] || !edges[1] || !edges[2] || edges[3] {
		t.Fatalf("liveness edges=%v want [false true true false]", edges)
	}
}
