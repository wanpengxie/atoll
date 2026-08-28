package gateway

import (
	"testing"
)

func TestHumanSessionsListsOnlyThePrincipalsLiveConnections(t *testing.T) {
	g := newTestGateway(t, Config{Resolver: newResolver()}, settings{})

	web, err := g.Attach("alice", nil)
	if err != nil {
		t.Fatal(err)
	}
	web.SetLabel("Mac Chrome")
	phone, err := g.Attach("alice", nil)
	if err != nil {
		t.Fatal(err)
	}
	phone.SetLabel("Android Chrome")
	other, err := g.Attach("bob", nil)
	if err != nil {
		t.Fatal(err)
	}
	other.SetLabel("Bob's browser")
	t.Cleanup(func() {
		web.Close()
		phone.Close()
		other.Close()
	})

	got := g.HumanSessions("alice")
	if len(got) != 2 {
		t.Fatalf("alice sessions=%v, want 2", got)
	}
	seen := map[string]string{}
	for _, session := range got {
		seen[session.ID] = session.Label
	}
	if seen[web.ID()] != "Mac Chrome" || seen[phone.ID()] != "Android Chrome" {
		t.Fatalf("alice sessions=%v", got)
	}
	if _, leaked := seen[other.ID()]; leaked {
		t.Fatalf("bob's session leaked into alice's result: %v", got)
	}

	phone.Close()
	got = g.HumanSessions("alice")
	if len(got) != 1 || got[0].ID != web.ID() {
		t.Fatalf("closed phone remained in directory: %v", got)
	}
	if empty := g.HumanSessions("nobody"); len(empty) != 0 || empty == nil {
		t.Fatalf("missing principal sessions=%v, want a non-nil empty snapshot", empty)
	}
}
