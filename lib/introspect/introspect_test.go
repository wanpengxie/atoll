package introspect

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
)

// plainImpl does NOT implement Describer — exercises the identity-only branch.
type plainImpl struct{}

// describerImpl implements Describer, returning a fixed API list.
type describerImpl struct {
	apis []APIDescriptor
	err  error
}

func (d describerImpl) Describe(ctx context.Context) ([]APIDescriptor, error) {
	return d.apis, d.err
}

// ctxAwareDescriber asserts the context threads through to the hook.
type ctxAwareDescriber struct {
	seenCtx context.Context
}

func (c *ctxAwareDescriber) Describe(ctx context.Context) ([]APIDescriptor, error) {
	c.seenCtx = ctx
	return nil, nil
}

func TestBuildDescribe_IdentityOnly_NonDescriber(t *testing.T) {
	got, err := BuildDescribe(context.Background(), "alice", plainImpl{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := Describe{Name: "alice"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
	if got.APIs != nil {
		t.Fatalf("expected nil APIs for non-Describer, got %+v", got.APIs)
	}
}

func TestBuildDescribe_IdentityOnly_NilImpl(t *testing.T) {
	// A nil impl is not a Describer; answer must be identity-only.
	got, err := BuildDescribe(context.Background(), "bob", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Name != "bob" || got.APIs != nil || got.Binding != "" {
		t.Fatalf("unexpected describe: %+v", got)
	}
}

func TestBuildDescribe_WithDescriberAPIs(t *testing.T) {
	apis := []APIDescriptor{
		{
			Name:   "notes.publish",
			Schema: json.RawMessage(`{"type":"object"}`),
			Desc:   "publish a note",
		},
		{Name: "notes.delete"},
	}
	got, err := BuildDescribe(context.Background(), "notes", describerImpl{apis: apis})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Name != "notes" {
		t.Fatalf("name: got %q, want %q", got.Name, "notes")
	}
	if !reflect.DeepEqual(got.APIs, apis) {
		t.Fatalf("APIs: got %+v, want %+v", got.APIs, apis)
	}
}

func TestBuildDescribe_DescriberReturnsNilAPIs(t *testing.T) {
	// A Describer may legitimately report no callable surface (nil slice).
	got, err := BuildDescribe(context.Background(), "quiet", describerImpl{apis: nil})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Name != "quiet" {
		t.Fatalf("name: got %q, want %q", got.Name, "quiet")
	}
	if got.APIs != nil {
		t.Fatalf("expected nil APIs, got %+v", got.APIs)
	}
}

func TestBuildDescribe_DescriberError(t *testing.T) {
	sentinel := errors.New("describe boom")
	got, err := BuildDescribe(context.Background(), "broken", describerImpl{err: sentinel})
	if !errors.Is(err, sentinel) {
		t.Fatalf("error: got %v, want %v", err, sentinel)
	}
	// On error the answer must be the zero Describe (no partial identity leak).
	if !reflect.DeepEqual(got, Describe{}) {
		t.Fatalf("expected zero Describe on error, got %+v", got)
	}
}

func TestBuildDescribe_ContextThreadsToHook(t *testing.T) {
	type ctxKey struct{}
	ctx := context.WithValue(context.Background(), ctxKey{}, "v")
	impl := &ctxAwareDescriber{}
	if _, err := BuildDescribe(ctx, "x", impl); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if impl.seenCtx == nil {
		t.Fatal("Describe hook was not invoked")
	}
	if got := impl.seenCtx.Value(ctxKey{}); got != "v" {
		t.Fatalf("context not threaded through: got %v", got)
	}
}

// Guard the frozen convention constants and response shapes so an accidental
// rename/field change trips a test.
func TestReservedQueryNames(t *testing.T) {
	if QueryDescribe != "actor.describe" {
		t.Fatalf("QueryDescribe drifted: %q", QueryDescribe)
	}
	if QueryList != "actor.list" {
		t.Fatalf("QueryList drifted: %q", QueryList)
	}
}

func TestResponseShapesMarshal(t *testing.T) {
	c := Catalog{Actors: []CatalogEntry{
		{ID: "a1", Kind: "agent", Binding: "b", Present: true, UptimeMs: 1500},
	}}
	b, err := json.Marshal(c)
	if err != nil {
		t.Fatalf("marshal catalog: %v", err)
	}
	var round Catalog
	if err := json.Unmarshal(b, &round); err != nil {
		t.Fatalf("unmarshal catalog: %v", err)
	}
	if !reflect.DeepEqual(round, c) {
		t.Fatalf("catalog round-trip mismatch: got %+v want %+v", round, c)
	}
}
