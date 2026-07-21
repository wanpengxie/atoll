package home

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

type fixedIntroductionResolver struct {
	facts channel.DeclarationFacts
	kind  actor.Kind
}

type mutableIntroductionResolver struct {
	facts       channel.DeclarationFacts
	kind        actor.Kind
	err         error
	kindErr     error
	kindMissing bool
	calls       int
}

func (r *mutableIntroductionResolver) ResolveDeclaration(context.Context, channel.ID, string) (channel.DeclarationFacts, error) {
	r.calls++
	return r.facts, r.err
}

func (r *mutableIntroductionResolver) ClassKind(context.Context, string) (actor.Kind, bool, error) {
	return r.kind, !r.kindMissing, r.kindErr
}

func (r fixedIntroductionResolver) ResolveDeclaration(context.Context, channel.ID, string) (channel.DeclarationFacts, error) {
	return r.facts, nil
}

func (r fixedIntroductionResolver) ClassKind(context.Context, string) (actor.Kind, bool, error) {
	return r.kind, true, nil
}

func TestOpEntryIntroduceApplyAndDetachUseOneDurableChain(t *testing.T) {
	ctx := context.Background()
	v1, err := (channel.RenderedSnapshot{
		Class: "test-agent", Config: json.RawMessage(`{"version":1}`),
		Placement: channel.Placement{Kind: channel.PlacementDaemon}, RenderSeq: 1,
	}).Seal()
	if err != nil {
		t.Fatal(err)
	}
	h, err := Open(Config{
		ChannelID: "opentry", DBPath: filepath.Join(t.TempDir(), "channel.sqlite"), Bootstrap: true,
		CompositionResolver: emptyCompositionResolver{},
		IntroductionResolver: fixedIntroductionResolver{kind: actor.KindAgent, facts: channel.DeclarationFacts{
			OwnerPrincipal: "owner", Visibility: "private", DefaultClass: v1.Class, Rendered: v1,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer h.closeInternal("test")
	if _, err := h.admitChannelOwner(ctx, "owner"); err != nil {
		t.Fatal(err)
	}
	ops := SystemOps(h)
	if _, err := ops.AttachDaemon(ctx, channel.DaemonRequest{Ref: "adm:attach", DaemonID: "daemon-a"}); err != nil {
		t.Fatal(err)
	}
	introduced, err := ops.Introduce(ctx, channel.IntroduceRequest{Ref: "adm:introduce", DeclID: "decl-a", InitiatorPrincipal: "owner", Rendered: &v1})
	if err != nil || !introduced.Created || introduced.ActorID == "" {
		t.Fatalf("introduce=(%+v,%v)", introduced, err)
	}
	replayed, err := ops.Introduce(ctx, channel.IntroduceRequest{Ref: "adm:introduce", DeclID: "decl-a", InitiatorPrincipal: "owner", Rendered: &v1})
	if err != nil || replayed != introduced {
		t.Fatalf("replay=(%+v,%v), want %+v", replayed, err, introduced)
	}
	rows, err := h.View().DeclaredBySource(ctx, "decl-a")
	if err != nil || len(rows) != 1 || rows[0].RenderSeq != 1 || rows[0].Placement.Host != "daemon-a" {
		t.Fatalf("introduced rows=%+v err=%v", rows, err)
	}
	v2, _ := (channel.RenderedSnapshot{
		Class: "test-agent", Config: json.RawMessage(`{"version":2}`),
		Placement: channel.Placement{Kind: channel.PlacementDaemon, DesiredHost: "daemon-a"}, RenderSeq: 2,
	}).Seal()
	applied, err := ops.ApplyDeclVersion(ctx, channel.ApplyDeclVersionRequest{
		Ref: "fo:apply-v2", DeclID: "decl-a", Rendered: v2, Authority: channel.AuthorityRealm,
	})
	if err != nil || applied.Status != channel.ApplyApplied || applied.Version != 2 {
		t.Fatalf("apply=(%+v,%v)", applied, err)
	}
	rows, _ = h.View().DeclaredBySource(ctx, "decl-a")
	if len(rows) != 1 || rows[0].RenderSeq != 2 || string(rows[0].Config) != `{"version":2}` {
		t.Fatalf("applied rows=%+v", rows)
	}
	resolvedV1, _ := (channel.RenderedSnapshot{
		Class: "test-agent", Config: json.RawMessage(`{"version":1}`),
		Placement: channel.Placement{Kind: channel.PlacementDaemon, DesiredHost: "daemon-a"}, RenderSeq: 1,
	}).Seal()
	stale, err := ops.ApplyDeclVersion(ctx, channel.ApplyDeclVersionRequest{
		Ref: "fo:stale-v1", DeclID: "decl-a", Rendered: resolvedV1, Authority: channel.AuthorityRealm,
	})
	if err != nil || stale.Status != channel.ApplyStale || stale.Version != 2 {
		t.Fatalf("stale=(%+v,%v)", stale, err)
	}
	detached, err := ops.DetachDaemon(ctx, channel.DaemonRequest{Ref: "adm:detach", DaemonID: "daemon-a"})
	if err != nil || detached.Bound || len(detached.ClearedInstances) != 1 || detached.ClearedInstances[0] != introduced.ActorID {
		t.Fatalf("detach=(%+v,%v)", detached, err)
	}
	rows, err = h.View().DeclaredBySource(ctx, "decl-a")
	if err != nil || len(rows) != 0 {
		t.Fatalf("revoked rows=%+v err=%v", rows, err)
	}
}

func TestOpEntryPermanentResolverRefusalReplaysWithoutResolver(t *testing.T) {
	ctx := context.Background()
	resolver := &mutableIntroductionResolver{err: channel.ErrDeclarationNotFound}
	h, err := Open(Config{
		ChannelID: "opentry-refusal", DBPath: filepath.Join(t.TempDir(), "channel.sqlite"), Bootstrap: true,
		CompositionResolver: emptyCompositionResolver{}, IntroductionResolver: resolver,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer h.closeInternal("test")
	if _, err := h.admitChannelOwner(ctx, "owner"); err != nil {
		t.Fatal(err)
	}
	req := channel.IntroduceRequest{Ref: "adm:missing", DeclID: "missing", InitiatorPrincipal: "owner"}
	for i := 0; i < 2; i++ {
		_, err := SystemOps(h).Introduce(ctx, req)
		var operationErr *channel.OperationError
		if !errors.As(err, &operationErr) || operationErr.Code != channel.ErrCodeDeclNotFound || operationErr.Retryable {
			t.Fatalf("attempt %d error=%v", i+1, err)
		}
		resolver.err = errors.New("resolver is now unavailable")
	}
	if resolver.calls != 1 {
		t.Fatalf("resolver calls=%d, terminal replay must not re-resolve", resolver.calls)
	}
	rows, err := h.cs.Query.ReadAfterSeq(ctx, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	anchor := channel.RefCorrelation(req.Ref)
	started, completed := 0, 0
	for _, row := range rows {
		if string(row.Envelope.CorrelationID) != anchor {
			continue
		}
		switch row.Envelope.Type {
		case "sysop_started":
			started++
		case "sysop_completed":
			completed++
		}
	}
	if started != 1 || completed != 1 {
		t.Fatalf("terminal event pair=(%d,%d), want (1,1)", started, completed)
	}
}

func TestOpEntryTransientResolverFailureLeavesNoPairAndSameRefRetries(t *testing.T) {
	ctx := context.Background()
	rendered, err := (channel.RenderedSnapshot{
		Class: "test-agent", Placement: channel.Placement{Kind: channel.PlacementServer}, RenderSeq: 1,
	}).Seal()
	if err != nil {
		t.Fatal(err)
	}
	resolver := &mutableIntroductionResolver{err: errors.New("temporary outage")}
	h, err := Open(Config{
		ChannelID: "opentry-transient", DBPath: filepath.Join(t.TempDir(), "channel.sqlite"), Bootstrap: true,
		CompositionResolver: emptyCompositionResolver{}, IntroductionResolver: resolver,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer h.closeInternal("test")
	if _, err := h.admitChannelOwner(ctx, "owner"); err != nil {
		t.Fatal(err)
	}
	req := channel.IntroduceRequest{Ref: "adm:retry", DeclID: "decl", InitiatorPrincipal: "owner"}
	_, err = SystemOps(h).Introduce(ctx, req)
	var operationErr *channel.OperationError
	if !errors.As(err, &operationErr) || operationErr.Code != channel.ErrCodeAuthorityUnavailable || !operationErr.Retryable {
		t.Fatalf("transient error=%v", err)
	}
	rows, err := h.cs.Query.ReadAfterSeq(ctx, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	anchor := channel.RefCorrelation(req.Ref)
	for _, row := range rows {
		if string(row.Envelope.CorrelationID) == anchor {
			t.Fatalf("transient resolver failure left event %q", row.Envelope.Type)
		}
	}
	resolver.err = nil
	resolver.kind = actor.KindAgent
	resolver.facts = channel.DeclarationFacts{OwnerPrincipal: "owner", Visibility: "private", DefaultClass: rendered.Class, Rendered: rendered}
	result, err := SystemOps(h).Introduce(ctx, req)
	if err != nil || !result.Created || result.ActorID == "" {
		t.Fatalf("same-ref retry=(%+v,%v)", result, err)
	}
	if resolver.calls != 2 {
		t.Fatalf("resolver calls=%d, want 2", resolver.calls)
	}
}

// TestClassKindFaultIsRetryableOnlyAbsenceIsDecisive pins the resolver
// contract split: a ClassKind infrastructure error must come back as a
// retryable authority_unavailable with ZERO ledger rows (the dependency may
// recover and the same ref must then succeed), while the definitive
// found=false answer is the decisive unknown_class terminal.
func TestClassKindFaultIsRetryableOnlyAbsenceIsDecisive(t *testing.T) {
	ctx := context.Background()
	rendered, err := (channel.RenderedSnapshot{
		Class: "test-agent", Placement: channel.Placement{Kind: channel.PlacementServer}, RenderSeq: 1,
	}).Seal()
	if err != nil {
		t.Fatal(err)
	}
	resolver := &mutableIntroductionResolver{
		facts:   channel.DeclarationFacts{OwnerPrincipal: "owner", Visibility: "private", DefaultClass: rendered.Class, Rendered: rendered},
		kindErr: errors.New("registry io fault"),
	}
	h, err := Open(Config{
		ChannelID: "opentry-classkind", DBPath: filepath.Join(t.TempDir(), "channel.sqlite"), Bootstrap: true,
		CompositionResolver: emptyCompositionResolver{}, IntroductionResolver: resolver,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer h.closeInternal("test")
	if _, err := h.admitChannelOwner(ctx, "owner"); err != nil {
		t.Fatal(err)
	}
	req := channel.IntroduceRequest{Ref: "adm:classkind", DeclID: "decl", InitiatorPrincipal: "owner"}
	_, err = SystemOps(h).Introduce(ctx, req)
	var operationErr *channel.OperationError
	if !errors.As(err, &operationErr) || operationErr.Code != channel.ErrCodeAuthorityUnavailable || !operationErr.Retryable {
		t.Fatalf("ClassKind infra error=%v, want retryable authority_unavailable", err)
	}
	rows, err := h.cs.Query.ReadAfterSeq(ctx, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	anchor := channel.RefCorrelation(req.Ref)
	for _, row := range rows {
		if string(row.Envelope.CorrelationID) == anchor {
			t.Fatalf("ClassKind infra error left event %q", row.Envelope.Type)
		}
	}
	resolver.kindErr = nil
	resolver.kind = actor.KindAgent
	result, err := SystemOps(h).Introduce(ctx, req)
	if err != nil || !result.Created || result.ActorID == "" {
		t.Fatalf("same-ref retry after fault=(%+v,%v)", result, err)
	}
	// The definitive absence answer, by contrast, is decisive: a fresh ref
	// lands a completed terminal carrying unknown_class.
	resolver.kindMissing = true
	missingReq := channel.IntroduceRequest{Ref: "adm:classkind-missing", DeclID: "decl", InitiatorPrincipal: "owner"}
	_, err = SystemOps(h).Introduce(ctx, missingReq)
	if !errors.As(err, &operationErr) || operationErr.Code != channel.ErrCodeUnknownClass || operationErr.Retryable {
		t.Fatalf("ClassKind absence=%v, want decisive unknown_class", err)
	}
	rows, err = h.cs.Query.ReadAfterSeq(ctx, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	completed := 0
	for _, row := range rows {
		if string(row.Envelope.CorrelationID) == channel.RefCorrelation(missingReq.Ref) && row.Envelope.Type == "sysop_completed" {
			completed++
		}
	}
	if completed != 1 {
		t.Fatalf("unknown_class terminal count=%d, want 1", completed)
	}
}

func TestDaemonBindingAndLiveAttachmentRemainIndependentAxes(t *testing.T) {
	ctx := context.Background()
	h, err := Open(Config{
		ChannelID: "binding-axes", DBPath: filepath.Join(t.TempDir(), "channel.sqlite"), Bootstrap: true,
		CompositionResolver: emptyCompositionResolver{}, IntroductionResolver: fixedIntroductionResolver{},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer h.closeInternal("test")
	ops := SystemOps(h)
	if _, err := ops.AttachDaemon(ctx, channel.DaemonRequest{Ref: "axis:attach", DaemonID: "daemon-a"}); err != nil {
		t.Fatal(err)
	}
	bound, err := h.View().IsBound(ctx, "daemon-a")
	if err != nil || !bound || h.View().IsAttached("daemon-a") {
		t.Fatalf("bound-offline axes=(bound=%v attached=%v err=%v)", bound, h.View().IsAttached("daemon-a"), err)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		h.serveAttach(w, req, "daemon-a")
	}))
	defer srv.Close()
	dialer, err := link.Dial(ctx, "ws"+srv.URL[4:], nil, link.DialConfig{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer dialer.Close()
	deadline := time.Now().Add(2 * time.Second)
	for !h.View().IsAttached("daemon-a") && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !h.View().IsAttached("daemon-a") {
		t.Fatal("daemon link did not publish attached state")
	}

	request := channel.DaemonRequest{Ref: "axis:detach", DaemonID: "daemon-a"}
	meta, err := systemMeta(request.Ref, request)
	if err != nil {
		t.Fatal(err)
	}
	// Exercise the exact commit boundary directly: the store returns the kick as
	// a post-commit effect, leaving an observable unbound-online window until
	// OpEntry consumes that advisory effect.
	result, err := h.cs.SysOps.DetachDaemon(ctx, storespec.DetachTx{SysOpMeta: meta, DaemonID: "daemon-a"})
	if err != nil || result.Effects.KickDaemon == nil {
		t.Fatalf("detach store result=(%+v,%v)", result, err)
	}
	bound, err = h.View().IsBound(ctx, "daemon-a")
	if err != nil || bound || !h.View().IsAttached("daemon-a") {
		t.Fatalf("unbound-online axes=(bound=%v attached=%v err=%v)", bound, h.View().IsAttached("daemon-a"), err)
	}
	h.links.KickDaemon("daemon-a")
}
