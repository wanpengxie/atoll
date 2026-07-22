package home

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

type fixedIntroductionResolver struct {
	facts channel.DeclarationFacts
	kind  actor.Kind
}

type mutableIntroductionResolver struct {
	mu          sync.Mutex
	facts       channel.DeclarationFacts
	kind        actor.Kind
	err         error
	kindErr     error
	kindMissing bool
	calls       int
	daemonFacts channel.DaemonFacts
	daemonErr   error
}

func (r *mutableIntroductionResolver) ResolveDeclaration(context.Context, channel.ID, string) (channel.DeclarationFacts, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	facts := r.facts
	facts.Config = append(json.RawMessage(nil), facts.Config...)
	return facts, r.err
}

func (r *mutableIntroductionResolver) ClassKind(context.Context, string) (actor.Kind, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.kind, !r.kindMissing, r.kindErr
}

func (r *mutableIntroductionResolver) DaemonFacts(context.Context, string) (channel.DaemonFacts, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.daemonFacts, r.daemonErr
}

func (r *mutableIntroductionResolver) update(fn func(*mutableIntroductionResolver)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	fn(r)
}

func (r *mutableIntroductionResolver) callCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

func TestDaemonTombstonePullDetachesDurableRootAndForkClosure(t *testing.T) {
	ctx := context.Background()
	resolver := &mutableIntroductionResolver{kind: actor.KindAgent, facts: channel.DeclarationFacts{
		OwnerPrincipal: "owner", Visibility: "private", Class: "test-agent",
	}}
	h, err := Open(Config{
		ChannelID: "daemon-tombstone-pull", DBPath: filepath.Join(t.TempDir(), "channel.sqlite"), Bootstrap: true,
		CompositionResolver: emptyCompositionResolver{}, IntroductionResolver: resolver,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer h.closeInternal("test")
	owner, err := h.admitChannelOwner(ctx, "owner")
	if err != nil {
		t.Fatal(err)
	}
	ops := SystemOps(h)
	if _, err := ops.AttachDaemon(ctx, channel.DaemonRequest{Ref: "tombstone:attach", DaemonID: "daemon-a"}); err != nil {
		t.Fatal(err)
	}
	introduced, err := ops.Introduce(ctx, channel.IntroduceRequest{Ref: "tombstone:introduce", DeclID: "decl-a", InitiatorActorID: owner})
	if err != nil {
		t.Fatal(err)
	}
	child, err := h.forkAdmission(ctx, introduced.ActorID, 1, actorrt.ForkSpec{Kind: actor.KindAgent, Class: "fork-child"}, "daemon-child")
	if err != nil {
		t.Fatal(err)
	}
	resolver.update(func(r *mutableIntroductionResolver) { r.daemonFacts.Deleted = true })
	h.reconcileDaemonTombstones(ctx)
	if bound, err := h.View().IsBound(ctx, "daemon-a"); err != nil || bound {
		t.Fatalf("binding after tombstone=(%v,%v)", bound, err)
	}
	for _, id := range []actor.ActorID{introduced.ActorID, child} {
		if _, active, err := h.controlIndex.LookupActive(ctx, id); err != nil || active {
			t.Fatalf("actor %s survived daemon closure: active=%v err=%v", id, active, err)
		}
	}
}

func TestDaemonFactsFailureDoesNotDetach(t *testing.T) {
	ctx := context.Background()
	resolver := &mutableIntroductionResolver{kind: actor.KindAgent, daemonErr: errors.New("realm unavailable"), facts: channel.DeclarationFacts{
		OwnerPrincipal: "owner", Visibility: "private", Class: "test-agent",
	}}
	h, err := Open(Config{
		ChannelID: "daemon-facts-failure", DBPath: filepath.Join(t.TempDir(), "channel.sqlite"), Bootstrap: true,
		CompositionResolver: emptyCompositionResolver{}, IntroductionResolver: resolver,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer h.closeInternal("test")
	if _, err := SystemOps(h).AttachDaemon(ctx, channel.DaemonRequest{Ref: "failure:attach", DaemonID: "daemon-a"}); err != nil {
		t.Fatal(err)
	}
	h.reconcileDaemonTombstones(ctx)
	if bound, err := h.View().IsBound(ctx, "daemon-a"); err != nil || !bound {
		t.Fatalf("resolver fault detached binding: bound=%v err=%v", bound, err)
	}
}

func TestDetachDaemonRacingForkLeavesNoOrphan(t *testing.T) {
	ctx := context.Background()
	resolver := &mutableIntroductionResolver{kind: actor.KindAgent, facts: channel.DeclarationFacts{
		OwnerPrincipal: "owner", Visibility: "private", Class: "test-agent",
	}}
	h, err := Open(Config{
		ChannelID: "detach-fork-race", DBPath: filepath.Join(t.TempDir(), "channel.sqlite"), Bootstrap: true,
		CompositionResolver: emptyCompositionResolver{}, IntroductionResolver: resolver,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer h.closeInternal("test")
	owner, err := h.admitChannelOwner(ctx, "owner")
	if err != nil {
		t.Fatal(err)
	}
	ops := SystemOps(h)
	for i := 0; i < 12; i++ {
		if _, err := ops.AttachDaemon(ctx, channel.DaemonRequest{Ref: fmt.Sprintf("race:attach:%d", i), DaemonID: "daemon-a"}); err != nil {
			t.Fatal(err)
		}
		parent, err := ops.Introduce(ctx, channel.IntroduceRequest{
			Ref: fmt.Sprintf("race:introduce:%d", i), DeclID: fmt.Sprintf("decl-%d", i), InitiatorActorID: owner,
		})
		if err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		var child actor.ActorID
		var forkErr, detachErr error
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			child, forkErr = h.forkAdmission(ctx, parent.ActorID, 1, actorrt.ForkSpec{Kind: actor.KindAgent, Class: "fork-child"}, fmt.Sprintf("race-child-%d", i))
		}()
		go func() {
			defer wg.Done()
			<-start
			_, detachErr = ops.DetachDaemon(ctx, channel.DaemonRequest{Ref: fmt.Sprintf("race:detach:%d", i), DaemonID: "daemon-a"})
		}()
		close(start)
		wg.Wait()
		if detachErr != nil {
			t.Fatalf("iteration %d detach: %v", i, detachErr)
		}
		if forkErr != nil && !errors.Is(forkErr, ErrForkParentGone) && !errors.Is(forkErr, ErrEndNotMember) {
			t.Fatalf("iteration %d fork: %v", i, forkErr)
		}
		for _, id := range []actor.ActorID{parent.ActorID, child} {
			if id == "" {
				continue
			}
			if _, active, err := h.controlIndex.LookupActive(ctx, id); err != nil || active {
				t.Fatalf("iteration %d orphan %s: active=%v err=%v", i, id, active, err)
			}
		}
	}
}

func (r fixedIntroductionResolver) ResolveDeclaration(context.Context, channel.ID, string) (channel.DeclarationFacts, error) {
	return r.facts, nil
}

func (r fixedIntroductionResolver) ClassKind(context.Context, string) (actor.Kind, bool, error) {
	return r.kind, true, nil
}

func (r fixedIntroductionResolver) DaemonFacts(context.Context, string) (channel.DaemonFacts, error) {
	return channel.DaemonFacts{}, nil
}

func TestPostCommitActorPublicationIgnoresExpiredRequestContext(t *testing.T) {
	h, err := Open(Config{
		ChannelID: "postcommit-publication", DBPath: filepath.Join(t.TempDir(), "channel.sqlite"), Bootstrap: true,
		CompositionResolver: emptyCompositionResolver{}, IntroductionResolver: inertIntroductionResolver{},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer h.closeInternal("test")

	request := channel.AdmitRequest{Ref: "postcommit:admit", Principal: "late-request"}
	meta, err := systemMeta(request.Ref, request)
	if err != nil {
		t.Fatal(err)
	}
	committed, err := h.opEntry.admission.Admit(context.Background(), storespec.AdmitTx{
		SysOpMeta: meta, Principal: request.Principal,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, found, _ := h.controlIndex.LookupActive(context.Background(), committed.ActorID); found {
		t.Fatal("fixture unexpectedly published the directly committed actor")
	}

	expired, cancel := context.WithCancel(context.Background())
	cancel()
	if err := h.opEntry.publishActor(expired, committed.ActorID); err != nil {
		t.Fatalf("post-commit publication inherited request cancellation: %v", err)
	}
	if _, found, err := h.controlIndex.LookupActive(context.Background(), committed.ActorID); err != nil || !found {
		t.Fatalf("committed actor not published: found=%v err=%v", found, err)
	}
}

func TestOpEntryIntroducePullAndDetachUseOneDurableChain(t *testing.T) {
	ctx := context.Background()
	resolver := &mutableIntroductionResolver{kind: actor.KindAgent, facts: channel.DeclarationFacts{
		OwnerPrincipal: "owner", Visibility: "private", Class: "test-agent", Config: json.RawMessage(`{"version":1}`),
	}}
	h, err := Open(Config{
		ChannelID: "opentry", DBPath: filepath.Join(t.TempDir(), "channel.sqlite"), Bootstrap: true,
		CompositionResolver:  emptyCompositionResolver{},
		IntroductionResolver: resolver,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer h.closeInternal("test")
	ownerActor, err := h.admitChannelOwner(ctx, "owner")
	if err != nil {
		t.Fatal(err)
	}
	ops := SystemOps(h)
	if _, err := ops.AttachDaemon(ctx, channel.DaemonRequest{Ref: "adm:attach", DaemonID: "daemon-a"}); err != nil {
		t.Fatal(err)
	}
	introduced, err := ops.Introduce(ctx, channel.IntroduceRequest{Ref: "adm:introduce", DeclID: "decl-a", InitiatorActorID: ownerActor})
	if err != nil || !introduced.Created || introduced.ActorID == "" {
		t.Fatalf("introduce=(%+v,%v)", introduced, err)
	}
	replayed, err := ops.Introduce(ctx, channel.IntroduceRequest{Ref: "adm:introduce", DeclID: "decl-a", InitiatorActorID: ownerActor})
	if err != nil || replayed != introduced {
		t.Fatalf("replay=(%+v,%v), want %+v", replayed, err, introduced)
	}
	rows, err := h.View().DeclaredBySource(ctx, "decl-a")
	if err != nil || len(rows) != 1 || rows[0].Placement.Host != "daemon-a" {
		t.Fatalf("introduced rows=%+v err=%v", rows, err)
	}
	resolver.update(func(r *mutableIntroductionResolver) { r.facts.Config = json.RawMessage(`{"version":2}`) })
	h.reconcileDeclarations(ctx)
	rows, _ = h.View().DeclaredBySource(ctx, "decl-a")
	if len(rows) != 1 || string(rows[0].Config) != `{"version":2}` || rows[0].Placement.Host != "daemon-a" {
		t.Fatalf("applied rows=%+v", rows)
	}
	before, active, err := h.controlIndex.LookupActive(ctx, introduced.ActorID)
	if err != nil || !active {
		t.Fatal(err)
	}
	h.reconcileDeclarations(ctx)
	after, active, err := h.controlIndex.LookupActive(ctx, introduced.ActorID)
	if err != nil || !active || after.CurrentDeclVersion != before.CurrentDeclVersion {
		t.Fatalf("equal pull wrote history: before=%+v after=%+v err=%v", before, after, err)
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

func TestDeclarationSyncRealOperationEntryMintsFreshUUIDPerChangedAttempt(t *testing.T) {
	ctx := context.Background()
	resolver := &mutableIntroductionResolver{kind: actor.KindAgent}
	h, err := Open(Config{
		ChannelID: "declaration-sync-uuid", DBPath: filepath.Join(t.TempDir(), "channel.sqlite"), Bootstrap: true,
		CompositionResolver: emptyCompositionResolver{}, IntroductionResolver: resolver, ReconcileInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.closeInternal("test") })
	initial := json.RawMessage(`{"version":1}`)
	declared, err := h.declare(ctx, DeclareRequest{
		SourceDeclID: "decl:uuid", Kind: actor.KindAgent, Class: "uuid-agent", Config: &initial,
		Placement: storespec.NewServerPlacement(), CreatedAt: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatal(err)
	}
	for version := 2; version <= 3; version++ {
		config := json.RawMessage(fmt.Sprintf(`{"version":%d}`, version))
		result, err := h.opEntry.applyResolvedDeclaration(ctx, declared.Row.ID, declared.Row.SourceDeclID, declared.Row.Class, config)
		if err != nil || result.Status != storespec.DeclarationApplied {
			t.Fatalf("attempt %d=(%+v,%v)", version, result, err)
		}
	}
	rows, err := h.cs.Query.ReadAfterSeq(ctx, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	refs := make(map[string]struct{})
	seen := make([]string, 0, len(rows))
	for _, row := range rows {
		seen = append(seen, row.Envelope.Type+":"+string(row.Envelope.CorrelationID))
		if row.Envelope.Type == "sysop_started" && strings.HasPrefix(string(row.Envelope.CorrelationID), "op:ref:v1:") {
			refs[string(row.Envelope.CorrelationID)] = struct{}{}
		}
	}
	if len(refs) != 2 {
		t.Fatalf("changed attempts refs=%v rows=%v, want two distinct UUID-derived refs", refs, seen)
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
	ownerActor, err := h.admitChannelOwner(ctx, "owner")
	if err != nil {
		t.Fatal(err)
	}
	req := channel.IntroduceRequest{Ref: "adm:missing", DeclID: "missing", InitiatorActorID: ownerActor}
	for i := 0; i < 2; i++ {
		_, err := SystemOps(h).Introduce(ctx, req)
		var operationErr *channel.OperationError
		if !errors.As(err, &operationErr) || operationErr.Code != channel.ErrCodeDeclNotFound || operationErr.Retryable {
			t.Fatalf("attempt %d error=%v", i+1, err)
		}
		resolver.update(func(r *mutableIntroductionResolver) { r.err = errors.New("resolver is now unavailable") })
	}
	if resolver.callCount() != 1 {
		t.Fatalf("resolver calls=%d, terminal replay must not re-resolve", resolver.callCount())
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
	resolver := &mutableIntroductionResolver{err: errors.New("temporary outage")}
	h, err := Open(Config{
		ChannelID: "opentry-transient", DBPath: filepath.Join(t.TempDir(), "channel.sqlite"), Bootstrap: true,
		CompositionResolver: emptyCompositionResolver{}, IntroductionResolver: resolver,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer h.closeInternal("test")
	ownerActor, err := h.admitChannelOwner(ctx, "owner")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := SystemOps(h).AttachDaemon(ctx, channel.DaemonRequest{Ref: "attach-retry", DaemonID: "daemon-a"}); err != nil {
		t.Fatal(err)
	}
	req := channel.IntroduceRequest{Ref: "adm:retry", DeclID: "decl", InitiatorActorID: ownerActor}
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
	resolver.update(func(r *mutableIntroductionResolver) {
		r.err = nil
		r.kind = actor.KindAgent
		r.facts = channel.DeclarationFacts{OwnerPrincipal: "owner", Visibility: "private", Class: "test-agent"}
	})
	result, err := SystemOps(h).Introduce(ctx, req)
	if err != nil || !result.Created || result.ActorID == "" {
		t.Fatalf("same-ref retry=(%+v,%v)", result, err)
	}
	if resolver.callCount() != 2 {
		t.Fatalf("resolver calls=%d, want 2", resolver.callCount())
	}
}

// TestClassKindFaultIsRetryableOnlyAbsenceIsDecisive pins the resolver
// contract split: a ClassKind infrastructure error must come back as a
// retryable authority_unavailable with ZERO ledger rows (the dependency may
// recover and the same ref must then succeed), while the definitive
// found=false answer is the decisive unknown_class terminal.
func TestClassKindFaultIsRetryableOnlyAbsenceIsDecisive(t *testing.T) {
	ctx := context.Background()
	resolver := &mutableIntroductionResolver{
		facts:   channel.DeclarationFacts{OwnerPrincipal: "owner", Visibility: "private", Class: "test-agent"},
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
	ownerActor, err := h.admitChannelOwner(ctx, "owner")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := SystemOps(h).AttachDaemon(ctx, channel.DaemonRequest{Ref: "attach-classkind", DaemonID: "daemon-a"}); err != nil {
		t.Fatal(err)
	}
	req := channel.IntroduceRequest{Ref: "adm:classkind", DeclID: "decl", InitiatorActorID: ownerActor}
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
	resolver.update(func(r *mutableIntroductionResolver) {
		r.kindErr = nil
		r.kind = actor.KindAgent
	})
	result, err := SystemOps(h).Introduce(ctx, req)
	if err != nil || !result.Created || result.ActorID == "" {
		t.Fatalf("same-ref retry after fault=(%+v,%v)", result, err)
	}
	// The definitive absence answer, by contrast, is decisive: a fresh ref
	// lands a completed terminal carrying unknown_class.
	resolver.update(func(r *mutableIntroductionResolver) { r.kindMissing = true })
	missingReq := channel.IntroduceRequest{Ref: "adm:classkind-missing", DeclID: "decl", InitiatorActorID: ownerActor}
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
