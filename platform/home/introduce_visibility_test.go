package home_test

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"

	"github.com/wanpengxie/atoll/platform/home"
	"github.com/wanpengxie/atoll/platform/internal/sysactor"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

// Introducing a declaration is a permission verdict, and it is reached in one
// place for both doors: the out-of-band admission call (SysOp().Introduce) and
// the in-gate operate frame (channel.introduce_actor) both land on
// Home.resolveIntroduction.
//
// The verdict for a NON-PUBLIC declaration is ownership, and ownership is a
// principal fact. These tests pin that the verdict reads the initiator's
// principal from the roster rather than accepting the door's claim about what
// kind of initiator it is — the two doors admit different populations (the
// operate gate authorizes any active actor, the HTTP gate resolves a session to
// a human), and a verdict parameterised by the door produces different answers
// for one and the same initiator.
//
// The non-human half matters on its own: the store forbids a non-human
// admission a login principal at all, so an agent initiator carries "". If the
// check were only "principal equals owner", an empty owner would match it. That
// no declaration currently has an empty owner is a realm-side invariant living
// two packages away; the verdict refuses a principal-less initiator outright
// instead of leaning on it.

type visibilityRealm struct {
	facts map[string]channel.DeclarationFacts
}

func (r visibilityRealm) ResolveDeclaration(
	_ context.Context, _ channel.ID, declID string,
) (channel.DeclarationFacts, error) {
	facts, ok := r.facts[declID]
	if !ok {
		return channel.DeclarationFacts{}, channel.ErrDeclarationNotFound
	}
	return facts, nil
}

func (visibilityRealm) ClassKind(context.Context, string) (actor.Kind, bool, error) {
	return actor.KindAgent, true, nil
}

func (visibilityRealm) DaemonFacts(context.Context, string) (channel.DaemonFacts, error) {
	return channel.DaemonFacts{}, nil
}

func openVisibilityHome(t *testing.T, realm visibilityRealm) *home.Home {
	t.Helper()
	h, err := home.Open(home.Config{
		CompositionResolver:  emptyCompositionResolver{},
		IntroductionResolver: realm,
		ChannelID:            testChannelID,
		DBPath:               filepath.Join(t.TempDir(), "home.sqlite"),
		Bootstrap:            true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = home.Shutdown(h) })
	// Every declaration-backed actor is daemon-placed, so an introduction that
	// clears the permission verdict still needs a host to land on. Bind one:
	// without it every case below would end in the same placement refusal and
	// the verdict itself would go untested.
	if _, err := home.SystemOps(h).AttachDaemon(context.Background(), channel.DaemonRequest{
		Ref: "attach-host", DaemonID: "daemon-1",
	}); err != nil {
		t.Fatalf("AttachDaemon: %v", err)
	}
	return h
}

func admitHuman(t *testing.T, h *home.Home, principal string) actor.ActorID {
	t.Helper()
	result, err := home.SystemOps(h).Admit(context.Background(), channel.AdmitRequest{
		Ref: "admit-" + principal, Principal: principal,
	})
	if err != nil {
		t.Fatalf("Admit(%s): %v", principal, err)
	}
	return result.ActorID
}

// introduceViaOperateFrame drives the OTHER door: the in-gate control frame
// (channel.introduce_actor), which reaches opEntry.Execute rather than
// opEntry.Introduce. Its gate authorizes any active actor — the comment on it
// says so outright ("an agent member may be delegated channel management") —
// which is precisely why the verdict may not be told by the door what kind of
// initiator it is dealing with.
func introduceViaOperateFrame(
	t *testing.T, h *home.Home, sender actor.ActorID, declID string,
) error {
	t.Helper()
	executor, ok := home.SystemOps(h).(sysactor.OperateExecutor)
	if !ok {
		t.Fatal("the realm adapter no longer serves the operate face; this test can no longer reach the second door")
	}
	payload, err := json.Marshal(struct {
		DeclID string `json:"decl_id"`
	}{declID})
	if err != nil {
		t.Fatal(err)
	}
	_, err = executor.Execute(context.Background(), sysactor.TypeIntroduceActor, sysactor.OperateRequest{
		ChannelID: testChannelID, Sender: sender, Anchor: "anchor-" + declID, Payload: payload,
	})
	return err
}

func forbidden(t *testing.T, err error) *channel.OperationError {
	t.Helper()
	var opErr *channel.OperationError
	if !errors.As(err, &opErr) {
		t.Fatalf("want a channel.OperationError, got %v", err)
	}
	if opErr.Code != channel.ErrCodeForbidden {
		t.Fatalf("want %q, got %q (%s)", channel.ErrCodeForbidden, opErr.Code, opErr.Detail)
	}
	return opErr
}

// A public declaration is anyone's to place: the owner field is not consulted.
func TestIntroducePublicDeclarationIgnoresOwnership(t *testing.T) {
	t.Parallel()
	h := openVisibilityHome(t, visibilityRealm{facts: map[string]channel.DeclarationFacts{
		"pub": {OwnerPrincipal: "alice", Visibility: "public", Class: "go-kimi"},
	}})
	bob := admitHuman(t, h, "bob")

	result, err := home.SystemOps(h).Introduce(context.Background(), channel.IntroduceRequest{
		Ref: "intro-pub", DeclID: "pub", InitiatorActorID: bob,
	})
	if err != nil {
		t.Fatalf("a public declaration must be placeable by any member: %v", err)
	}
	if result.ActorID == "" {
		t.Fatal("introduce returned no actor id")
	}
}

// The owner of a private declaration may place it. This is the case the two
// doors used to disagree on: reached through the admission call it succeeded,
// reached through the operate frame it was refused, for the same human and the
// same declaration.
func TestIntroducePrivateDeclarationByItsOwnerIsAllowed(t *testing.T) {
	t.Parallel()
	h := openVisibilityHome(t, visibilityRealm{facts: map[string]channel.DeclarationFacts{
		"mine": {OwnerPrincipal: "alice", Visibility: "private", Class: "go-kimi"},
	}})
	alice := admitHuman(t, h, "alice")

	result, err := home.SystemOps(h).Introduce(context.Background(), channel.IntroduceRequest{
		Ref: "intro-mine", DeclID: "mine", InitiatorActorID: alice,
	})
	if err != nil {
		t.Fatalf("the owner must be able to place their own private declaration: %v", err)
	}
	if result.ActorID == "" {
		t.Fatal("introduce returned no actor id")
	}
}

// A12's regression guard: one human, one private declaration they own, both
// doors. The admission call used to allow it and the operate frame used to
// refuse it, because the verdict took a boolean from the door instead of
// reading the initiator's own facts. Whatever the answer is, it must be the
// same answer.
func TestBothIntroductionDoorsAgreeOnAnOwnersPrivateDeclaration(t *testing.T) {
	t.Parallel()
	h := openVisibilityHome(t, visibilityRealm{facts: map[string]channel.DeclarationFacts{
		"mine":  {OwnerPrincipal: "alice", Visibility: "private", Class: "go-kimi"},
		"mine2": {OwnerPrincipal: "alice", Visibility: "private", Class: "go-kimi"},
	}})
	alice := admitHuman(t, h, "alice")

	_, viaAdmission := home.SystemOps(h).Introduce(context.Background(), channel.IntroduceRequest{
		Ref: "intro-mine", DeclID: "mine", InitiatorActorID: alice,
	})
	viaFrame := introduceViaOperateFrame(t, h, alice, "mine2")

	if (viaAdmission == nil) != (viaFrame == nil) {
		t.Fatalf("the two doors disagree on one owner placing their own private declaration: admission=%v operate frame=%v",
			viaAdmission, viaFrame)
	}
	if viaAdmission != nil {
		t.Fatalf("the owner was refused their own private declaration by both doors: %v", viaAdmission)
	}
}

// The same agreement on the refusing side, so the guard above cannot be
// satisfied by both doors simply saying yes to everything.
func TestBothIntroductionDoorsAgreeOnAnotherPrincipalsPrivateDeclaration(t *testing.T) {
	t.Parallel()
	h := openVisibilityHome(t, visibilityRealm{facts: map[string]channel.DeclarationFacts{
		"hers":  {OwnerPrincipal: "alice", Visibility: "private", Class: "go-kimi"},
		"hers2": {OwnerPrincipal: "alice", Visibility: "private", Class: "go-kimi"},
	}})
	bob := admitHuman(t, h, "bob")

	_, viaAdmission := home.SystemOps(h).Introduce(context.Background(), channel.IntroduceRequest{
		Ref: "intro-hers", DeclID: "hers", InitiatorActorID: bob,
	})
	viaFrame := introduceViaOperateFrame(t, h, bob, "hers2")

	if viaAdmission == nil || viaFrame == nil {
		t.Fatalf("another principal's private declaration was placed: admission=%v operate frame=%v",
			viaAdmission, viaFrame)
	}
}

// Someone else's private declaration is refused, whichever door asked.
func TestIntroducePrivateDeclarationByAnotherPrincipalIsForbidden(t *testing.T) {
	t.Parallel()
	h := openVisibilityHome(t, visibilityRealm{facts: map[string]channel.DeclarationFacts{
		"hers": {OwnerPrincipal: "alice", Visibility: "private", Class: "go-kimi"},
	}})
	bob := admitHuman(t, h, "bob")

	_, err := home.SystemOps(h).Introduce(context.Background(), channel.IntroduceRequest{
		Ref: "intro-hers", DeclID: "hers", InitiatorActorID: bob,
	})
	if err == nil {
		t.Fatal("another principal's private declaration was placed")
	}
	forbidden(t, err)
}

// An initiator carrying no principal owns nothing. Non-human admissions are
// exactly that population — the store refuses them a login principal — and the
// operate gate authorizes any active actor, so this initiator is reachable.
// The refusal must not depend on declaration owners happening to be non-empty.
func TestIntroducePrivateDeclarationByPrincipalLessInitiatorIsForbidden(t *testing.T) {
	t.Parallel()
	h := openVisibilityHome(t, visibilityRealm{facts: map[string]channel.DeclarationFacts{
		"pub":     {OwnerPrincipal: "alice", Visibility: "public", Class: "go-kimi"},
		"private": {OwnerPrincipal: "alice", Visibility: "private", Class: "go-kimi"},
		// An owner field that never got filled. No realm path writes this today;
		// the verdict must refuse a principal-less initiator on its own terms
		// rather than because "" never equals a real owner.
		"ownerless": {OwnerPrincipal: "", Visibility: "private", Class: "go-kimi"},
	}})
	ctx := context.Background()
	alice := admitHuman(t, h, "alice")

	// A declared agent is an active member with no principal of its own.
	agent, err := home.SystemOps(h).Introduce(ctx, channel.IntroduceRequest{
		Ref: "intro-agent", DeclID: "pub", InitiatorActorID: alice,
	})
	if err != nil {
		t.Fatalf("seeding the agent initiator: %v", err)
	}
	facts, found, err := h.View().ActorFacts(ctx, agent.ActorID)
	if err != nil || !found {
		t.Fatalf("ActorFacts(agent): found=%v err=%v", found, err)
	}
	if facts.Kind == actor.KindHuman || facts.Principal != "" {
		t.Fatalf("premise void: the seeded initiator carries kind=%q principal=%q, want a non-human with no principal",
			facts.Kind, facts.Principal)
	}

	if _, err := home.SystemOps(h).Introduce(ctx, channel.IntroduceRequest{
		Ref: "intro-private-by-agent", DeclID: "private", InitiatorActorID: agent.ActorID,
	}); err == nil {
		t.Fatal("a principal-less initiator placed a private declaration")
	} else {
		forbidden(t, err)
	}

	if _, err := home.SystemOps(h).Introduce(ctx, channel.IntroduceRequest{
		Ref: "intro-ownerless-by-agent", DeclID: "ownerless", InitiatorActorID: agent.ActorID,
	}); err == nil {
		t.Fatal("empty principal matched an empty owner — the refusal is leaning on realm-side invariants")
	} else {
		forbidden(t, err)
	}
}
