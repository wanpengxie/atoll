package home

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/actorctl"
)

const admissionProbeKey = resource.ResourceID("admission-probe")

// openAdmissionHome is a bare channel with no genesis owner, so the owner
// terminal guard has nobody to protect and Remove is free to aim at any
// admitted human — which is exactly the population these two tests are about.
func openAdmissionHome(t *testing.T, name string) *Home {
	t.Helper()
	h, err := Open(Config{
		ChannelID:            channel.ID(name),
		DBPath:               filepath.Join(t.TempDir(), "channel.sqlite"),
		CompositionResolver:  routingResolver{},
		IntroductionResolver: inertIntroductionResolver{},
		ReconcileInterval:    time.Hour,
		Bootstrap:            true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.closeInternal("test") })
	return h
}

func admissionHumanRoster(t *testing.T, h *Home, principal string) []actor.ActorID {
	t.Helper()
	roster, err := h.View().HumanRoster(context.Background())
	if err != nil {
		t.Fatalf("HumanRoster: %v", err)
	}
	var out []actor.ActorID
	for _, entry := range roster {
		if entry.Principal == principal {
			out = append(out, entry.ActorID)
		}
	}
	return out
}

// T17. A principal is the semantic key of a human identity, so admission is a
// replay-safe question, not an event: asking twice must hand back the one
// answer, and the second asking must not claim to have created anything. Losing
// this quietly mints a second identity for a person who already has one — every
// permission, every state namespace and every outstanding conversation then
// belongs to whichever id the caller happened to be holding.
func TestAdmittingTheSamePrincipalTwiceIsOneIdentity(t *testing.T) {
	h := openAdmissionHome(t, "admission-idempotent")
	ctx := context.Background()

	first, err := h.opEntry.Admit(ctx, channelspec.AdmitRequest{
		Ref: "admit:1", Principal: "carol",
	})
	if err != nil || first.ActorID == "" {
		t.Fatalf("first admit: %+v err=%v", first, err)
	}
	if !first.Created {
		t.Fatal("the first admission of a principal did not report a creation")
	}

	second, err := h.opEntry.Admit(ctx, channelspec.AdmitRequest{
		Ref: "admit:2", Principal: "carol",
	})
	if err != nil {
		t.Fatalf("second admit: %v", err)
	}
	if second.ActorID != first.ActorID {
		t.Fatalf("re-admitting carol minted a second identity: %s then %s",
			first.ActorID, second.ActorID)
	}
	if second.Created {
		t.Fatal("a replayed admission reported itself as a creation")
	}

	// The same question asked by eight callers at once has exactly one answer.
	const callers = 8
	var wg sync.WaitGroup
	answers := make(chan channel.AdmitResult, callers)
	failures := make(chan error, callers)
	for i := range callers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			result, err := h.opEntry.Admit(ctx, channelspec.AdmitRequest{
				Ref: "admit:storm:" + string(rune('a'+i)), Principal: "carol",
			})
			if err != nil {
				failures <- err
				return
			}
			answers <- result
		}(i)
	}
	wg.Wait()
	close(answers)
	close(failures)
	for err := range failures {
		t.Fatalf("concurrent admit: %v", err)
	}
	for answer := range answers {
		if answer.ActorID != first.ActorID {
			t.Fatalf("a concurrent admit minted %s, want the single identity %s",
				answer.ActorID, first.ActorID)
		}
		if answer.Created {
			t.Fatalf("a concurrent replay reported itself as a creation: %+v", answer)
		}
	}

	// One row, one roster entry, and a different person is still a different
	// identity — the idempotence is keyed on the principal, not blanket.
	if held := admissionHumanRoster(t, h, "carol"); len(held) != 1 || held[0] != first.ActorID {
		t.Fatalf("carol holds %v in the roster, want exactly [%s]", held, first.ActorID)
	}
	other, err := h.opEntry.Admit(ctx, channelspec.AdmitRequest{
		Ref: "admit:dave", Principal: "dave",
	})
	if err != nil || other.ActorID == "" {
		t.Fatalf("admit dave: %+v err=%v", other, err)
	}
	if other.ActorID == first.ActorID {
		t.Fatalf("dave was handed carol's identity %s", other.ActorID)
	}
	identities, err := h.controller.ActiveIdentities()
	if err != nil {
		t.Fatal(err)
	}
	humans := 0
	for _, identity := range identities {
		if identity.Kind == actor.KindHuman {
			humans++
		}
	}
	if humans != 2 {
		t.Fatalf("the channel holds %d human identities after 11 admissions of 2 people: %+v",
			humans, identities)
	}
}

// T18. Idempotence is scoped to the LIVING row: once a principal's identity has
// been removed, the person is a stranger again, and coming back must mint a
// brand new identity rather than reanimating the dead one. The dead id stays
// dead — no membership, no authority, no state door — and the newcomer does not
// inherit its private state.
func TestReadmittingARemovedPrincipalMintsABrandNewIdentity(t *testing.T) {
	h := openAdmissionHome(t, "admission-rebirth")
	ctx := context.Background()

	first, err := h.opEntry.Admit(ctx, channelspec.AdmitRequest{
		Ref: "admit:erin", Principal: "erin",
	})
	if err != nil || first.ActorID == "" {
		t.Fatalf("admit erin: %+v err=%v", first, err)
	}
	dead := first.ActorID

	handle, err := h.stateHandles.ResolveAuthority(ctx, h.controller.IdentityAuthorityFor(dead))
	if err != nil {
		t.Fatalf("state handle for the first identity: %v", err)
	}
	if out, err := handle.Invoke(
		ctx, access.OpCreate, admissionProbeKey, []byte(`"first-life"`), nil,
	); err != nil || !out.Accepted() {
		t.Fatalf("first identity state create: %+v err=%v", out, err)
	}

	result, err := h.opEntry.Remove(ctx, channelspec.RemoveRequest{
		Ref: "remove:erin", Target: dead, InitiatorActorID: dead,
	})
	if err != nil {
		t.Fatalf("remove erin: %v", err)
	}
	if len(result.Removed) != 1 || result.Removed[0] != dead {
		t.Fatalf("removed=%v, want [%s]", result.Removed, dead)
	}
	if active, err := h.controller.IsActive(ctx, dead); err != nil || active {
		t.Fatalf("the removed identity active=%v err=%v", active, err)
	}

	second, err := h.opEntry.Admit(ctx, channelspec.AdmitRequest{
		Ref: "readmit:erin", Principal: "erin",
	})
	if err != nil || second.ActorID == "" {
		t.Fatalf("re-admit erin: %+v err=%v", second, err)
	}
	reborn := second.ActorID
	if reborn == dead {
		t.Fatalf("re-admitting a removed principal resurrected %s", dead)
	}
	if !second.Created {
		t.Fatal("the rebirth did not report a creation; it replayed the dead row")
	}

	if active, err := h.controller.IsActive(ctx, reborn); err != nil || !active {
		t.Fatalf("the reborn identity active=%v err=%v", active, err)
	}
	if active, err := h.controller.IsActive(ctx, dead); err != nil || active {
		t.Fatalf("the dead identity came back with its successor: active=%v err=%v", active, err)
	}
	if err := h.controller.AuthorActive(dead); !errors.Is(err, actorctl.ErrInactive) {
		t.Fatalf("the dead identity passed AuthorActive: %v", err)
	}
	if _, err := h.stateHandles.ResolveAuthority(
		ctx, h.controller.IdentityAuthorityFor(dead),
	); !errors.Is(err, accessdoor.ErrStateHandleUnavailable) {
		t.Fatalf("the dead identity minted a state handle: %v", err)
	}
	if held := admissionHumanRoster(t, h, "erin"); len(held) != 1 || held[0] != reborn {
		t.Fatalf("erin holds %v in the roster, want exactly [%s]", held, reborn)
	}

	// The newcomer's private state is its own: the predecessor's value is not
	// waiting for it under the same key.
	rebornHandle, err := h.stateHandles.ResolveAuthority(
		ctx, h.controller.IdentityAuthorityFor(reborn))
	if err != nil {
		t.Fatalf("state handle for the reborn identity: %v", err)
	}
	out, err := rebornHandle.Invoke(ctx, access.OpRead, admissionProbeKey, nil, nil)
	if err != nil {
		t.Fatalf("reborn identity state read: %v", err)
	}
	if out.Accepted() && string(out.Value) == `"first-life"` {
		t.Fatal("the reborn identity inherited its predecessor's private state")
	}
}
