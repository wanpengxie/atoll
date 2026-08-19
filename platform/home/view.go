package home

import (
	"context"
	"slices"
	"time"

	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/platform/internal/presence"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// ---------------------------------------------------------------------------
// View -- the read-only observation capability
// ---------------------------------------------------------------------------

// View is channel-home's read-only observation set. Some methods are private
// substrate projections used by Home itself; channelhost deliberately exports
// only the smaller policy-safe View interface at the membrane boundary. There
// is no write path through either surface.
// viewAuthority is the narrow actor-truth surface the business membrane asks.
// Every method is question-shaped; there is no whole-record face here and no
// way to obtain one.
type viewAuthority interface {
	storespec.IdentityPresence
	storespec.ActorFactsAuthority
	storespec.IdentityRoster
	storespec.PrincipalIdentity
}

type View struct {
	visible   storespec.VisibleMessageQuery
	authority viewAuthority
	presence  presence.View
	actors    *actorSystem
	nowMs     func() int64
	bindings  BindingReader
	resolver  IntroductionResolver
	channelID channel.ID

	ownerPrincipal string
}

// View returns the full substrate observation set. It carries no write
// capability. channelhost adapters select the narrower cross-membrane subset;
// in-universe actors ask the system actor by message so their reads remain in
// the channel interaction model.
func (h *Home) View() View {
	return View{
		visible:        h.visible,
		authority:      h.actors,
		presence:       presence.NewView(h.presenceFold, h.actors, h.actors),
		actors:         h.actors,
		nowMs:          h.nowMs,
		bindings:       h.registryBindings,
		resolver:       h.resolver,
		channelID:      h.channelID,
		ownerPrincipal: h.ownerPrincipal,
	}
}

// Snapshot composes membership, current execution and testimony at read time. The
// fields are advisory and intentionally not a linearizable transaction.
func (v View) Snapshot(ctx context.Context, id actor.ActorID) (presence.Snapshot, error) {
	return v.presence.Snapshot(ctx, id)
}

// TestimonyAgeMs projects a fold receipt timestamp through the same clock used
// to stamp it. Clock skew is represented as age zero, never a negative age.
func (v View) TestimonyAgeMs(receivedAt int64) int64 {
	age := v.nowMs() - receivedAt
	if age < 0 {
		return 0
	}
	return age
}

// Stat reads authoritative current execution for id: live=true means the
// Server Host currently has a local body or an attached remote route. This is
// distinct from the device/L3 advisory axis (Snapshot above), which is a
// self-reported, decays-to-unknown signal.
func (v View) Stat(id actor.ActorID) (startedAt time.Time, live bool) {
	if v.actors == nil {
		return time.Time{}, false
	}
	stat, ok := v.actors.Stat(id)
	if !ok {
		return time.Time{}, false
	}
	return stat.StartedAt, true
}

func (v View) ReadVisibleAfterSeq(ctx context.Context, afterSeq int64, limit int) ([]storespec.StoredRow, int64, error) {
	return v.visible.ReadVisibleAfterSeq(ctx, afterSeq, limit)
}

// IsActive is the narrow membership question used by boundary readers before
// they enter the unscoped visible-log read port.
func (v View) IsActive(ctx context.Context, id actor.ActorID) (bool, error) {
	return v.authority.IsActive(ctx, id)
}

// ActorFacts is the requester-authorization projection: who is behind this
// actor and what kind it is.
func (v View) ActorFacts(ctx context.Context, id actor.ActorID) (channelspec.ActorFacts, bool, error) {
	facts, found, err := v.authority.ActorFacts(ctx, id)
	if err != nil || !found {
		return channelspec.ActorFacts{}, found, err
	}
	return channelspec.ActorFacts{
		Principal: facts.Principal, SourceDeclID: facts.SourceDeclID,
		Kind: facts.Kind, Active: true,
	}, true, nil
}

// ResolvePrincipal turns a login principal into the member behind it in THIS
// channel. It asks the Controller, which is the authority on who is a member
// right now — reading the registry directly would answer from a second ledger,
// one that can hold a member the Controller has not published yet and can still
// hold one it has already ended.
//
// There is no kind to pass: a principal is a human-only fact.
func (v View) ResolvePrincipal(_ context.Context, principal string) (actor.ActorID, bool, error) {
	return v.authority.ResolvePrincipal(principal)
}

// HumanRoster is the entitlement projection: which login principals hold human
// membership right now. It composes two question-shaped Controller reads — the
// membership roster and the per-identity facts — inside the business membrane,
// which is where composition belongs. An actor that ends between the two reads
// simply drops out of the roster; this is a level-reconciled sweep input, not a
// linearizable transaction.
func (v View) HumanRoster(ctx context.Context) ([]channelspec.HumanRosterEntry, error) {
	identities, err := v.authority.ActiveIdentities()
	if err != nil {
		return nil, err
	}
	out := make([]channelspec.HumanRosterEntry, 0, len(identities))
	for _, identity := range identities {
		if identity.Kind != actor.KindHuman {
			continue
		}
		facts, found, err := v.authority.ActorFacts(ctx, identity.ID)
		if err != nil {
			return nil, err
		}
		if !found || facts.Principal == "" {
			continue
		}
		out = append(out, channelspec.HumanRosterEntry{
			ActorID: identity.ID, Principal: facts.Principal,
		})
	}
	return out, nil
}

// Roster projects the channel's complete actor directory from the membrane's
// existing read faces. Membership is the enumeration authority; presence is an
// advisory per-row read and never removes an enumerated identity.
func (v View) Roster(ctx context.Context) ([]channelspec.ObsRosterRow, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	identities, err := v.authority.ActiveIdentities()
	if err != nil {
		return nil, err
	}
	declByActor := make(map[actor.ActorID]string, len(identities))
	declIDs := make([]string, 0, len(identities))
	seenDecl := make(map[string]bool, len(identities))
	for _, identity := range identities {
		facts, found, factsErr := v.authority.ActorFacts(ctx, identity.ID)
		if factsErr != nil {
			return nil, factsErr
		}
		if found && facts.SourceDeclID != "" {
			declByActor[identity.ID] = facts.SourceDeclID
			if !seenDecl[facts.SourceDeclID] {
				seenDecl[facts.SourceDeclID] = true
				declIDs = append(declIDs, facts.SourceDeclID)
			}
		}
	}
	declarations := map[string]channelspec.DeclarationFacts{}
	if len(declIDs) > 0 && v.resolver != nil {
		declarations, err = resolveDeclarationCatalog(ctx, v.resolver, v.channelID, declIDs)
		if err != nil {
			return nil, err
		}
	}
	out := make([]channelspec.ObsRosterRow, 0, len(identities)+1)
	for _, identity := range identities {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		declID := declByActor[identity.ID]
		declaration := declarations[declID]
		snapshot, err := v.presence.Snapshot(ctx, identity.ID)
		if err != nil {
			return nil, err
		}
		out = append(out, channelspec.ObsRosterRow{
			ID: identity.ID, Kind: identity.Kind, DeclID: declID,
			Name: declaration.Name, Description: declaration.Description,
			Bound: snapshot.L1Present, Device: rosterDeviceState(snapshot),
		})
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	kernelSnapshot, err := v.presence.Snapshot(ctx, actor.SystemActorID)
	if err != nil {
		return nil, err
	}
	out = append(out, channelspec.ObsRosterRow{
		ID: actor.SystemActorID, Kind: actor.KindSystem,
		Bound: kernelSnapshot.L1Present, Device: rosterDeviceState(kernelSnapshot),
	})
	slices.SortFunc(out, func(left, right channelspec.ObsRosterRow) int {
		switch {
		case left.ID < right.ID:
			return -1
		case left.ID > right.ID:
			return 1
		default:
			return 0
		}
	})
	return out, nil
}

func rosterDeviceState(snapshot presence.Snapshot) channelspec.DeviceState {
	testimony, found := snapshot.L3[actorrt.ObsKind(introspect.ObsDevicePresence)]
	if !found {
		return channelspec.DeviceState{Kind: channelspec.DeviceAbsent}
	}
	if testimony.StaleFromPriorLife {
		return channelspec.DeviceState{Kind: channelspec.DeviceStale, ReceivedAt: testimony.ReceivedAt}
	}
	value, ok := introspect.ParseDevicePresence(testimony.Val)
	if !ok {
		return channelspec.DeviceState{Kind: channelspec.DeviceMalformed}
	}
	return channelspec.DeviceState{Kind: channelspec.DeviceKnown, Online: value.Online, ReceivedAt: testimony.ReceivedAt}
}

func (v View) IsBound(ctx context.Context, daemonID string) (bool, error) {
	return v.bindings.IsBound(ctx, v.channelID, daemonID)
}

// OwnerPrincipal returns the channel owner straight from the immutable genesis
// pointer — its one and only home. It never scans the registry: owner is a
// property of the channel, not a bit on a member row.
func (v View) OwnerPrincipal(context.Context) (string, bool, error) {
	if v.ownerPrincipal == "" {
		return "", false, nil
	}
	return v.ownerPrincipal, true, nil
}
