package home

import (
	"bytes"
	"context"
	"errors"
	"io"
	"time"

	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/platform/internal/presence"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// ---------------------------------------------------------------------------
// View -- the read-only observation capability
// ---------------------------------------------------------------------------

// View is channel-home's read-only observation set. Some methods are private
// substrate projections used by Home itself; channelhost deliberately exports
// only the smaller policy-safe View interface at the membrane boundary. There
// is no write path through either surface.
type View struct {
	visible    storespec.VisibleMessageQuery
	authority  storespec.ActorDirectory
	links      *link.Acceptor
	presence   presence.View
	actors     *actorSystem
	nowMs      func() int64
	resources  storespec.ResourceReadStore
	routing    storespec.ChannelRouting
	principals storespec.PrincipalRegistry
	bindings   storespec.DaemonBindingReader
}

// View returns the full substrate observation set. It carries no write
// capability. channelhost adapters select the narrower cross-membrane subset;
// in-universe actors ask the system actor by message so their reads remain in
// the channel interaction model.
func (h *Home) View() View {
	return View{
		visible:    h.cs.Visible,
		authority:  h.cs.Authority,
		links:      h.links,
		presence:   presence.NewView(h.presenceFold, h.actors, h.cs.Authority),
		actors:     h.actors,
		nowMs:      h.nowMs,
		resources:  h.cs.ResourceRead,
		routing:    h.cs.Routing,
		principals: h.cs.Principals,
		bindings:   h.cs.Bindings,
	}
}

type ResourceView struct {
	store     storespec.ResourceReadStore
	authority storespec.ActorDirectory
}

func (v View) Resources() ResourceView {
	return ResourceView{store: v.resources, authority: v.authority}
}

func validateReader(ctx context.Context, authority storespec.ActorDirectory, as channel.Reader) error {
	if !as.Valid() {
		return &channel.RealmError{Code: channel.RealmForbidden}
	}
	if as.Mode != channel.ReaderMember {
		return nil
	}
	_, found, err := authority.LookupActive(ctx, as.ActorID)
	if err != nil {
		return err
	}
	if !found {
		return &channel.RealmError{Code: channel.RealmForbidden}
	}
	return nil
}

func (v ResourceView) List(ctx context.Context, as channel.Reader, q channel.ResourceListQuery) (channel.ResourcePage, error) {
	if err := validateReader(ctx, v.authority, as); err != nil {
		return channel.ResourcePage{}, err
	}
	return v.store.ListReadable(ctx, q)
}

func (v ResourceView) Stat(ctx context.Context, as channel.Reader, id resource.ResourceID) (channel.ResourceMeta, error) {
	if err := validateReader(ctx, v.authority, as); err != nil {
		return channel.ResourceMeta{}, err
	}
	meta, found, err := v.store.StatReadable(ctx, id)
	if err != nil {
		return channel.ResourceMeta{}, err
	}
	if !found {
		return channel.ResourceMeta{}, &channel.RealmError{Code: channel.RealmResourceNotFound}
	}
	return meta, nil
}

func (v ResourceView) Fetch(ctx context.Context, as channel.Reader, id resource.ResourceID) (channel.ResourceFetch, error) {
	if err := validateReader(ctx, v.authority, as); err != nil {
		return channel.ResourceFetch{}, err
	}
	meta, value, found, err := v.store.FetchReadable(ctx, id)
	if errors.Is(err, storespec.ErrResourceCapabilityUnavailable) {
		return channel.ResourceFetch{}, &channel.RealmError{Code: channel.RealmCapabilityUnavailable}
	}
	if err != nil {
		return channel.ResourceFetch{}, err
	}
	if !found {
		return channel.ResourceFetch{}, &channel.RealmError{Code: channel.RealmResourceNotFound}
	}
	return channel.ResourceFetch{Meta: meta, Body: io.NopCloser(bytes.NewReader(value))}, nil
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

// IsAttached reports whether daemon (compute) id has a live attach right now
// (L0 attachment) — read-time, derived from the link acceptor, never stored.
func (v View) IsAttached(daemonID string) bool {
	if v.links == nil {
		return false
	}
	return v.links.IsAttached(daemonID)
}

func (v View) ReadVisibleAfterSeq(ctx context.Context, reader channel.Reader, afterSeq int64, limit int) ([]storespec.StoredRow, int64, error) {
	if err := validateReader(ctx, v.authority, reader); err != nil {
		return nil, afterSeq, err
	}
	return v.visible.ReadVisibleAfterSeq(ctx, reader, afterSeq, limit)
}

func (v View) ActorFacts(ctx context.Context, id actor.ActorID) (channel.ActorFacts, bool, error) {
	row, found, err := v.authority.LookupActive(ctx, id)
	if err != nil || !found {
		return channel.ActorFacts{}, found, err
	}
	return channel.ActorFacts{Principal: row.Principal, Kind: row.Kind, Active: true}, true, nil
}

func (v View) DefaultAgent(ctx context.Context) (actor.ActorID, bool, error) {
	return v.routing.DefaultAgent(ctx)
}

func (v View) ResolvePrincipal(ctx context.Context, kind actor.Kind, principal string) (actor.ActorID, bool, error) {
	record, found, err := v.principals.LookupActivePrincipal(ctx, kind, principal)
	return record.ID, found, err
}

func (v View) ActiveActors(ctx context.Context) ([]storespec.ActorControlRow, error) {
	return v.authority.ListActive(ctx)
}

func (v View) DeclaredBySource(ctx context.Context, source string) ([]storespec.ActorControlRow, error) {
	rows, err := v.authority.ListActive(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]storespec.ActorControlRow, 0)
	for _, row := range rows {
		if row.SourceDeclID == source {
			out = append(out, row)
		}
	}
	return out, nil
}

func (v View) DeclaredBySourceOne(ctx context.Context, source string) (storespec.ActorControlRow, bool, error) {
	rows, err := v.authority.ListActive(ctx)
	if err != nil {
		return storespec.ActorControlRow{}, false, err
	}
	for _, row := range rows {
		if row.SourceDeclID == source {
			return row, true, nil
		}
	}
	return storespec.ActorControlRow{}, false, nil
}

func (v View) IsBound(ctx context.Context, daemonID string) (bool, error) {
	return v.bindings.IsBound(ctx, storespec.DaemonID(daemonID))
}

// OwnerPrincipal returns the unique active channel owner from the channel-local
// authority. It is the owner-only realm delete policy's trusted read source.
func (v View) OwnerPrincipal(ctx context.Context) (string, bool, error) {
	rows, err := v.authority.ListActive(ctx)
	if err != nil {
		return "", false, err
	}
	for _, row := range rows {
		if row.Role == storespec.RoleOwner {
			return row.Principal, true, nil
		}
	}
	return "", false, nil
}
