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
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

func (h *Home) isBound(ctx context.Context, daemonID string) (bool, error) {
	if h.closed.Load() {
		return false, ErrClosed
	}
	return h.cs.Bindings.IsBound(ctx, storespec.DaemonID(daemonID))
}

// ---------------------------------------------------------------------------
// View -- the read-only observation capability
// ---------------------------------------------------------------------------

// View is the channel-home's read-only observation set: committed message tail
// (ReadAfterSeq), head cursor (MaxSeq), and active actor roster (ListActors). It
// holds only read interfaces — there is no write path through a View.
type View struct {
	query      storespec.MessageQuery
	visible    storespec.VisibleMessageQuery
	authority  storespec.ActorAuthority
	links      *link.Acceptor
	presence   presence.View
	rt         *actorrt.Runtime
	nowMs      func() int64
	resources  storespec.ResourceReadStore
	control    *actorControlIndex
	routing    storespec.ChannelRouting
	principals storespec.PrincipalRegistry
	bindings   storespec.DaemonBindingReader
}

// View returns the read-only observation set (ReadAfterSeq / MaxSeq /
// ListActors / daemon attachment). It carries no write capability — observation
// only. The host (app) reads these projections OUT-OF-BAND (no message, no
// truth-log write) — UI status polling must not pollute the log; in-universe
// actors instead ask the system actor by message (that path is logged).
func (h *Home) View() View {
	return View{
		query:      h.cs.Query,
		visible:    h.cs.Visible,
		authority:  h.cs.Authority,
		links:      h.links,
		presence:   presence.NewView(h.presenceFold, h.channel.Cells(), h.cs.Authority),
		rt:         h.channel.Cells(),
		nowMs:      h.nowMs,
		resources:  h.cs.ResourceRead,
		control:    h.controlIndex,
		routing:    h.cs.Routing,
		principals: h.cs.Principals,
		bindings:   h.cs.Bindings,
	}
}

type ResourceView struct {
	store     storespec.ResourceReadStore
	authority storespec.ActorAuthority
}

func (v View) Resources() ResourceView {
	return ResourceView{store: v.resources, authority: v.authority}
}

func validateReader(ctx context.Context, authority storespec.ActorAuthority, as channel.Reader) error {
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

// Snapshot composes membership, embodiment and testimony at read time. The
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

// Stat reads the authoritative embodiment presence for id: live=true means id
// has a live embodiment on THIS home right now (cell or attached port — the
// `kill -0` read, actorrt.Runtime.Stat, transport-neutral). This is NOT the
// device/L3 advisory axis (Snapshot above): that is a self-reported,
// three-state, decays-to-unknown push signal from the actor's own client;
// this is the substrate's own authoritative self-read of embodiment, never
// asked of the actor, never advisory. The two axes answer different
// questions and must not be conflated.
func (v View) Stat(id actor.ActorID) (startedAt time.Time, live bool) {
	if v.rt == nil {
		return time.Time{}, false
	}
	stat, ok := v.rt.Stat(id)
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
	return v.control.ListActive(ctx)
}

func (v View) DeclaredBySource(ctx context.Context, source string) ([]storespec.ActorControlRow, error) {
	rows, err := v.control.ListActive(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]storespec.ActorControlRow, 0)
	for _, row := range rows {
		world, ok, worldErr := v.control.WorldOf(ctx, row.ID)
		if worldErr != nil {
			return nil, worldErr
		}
		if ok && world == storespec.WorldDurable && row.SourceDeclID == source {
			out = append(out, row)
		}
	}
	return out, nil
}

func (v View) DeclaredBySourceOne(ctx context.Context, source string) (storespec.ActorControlRow, bool, error) {
	rows, err := v.control.ListActive(ctx)
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

// MaxSeq returns the channel's current head seq (client cursor anchor).
func (v View) MaxSeq(ctx context.Context) (int64, error) {
	return v.query.MaxSeq(ctx)
}

// ListActors returns the compatibility membership projection of every active
// identity. The source is ActorAuthority; durable registry history is not a
// live control-plane read path.
func (v View) ListActors(ctx context.Context) ([]storespec.Record, error) {
	rows, err := v.authority.ListActive(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]storespec.Record, 0, len(rows))
	for _, row := range rows {
		out = append(out, storespec.Record{
			ID: row.ID, Kind: row.Kind, Principal: row.Principal,
			Binding: row.Binding, CreatedAt: row.CreatedAt,
		})
	}
	return out, nil
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
