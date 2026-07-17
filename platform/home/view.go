package home

import (
	"context"
	"time"

	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/platform/internal/presence"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// ---------------------------------------------------------------------------
// View -- the read-only observation capability
// ---------------------------------------------------------------------------

// View is the channel-home's read-only observation set: committed message tail
// (ReadAfterSeq), head cursor (MaxSeq), and active actor roster (ListActors). It
// holds only read interfaces — there is no write path through a View.
type View struct {
	query     storespec.MessageQuery
	authority storespec.ActorAuthority
	links     *link.Acceptor
	presence  presence.View
	rt        *actorrt.Runtime
	nowMs     func() int64
}

// View returns the read-only observation set (ReadAfterSeq / MaxSeq /
// ListActors / daemon attachment). It carries no write capability — observation
// only. The host (app) reads these projections OUT-OF-BAND (no message, no
// truth-log write) — UI status polling must not pollute the log; in-universe
// actors instead ask the system actor by message (that path is logged).
func (h *Home) View() View {
	return View{
		query:     h.cs.Query,
		authority: h.cs.Authority,
		links:     h.links,
		presence:  presence.NewView(h.presenceFold, h.channel.Cells(), h.cs.Authority),
		rt:        h.channel.Cells(),
		nowMs:     h.nowMs,
	}
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

// ReadAfterSeq returns committed envelopes with seq > afterSeq (client tail).
func (v View) ReadAfterSeq(ctx context.Context, afterSeq int64, limit int) ([]storespec.StoredRow, error) {
	return v.query.ReadAfterSeq(ctx, afterSeq, limit)
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
