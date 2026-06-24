package harness

import (
	"context"

	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/channel"
	"github.com/wanpengxie/ActOS/protocol/message"
)

// minter is the铸笔机: it holds the bare chain and welds an identity onto it on
// every Mint. The platform receives a Minter from New (never the bare chain),
// so the bare writer's visibility is compile-time封顶 inside this package.
type minter struct {
	chain *chain
}

// Mint produces a Pen welded to (actorID, chID). The returned pen commits every
// write under that identity and the holder cannot change it — the substrate's
// "actorID 与写能力焊死不分离" invariant. Mint is deterministic and cheap (no
// per-pen state beyond the welded principal), so admission points may Mint
// per-emit freely.
func (m *minter) Mint(actorID actor.ActorID, chID channel.ID) Pen {
	return &boundPen{
		chain:     m.chain,
		principal: caller{actorID: actorID, chID: chID},
	}
}

// boundPen is a Pen welded to one identity. It is the substrate's outward write
// capability: actors and the system closure hold one of these, never the bare
// chain or the minter.
type boundPen struct {
	chain     *chain
	principal caller
}

// Write injects the welded identity into the envelope and drives the chain.
//
// Identity injection is FAIL-FAST, not silent-overwrite. env.Sender.ID /
// env.ChannelID are substrate-injected fields, NOT write者 input: the write者
// (via behavior builders) leaves them empty and the pen welds the principal. A
// non-empty value means someone hand-stuffed identity (a bug / a bypass of the
// behavior builders) and is rejected loudly (HarnessIdentityNotCallerSettable)
// rather than quietly corrected — a static overwrite would hide the misuse
// (feedback_agent_consumer_structural_boundary).
//
// Sender.Kind is NOT filled here: step 4 (sender_consistent) force-overwrites it
// from the registry truth. The welded caller is set on the outermost ctx layer
// (shadow-proof against any pre-stuffed value) before the chain runs.
func (p *boundPen) Write(ctx context.Context, env *message.Envelope) (WriteResult, error) {
	if env == nil {
		// Defer the nil check to the chain so the error vocabulary stays in one
		// place; the chain returns a hard error for a nil envelope.
		return p.chain.write(ctx, env)
	}
	if env.Sender.ID != "" || env.ChannelID != "" {
		return WriteResult{
			RejectReason: HarnessIdentityNotCallerSettable,
			RejectDetail: "sender.id/channel_id are substrate-injected, not caller-settable",
		}, nil
	}
	env.Sender.ID = p.principal.actorID
	env.ChannelID = p.principal.chID
	ctx = ctxWithCaller(ctx, p.principal)
	return p.chain.write(ctx, env)
}
