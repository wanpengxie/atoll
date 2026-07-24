package harness

import (
	"context"
	"errors"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/capauth"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// minter is the mint machine: it holds the bare chain and welds an identity onto it on
// every Mint. The platform receives a Minter from New (never the bare chain),
// so the bare writer's visibility is compile-time capped inside this package.
type minter struct {
	chain *chain
}

// Mint produces a Pen welded to (actorID, kind, chID). The returned pen commits
// every write under that identity and the holder cannot change it — the
// substrate's "actorID and write capability are welded inseparably" invariant.
// kind is welded here too (not backfilled from a registry lookup mid-chain):
// stepSenderConsistent reads the welded kind, so Mint is the single source of
// truth for both id and kind. Mint is deterministic and cheap (no per-pen state
// beyond the welded principal), so admission points may Mint per-emit freely.
func (m *minter) Mint(actorID actor.ActorID, kind actor.Kind, chID channel.ID) Pen {
	return &boundPen{
		chain:     m.chain,
		principal: caller{actorID: actorID, kind: kind, chID: chID},
	}
}

func (m *minter) MintAdmitted(
	admission storespec.IdentityAdmission,
	chID channel.ID,
) Pen {
	if !admission.Valid() || chID == "" {
		return rejectedPen{}
	}
	return &boundPen{
		chain: m.chain,
		principal: caller{
			actorID:  admission.Row.ID,
			kind:     admission.Row.Kind,
			chID:     chID,
			admitted: true,
		},
	}
}

func (m *minter) MintAuthority(
	authority capauth.Authority,
	kind actor.Kind,
	chID channel.ID,
) Pen {
	if authority == nil || authority.ActorID() == "" {
		return rejectedPen{}
	}
	return &boundPen{
		chain: m.chain,
		principal: caller{
			actorID:  authority.ActorID(),
			kind:     kind,
			chID:     chID,
			admitted: true,
		},
		authority: authority,
	}
}

// boundPen is a Pen welded to one identity. It is the substrate's outward write
// capability: actors and the system closure hold one of these, never the bare
// chain or the minter.
type boundPen struct {
	chain     *chain
	principal caller
	authority capauth.Authority
}

// Write injects the welded identity into the envelope and drives the chain.
//
// Identity injection is FAIL-FAST, not silent-overwrite. env.Sender.ID /
// env.ChannelID are substrate-injected fields, NOT the writer's input: the writer
// (via behavior builders) leaves them empty and the pen welds the principal. A
// non-empty value means someone hand-stuffed identity (a bug / a bypass of the
// behavior builders) and is rejected loudly (HarnessIdentityNotCallerSettable)
// rather than quietly corrected — a static overwrite would hide the misuse
// (feedback_agent_consumer_structural_boundary).
//
// Sender.Kind is NOT filled here either: step 4 (sender_consistent) force-
// overwrites it from the pen-welded caller.kind (read via callerFromCtx), same
// truth source as env.Sender.ID. The welded caller is set on the outermost ctx
// layer (shadow-proof against any pre-stuffed value) before the chain runs.
func (p *boundPen) Write(ctx context.Context, env *message.Envelope) (WriteResult, error) {
	if p.authority != nil {
		if err := p.authority.Admit(); err != nil {
			return WriteResult{}, err
		}
	}
	if env == nil {
		// Defer the nil check to the chain so the error vocabulary stays in one
		// place; the chain returns a hard error for a nil envelope.
		return p.chain.write(ctx, env)
	}
	if env.Sender.ID != "" || env.ChannelID != "" {
		const detail = "sender.id/channel_id are substrate-injected, not caller-settable"
		p.chain.observeReject(ctx, env, StepCallerAuth, HarnessIdentityNotCallerSettable, detail)
		return WriteResult{
			RejectReason: HarnessIdentityNotCallerSettable,
			RejectDetail: detail,
		}, nil
	}
	env.Sender.ID = p.principal.actorID
	env.ChannelID = p.principal.chID
	ctx = ctxWithCaller(ctx, p.principal)
	return p.chain.write(ctx, env)
}

type rejectedPen struct{}

func (rejectedPen) Write(context.Context, *message.Envelope) (WriteResult, error) {
	return WriteResult{}, errors.New("harness: invalid authority")
}
