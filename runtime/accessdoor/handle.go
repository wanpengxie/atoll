package accessdoor

import (
	"context"
	"errors"

	"github.com/wanpengxie/ActOS/protocol/access"
	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/resource"
	"github.com/wanpengxie/ActOS/runtime/resourcespec"
)

// AccessHandle is the substrate's off-log capability — the access-plane dual of
// harness.Pen. It is welded to ONE caller at construction and NEVER self-reports
// identity. It is an INTERFACE: the cell implementation (boundHandle) and the
// future port implementation (cross-wire) are twins of one contract, so a
// liveness wrapper can wrap it the way livePen wraps Pen, zero friction.
type AccessHandle interface {
	Invoke(ctx context.Context, op access.Operation, id resource.ResourceID, args []byte, grant *access.Grant) (Outcome, error)
}

// boundHandle is an AccessHandle welded to one caller (the cell implementation).
// The caller is a struct field, not a wire field — structurally there is nowhere
// to self-report it.
type boundHandle struct {
	door   *door
	caller actor.ActorID
}

// Invoke runs ingress (structure → ErrMalformed), then the day-1 Ops-overreach
// judgment (→ access_denied verdict), then the decision tree under the welded
// caller. The two rejection layers stay distinct: a structural fault is a Go
// error before anything resolves; overreach is a verdict.
func (h boundHandle) Invoke(ctx context.Context, op access.Operation, id resource.ResourceID, args []byte, grant *access.Grant) (Outcome, error) {
	if err := ingress(op, id, args, grant); err != nil {
		return Outcome{}, err
	}
	if over, ok := day1OpsOverreach(op, grant); ok && over {
		return Outcome{RejectReason: access.AccessDenied}, nil
	}
	return h.door.invoke(ctx, h.caller, op, id, args, grant)
}

// AccessMinter is the door's ONE outward face (mirroring harness.Minter's
// discipline: New hands out only a Minter, the bare door stays sealed). Mint
// takes just the caller — the door is already bound to its channel/Registry via
// Deps, and R authorization needs no kind, so one parameter suffices.
type AccessMinter interface {
	Mint(caller actor.ActorID) AccessHandle
}

type minter struct{ door *door }

// Mint welds caller onto the door and returns a handle. Deterministic and cheap;
// admission points may Mint per-caller freely.
func (m *minter) Mint(caller actor.ActorID) AccessHandle {
	return boundHandle{door: m.door, caller: caller}
}

// New assembles the door from Deps and returns a Minter — never the bare door.
// It fail-fasts at assembly, not at first op: every Dep is required, and the
// day-1 KindKV driver must be present (op=create hardcodes KindKV, so a missing
// one would otherwise surface only when someone first creates).
func New(deps Deps) (AccessMinter, error) {
	if deps.Registry == nil || deps.Drivers == nil || deps.Membership == nil {
		return nil, errors.New("accessdoor: Deps incomplete")
	}
	if deps.Drivers[resourcespec.KindKV] == nil {
		return nil, errors.New("accessdoor: KindKV driver missing")
	}
	return &minter{door: &door{deps: deps}}, nil
}
