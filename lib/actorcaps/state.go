package actorcaps

import (
	"context"
	"fmt"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
)

// StateKV is the paved-path facade over Caps.State — Get/Put/Del in place of
// a raw Invoke call. It carries no authority of its own: H is the
// already-welded (actor-scoped, collapsed) handle Caps hands out, so every
// call still runs the full door, zero bypass. grant is always nil — the
// state locus has no R (ingressState rejects op=set as a category error
// before any grant shape is even considered), so a grant operand is not
// merely unused here, it is not applicable.
type StateKV struct {
	H accessdoor.AccessHandle
}

// Get reads key's bytes. found=false covers BOTH "no such key" (the door's
// resource_not_found verdict) and "resolved but empty" (Outcome.Found=false,
// the same bit meaning door.go already documents) — a caller doing a
// to-Put existence probe cares only "is there a value to use", not why
// there wasn't one.
func (s StateKV) Get(ctx context.Context, key string) ([]byte, bool, error) {
	out, err := s.H.Invoke(ctx, access.OpRead, resource.ResourceID(key), nil, nil)
	if err != nil {
		return nil, false, err
	}
	if out.RejectReason == access.ResourceNotFound {
		return nil, false, nil
	}
	if !out.Accepted() {
		return nil, false, fmt.Errorf("actorcaps: state get %q: %s", key, out.RejectReason)
	}
	return out.Value, out.Found, nil
}

// Put upserts key. The door's Write is PUT-on-an-EXISTING-row only (birth is
// Create, not Write — state.go's invokeActorScoped): Put hides that split
// behind one upsert call. A first Write that comes back resource_not_found
// means the row does not exist yet, so Put falls through to Create. The
// two-step is a benign race, not a TOCTOU the caller must guard against —
// this locus has exactly one writer (self, per Caps' non-ambient welding).
func (s StateKV) Put(ctx context.Context, key string, val []byte) error {
	out, err := s.H.Invoke(ctx, access.OpWrite, resource.ResourceID(key), val, nil)
	if err != nil {
		return err
	}
	if out.Accepted() {
		return nil
	}
	if out.RejectReason != access.ResourceNotFound {
		return fmt.Errorf("actorcaps: state put %q: %s", key, out.RejectReason)
	}
	out, err = s.H.Invoke(ctx, access.OpCreate, resource.ResourceID(key), val, nil)
	if err != nil {
		return err
	}
	if !out.Accepted() {
		return fmt.Errorf("actorcaps: state put %q: %s", key, out.RejectReason)
	}
	return nil
}

// Del removes key. A not_found verdict is swallowed — the door's own doc:
// "repeated delete is honestly not-found" — not an error a caller must
// special-case.
func (s StateKV) Del(ctx context.Context, key string) error {
	out, err := s.H.Invoke(ctx, access.OpDelete, resource.ResourceID(key), nil, nil)
	if err != nil {
		return err
	}
	if out.Accepted() || out.RejectReason == access.ResourceNotFound {
		return nil
	}
	return fmt.Errorf("actorcaps: state del %q: %s", key, out.RejectReason)
}
