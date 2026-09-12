package home

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorcaps"
	"github.com/wanpengxie/atoll/runtime/harness"
)

// BuildSession expands a session from an already-read View snapshot. It makes
// no additional storage calls, so every ancestor uses the same read budget and
// ledger head. Session structure belongs to this platform projection.
func (v View) BuildSession(ctx context.Context, snapshot actorcaps.LedgerSnapshot, session string, upto message.ID) ([]actorcaps.LedgerRow, error) {
	return buildSessionPrefix(ctx, snapshot, session, upto, snapshot.HeadSeq, map[string]bool{})
}

// Session is retained during the migration of session projection to the agent
// driver. It disappears with this file.
func (v View) Session(ctx context.Context, session string, upto message.ID) ([]actorcaps.LedgerRow, error) {
	snapshot, err := v.Read(ctx, actorcaps.LedgerRead{})
	if err != nil {
		return nil, err
	}
	return v.BuildSession(ctx, snapshot, session, upto)
}

// Tail is retained only until every consumer uses the visible cursor method.
func (v View) Tail(ctx context.Context, afterSeq int64, limit int) ([]actorcaps.LedgerRow, int64, error) {
	return v.ReadVisibleAfterSeq(ctx, afterSeq, limit)
}

func buildSessionPrefix(ctx context.Context, s actorcaps.LedgerSnapshot, session string, upto message.ID, ceiling int64, seen map[string]bool) ([]actorcaps.LedgerRow, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if seen[session] {
		return nil, fmt.Errorf("session base cycle at %s", session)
	}
	seen[session] = true
	defer delete(seen, session)
	if upto != "" {
		found := false
		for _, r := range s.Rows {
			if r.Envelope.ID != upto {
				continue
			}
			app, _, err := harness.UnwrapPayload(r.Envelope.Payload)
			if err != nil {
				return nil, err
			}
			if app.Session != session || r.Seq > ceiling {
				return nil, fmt.Errorf("invalid session boundary %s", upto)
			}
			ceiling, found = r.Seq, true
			break
		}
		if !found {
			return nil, fmt.Errorf("session boundary %s not found", upto)
		}
	}
	var own []actorcaps.LedgerRow
	var base *struct {
		Session string     `json:"session"`
		At      message.ID `json:"at"`
	}
	var opened bool
	var baseCeiling int64
	for _, r := range s.Rows {
		if r.Seq > ceiling {
			continue
		}
		app, body, err := harness.UnwrapPayload(r.Envelope.Payload)
		if err != nil {
			return nil, fmt.Errorf("invalid ledger row %s: %w", r.Envelope.ID, err)
		}
		if app.Session != session {
			continue
		}
		own = append(own, r)
		if r.Envelope.Type == "session.opened" {
			if opened {
				return nil, fmt.Errorf("duplicate session opening %s", session)
			}
			opened = true
			var opening struct {
				Base *struct {
					Session string     `json:"session"`
					At      message.ID `json:"at"`
				} `json:"base"`
			}
			if err := json.Unmarshal(body, &opening); err != nil {
				return nil, fmt.Errorf("invalid session opening: %w", err)
			}
			base, baseCeiling = opening.Base, r.Seq-1
		}
	}
	if base == nil {
		return own, nil
	}
	if base.Session == "" {
		return nil, fmt.Errorf("session base is empty")
	}
	prefix, err := buildSessionPrefix(ctx, s, base.Session, base.At, baseCeiling, seen)
	if err != nil {
		return nil, err
	}
	if len(prefix) == 0 {
		return nil, fmt.Errorf("session base %s not found", base.Session)
	}
	if len(prefix)+len(own) > actorcaps.MaxLedgerRows {
		return nil, actorcaps.ErrLedgerLimit
	}
	return append(prefix, own...), nil
}
