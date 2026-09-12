package base

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorcaps"
	"github.com/wanpengxie/atoll/runtime/harness"
)

// FilterSession projects rows carrying session from one already-bounded
// ledger snapshot. Session is agent policy, not a runtime view operation.
func FilterSession(snapshot actorcaps.LedgerSnapshot, session string) []actorcaps.LedgerRow {
	if session == "" {
		return append([]actorcaps.LedgerRow(nil), snapshot.Rows...)
	}
	out := make([]actorcaps.LedgerRow, 0)
	for _, row := range snapshot.Rows {
		app, _, err := harness.UnwrapPayload(row.Envelope.Payload)
		// Keep malformed rows in the projection so the consumer's normal body
		// decoder reports ledger corruption. Silently filtering them would turn
		// a broken snapshot into apparently valid, incomplete history.
		if err != nil || app.Session == session {
			out = append(out, row)
		}
	}
	return out
}

// BuildSessionPrefix expands a session and its ancestor bases from one
// snapshot. It makes no storage calls, so the whole expansion shares one read
// budget and ledger head.
func BuildSessionPrefix(ctx context.Context, snapshot actorcaps.LedgerSnapshot, session string, upto message.ID) ([]actorcaps.LedgerRow, error) {
	return buildSessionPrefix(ctx, snapshot, session, upto, snapshot.HeadSeq, map[string]bool{})
}

func buildSessionPrefix(ctx context.Context, snapshot actorcaps.LedgerSnapshot, session string, upto message.ID, ceiling int64, seen map[string]bool) ([]actorcaps.LedgerRow, error) {
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
		for _, row := range snapshot.Rows {
			if row.Envelope.ID != upto {
				continue
			}
			app, _, err := harness.UnwrapPayload(row.Envelope.Payload)
			if err != nil {
				return nil, err
			}
			if app.Session != session || row.Seq > ceiling {
				return nil, fmt.Errorf("invalid session boundary %s", upto)
			}
			ceiling, found = row.Seq, true
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
	for _, row := range snapshot.Rows {
		if row.Seq > ceiling {
			continue
		}
		app, body, err := harness.UnwrapPayload(row.Envelope.Payload)
		if err != nil {
			return nil, fmt.Errorf("invalid ledger row %s: %w", row.Envelope.ID, err)
		}
		if app.Session != session {
			continue
		}
		own = append(own, row)
		if row.Envelope.Type == "session.opened" {
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
			base, baseCeiling = opening.Base, row.Seq-1
		}
	}
	if base == nil {
		return own, nil
	}
	if base.Session == "" {
		return nil, fmt.Errorf("session base is empty")
	}
	prefix, err := buildSessionPrefix(ctx, snapshot, base.Session, base.At, baseCeiling, seen)
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
