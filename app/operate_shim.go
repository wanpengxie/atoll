package app

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
)

// operate_shim.go is the HTTP垫片 half of NP-1=c: the four channel-control HTTP
// endpoints (introduce / remove / restart / set_default_agent) do NOT call the
// executor / Home face directly — they replay the SESSION USER as a channel member
// submitting a control request (audience=[system]) through the SAME in-gate door a
// message frame will use, then bounded-wait for the door's terminal reply. The pen
// welds user:X, so the whole action (request + terminal) lands in the log, is
// replayable, and the permission verdict has ONE authority (the gate). This is not
// a bypass — it is "the subject speaking" over a transitional outermost transport;
// it retires with the H2/H3 frame族. 红线11: no channel-internal control ability
// may exist on an HTTP-only private path — the shim only "walks the user through
// the door", it grows no logic outside it.

const defaultControlShimTimeout = 5 * time.Second

// errShimChannelUnavailable is the getHome==nil sentinel (the channel's universe
// is not open) → 503, distinct from a non-member rejection (→403).
var errShimChannelUnavailable = errors.New("app: channel home not open")

// doorReceipt is the parsed outcome of one control-shim submit: either the door's
// terminal reply (settled), or a timeout (settled=false → 202+request_id).
type doorReceipt struct {
	requestID string
	settled   bool           // false → the bounded wait timed out
	completed bool           // door replied `completed` (vs `failed`)
	body      map[string]any // completed payload, `status` stripped
	errorCode string         // failed: error_code
	detail    string         // failed: detail
}

// submitControlThroughDoor is the shared shim path. Non-member session user →
// platform.ErrNotMember (膜律: 严禁 Admit 兜底 — the UI引导 "先加入频道"). It subscribes
// the commit Signal BEFORE Submit (so the terminal's wake is never missed), then
// polls the log by request-id (the existing tap fan-out — no new correlator) until
// the door's terminal for this request appears or the bounded wait elapses.
func (a *App) submitControlThroughDoor(ctx context.Context, chID, userID, msgType string, payload json.RawMessage) (*doorReceipt, error) {
	home := a.getHome(channel.ID(chID))
	if home == nil {
		return nil, errShimChannelUnavailable
	}
	// 户籍校验 lives in the door (Home.Human): a non-member gets ErrNotMember. The
	// shim never admits — that would be the humanFor 膜旁路 reincarnated.
	handle, err := home.HumanPrincipal(ctx, userID)
	if err != nil {
		return nil, err
	}
	wake, unsub := home.Subscribe()
	defer unsub()
	// Explicit deadline (期12 v0.4): in the reaper world an undeclared
	// deadline only gets the harness's 24h default — a control request that
	// nobody serves would sit open a day. Align with the ws write path's TTL.
	exp := time.Now().UnixMilli() + clientRequestTTLMs
	reqID, seq, err := handle.Submit(ctx, platform.SubmitSpec{
		Type:      msgType,
		Kind:      message.KindRequest,
		Audience:  []actor.ActorID{actor.SystemActorID},
		Payload:   payload,
		ExpiresAt: &exp,
	})
	if err != nil {
		return nil, err
	}
	receipt := &doorReceipt{requestID: string(reqID)}
	deadline := time.Now().Add(a.controlShimTimeout)
	after := seq // the terminal commits at a seq strictly greater than the request's
	for {
		// Deadline check at the top so a已过期 window returns settled=false (202)
		// without racing a scan against the async reply — deterministic for tests.
		if time.Now().After(deadline) {
			return receipt, nil
		}
		found, term, serr := scanForTerminal(ctx, home, &after, reqID)
		if serr != nil {
			return nil, serr
		}
		if found {
			fillReceipt(receipt, term)
			return receipt, nil
		}
		select {
		case <-wake:
		case <-time.After(time.Until(deadline)):
		case <-ctx.Done():
			return receipt, nil
		}
	}
}

// scanForTerminal reads forward from *after (advancing it) looking for the terminal
// response row whose parent_id is reqID. It drains the whole tail before returning
// not-found so a batch cap never strands the terminal beyond the read window.
func scanForTerminal(ctx context.Context, home *platform.Home, after *int64, reqID message.ID) (bool, message.Envelope, error) {
	const batch = 200
	for {
		rows, err := home.View().ReadAfterSeq(ctx, *after, batch)
		if err != nil {
			return false, message.Envelope{}, err
		}
		for _, r := range rows {
			*after = r.Seq
			if r.IsTerminal && r.Envelope.Kind == message.KindResponse && r.Envelope.ParentID == reqID {
				return true, r.Envelope, nil
			}
		}
		if len(rows) < batch {
			return false, message.Envelope{}, nil
		}
	}
}

// fillReceipt parses the door's terminal payload ({status, ...} for a Reply,
// {status, error_code, detail} for a Fail) into the receipt.
func fillReceipt(r *doorReceipt, env message.Envelope) {
	r.settled = true
	m := map[string]any{}
	_ = json.Unmarshal(env.Payload, &m)
	status, _ := m["status"].(string)
	if status == message.StatusCompleted {
		r.completed = true
		delete(m, "status")
		r.body = m
	} else {
		r.errorCode, _ = m["error_code"].(string)
		r.detail, _ = m["detail"].(string)
	}
}

// finishControlShim writes the HTTP response for a shim submit: the pre-door error
// (membership/unavailable/internal), a timeout (202+request_id), the door's failure
// (mapped code), or the door's success (onSuccess maps the receipt body to the
// handler's status + shape).
func (a *App) finishControlShim(c *gin.Context, r *doorReceipt, err error, onSuccess func(map[string]any) (int, any)) {
	if err != nil {
		switch {
		case errors.Is(err, platform.ErrNotMember):
			c.JSON(http.StatusForbidden, gin.H{"error": "not an active channel member"})
		case errors.Is(err, errShimChannelUnavailable):
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "channel home not open"})
		// 期12 v0.4 P1-5: a member whose cell is mid-re-mint, or a home in
		// teardown, is a retryable 503 — not a 500.
		case errors.Is(err, platform.ErrCellUnavailable):
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "subject cell unavailable — retry"})
		case errors.Is(err, platform.ErrClosed):
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "channel home is closing"})
		default:
			c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		}
		return
	}
	if !r.settled {
		c.JSON(http.StatusAccepted, gin.H{"request_id": r.requestID, "status": "pending"})
		return
	}
	if !r.completed {
		status, detail := controlErrorHTTP(r.errorCode, r.detail)
		c.JSON(status, gin.H{"error": detail})
		return
	}
	status, body := onSuccess(r.body)
	c.JSON(status, body)
}

// controlErrorHTTP maps a door terminal's error_code to an HTTP status + detail.
// It is the SOLE operate-error→HTTP mapping now (the former direct-executor
// operateHTTPError retired into this function when the shim door path became the
// only control path), covering both the executor's own codes and the gate's
// (unauthorized_sender, internal_error) — the codes only the door路径 can surface.
func controlErrorHTTP(code, detail string) (int, string) {
	switch code {
	case "decl_not_found":
		return http.StatusNotFound, "decl not found"
	case "forbidden", "unauthorized_sender":
		return http.StatusForbidden, detail
	case "channel_unavailable":
		return http.StatusServiceUnavailable, detail
	case "bad_payload", "unknown_class", "invalid_placement", "not_in_composition":
		return http.StatusBadRequest, detail
	case "internal_error", "":
		return http.StatusInternalServerError, "internal error"
	default:
		return http.StatusBadRequest, detail
	}
}
