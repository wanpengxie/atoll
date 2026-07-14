package app

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/wanpengxie/atoll/platform/home"
	"github.com/wanpengxie/atoll/platform/subjectgate"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
)

// operate_http.go is the HTTP adapter for the four channel-control endpoints.
// endpoints (introduce / remove / restart / set_default_agent) do NOT call the
// executor / Home face directly — they replay the SESSION USER as a channel member
// submitting a control request (audience=[system]) through the SAME subjectgate
// frame path a ws message frame uses (a submit frame delivered onto the subject's
// own cell through its slot), then bounded-wait for the door's terminal reply. The
// cell's own welded pen stamps user:X, so the whole action (request + terminal)
// lands in the log, is replayable, and the permission verdict has ONE authority
// (the gate). This is not a bypass — it is "the subject speaking" over a
// transitional outermost transport; it retires with the H2/H3 frame族. 红线11: no
// channel-internal control ability may exist on an HTTP-only private path — the
// The adapter only walks the user through the door; it grows no logic outside it.

const defaultControlRequestTimeout = 5 * time.Second

// clientRequestTTLMs is the explicit short deadline an HTTP control request declares
// (product default: 30s). A channel-control action is a bounded interactive request —
// it must not linger open indefinitely if no one serves it; the expiry reaper closes
// it at this deadline (restored from the pre-gateway edge, now透传 via the submit
// frame's expires_at_ms field).
const clientRequestTTLMs int64 = 30_000

// errControlChannelUnavailable is the getHome==nil / no-slot sentinel (the channel's
// universe is not open) → 503, distinct from a non-member rejection (→403).
var errControlChannelUnavailable = errors.New("app: channel home not open")

// errControlNotMember is the non-member rejection (膜律: 严禁 Admit 兜底 — the UI引导
// "先加入频道") → 403. The session user resolved no active subject in this channel.
var errControlNotMember = errors.New("app: not an active channel member")

// errControlCellUnavailable is the honest transient: the subject IS a member but its
// cell is not currently drivable (the supply ring's re-mint window / a torn-down
// slot). Retryable → 503, never a 500.
var errControlCellUnavailable = errors.New("app: subject cell unavailable — retry")

// controlFrameError carries a subjectgate submit error frame's flat code + detail
// (表①) up to the HTTP mapping. It surfaces only when the submit itself is
// refused before any terminal (a write reject, or a transient) — the door's own
// terminal failures ride the doorReceipt path instead.
type controlFrameError struct {
	code   string
	detail string
}

func (e *controlFrameError) Error() string {
	return "app: submit rejected: " + e.code + " (" + e.detail + ")"
}

// doorReceipt is the parsed outcome of one HTTP control submit: either the door's
// terminal reply (settled), or a timeout (settled=false → 202+request_id).
type doorReceipt struct {
	requestID string
	settled   bool           // false → the bounded wait timed out
	completed bool           // door replied `completed` (vs `failed`)
	body      map[string]any // completed payload, `status` stripped
	errorCode string         // failed: error_code
	detail    string         // failed: detail
}

// submitControlThroughDoor is the shared HTTP adapter path. It resolves the session
// user's principal to the subject's active actor id (a non-member →
// errControlNotMember; the adapter never admits — that would be the humanFor 膜旁路
// reincarnated), subscribes the commit Signal BEFORE the submit (so the
// terminal's wake is never missed), delivers a submit frame onto the subject's
// own cell through its slot, then polls the log by request-id (the existing tap
// fan-out — no new correlator) until the door's terminal for this request
// appears or the bounded wait elapses.
func (a *App) submitControlThroughDoor(ctx context.Context, chID, userID, msgType string, payload json.RawMessage) (*doorReceipt, error) {
	home := a.getHome(channel.ID(chID))
	if home == nil {
		return nil, errControlChannelUnavailable
	}
	subjectID, found, err := home.ResolvePrincipal(ctx, actor.KindHuman, userID)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, errControlNotMember
	}
	slot, ok := home.SubjectSlotFor(subjectID)
	if !ok || slot == nil {
		// 装配链 (v0.4.1): the adapter only LOOKS up the slot (ensure is户籍准入's job). A
		// member with no slot is the embodiment-lag transient → channel-unavailable.
		return nil, errControlChannelUnavailable
	}
	wake, unsub := home.Subscribe()
	defer unsub()
	// The control request declares an explicit short TTL (clientRequestTTLMs) via the
	// submit frame's expires_at_ms — a bounded interactive action the reaper closes if
	// no one serves it (restored from the pre-gateway edge; §S2 status v0.4.1 勘误).
	exp := time.Now().UnixMilli() + clientRequestTTLMs
	f, ferr := subjectgate.NewFrame(subjectgate.FrameSubmit, "", subjectgate.SubmitPayload{
		ChannelID: chID,
		MsgType:   msgType,
		Kind:      string(message.KindRequest),
		Audience:  []string{string(actor.SystemActorID)},
		Payload:   payload,
		ExpiresAt: &exp,
	})
	if ferr != nil {
		return nil, ferr
	}
	// Deliver carries no gateway binding-generation argument (连接模型勘误期: the
	// client-visible binding axis was proven a false axis and整删, and the trusted-path
	// exemption sentinel went with it). This trusted platform-internal path delivers
	// the submit straight onto the subject's own cell through its slot.
	res, derr := slot.Deliver(ctx, f)
	if derr != nil {
		// ErrNoOccupant (cell mid-re-mint / torn down) → retryable unavailable.
		return nil, errControlCellUnavailable
	}
	reqID, seq, rerr := parseSubmitReceipt(res.Frame)
	if rerr != nil {
		return nil, rerr
	}
	receipt := &doorReceipt{requestID: string(reqID)}
	deadline := time.Now().Add(a.controlRequestTimeout)
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

// parseSubmitReceipt reads a submit Deliver result: a receipt frame yields the
// request id + write seq; an error frame becomes a *controlFrameError carrying its
// flat code (表①). Any other frame type is an internal inconsistency.
func parseSubmitReceipt(f subjectgate.Frame) (message.ID, int64, error) {
	switch f.Type {
	case subjectgate.FrameReceipt:
		var r subjectgate.SubmitReceipt
		if err := f.DecodePayload(&r); err != nil {
			return "", 0, err
		}
		return message.ID(r.MessageID), r.Seq, nil
	case subjectgate.FrameError:
		var e subjectgate.ErrorPayload
		if err := f.DecodePayload(&e); err != nil {
			return "", 0, err
		}
		return "", 0, &controlFrameError{code: e.Code, detail: e.Detail}
	default:
		return "", 0, errors.New("app: unexpected submit result frame " + string(f.Type))
	}
}

// scanForTerminal reads forward from *after (advancing it) looking for the terminal
// response row whose parent_id is reqID. It drains the whole tail before returning
// not-found so a batch cap never strands the terminal beyond the read window.
func scanForTerminal(ctx context.Context, home *home.Home, after *int64, reqID message.ID) (bool, message.Envelope, error) {
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

// finishControlRequest writes the HTTP response for a control submit: the pre-door error
// (membership/unavailable/internal), a timeout (202+request_id), the door's failure
// (mapped code), or the door's success (onSuccess maps the receipt body to the
// handler's status + shape).
func (a *App) finishControlRequest(c *gin.Context, r *doorReceipt, err error, onSuccess func(map[string]any) (int, any)) {
	if err != nil {
		var fe *controlFrameError
		switch {
		case errors.Is(err, errControlNotMember):
			c.JSON(http.StatusForbidden, gin.H{"error": "not an active channel member"})
		case errors.Is(err, errControlChannelUnavailable):
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "channel home not open"})
		// A member whose cell is mid-re-mint, or a home in teardown, is a
		// retryable 503 — not a 500.
		case errors.Is(err, errControlCellUnavailable):
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "subject cell unavailable — retry"})
		case errors.As(err, &fe):
			// A submit error frame before any terminal (write reject / transient):
			// unavailable is retryable, everything else is the write reject reason.
			if fe.code == "unavailable" || fe.code == "closed" {
				c.JSON(http.StatusServiceUnavailable, gin.H{"error": "subject cell unavailable — retry"})
			} else if fe.code == "bad_payload" {
				c.JSON(http.StatusBadRequest, gin.H{"error": fe.detail})
			} else {
				c.JSON(http.StatusUnprocessableEntity, gin.H{"error": fe.detail})
			}
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
// It is the sole operate-error→HTTP mapping for the canonical door path,
// covering both the executor's own codes and the gate's
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
