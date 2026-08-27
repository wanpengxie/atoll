package link

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/ipc"
	"github.com/wanpengxie/atoll/runtime/remoteingress"
	"github.com/wanpengxie/atoll/runtime/schedule"
)

// The wire payloads for the plane-2 (access/state) and time-axis (schedule) arms.
// They live HERE in link — the composition layer — not in ipc: ipc is a protocol-
// only wire leaf and must not import runtime siblings (schedule imports actorrt,
// which imports ipc — an ipc→schedule edge would cycle). The substrate port
// forwards these bytes OPAQUELY (RelaySink), so only the two link ends (home
// emitSink-side and daemon proxy-side) ever decode them.

// accessScope selects which minter face the home welds the caller onto — the
// channel-scoped tree (Mint) or the actor-scoped collapsed branch (MintState).
// State is not a separate frame family: it is Access's collapsed branch, carried
// on the same KindAccess arm distinguished only by this field. ONLY the
// Invocation arm carries a Scope (期11 spec §3.3's "Scope 保留律") — Create
// and Query are structurally channel-scoped-only, so they carry none.
type accessScope string

const (
	accessScopeChannel accessScope = "channel"
	accessScopeState   accessScope = "state"
)

// accessRequestKind is the KindAccess payload's own sum discriminator (期11
// spec §3.3: ipc's frame Kind closed set stays untouched/opaque — this sum
// lives ONE LAYER DOWN, inside the KindAccess payload the two link ends
// alone decode). Invocation is the pre-existing Invoke arm (state OR
// channel-scoped, by Scope); Create and Query are the two NEW arms §3.1's
// door-side split needed a wire home for.
type accessRequestKind string

const (
	accessKindInvocation accessRequestKind = "invocation"
	accessKindCreate     accessRequestKind = "create"
	accessKindQuery      accessRequestKind = "query"
)

// accessQueryKind selects which Query method (Stat or List) a accessKindQuery
// request carries — the two Query-arm methods share one wire arm the same
// way state/channel share the Invocation arm (via Scope).
type accessQueryKind string

const (
	accessQueryStat accessQueryKind = "stat"
	accessQueryList accessQueryKind = "list"
)

// accessRequest is the daemon→home KindAccess payload — a SUM over
// {Invocation | Create | Query} (期11 spec §3.3), exactly one of Inv/Create/
// Query populated per Kind. Every arm door-welds caller structurally: the
// Invocation arm carries Invocation.Caller (which MUST arrive empty — the
// home welds the connection's authenticated id, fail-fast on a non-empty
// caller, exactly like the pen rejecting a pre-filled Sender); Create/Query
// carry NO caller field at all — a stronger, structural version of the same
// rule (there is nothing to self-report).
type accessRequest struct {
	Kind  accessRequestKind  `json:"kind"`
	Scope accessScope        `json:"scope,omitempty"`
	Inv   *access.Invocation `json:"inv,omitempty"`

	Create *accessCreatePayload `json:"create,omitempty"`
	Query  *accessQueryPayload  `json:"query,omitempty"`
}

// accessCreatePayload is the Create arm's operand — CreateSpec's in-proc
// carrier law (期11 spec §1's red line: CreateSpec never rides
// access.Invocation) extended over the wire: its ONLY over-wire carrier is
// this dedicated arm, never access.Invocation.Args.
type accessCreatePayload struct {
	Resource resource.ResourceID   `json:"resource"`
	Spec     accessdoor.CreateSpec `json:"spec"`
	Initial  []byte                `json:"initial,omitempty"`
}

// accessQueryPayload is the Query arm's operand — a further sum over
// {Stat | List}, discriminated by QueryKind.
type accessQueryPayload struct {
	QueryKind accessQueryKind      `json:"query_kind"`
	Resource  resource.ResourceID  `json:"resource,omitempty"` // Stat
	List      *accessListReqFields `json:"list,omitempty"`     // List
}

// accessListReqFields carries accessdoor.ListQuery's fields (an inline
// struct rather than reusing ListQuery directly keeps the wire shape's own
// documentation local to this file — the two structs are field-identical by
// construction, not by accident).
type accessListReqFields struct {
	Prefix string `json:"prefix,omitempty"`
	Limit  int    `json:"limit,omitempty"`
	Cursor string `json:"cursor,omitempty"`
}

// accessResponse is the home→daemon KindAccess verdict — the sum-typed twin
// of accessRequest. A host-side Go error (structural malformed / not-live
// fence) rides the coded ack error fields instead, so a remote cell observes
// the same (Outcome, error) / (StatResult, error) / (ListPage, error) split a
// local cell does. Value/Found/RejectReason back BOTH the Invocation and
// Create arms (both resolve to an accessdoor.Outcome); Stat/List each get
// their own payload field.
type accessResponse struct {
	Kind accessRequestKind `json:"kind"`

	Value        []byte               `json:"value,omitempty"`
	Found        bool                 `json:"found,omitempty"`
	RejectReason access.FailureReason `json:"reject_reason,omitempty"`
	// Route carries a file-kind Invocation/Create's byte-access
	// authorization product (期11 spec §5 item 0) — nil for every kv
	// response and every non-accepted file response. Never bytes, never a
	// Only redemption, token, path, and mode cross this control envelope.
	Route *accessdoor.FileRoute `json:"route,omitempty"`

	Stat *accessStatRespFields `json:"stat,omitempty"`
	List *accessListRespFields `json:"list,omitempty"`
}

// accessStatRespFields carries accessdoor.StatResult's fields (StatMeta/OpSet
// reused directly — both are already JSON-friendly exported types, no
// separate wire DTO needed for them).
type accessStatRespFields struct {
	Meta   accessdoor.StatMeta    `json:"meta"`
	Ops    accessdoor.OpSet       `json:"ops,omitempty"`
	Reject accessdoor.QueryReject `json:"reject,omitempty"`
}

// accessListRespFields carries accessdoor.ListPage's fields.
type accessListRespFields struct {
	Entries []accessdoor.ListEntry `json:"entries,omitempty"`
	Next    string                 `json:"next,omitempty"`
	Reject  accessdoor.QueryReject `json:"reject,omitempty"`
}

// decode turns one decoded access frame into the ingress's operand. It is pure
// translation — shape checks only, no judgment: a request that names no legal
// arm is malformed (a Go error), never a denial. The Invocation arm's Caller
// field MUST arrive empty; a non-empty one is a caller trying to self-report
// identity, rejected the same way the pen rejects a pre-filled Sender.
func (r accessRequest) decode() (remoteingress.AccessRequest, error) {
	switch r.Kind {
	case accessKindInvocation:
		if r.Inv == nil || r.Inv.Caller != "" {
			return remoteingress.AccessRequest{}, errors.New("link: invalid access invocation")
		}
		scope := remoteingress.ScopeChannel
		switch r.Scope {
		case accessScopeChannel:
		case accessScopeState:
			scope = remoteingress.ScopeState
		default:
			return remoteingress.AccessRequest{}, errors.New("link: invalid access scope")
		}
		return remoteingress.AccessRequest{
			Kind: remoteingress.AccessInvoke, Scope: scope,
			Operation: r.Inv.Operation, Resource: r.Inv.Resource,
			Args: r.Inv.Args,
		}, nil
	case accessKindCreate:
		if r.Create == nil {
			return remoteingress.AccessRequest{}, errors.New("link: missing access create")
		}
		return remoteingress.AccessRequest{
			Kind: remoteingress.AccessCreate, Resource: r.Create.Resource,
			Spec: r.Create.Spec, Initial: r.Create.Initial,
		}, nil
	case accessKindQuery:
		if r.Query == nil {
			return remoteingress.AccessRequest{}, errors.New("link: missing access query")
		}
		switch r.Query.QueryKind {
		case accessQueryStat:
			return remoteingress.AccessRequest{
				Kind: remoteingress.AccessStat, Resource: r.Query.Resource,
			}, nil
		case accessQueryList:
			if r.Query.List == nil {
				return remoteingress.AccessRequest{}, errors.New("link: missing access list")
			}
			return remoteingress.AccessRequest{
				Kind: remoteingress.AccessList,
				List: accessdoor.ListQuery{
					Prefix: r.Query.List.Prefix,
					Limit:  r.Query.List.Limit,
					Cursor: r.Query.List.Cursor,
				},
			}, nil
		default:
			return remoteingress.AccessRequest{}, errors.New("link: invalid access query")
		}
	default:
		return remoteingress.AccessRequest{}, errors.New("link: invalid access request")
	}
}

// accessResponseOf encodes the ingress's product back onto the wire shape the
// requesting arm expects — one field family per driven verb, exactly as the
// daemon-side proxy reads it.
func accessResponseOf(
	kind remoteingress.AccessKind,
	response remoteingress.AccessResponse,
) accessResponse {
	switch kind {
	case remoteingress.AccessStat:
		return accessResponse{
			Kind: accessKindQuery,
			Stat: &accessStatRespFields{
				Meta:   response.Stat.Meta,
				Ops:    response.Stat.Ops,
				Reject: response.Stat.Reject,
			},
		}
	case remoteingress.AccessList:
		return accessResponse{
			Kind: accessKindQuery,
			List: &accessListRespFields{
				Entries: response.List.Entries,
				Next:    response.List.Next,
				Reject:  response.List.Reject,
			},
		}
	case remoteingress.AccessCreate:
		return accessResponse{
			Kind: accessKindCreate, Value: response.Outcome.Value,
			Found: response.Outcome.Found, RejectReason: response.Outcome.RejectReason,
			Route: response.Outcome.Route,
		}
	default:
		return accessResponse{
			Kind: accessKindInvocation, Value: response.Outcome.Value,
			Found: response.Outcome.Found, RejectReason: response.Outcome.RejectReason,
			Route: response.Outcome.Route,
		}
	}
}

// scheduleMethod names which ScheduleHandle call the frame carries.
type scheduleMethod string

const (
	scheduleMethodSchedule scheduleMethod = "schedule"
	scheduleMethodCancel   scheduleMethod = "cancel"
	scheduleMethodAck      scheduleMethod = "ack"
	scheduleMethodList     scheduleMethod = "list"
)

// scheduleRequest is the daemon→home KindSchedule payload. The whole ScheduleReq
// is carried, so CorrelationID (the caller's causal coordinate) crosses the wire
// intact — dropping it would lose the domain session semantics the substrate is
// obliged to relay. No author field: author is welded at the home Mint, never
// self-reported (mirrors access's empty Caller).
type scheduleRequest struct {
	Method scheduleMethod       `json:"method"`
	Req    schedule.ScheduleReq `json:"req,omitempty"`
	ID     schedule.TimerID     `json:"id,omitempty"`
}

// scheduleResponse is the home→daemon KindSchedule verdict (the minted TimerID on
// the schedule path; empty on cancel). Errors ride coded ack fields.
type scheduleResponse struct {
	ID schedule.TimerID `json:"id,omitempty"`
	// Timers carries the list verdict. Absent on every other method.
	Timers []schedule.TimerInfo `json:"timers,omitempty"`
}

// decode turns one decoded schedule frame into the ingress's operand. It
// carries no author and no attempt key: a timer belongs to the identity, and
// the identity is the endpoint's, not the frame's.
func (r scheduleRequest) decode() (remoteingress.ScheduleRequest, error) {
	switch r.Method {
	case scheduleMethodSchedule:
		return remoteingress.ScheduleRequest{
			Method: remoteingress.ScheduleSet, Req: r.Req,
		}, nil
	case scheduleMethodCancel:
		return remoteingress.ScheduleRequest{
			Method: remoteingress.ScheduleCancel, ID: r.ID,
		}, nil
	case scheduleMethodAck:
		return remoteingress.ScheduleRequest{
			Method: remoteingress.ScheduleAck, ID: r.ID,
		}, nil
	case scheduleMethodList:
		return remoteingress.ScheduleRequest{
			Method: remoteingress.ScheduleList,
		}, nil
	default:
		return remoteingress.ScheduleRequest{}, errors.New("link: invalid schedule method")
	}
}

// ---------------------------------------------------------------------------
// relayClient — the daemon-side FIFO round-trip over one opaque capability arm.
// ---------------------------------------------------------------------------

// errRelayClosed is returned to a blocked or new round-trip once the arm is torn
// down (the connection died with an invocation in flight). It is this dialect's
// close sentinel (relayCore.closedErr).
var errRelayClosed = errors.New("link: relay arm closed")

// relayClient is the out-of-process end of one opaque capability arm (KindAccess
// or KindSchedule) — the plane-agnostic twin of RemoteWriter. Both are dialects of
// the SAME machine: relayClient is the OPAQUE-BYTES dialect of relayCore
// (relaycore.go), which owns the FIFO no-id synchronous round-trip (six axioms
// there, in one place). This type only translates the opaque dialect — request
// bytes out, RelayAckPayload rebuilt into (payload, decoded error) back — and
// maps core.roundTrip's outcome triple onto its own (ackPayload, ackErr,
// transportErr) contract.
type relayClient struct {
	core *relayCore[relayAck]
}

// relayAck carries one resolved ack back to the blocked round-trip: the opaque
// response bytes and the host-side error reconstructed from coded ack fields. The
// teardown/unconfirmed distinction is NOT carried here — it lives on the core's
// coreResult.transport (axiom 5) and is surfaced through roundTrip's transportErr.
type relayAck struct {
	payload json.RawMessage
	err     error
}

func newRelayClient(codec *ipc.Codec, requestKind ipc.Kind) *relayClient {
	return &relayClient{core: newRelayCore[relayAck](codec, requestKind, errRelayClosed)}
}

// roundTrip sends payload as a request frame and blocks for the ack. It reports
// three distinct outcomes (a straight remap of relayCore.roundTrip's triple):
//   - transportErr != nil: the op's result is UNCONFIRMED — the ctx was cancelled
//     AFTER the frame was sent, the arm died with the request in flight
//     (errRelayClosed), or a pre-send wire-write failed (nothing to confirm) — the
//     caller surfaces this as an in-flight unknown (access → outcome_unknown,
//     schedule → error);
//   - transportErr == nil, ackErr != nil: a DEFINITE, op-did-not-produce-an-unknown
//     failure — either the host returned a definite error verdict (structural /
//     not-live), OR the ctx was ALREADY cancelled before the frame left (a pre-send
//     abort: the op provably never reached the home, core's definiteErr). Both are
//     relayed as a plain error — NEVER outcome_unknown, because the op verifiably
//     did not execute-with-unknown-result;
//   - both nil: ackPayload is the host's opaque verdict bytes.
func (c *relayClient) roundTrip(ctx context.Context, payload []byte) (ackPayload json.RawMessage, ackErr error, transportErr error) {
	ack, txErr, definiteErr := c.core.roundTrip(ctx, payload)
	if definiteErr != nil {
		return nil, definiteErr, nil
	}
	if txErr != nil {
		return nil, nil, txErr
	}
	return ack.payload, ack.err, nil
}

// deliverAck reconstructs the opaque-dialect Ack from one inbound RelayAckPayload
// and routes it into the core's FIFO head waiter.
func (c *relayClient) deliverAck(ack ipc.RelayAckPayload) {
	c.core.deliverAck(relayAck{payload: ack.Payload, err: decodeAckError(ack.ErrorCode, ack.ErrorMessage)})
}

// close fails every pending round-trip with errRelayClosed and rejects new ones.
func (c *relayClient) close() { c.core.close() }

// ---------------------------------------------------------------------------
// remote handles — the daemon-side capability faces an out-of-process cell holds.
// ---------------------------------------------------------------------------

// remoteAccessHandle is the out-of-process end of accessdoor.AccessHandle —
// the STATE-FACE wire proxy (Invoke only): a relay-only proxy over one
// access arm. Like RemoteWriter it injects NO identity (Caller stays empty —
// the home welds the authenticated bound id) and holds no authorization
// state.
type remoteAccessHandle struct {
	relay *relayClient
	scope accessScope
}

// Invoke satisfies accessdoor.AccessHandle over the wire. A transport failure
// (unconfirmed in-flight) yields the outcome_unknown VERDICT — the one reason
// only the wire path can produce (an in-proc invoke is synchronous and never
// does). A definite host error (malformed / not-live) is returned as-is.
func (h *remoteAccessHandle) Invoke(ctx context.Context, op access.Operation, id resource.ResourceID, args []byte) (accessdoor.Outcome, error) {
	return invokeRoundTrip(ctx, h.relay, h.scope, op, id, args)
}

// invokeRoundTrip is the Invocation arm's round-trip, shared by BOTH the
// state-face (remoteAccessHandle) and resource-face (remoteResourceHandle)
// wire proxies — they carry byte-for-byte the same Invoke contract, so the
// wire encoding must not drift between the two even though the two Go types
// stay separate (§3.2's "膜层与 wire 层各拆两型").
func invokeRoundTrip(ctx context.Context, relay *relayClient, scope accessScope, op access.Operation, id resource.ResourceID, args []byte) (accessdoor.Outcome, error) {
	payload, err := json.Marshal(accessRequest{
		Kind:  accessKindInvocation,
		Scope: scope,
		Inv:   &access.Invocation{Resource: id, Operation: op, Args: args},
	})
	if err != nil {
		return accessdoor.Outcome{}, err
	}
	raw, ackErr, txErr := relay.roundTrip(ctx, payload)
	if txErr != nil {
		return accessdoor.Outcome{RejectReason: access.OutcomeUnknown}, nil
	}
	if ackErr != nil {
		return accessdoor.Outcome{}, ackErr
	}
	var resp accessResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		return accessdoor.Outcome{}, err
	}
	return accessdoor.Outcome{Value: resp.Value, Found: resp.Found, RejectReason: resp.RejectReason, Route: resp.Route}, nil
}

// remoteResourceHandle is the out-of-process end of
// accessdoor.ResourceAccessHandle — the RESOURCE-FACE wire proxy (§3.2's
// three-avatar parity: this is the "wire 代理" avatar's resource-face half,
// remoteAccessHandle's twin). Always channel-scoped (Create/Query are
// structurally resource-face-only, §3.3's "Create/Query 天然只属资源面"), so
// unlike remoteAccessHandle it carries no scope field — Invoke always sends
// accessScopeChannel.
type remoteResourceHandle struct {
	relay *relayClient
	// dialer backs FileOpener (§5, filebytes.go/dial.go): a daemon-hosted
	// actor's resource face needs its exact lane redeemer (not just the relay
	// arm) to redeem a FileRoute (file bytes never ride the ipc
	// access arm at all, §8.1). nil on any avatar this section does not wire as a
	// FileOpener (day-1: none — every remoteResourceHandle is daemon-hosted
	// and gets one, see OpenStream).
	redeemer fileRouteRedeemer
}

// Invoke satisfies accessdoor.AccessHandle (embedded in ResourceAccessHandle)
// over the wire — always channel-scoped.
func (h *remoteResourceHandle) Invoke(ctx context.Context, op access.Operation, id resource.ResourceID, args []byte) (accessdoor.Outcome, error) {
	return invokeRoundTrip(ctx, h.relay, accessScopeChannel, op, id, args)
}

// Create satisfies accessdoor.ResourceAccessHandle's create-arm over the
// wire — the Create wire arm (§3.3), never the Invocation arm (§3.1's
// "create 单入口" carried over the wire: there is no bare op=create frame
// shape to even construct here).
func (h *remoteResourceHandle) Create(ctx context.Context, id resource.ResourceID, spec accessdoor.CreateSpec, initial []byte) (accessdoor.Outcome, error) {
	payload, err := json.Marshal(accessRequest{
		Kind:   accessKindCreate,
		Create: &accessCreatePayload{Resource: id, Spec: spec, Initial: initial},
	})
	if err != nil {
		return accessdoor.Outcome{}, err
	}
	raw, ackErr, txErr := h.relay.roundTrip(ctx, payload)
	if txErr != nil {
		return accessdoor.Outcome{RejectReason: access.OutcomeUnknown}, nil
	}
	if ackErr != nil {
		return accessdoor.Outcome{}, ackErr
	}
	var resp accessResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		return accessdoor.Outcome{}, err
	}
	return accessdoor.Outcome{Value: resp.Value, Found: resp.Found, RejectReason: resp.RejectReason, Route: resp.Route}, nil
}

// Stat satisfies accessdoor.ResourceAccessHandle's Stat over the wire — a
// Query-arm round trip. A transport failure is a plain Go error (Stat is a
// read-only, idempotent, freely-retryable Query — there is no "unconfirmed
// mutation" concern the way Invoke/Create have, and QueryReject's own closed
// set has no "unknown" member to lie through, unlike access.FailureReason).
func (h *remoteResourceHandle) Stat(ctx context.Context, id resource.ResourceID) (accessdoor.StatResult, error) {
	payload, err := json.Marshal(accessRequest{
		Kind:  accessKindQuery,
		Query: &accessQueryPayload{QueryKind: accessQueryStat, Resource: id},
	})
	if err != nil {
		return accessdoor.StatResult{}, err
	}
	raw, ackErr, txErr := h.relay.roundTrip(ctx, payload)
	if txErr != nil {
		return accessdoor.StatResult{}, txErr
	}
	if ackErr != nil {
		return accessdoor.StatResult{}, ackErr
	}
	var resp accessResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		return accessdoor.StatResult{}, err
	}
	if resp.Stat == nil {
		return accessdoor.StatResult{}, errors.New("link: access stat response missing its stat payload")
	}
	return accessdoor.StatResult{Meta: resp.Stat.Meta, Ops: resp.Stat.Ops, Reject: resp.Stat.Reject}, nil
}

// List satisfies accessdoor.ResourceAccessHandle's List over the wire — same
// "plain Go error on transport failure" discipline as Stat.
func (h *remoteResourceHandle) List(ctx context.Context, q accessdoor.ListQuery) (accessdoor.ListPage, error) {
	payload, err := json.Marshal(accessRequest{
		Kind: accessKindQuery,
		Query: &accessQueryPayload{
			QueryKind: accessQueryList,
			List:      &accessListReqFields{Prefix: q.Prefix, Limit: q.Limit, Cursor: q.Cursor},
		},
	})
	if err != nil {
		return accessdoor.ListPage{}, err
	}
	raw, ackErr, txErr := h.relay.roundTrip(ctx, payload)
	if txErr != nil {
		return accessdoor.ListPage{}, txErr
	}
	if ackErr != nil {
		return accessdoor.ListPage{}, ackErr
	}
	var resp accessResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		return accessdoor.ListPage{}, err
	}
	if resp.List == nil {
		return accessdoor.ListPage{}, errors.New("link: access list response missing its list payload")
	}
	return accessdoor.ListPage{Entries: resp.List.Entries, Next: resp.List.Next, Reject: resp.List.Reject}, nil
}

// remoteScheduleHandle is the out-of-process end of schedule.ScheduleHandle: a
// relay-only proxy over the schedule arm. No author injection (the home welds it).
type remoteScheduleHandle struct {
	relay *relayClient
}

// Schedule satisfies schedule.ScheduleHandle over the wire. A transport failure is
// a plain error (the time axis has no unknown-verdict slot — the timer may or may
// not exist, and the caller sees an error, current at-least-once semantics).
func (h *remoteScheduleHandle) Schedule(ctx context.Context, req schedule.ScheduleReq) (schedule.TimerID, error) {
	payload, err := json.Marshal(scheduleRequest{Method: scheduleMethodSchedule, Req: req})
	if err != nil {
		return "", err
	}
	raw, ackErr, txErr := h.relay.roundTrip(ctx, payload)
	if txErr != nil {
		return "", txErr
	}
	if ackErr != nil {
		return "", ackErr
	}
	var resp scheduleResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		return "", err
	}
	return resp.ID, nil
}

// Cancel satisfies schedule.ScheduleHandle over the wire.
func (h *remoteScheduleHandle) Cancel(ctx context.Context, id schedule.TimerID) error {
	payload, err := json.Marshal(scheduleRequest{Method: scheduleMethodCancel, ID: id})
	if err != nil {
		return err
	}
	_, ackErr, txErr := h.relay.roundTrip(ctx, payload)
	if txErr != nil {
		return txErr
	}
	return ackErr
}

func (h *remoteScheduleHandle) Ack(ctx context.Context, id schedule.TimerID) error {
	payload, err := json.Marshal(scheduleRequest{Method: scheduleMethodAck, ID: id})
	if err != nil {
		return err
	}
	_, ackErr, txErr := h.relay.roundTrip(ctx, payload)
	if txErr != nil {
		return txErr
	}
	return ackErr
}

// List satisfies schedule.ScheduleHandle over the wire. Like the other verbs it
// carries no author: the home mints the handle against the endpoint-authenticated
// identity, so a daemon cell can only ever list its own alarms.
func (h *remoteScheduleHandle) List(ctx context.Context) ([]schedule.TimerInfo, error) {
	payload, err := json.Marshal(scheduleRequest{Method: scheduleMethodList})
	if err != nil {
		return nil, err
	}
	raw, ackErr, txErr := h.relay.roundTrip(ctx, payload)
	if txErr != nil {
		return nil, txErr
	}
	if ackErr != nil {
		return nil, ackErr
	}
	var resp scheduleResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil, err
	}
	if resp.Timers == nil {
		return []schedule.TimerInfo{}, nil
	}
	return resp.Timers, nil
}

// Open satisfies accessdoor.FileOpener (期11 spec §5/§3.9'): runs
// OpRead/OpWrite(file) via Invoke, then redeems the accepted outcome's
// Route into a live FileAccess in one call.
func (h *remoteResourceHandle) Open(ctx context.Context, id resource.ResourceID, mode access.Operation) (accessdoor.FileAccess, accessdoor.Outcome, error) {
	out, err := h.Invoke(ctx, mode, id, nil)
	if err != nil {
		return accessdoor.FileAccess{}, accessdoor.Outcome{}, err
	}
	if !out.Accepted() || out.Route == nil {
		return accessdoor.FileAccess{}, out, nil
	}
	fa, rerr := h.Redeem(ctx, *out.Route)
	return fa, out, rerr
}

// Redeem satisfies accessdoor.FileOpener: turns an ALREADY-obtained
// accepted FileRoute (e.g. from Create(with_content=true)'s own Outcome —
// Open cannot re-derive it via Invoke since the row does not exist yet)
// into a live FileAccess. The actual mechanics (direct local open)
// live on the exact client lane because they need its current-lane RPC
// state (the control-RPC arm, the injected LocalFileOpener) this thin wrapper
// does not itself hold.
func (h *remoteResourceHandle) Redeem(ctx context.Context, route accessdoor.FileRoute) (accessdoor.FileAccess, error) {
	if h.redeemer == nil {
		return accessdoor.FileAccess{}, errors.New("link: this resource handle has no dialer wired for file byte redemption")
	}
	return h.redeemer.redeemFileRoute(ctx, route)
}

type fileRouteRedeemer interface {
	redeemFileRoute(context.Context, accessdoor.FileRoute) (accessdoor.FileAccess, error)
}

// Compile-time proof the relay proxies satisfy the substrate capability contracts
// — an out-of-process cell's plane-2 / time-axis handle is indistinguishable from
// a local one (the whole point of the arms).
var (
	_ accessdoor.AccessHandle         = (*remoteAccessHandle)(nil)
	_ accessdoor.ResourceAccessHandle = (*remoteResourceHandle)(nil)
	_ accessdoor.FileOpener           = (*remoteResourceHandle)(nil)
	_ schedule.ScheduleHandle         = (*remoteScheduleHandle)(nil)
)
