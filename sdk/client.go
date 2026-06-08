package coagentsdk

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/wanpengxie/ActOS/protocol/message"
)

const (
	defaultHTTPTimeout = 30 * time.Second
	defaultCallTimeout = 30 * time.Second
	sessionCookieName  = "coagent_session"
)

// Client is a minimal Go SDK client for calling an actor inside a channel.
type Client struct {
	BaseURL      string
	SessionToken string
	HTTPClient   *http.Client
}

type CallRequest struct {
	ChannelID string          `json:"channel_id"`
	ActorID   string          `json:"actor_id"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	Timeout   time.Duration   `json:"-"`
}

type CallResult struct {
	OK    bool            `json:"ok"`
	Data  json.RawMessage `json:"data,omitempty"`
	Error *CallError      `json:"error,omitempty"`
	Raw   json.RawMessage `json:"-"`
}

// SubmitResult is what Submit returns once the request has been accepted
// by the server. RequestID is the envelope id the caller passes to Watch
// / Await — the client preserves caller-supplied ids when set, otherwise
// it generates one and surfaces it here.
//
// SinceSeq is the channel cursor captured BEFORE the emit POST. Callers
// that subscribe AFTER Submit pass it to Watch via WithSinceSeq so the
// server's replay window covers the request's reply even if it landed
// in viewcache before the WS subscribe completed. Without it the
// subscribe-after-submit ordering creates a race: server may process the
// request and emit final before the WS subscription is registered, and
// the client never sees the final response (the D18 / F27 bug).
type SubmitResult struct {
	RequestID string
	SinceSeq  int64
	// Ack is the substrate-level acknowledgement captured at the moment the
	// request write was accepted by the server (§2.3.3 machine kernel). Over
	// the HTTP transport the SDK can only fill the machine-kernel fields the
	// emit endpoint returns — the receiver's own NL semantics arrive later on
	// the first provisional, observable via Watch.
	Ack AckDescriptor
}

// AckDescriptor mirrors kernel/behavior.AckDescriptor's machine-kernel surface
// (§2.3.3). Submit returns it inside SubmitResult; over the SDK's HTTP
// transport only the substrate-level fields the emit endpoint carries are
// populated (Accepted + RequestID + Status). The receiver's NL guidance /
// ToWait hint is template-synthesized framework-side and surfaces later on the
// first provisional response, not on the immediate emit ack.
type AckDescriptor struct {
	// RequestID is the envelope id the request was written under.
	RequestID string
	// Accepted reports whether the harness accepted the write (substrate
	// level, not business). A rejected write surfaces as a Submit error
	// before SubmitResult is returned, so on a successful Submit this is true.
	Accepted bool
	// Status is the immediate substrate ack status — always "accepted" on a
	// successful Submit. Business status arrives later on a provisional.
	Status string
}

// SubmitRequest mirrors CallRequest but is intended for the
// Submit + Watch / Await split surface: callers that need streaming
// access to provisional responses, or fan-in across many in-flight
// requests, use it instead of Call (which collapses everything
// into one blocking final-response call).
type SubmitRequest struct {
	ChannelID string
	ActorID   string
	Type      string
	Payload   json.RawMessage
	// RequestID overrides the auto-generated envelope id when non-empty.
	// Callers that want a deterministic id (idempotency / pre-allocated
	// correlation ids) supply it here.
	RequestID string
}

// WatchEvent is one frame on the Watch stream. Either Envelope is
// non-nil (a substrate envelope matched parent_id == requestID), or
// Err is non-nil (transport / decode failure terminating the watch).
// Final responses (payload.status ∈ {completed, failed}) are delivered
// with IsFinal=true so consumers can detect closure without re-parsing
// the payload.
type WatchEvent struct {
	Envelope *message.Envelope
	// IsFinal mirrors the protocol-level is_terminal derivation: true
	// when Envelope is a kind=response with payload.status ∈
	// {completed, failed}. Provisional responses (Layer 2 core + Layer
	// 3 extension) keep IsFinal=false.
	IsFinal bool
	Err     error
}

// WatchHandle is the live subscription returned by Watch. Consumers
// range over Events() until the channel closes, then call Close to
// release resources (Close is idempotent + safe to call from any
// goroutine).
type WatchHandle struct {
	events chan WatchEvent
	cancel context.CancelFunc
	done   chan struct{}
}

// Events returns the receive-only stream of envelopes whose
// parent_id == requestID. The channel closes when the watch is closed
// or the underlying transport terminates.
func (w *WatchHandle) Events() <-chan WatchEvent { return w.events }

// Close stops the watch and releases the underlying WebSocket. Safe
// to call multiple times.
func (w *WatchHandle) Close() {
	if w == nil {
		return
	}
	if w.cancel != nil {
		w.cancel()
	}
	if w.done != nil {
		<-w.done
	}
}

type CallError struct {
	Code         string `json:"code"`
	Message      string `json:"message"`
	RecoveryHint string `json:"recovery_hint,omitempty"`
}

type ActorInfo struct {
	ActorID     string `json:"actor_id"`
	Kind        string `json:"kind,omitempty"`
	Binding     string `json:"binding,omitempty"`
	DisplayName string `json:"display_name,omitempty"`
	// Ready is a display projection from the server actor list. Use
	// ActorStatus for authoritative current state of one actor.
	Ready             bool            `json:"ready"`
	ReadyReason       string          `json:"ready_reason,omitempty"`
	ReadyDetail       json.RawMessage `json:"ready_detail,omitempty"`
	LastReadyAt       int64           `json:"last_ready_at,omitempty"`
	LastStateChangeAt int64           `json:"last_state_change_at,omitempty"`
	DaemonID          string          `json:"daemon_id,omitempty"`
	DaemonName        string          `json:"daemon_name,omitempty"`
	Types             []ActorTypeInfo `json:"types,omitempty"`
}

type ActorTypeInfo struct {
	Type           string   `json:"type"`
	AllowedKinds   []string `json:"allowed_kinds,omitempty"`
	HandlerBinding string   `json:"handler_binding,omitempty"`
	MaxPendingMs   int64    `json:"max_pending_ms,omitempty"`
}

type ActorStatusResult struct {
	Available         bool            `json:"available"`
	Reason            string          `json:"reason,omitempty"`
	Kind              string          `json:"kind,omitempty"`
	Binding           string          `json:"binding,omitempty"`
	LastReadyAt       int64           `json:"last_ready_at,omitempty"`
	LastStateChangeAt int64           `json:"last_state_change_at,omitempty"`
	Detail            json.RawMessage `json:"detail,omitempty"`
	CheckedAt         int64           `json:"checked_at,omitempty"`
	Raw               json.RawMessage `json:"-"`
}

type actorListResponse struct {
	ChannelID string      `json:"channel_id"`
	Actors    []ActorInfo `json:"actors"`
}

type emitRequest struct {
	ID       string          `json:"id"`
	Type     string          `json:"type"`
	Kind     string          `json:"kind"`
	Payload  json.RawMessage `json:"payload"`
	Audience []string        `json:"audience"`
}

type emitAck struct {
	MessageID    string `json:"message_id"`
	Accepted     bool   `json:"accepted"`
	RejectReason string `json:"reject_reason"`
	RejectDetail string `json:"reject_detail"`
}

type cursorResponse struct {
	LastReceivedSeq int64 `json:"last_received_seq"`
}

type wsPushFrame struct {
	Type      string          `json:"type"`
	ChannelID string          `json:"channel_id"`
	Seq       int64           `json:"seq"`
	Envelope  json.RawMessage `json:"envelope"`
}

// Call emits a kind=request envelope to req.ActorID and waits for the
// matching kind=response envelope on the channel push WebSocket.
func (c *Client) Call(ctx context.Context, req CallRequest) (*CallResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := validateCallRequest(req); err != nil {
		return nil, err
	}

	timeout := req.Timeout
	if timeout <= 0 {
		timeout = defaultCallTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	baseURL, err := normalizeBaseURL(c.BaseURL)
	if err != nil {
		return nil, err
	}
	hc := c.httpClient()

	cursor, err := c.fetchCursor(ctx, hc, baseURL, req.ChannelID)
	if err != nil {
		return nil, err
	}

	wsURL, err := websocketURL(baseURL)
	if err != nil {
		return nil, err
	}
	ws, _, err := c.dialWebSocket(ctx, wsURL)
	if err != nil {
		return nil, fmt.Errorf("coagentsdk: websocket connect: %w", err)
	}
	defer func() { _ = ws.Close() }()

	if err := ws.WriteJSON(map[string]any{
		"type":       "subscribe",
		"channel_id": req.ChannelID,
		"since_seq":  cursor,
	}); err != nil {
		return nil, fmt.Errorf("coagentsdk: websocket subscribe: %w", err)
	}
	// Subscribe replay (server pushhub) covers the race window between
	// the WS subscribe frame and the emit POST: even if the daemon emits
	// the final response before the server-side subscription is
	// registered, the since_seq=cursor replay will deliver it. No
	// settle-delay needed.

	requestID, err := newRequestID()
	if err != nil {
		return nil, err
	}
	ack, err := c.emitRequest(ctx, hc, baseURL, req, requestID)
	if err != nil {
		return nil, err
	}
	matchIDs := map[string]struct{}{requestID: {}}
	if ack.MessageID != "" {
		matchIDs[ack.MessageID] = struct{}{}
	}

	return c.waitResponse(ctx, ws, req, matchIDs, timeout)
}

// Submit emits a kind=request envelope without waiting for the response.
// Callers pair Submit with Watch (for streaming provisional + final) or
// Await (for blocking on the final response). The returned RequestID is
// the envelope id; pass it to Watch / Await for correlation.
//
// Submit + Watch / Await is the "first-class async" surface: the client
// does NOT hold any pending future, so multiple in-flight requests fan
// in over one shared substrate connection without sync wrap collapsing
// them into RPC.
//
// Race fix (D18 / F27): Submit captures the channel cursor BEFORE the
// emit POST and returns it as SubmitResult.SinceSeq. Callers MUST pass
// this seq to Watch / WatchFrom (or Await) via WithSinceSeq so the
// server's subscribe replay window covers the request's reply even when
// it lands in viewcache before the client's WS subscribe completes.
//
// Forgetting WithSinceSeq silently loses a fast final (NF3). Prefer the
// SubmitAndAwait / SubmitAndWatch sugar, which threads SinceSeq for you;
// reach for raw Submit + Watch only when splitting the steps is required
// (e.g. fan-in across many in-flight requests).
func (c *Client) Submit(ctx context.Context, req SubmitRequest) (SubmitResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := validateSubmitRequest(req); err != nil {
		return SubmitResult{}, err
	}
	baseURL, err := normalizeBaseURL(c.BaseURL)
	if err != nil {
		return SubmitResult{}, err
	}
	hc := c.httpClient()
	requestID := req.RequestID
	if requestID == "" {
		requestID, err = newRequestID()
		if err != nil {
			return SubmitResult{}, err
		}
	}
	// Capture the cursor BEFORE emit so a subsequent Watch with
	// WithSinceSeq(cursor) replays everything the substrate has appended
	// since (including the reply if it lands before the watch
	// subscribes server-side).
	cursor, err := c.fetchCursor(ctx, hc, baseURL, req.ChannelID)
	if err != nil {
		return SubmitResult{}, err
	}
	callReq := CallRequest{
		ChannelID: req.ChannelID,
		ActorID:   req.ActorID,
		Type:      req.Type,
		Payload:   req.Payload,
	}
	ack, err := c.emitRequest(ctx, hc, baseURL, callReq, requestID)
	if err != nil {
		return SubmitResult{}, err
	}
	return SubmitResult{
		RequestID: requestID,
		SinceSeq:  cursor,
		Ack: AckDescriptor{
			RequestID: requestID,
			Accepted:  ack.Accepted,
			Status:    "accepted",
		},
	}, nil
}

// SubmitAndAwait is the one-call ergonomic form of Submit + Await: it
// emits the request and blocks for the final response, automatically
// threading the cursor captured at submit time into the watch's
// since_seq so a fast final emitted before the WS subscribe completes is
// never lost (the NF3 footgun — callers using Submit + Await manually
// must remember WithSinceSeq(result.SinceSeq); this method removes that
// requirement). Use Submit + Watch(WithSinceSeq(...)) directly only when
// you need to observe provisional responses.
func (c *Client) SubmitAndAwait(ctx context.Context, req SubmitRequest, timeout time.Duration) (*CallResult, error) {
	res, err := c.Submit(ctx, req)
	if err != nil {
		return nil, err
	}
	return c.Await(ctx, req.ChannelID, res.RequestID, timeout, WithSinceSeq(res.SinceSeq))
}

// SubmitAndWatch is the one-call ergonomic form of Submit + Watch: it
// emits the request and opens a streaming subscription, automatically
// threading the submit-time cursor into the watch's since_seq so neither
// provisional ticks nor a fast final are lost in the emit→subscribe race
// (the NF3 footgun). The returned WatchHandle is owned by the caller —
// close it via WatchHandle.Close.
func (c *Client) SubmitAndWatch(ctx context.Context, req SubmitRequest) (*WatchHandle, error) {
	res, err := c.Submit(ctx, req)
	if err != nil {
		return nil, err
	}
	return c.Watch(ctx, req.ChannelID, res.RequestID, WithSinceSeq(res.SinceSeq))
}

// Cursor returns the server's current last_received_seq for channelID.
// Callers that want to subscribe-after-submit (e.g. fire-and-forget
// flows that materialize a Watch later) capture this BEFORE the
// emit POST and pass it to Watch via WithSinceSeq.
func (c *Client) Cursor(ctx context.Context, channelID string) (int64, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if strings.TrimSpace(channelID) == "" {
		return 0, fmt.Errorf("coagentsdk: channel_id is required")
	}
	baseURL, err := normalizeBaseURL(c.BaseURL)
	if err != nil {
		return 0, err
	}
	return c.fetchCursor(ctx, c.httpClient(), baseURL, channelID)
}

// WatchOption configures Watch. Today only WithSinceSeq is supported.
type WatchOption func(*watchOptions)

type watchOptions struct {
	sinceSeq    int64
	sinceSeqSet bool
}

// WithSinceSeq overrides the auto-fetched cursor: Watch will subscribe
// with the supplied since_seq so the server replays any persisted
// envelope with seq > sinceSeq. Pair with SubmitResult.SinceSeq for
// subscribe-after-submit flows so an early-completing request isn't
// lost in the race between emit POST and WS subscribe.
func WithSinceSeq(sinceSeq int64) WatchOption {
	return func(o *watchOptions) {
		o.sinceSeq = sinceSeq
		o.sinceSeqSet = true
	}
}

// Watch opens a streaming subscription on the channel WebSocket and
// delivers every envelope whose parent_id matches requestID. The
// stream includes both provisional and final responses; consumers
// inspect WatchEvent.IsFinal to detect closure.
//
// The stream is owned by the caller — close it via WatchHandle.Close
// (Close is also fired automatically when ctx is cancelled). The watch
// subscribes with a since_seq cursor: by default it fetches the
// channel's current cursor first (preserves the legacy "subscribe-then-
// submit" ordering); callers that submit BEFORE starting the watch MUST
// pass WithSinceSeq(SubmitResult.SinceSeq) to avoid the race where the
// reply lands in viewcache before the subscribe completes server-side.
func (c *Client) Watch(ctx context.Context, channelID, requestID string, opts ...WatchOption) (*WatchHandle, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if strings.TrimSpace(channelID) == "" {
		return nil, fmt.Errorf("coagentsdk: channel_id is required")
	}
	if strings.TrimSpace(requestID) == "" {
		return nil, fmt.Errorf("coagentsdk: request_id is required")
	}
	var wopts watchOptions
	for _, o := range opts {
		o(&wopts)
	}
	baseURL, err := normalizeBaseURL(c.BaseURL)
	if err != nil {
		return nil, err
	}
	hc := c.httpClient()
	var cursor int64
	if wopts.sinceSeqSet {
		cursor = wopts.sinceSeq
	} else {
		cursor, err = c.fetchCursor(ctx, hc, baseURL, channelID)
		if err != nil {
			return nil, err
		}
	}
	wsURL, err := websocketURL(baseURL)
	if err != nil {
		return nil, err
	}

	watchCtx, cancel := context.WithCancel(ctx)
	ws, _, err := c.dialWebSocket(watchCtx, wsURL)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("coagentsdk: websocket connect: %w", err)
	}
	if err := ws.WriteJSON(map[string]any{
		"type":       "subscribe",
		"channel_id": channelID,
		"since_seq":  cursor,
	}); err != nil {
		_ = ws.Close()
		cancel()
		return nil, fmt.Errorf("coagentsdk: websocket subscribe: %w", err)
	}

	// matched carries envelopes whose parent_id == requestID from the WS
	// reader goroutine to the forwarder goroutine (the sole writer of the
	// SDK events channel). Each item is a matchedEnvelope carrying the
	// parsed envelope and whether it is a final response. The reader
	// closes matched after a final or a transport error; the forwarder
	// observes closure and tears down.
	type matchedEnvelope struct {
		env     *message.Envelope
		isFinal bool
		err     error
	}
	matched := make(chan matchedEnvelope, 16)

	events := make(chan WatchEvent, 16)
	done := make(chan struct{})
	// closeWS guards the single Close call so the ctx-watcher and the
	// read goroutine's defer don't race on gorilla's underlying conn.
	var closeWSOnce sync.Once
	closeWS := func() { closeWSOnce.Do(func() { _ = ws.Close() }) }

	// ctx-watcher: when watchCtx fires, close the ws to interrupt the
	// blocking ReadMessage. This is the *only* unblock mechanism — we
	// deliberately do NOT use per-iter SetReadDeadline, because gorilla
	// marks the connection corrupt after the first i/o timeout
	// (`hideTempErr` drops Temporary()), and any subsequent ReadMessage
	// spins toward gorilla's 1000-call panic "repeated read on failed
	// websocket connection". The tight 1ns window returned by the old
	// nextReadWindow once ctx expired made the spin instant.
	stopWatcher := make(chan struct{})
	go func() {
		select {
		case <-watchCtx.Done():
			closeWS()
		case <-stopWatcher:
		}
	}()

	// Forwarder: drain the matched channel onto the SDK's WatchEvent
	// channel, and tear down when the matched channel closes (final
	// delivered or reader exited) or watchCtx fires.
	go func() {
		defer close(done)
		defer close(events)
		defer closeWS()
		defer close(stopWatcher)
		for {
			select {
			case <-watchCtx.Done():
				return
			case m, ok := <-matched:
				if !ok {
					return
				}
				var ev WatchEvent
				if m.err != nil {
					ev = WatchEvent{Err: m.err}
				} else {
					ev = WatchEvent{Envelope: m.env, IsFinal: m.isFinal}
				}
				select {
				case events <- ev:
				case <-watchCtx.Done():
					return
				}
				if m.err != nil || m.isFinal {
					return
				}
			}
		}
	}()

	// Reader: pull frames off the WS, match parent_id == requestID,
	// classify IsFinal via message.IsFinalStatus, and push matched
	// envelopes to the forwarder via the matched channel. The reader
	// NEVER writes to events — it communicates solely through matched.
	matchID := message.ID(requestID)
	go func() {
		defer close(matched)
		// Defensive: if gorilla ever panics on a corrupted conn after
		// ctx cancel, swallow it. The caller has already asked us to
		// shut down via watchCtx; surfacing a panic into the SDK
		// goroutine would kill the process. A panic while ctx is still
		// alive is a real bug — re-raise it.
		defer func() {
			if r := recover(); r != nil {
				if watchCtx.Err() == nil {
					panic(r)
				}
			}
		}()
		for {
			if watchCtx.Err() != nil {
				return
			}
			mt, raw, err := ws.ReadMessage()
			if err != nil {
				if watchCtx.Err() != nil {
					return
				}
				select {
				case matched <- matchedEnvelope{err: fmt.Errorf("coagentsdk: websocket read: %w", err)}:
				case <-watchCtx.Done():
				}
				return
			}
			if mt != websocket.TextMessage {
				continue
			}
			var frame wsPushFrame
			if err := json.Unmarshal(raw, &frame); err != nil {
				continue
			}
			if frame.Type != "message" || frame.ChannelID != channelID || len(frame.Envelope) == 0 {
				continue
			}
			var env message.Envelope
			if err := json.Unmarshal(frame.Envelope, &env); err != nil {
				continue
			}
			// Match by parent_id: only envelopes that are responses to
			// requestID pass through. Non-matching envelopes are silently
			// dropped (same semantics as the former futurereg.Deliver).
			if env.ParentID != matchID {
				continue
			}
			if env.Kind != message.KindResponse {
				continue
			}
			status, _ := parsePayloadStatus(env.Payload)
			isFinal := message.IsFinalStatus(status)
			envCopy := env
			select {
			case matched <- matchedEnvelope{env: &envCopy, isFinal: isFinal}:
			case <-watchCtx.Done():
				return
			}
			if isFinal {
				return
			}
		}
	}()

	return &WatchHandle{events: events, cancel: cancel, done: done}, nil
}

// Await blocks until the final response (payload.status ∈
// {completed, failed}) for requestID arrives on channelID, or the
// supplied timeout elapses. Provisional responses are silently dropped;
// callers that need to observe them should use Watch instead.
//
// Await is sugar over Watch — internally it filters the watch stream to
// the first IsFinal event and converts it to a CallResult. Timeout
// failure does NOT cancel substrate state: the daemon may still emit a
// final response later (Watch can observe it if the caller reconnects).
//
// opts forward to Watch — pass WithSinceSeq(SubmitResult.SinceSeq) when
// awaiting a request that was Submit'd before this Await call.
func (c *Client) Await(ctx context.Context, channelID, requestID string, timeout time.Duration, opts ...WatchOption) (*CallResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if timeout <= 0 {
		timeout = defaultCallTimeout
	}
	awaitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	watch, err := c.Watch(awaitCtx, channelID, requestID, opts...)
	if err != nil {
		return nil, err
	}
	defer watch.Close()
	for {
		select {
		case <-awaitCtx.Done():
			return timeoutResult(CallRequest{ChannelID: channelID, Type: requestID}, timeout), nil
		case ev, ok := <-watch.Events():
			if !ok {
				return timeoutResult(CallRequest{ChannelID: channelID, Type: requestID}, timeout), nil
			}
			if ev.Err != nil {
				return nil, ev.Err
			}
			if ev.Envelope == nil || !ev.IsFinal {
				continue
			}
			return resultFromResponse(*ev.Envelope)
		}
	}
}

// AwaitItem identifies one in-flight request to await as part of AwaitAll.
// Each item carries its own channel + request id + optional submit-time
// cursor, because a fan-in set is built from independent Submit calls — each
// SubmitResult has its own SinceSeq (the cursor captured before that request's
// emit), and threading it per-item avoids the fast-final race (NF3) for every
// member of the set.
type AwaitItem struct {
	ChannelID string
	RequestID string
	// SinceSeq, when SinceSeqSet is true, is threaded into the per-item watch
	// (equivalent to WithSinceSeq). Pass SubmitResult.SinceSeq here.
	SinceSeq    int64
	SinceSeqSet bool
}

// Outcome is one per-item result of AwaitAll (all-settled, §0②). Exactly one
// of Result / Err is set: Result holds the final response (which may itself be
// a business failure — res.OK=false — or a timeout result); Err holds a
// transport / decode error that prevented producing a result.
type Outcome struct {
	RequestID string
	Result    *CallResult
	Err       error
}

// AwaitAll awaits every item to settlement and returns one Outcome per item,
// in the input order. It is ALL-SETTLED (§0②): it launches one goroutine per
// id and waits for ALL of them — it never returns early on the first failure.
// There is no protocol-level cancel, so fail-fast could not actually stop the
// sibling waiters; all-settled is the honest semantics. A per-item timeout
// produces a timeout Result (not an Err), matching Await.
//
// AwaitAll is an optional combinator (§0.1), not a mandatory orchestration
// path — callers can always compose Submit + Await/Watch themselves.
func (c *Client) AwaitAll(ctx context.Context, items []AwaitItem, timeout time.Duration) ([]Outcome, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	out := make([]Outcome, len(items))
	var wg sync.WaitGroup
	for i, it := range items {
		wg.Add(1)
		go func(i int, it AwaitItem) {
			defer wg.Done()
			out[i].RequestID = it.RequestID
			var opts []WatchOption
			if it.SinceSeqSet {
				opts = append(opts, WithSinceSeq(it.SinceSeq))
			}
			res, err := c.Await(ctx, it.ChannelID, it.RequestID, timeout, opts...)
			out[i].Result = res
			out[i].Err = err
		}(i, it)
	}
	wg.Wait()
	return out, nil
}

func validateSubmitRequest(req SubmitRequest) error {
	switch {
	case strings.TrimSpace(req.ChannelID) == "":
		return fmt.Errorf("coagentsdk: channel_id is required")
	case strings.TrimSpace(req.ActorID) == "":
		return fmt.Errorf("coagentsdk: actor_id is required")
	case strings.TrimSpace(req.Type) == "":
		return fmt.Errorf("coagentsdk: type is required")
	}
	if len(req.Payload) > 0 && !json.Valid(req.Payload) {
		return fmt.Errorf("coagentsdk: payload must be valid JSON")
	}
	return nil
}

// ListActors returns the server's display projection for channel actors.
// It is useful for catalogs and UI lists, but it is not authoritative
// current truth. Use ActorStatus / DescribeActor for reserved envelope
// reads against one actor.
func (c *Client) ListActors(ctx context.Context, channelID string) ([]ActorInfo, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if strings.TrimSpace(channelID) == "" {
		return nil, fmt.Errorf("coagentsdk: channel_id is required")
	}
	baseURL, err := normalizeBaseURL(c.BaseURL)
	if err != nil {
		return nil, err
	}
	var out actorListResponse
	if err := c.doJSON(ctx, c.httpClient(), http.MethodGet, baseURL+"/api/channels/"+url.PathEscape(channelID)+"/actors", nil, &out); err != nil {
		return nil, fmt.Errorf("coagentsdk: list actors: %w", err)
	}
	return out.Actors, nil
}

func (c *Client) ActorStatus(ctx context.Context, channelID, actorID string) (*ActorStatusResult, error) {
	res, err := c.Call(ctx, CallRequest{
		ChannelID: channelID,
		ActorID:   actorID,
		Type:      "actor.status",
		Payload:   json.RawMessage(`{}`),
	})
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, fmt.Errorf("coagentsdk: actor.status returned nil result")
	}
	if !res.OK {
		if res.Error != nil {
			return nil, fmt.Errorf("coagentsdk: actor.status failed: %s: %s", res.Error.Code, res.Error.Message)
		}
		return nil, fmt.Errorf("coagentsdk: actor.status failed")
	}
	var payload struct {
		Status            string          `json:"status"`
		Available         bool            `json:"available"`
		Reason            string          `json:"reason"`
		Kind              string          `json:"kind"`
		Binding           string          `json:"binding"`
		LastReadyAt       int64           `json:"last_ready_at"`
		LastStateChangeAt int64           `json:"last_state_change_at"`
		Detail            json.RawMessage `json:"detail"`
		CheckedAt         int64           `json:"checked_at"`
	}
	if err := json.Unmarshal(res.Raw, &payload); err != nil {
		return nil, fmt.Errorf("coagentsdk: decode actor.status: %w", err)
	}
	return &ActorStatusResult{
		Available:         payload.Available,
		Reason:            payload.Reason,
		Kind:              payload.Kind,
		Binding:           payload.Binding,
		LastReadyAt:       payload.LastReadyAt,
		LastStateChangeAt: payload.LastStateChangeAt,
		Detail:            append(json.RawMessage(nil), payload.Detail...),
		CheckedAt:         payload.CheckedAt,
		Raw:               append(json.RawMessage(nil), res.Raw...),
	}, nil
}

// DescribeActorResult is the full Declaration projection returned by
// Client.DescribeActor — the actor-CLI describe_actor surface backed
// by the actor.describe reserved type (framework-intercepted; daemon
// is single source of truth).
type DescribeActorResult struct {
	ActorID     string                       `json:"actor_id"`
	Name        string                       `json:"name,omitempty"`
	Binding     string                       `json:"binding,omitempty"`
	Description string                       `json:"description,omitempty"`
	SkillDoc    string                       `json:"skill_doc,omitempty"`
	Types       map[string]TypeConventionDoc `json:"types,omitempty"`
	Raw         json.RawMessage              `json:"-"`
}

// TypeConventionDoc mirrors kernel/behavior.TypeConventionDoc for the wire.
type TypeConventionDoc struct {
	Description    string          `json:"description,omitempty"`
	PayloadExample json.RawMessage `json:"payload_example,omitempty"`
	PayloadFields  []FieldDoc      `json:"payload_fields,omitempty"`
	ErrorCodes     []ErrorDoc      `json:"error_codes,omitempty"`
	Notes          string          `json:"notes,omitempty"`
}

// FieldDoc mirrors kernel/behavior.FieldDoc.
type FieldDoc struct {
	Name        string `json:"name"`
	Required    bool   `json:"required,omitempty"`
	Description string `json:"description,omitempty"`
	Example     any    `json:"example,omitempty"`
}

// ErrorDoc mirrors kernel/behavior.ErrorDoc.
type ErrorDoc struct {
	Code        string `json:"code"`
	Description string `json:"description,omitempty"`
	Recovery    string `json:"recovery,omitempty"`
}

// DescribeTypeResult is the single-type projection returned by
// Client.DescribeType.
type DescribeTypeResult struct {
	ActorID        string          `json:"actor_id"`
	Type           string          `json:"type"`
	Description    string          `json:"description,omitempty"`
	PayloadExample json.RawMessage `json:"payload_example,omitempty"`
	PayloadFields  []FieldDoc      `json:"payload_fields,omitempty"`
	ErrorCodes     []ErrorDoc      `json:"error_codes,omitempty"`
	Notes          string          `json:"notes,omitempty"`
	AllowedKinds   []string        `json:"allowed_kinds,omitempty"`
	MaxPendingMs   int64           `json:"max_pending_ms,omitempty"`
	HandlerBinding string          `json:"handler_binding,omitempty"`
	Raw            json.RawMessage `json:"-"`
}

// DescribeActor returns the actor's static Declaration projection
// (description / skill_doc / per-type metadata). Implemented as sugar
// over Call with the actor.describe reserved type — framework
// intercepts at dispatch, daemon answers from Module.Declares() with
// no server-side mirror.
func (c *Client) DescribeActor(ctx context.Context, channelID, actorID string) (*DescribeActorResult, error) {
	res, err := c.Call(ctx, CallRequest{
		ChannelID: channelID,
		ActorID:   actorID,
		Type:      "actor.describe",
		Payload:   json.RawMessage(`{}`),
	})
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, fmt.Errorf("coagentsdk: actor.describe returned nil result")
	}
	if !res.OK {
		if res.Error != nil {
			return nil, fmt.Errorf("coagentsdk: actor.describe failed: %s: %s", res.Error.Code, res.Error.Message)
		}
		return nil, fmt.Errorf("coagentsdk: actor.describe failed")
	}
	var out DescribeActorResult
	if err := json.Unmarshal(res.Data, &out); err != nil {
		return nil, fmt.Errorf("coagentsdk: decode actor.describe: %w", err)
	}
	out.Raw = append(json.RawMessage(nil), res.Data...)
	return &out, nil
}

// DescribeType returns one type's full metadata. Filter is passed via
// payload so a single Call round-trip carries it.
func (c *Client) DescribeType(ctx context.Context, channelID, actorID, typeName string) (*DescribeTypeResult, error) {
	if strings.TrimSpace(typeName) == "" {
		return nil, fmt.Errorf("coagentsdk: type is required")
	}
	body, _ := json.Marshal(map[string]string{"type": typeName})
	res, err := c.Call(ctx, CallRequest{
		ChannelID: channelID,
		ActorID:   actorID,
		Type:      "actor.describe",
		Payload:   body,
	})
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, fmt.Errorf("coagentsdk: actor.describe(type=%s) returned nil result", typeName)
	}
	if !res.OK {
		if res.Error != nil {
			return nil, fmt.Errorf("coagentsdk: actor.describe(type=%s) failed: %s: %s", typeName, res.Error.Code, res.Error.Message)
		}
		return nil, fmt.Errorf("coagentsdk: actor.describe(type=%s) failed", typeName)
	}
	var out DescribeTypeResult
	if err := json.Unmarshal(res.Data, &out); err != nil {
		return nil, fmt.Errorf("coagentsdk: decode actor.describe(type=%s): %w", typeName, err)
	}
	out.Raw = append(json.RawMessage(nil), res.Data...)
	return &out, nil
}

func validateCallRequest(req CallRequest) error {
	switch {
	case strings.TrimSpace(req.ChannelID) == "":
		return fmt.Errorf("coagentsdk: channel_id is required")
	case strings.TrimSpace(req.ActorID) == "":
		return fmt.Errorf("coagentsdk: actor_id is required")
	case strings.TrimSpace(req.Type) == "":
		return fmt.Errorf("coagentsdk: type is required")
	}
	if len(req.Payload) > 0 && !json.Valid(req.Payload) {
		return fmt.Errorf("coagentsdk: payload must be valid JSON")
	}
	return nil
}

func normalizeBaseURL(raw string) (string, error) {
	if strings.TrimSpace(raw) == "" {
		return "", fmt.Errorf("coagentsdk: base URL is required")
	}
	raw = strings.TrimRight(strings.TrimSpace(raw), "/")
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("coagentsdk: parse base URL: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", fmt.Errorf("coagentsdk: base URL scheme must be http or https")
	}
	if u.Host == "" {
		return "", fmt.Errorf("coagentsdk: base URL host is required")
	}
	return raw, nil
}

func (c *Client) httpClient() *http.Client {
	if c.HTTPClient != nil {
		return c.HTTPClient
	}
	return &http.Client{Timeout: defaultHTTPTimeout}
}

func (c *Client) fetchCursor(ctx context.Context, hc *http.Client, baseURL, channelID string) (int64, error) {
	var out cursorResponse
	if err := c.doJSON(ctx, hc, http.MethodGet, baseURL+"/api/channels/"+url.PathEscape(channelID)+"/cursor", nil, &out); err != nil {
		return 0, fmt.Errorf("coagentsdk: fetch cursor: %w", err)
	}
	return out.LastReceivedSeq, nil
}

func (c *Client) emitRequest(ctx context.Context, hc *http.Client, baseURL string, req CallRequest, requestID string) (emitAck, error) {
	payload := req.Payload
	if len(payload) == 0 {
		payload = json.RawMessage(`{}`)
	}
	body := emitRequest{
		ID:       requestID,
		Type:     req.Type,
		Kind:     string(message.KindRequest),
		Payload:  payload,
		Audience: []string{req.ActorID},
	}
	var ack emitAck
	if err := c.doJSON(ctx, hc, http.MethodPost, baseURL+"/api/channels/"+url.PathEscape(req.ChannelID)+"/messages", body, &ack); err != nil {
		return emitAck{}, fmt.Errorf("coagentsdk: emit request: %w", err)
	}
	if ack.RejectReason != "" {
		return emitAck{}, fmt.Errorf("coagentsdk: emit rejected: %s %s", ack.RejectReason, ack.RejectDetail)
	}
	return ack, nil
}

func (c *Client) doJSON(ctx context.Context, hc *http.Client, method, endpoint string, body any, out any) error {
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("encode request: %w", err)
		}
		reader = bytes.NewReader(raw)
	}
	httpReq, err := http.NewRequestWithContext(ctx, method, endpoint, reader)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	httpReq.Header.Set("Accept", "application/json")
	if body != nil {
		httpReq.Header.Set("Content-Type", "application/json")
	}
	c.applySession(httpReq.Header)

	resp, err := hc.Do(httpReq)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &HTTPError{StatusCode: resp.StatusCode, Body: string(raw)}
	}
	if out == nil || len(raw) == 0 {
		return nil
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	return nil
}

func (c *Client) applySession(header http.Header) {
	if c.SessionToken == "" {
		return
	}
	header.Set("Cookie", (&http.Cookie{
		Name:  sessionCookieName,
		Value: c.SessionToken,
	}).String())
}

func websocketURL(baseURL string) (string, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return "", err
	}
	switch u.Scheme {
	case "http":
		u.Scheme = "ws"
	case "https":
		u.Scheme = "wss"
	default:
		return "", fmt.Errorf("unsupported scheme %q", u.Scheme)
	}
	u.Path = strings.TrimRight(u.Path, "/") + "/ws"
	u.RawQuery = ""
	u.Fragment = ""
	return u.String(), nil
}

func (c *Client) dialWebSocket(ctx context.Context, wsURL string) (*websocket.Conn, *http.Response, error) {
	dialer := *websocket.DefaultDialer
	dialer.HandshakeTimeout = 5 * time.Second
	header := http.Header{}
	c.applySession(header)
	return dialer.DialContext(ctx, wsURL, header)
}

func (c *Client) waitResponse(ctx context.Context, ws *websocket.Conn, req CallRequest, matchIDs map[string]struct{}, timeout time.Duration) (result *CallResult, err error) {
	// Same ctx-cancel-via-ws.Close pattern as Watch: gorilla's
	// SetReadDeadline-based polling marks the conn corrupt after the
	// first i/o timeout and risks the "repeated read on failed
	// websocket connection" panic. Instead, fire a watcher that closes
	// the ws when ctx expires, and let ReadMessage block indefinitely.
	var closeOnce sync.Once
	closeWS := func() { closeOnce.Do(func() { _ = ws.Close() }) }
	stopWatcher := make(chan struct{})
	defer close(stopWatcher)
	go func() {
		select {
		case <-ctx.Done():
			closeWS()
		case <-stopWatcher:
		}
	}()
	defer func() {
		// Recover swallows gorilla's panic on a corrupted-after-close
		// conn so the caller goroutine doesn't crash mid-test/CLI run.
		// If ctx is still alive we propagate (real bug, not shutdown).
		if r := recover(); r != nil {
			if ctx.Err() == nil {
				panic(r)
			}
			result = timeoutResult(req, timeout)
			err = nil
		}
	}()
	for {
		if err := ctx.Err(); err != nil {
			return timeoutResult(req, timeout), nil
		}
		mt, raw, err := ws.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return timeoutResult(req, timeout), nil
			}
			return nil, fmt.Errorf("coagentsdk: websocket read: %w", err)
		}
		if mt != websocket.TextMessage {
			continue
		}
		var frame wsPushFrame
		if err := json.Unmarshal(raw, &frame); err != nil {
			continue
		}
		if frame.Type != "message" || frame.ChannelID != req.ChannelID || len(frame.Envelope) == 0 {
			continue
		}
		var env message.Envelope
		if err := json.Unmarshal(frame.Envelope, &env); err != nil {
			continue
		}
		if env.Kind != message.KindResponse {
			continue
		}
		if _, ok := matchIDs[env.ParentID.String()]; !ok {
			continue
		}
		// Call only resolves on a final response (proto-foundation
		// §1.6.3). Provisional responses (payload.status ∈ Layer 2/3) on
		// the same parent_id are silently dropped here — callers that
		// need to observe them must use Submit + Watch instead.
		status, parseErr := parsePayloadStatus(env.Payload)
		if parseErr != nil {
			return nil, parseErr
		}
		if !message.IsFinalStatus(status) {
			continue
		}
		return resultFromResponse(env)
	}
}

// parsePayloadStatus extracts the response envelope's payload.status
// field. Returns an empty string when payload is empty / malformed; the
// error path is reserved for genuinely undecodable payloads.
func parsePayloadStatus(raw json.RawMessage) (string, error) {
	if len(raw) == 0 {
		return "", nil
	}
	var obj struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(raw, &obj); err != nil {
		return "", fmt.Errorf("coagentsdk: response payload must be a JSON object: %w", err)
	}
	return obj.Status, nil
}

func resultFromResponse(env message.Envelope) (*CallResult, error) {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(env.Payload, &obj); err != nil {
		return nil, fmt.Errorf("coagentsdk: response payload must be a JSON object: %w", err)
	}
	status := rawString(obj["status"])
	switch status {
	case "completed":
		data := removePayloadFields(obj, "status", "reason")
		return &CallResult{
			OK:   true,
			Data: data,
			Raw:  append(json.RawMessage(nil), env.Payload...),
		}, nil
	case "failed":
		code := firstNonEmpty(rawString(obj["error_code"]), rawString(obj["reason"]), "failed")
		msg := firstNonEmpty(rawString(obj["detail"]), rawString(obj["message"]))
		return &CallResult{
			OK: false,
			Error: &CallError{
				Code:         code,
				Message:      msg,
				RecoveryHint: rawString(obj["recovery_hint"]),
			},
			Raw: append(json.RawMessage(nil), env.Payload...),
		}, nil
	default:
		return nil, fmt.Errorf("coagentsdk: response payload status=%q", status)
	}
}

func removePayloadFields(obj map[string]json.RawMessage, keys ...string) json.RawMessage {
	cp := make(map[string]json.RawMessage, len(obj))
	for k, v := range obj {
		cp[k] = v
	}
	for _, k := range keys {
		delete(cp, k)
	}
	raw, err := json.Marshal(cp)
	if err != nil {
		return json.RawMessage(`{}`)
	}
	return raw
}

func rawString(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	return ""
}

func timeoutResult(req CallRequest, timeout time.Duration) *CallResult {
	return &CallResult{
		OK: false,
		Error: &CallError{
			Code:    "timeout",
			Message: fmt.Sprintf("timed out waiting %s for response to %s on channel %s", timeout, req.Type, req.ChannelID),
		},
	}
}

func newRequestID() (string, error) {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", fmt.Errorf("coagentsdk: generate request id: %w", err)
	}
	return "req-" + hex.EncodeToString(buf[:]), nil
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

// HTTPError reports a non-2xx server response.
type HTTPError struct {
	StatusCode int
	Body       string
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("http status %d: %s", e.StatusCode, truncate(e.Body, 500))
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
