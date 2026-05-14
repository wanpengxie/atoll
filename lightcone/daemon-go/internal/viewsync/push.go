package viewsync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	pkgharness "github.com/coagent-ai/daemon-go/pkg/harness"
	"github.com/coagent-ai/daemon-go/pkg/v4types"
)

// DefaultPushPath is the server-side view sync ingest path the daemon
// posts envelopes to. The path is a baseline default — callers can
// override via HTTPPusherOptions.Path when the server route changes.
const DefaultPushPath = "/api/view/sync"

// DefaultSystemActorID is the sender actor id stamped on the system.event
// fallback emit when the caller does not supply one. Mirrors the
// L2 §3.7.3 scheduler convention of using a fixed "system" id.
const DefaultSystemActorID = "system"

// PushAck is the structured ack the server returns on a 200 reply.
// L1 §8.1 contract: server is a read-only view cache, so the ack carries
// the message_id round-trip (so caller can correlate) and an optional
// dedupe flag mirroring the harness's idempotent insert path.
type PushAck struct {
	MessageID string `json:"message_id"`
	Dedupe    bool   `json:"dedupe,omitempty"`
}

// PushError is the error type returned by HTTPPusher.PushToServer when
// the server replied but the call failed. Carries the HTTP status, the
// raw body (capped) for diagnostics, and the kind tag used by the
// FailureSink event payload.
type PushError struct {
	// Kind classifies the failure for the system.event payload. One of
	// "transport_error" (network / encode failed before server replied)
	// or "http_status" (server replied but non-2xx).
	Kind string

	// HTTPStatus is the server's status code when Kind == "http_status";
	// 0 for transport errors.
	HTTPStatus int

	// Body holds up to maxErrBody bytes of the response body for log /
	// system.event detail. Empty for transport errors.
	Body string

	// Cause is the wrapped transport / parse error (nil for non-2xx).
	Cause error
}

// Error implements the standard error interface.
func (e *PushError) Error() string {
	switch e.Kind {
	case "transport_error":
		if e.Cause != nil {
			return "viewsync: push transport error: " + e.Cause.Error()
		}
		return "viewsync: push transport error"
	case "http_status":
		return fmt.Sprintf("viewsync: push rejected status=%d body=%s", e.HTTPStatus, e.Body)
	}
	return "viewsync: push error"
}

// Unwrap returns the wrapped transport error so errors.Is / errors.As
// callers can inspect the underlying network failure.
func (e *PushError) Unwrap() error { return e.Cause }

// Pusher is the L1 §8 daemon → server view sync interface.
//
// L1 §8.1.4 requires this be a callable interface (not fire-and-forget
// fetch().catch(noop)) so M1.x can swap the implementation for an outbox
// queue + worker without touching call sites. Implementations MUST:
//
//   - return success / failure unambiguously (no swallowed errors)
//   - be safe for concurrent calls
//   - leave the daemon's local message store untouched on failure
//     (per L1 §8.1.2 contract: daemon is source of truth)
type Pusher interface {
	// PushToServer ships env to the server view cache. On success it
	// returns the server-side ack. On any failure (transport / 4xx /
	// 5xx), the implementation MAY emit a local system.event payload.kind
	// = view_sync_failed (via a FailureSink) and MUST return the failure
	// via the second return value — callers never need to retry to
	// observe the error.
	PushToServer(ctx context.Context, env *v4types.Envelope) (*PushAck, error)
}

// FailureSink is the local-side emitter the Pusher routes failures to.
// The default implementation HarnessFailureSink builds a system.event
// envelope and runs it through harness.InWorkerBus so the local channel
// store records the failure — operators / dashboards then surface
// view_sync_failed rate as a freshness signal (L1 §8.1.1 监控建议).
//
// FailureSink MUST be non-blocking on the happy path: implementations
// should not retry, log only at warn level, and treat their own errors
// as observability loss (the daemon truth is still safe — failing twice
// to record an audit row is acceptable).
type FailureSink interface {
	EmitViewSyncFailed(ctx context.Context, params FailureParams) error
}

// FailureParams describes a single failed push. The Pusher fills it in
// and hands it to FailureSink; the sink translates to envelope shape.
type FailureParams struct {
	// ChannelID identifies the channel the failed envelope belonged to.
	ChannelID string
	// MessageID is the envelope id that failed to push.
	MessageID string
	// TargetURL is the full URL the Pusher tried (informational; goes
	// into payload for operator diagnosis).
	TargetURL string
	// Kind is the PushError.Kind tag (transport_error / http_status).
	Kind string
	// HTTPStatus is the server-side status; 0 for transport failures.
	HTTPStatus int
	// Detail captures up to maxErrBody bytes of body / transport error.
	Detail string
	// OccurredAt is the wall-clock ms when the failure was observed.
	OccurredAt int64
}

// HTTPPusherOptions tunes the HTTP-based Pusher. BaseURL is required;
// every other field has a sensible default.
type HTTPPusherOptions struct {
	// BaseURL is the server origin (e.g. "http://127.0.0.1:3001"). Required.
	BaseURL string

	// Path overrides the view sync ingest path. Defaults to
	// DefaultPushPath ("/api/view/sync").
	Path string

	// AuthToken is sent as `Authorization: Bearer <token>` when non-empty.
	// Daemons authenticate to the server via a long-lived machine token.
	AuthToken string

	// HTTPClient overrides the http client. Defaults to a client with a
	// 10s overall timeout — view sync is best-effort, blocking longer
	// than a few seconds defeats the "异步不阻塞" L1 §8.1 contract for
	// callers that wrap PushToServer in a goroutine.
	HTTPClient *http.Client

	// Failure receives PushToServer failures. nil → push failures still
	// surface via the returned error, but the system.event fallback row
	// is skipped (useful for tests that want to assert the raw error
	// without seeding actor_registry rows).
	Failure FailureSink

	// MaxBodyBytes caps the response body read on error. Default 1 KiB.
	// Keeps an upstream proxy that returns megabytes of HTML from
	// blowing up the log line.
	MaxBodyBytes int64

	// Clock provides wall-clock ms; defaults to time.Now().UnixMilli.
	// Tests inject a deterministic clock.
	Clock func() int64
}

// maxErrBody is the default response body read cap on error (1 KiB).
const maxErrBody int64 = 1 << 10

// defaultHTTPTimeout caps a single push attempt — view sync MUST not
// block the daemon for arbitrarily long.
const defaultHTTPTimeout = 10 * time.Second

// HTTPPusher is the default Pusher implementation — wraps an HTTP POST
// to the server's view sync ingest endpoint. On failure it routes the
// PushError through its FailureSink (if configured) before returning.
type HTTPPusher struct {
	baseURL    string
	path       string
	authToken  string
	httpClient *http.Client
	failure    FailureSink
	maxBody    int64
	clock      func() int64
}

// NewHTTPPusher constructs a Pusher backed by an HTTP client.
//
// Returns an error when BaseURL is empty — wiring a Pusher without an
// origin is always a configuration bug, so failing loudly at construct
// time beats discovering it on the first push.
func NewHTTPPusher(opts HTTPPusherOptions) (*HTTPPusher, error) {
	if strings.TrimSpace(opts.BaseURL) == "" {
		return nil, errors.New("viewsync: HTTPPusherOptions.BaseURL is required")
	}
	path := opts.Path
	if path == "" {
		path = DefaultPushPath
	}
	client := opts.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: defaultHTTPTimeout}
	}
	maxBody := opts.MaxBodyBytes
	if maxBody <= 0 {
		maxBody = maxErrBody
	}
	clock := opts.Clock
	if clock == nil {
		clock = func() int64 { return time.Now().UnixMilli() }
	}
	return &HTTPPusher{
		baseURL:    strings.TrimRight(opts.BaseURL, "/"),
		path:       path,
		authToken:  opts.AuthToken,
		httpClient: client,
		failure:    opts.Failure,
		maxBody:    maxBody,
		clock:      clock,
	}, nil
}

// PushToServer ships env to the configured server endpoint. The body is
// the raw envelope JSON; the response is decoded into PushAck on 2xx.
//
// On any failure the method routes a FailureParams through the FailureSink
// (if configured), then returns a *PushError. The local message store is
// never touched — daemon truth is preserved per L1 §8.1.2.
func (p *HTTPPusher) PushToServer(ctx context.Context, env *v4types.Envelope) (*PushAck, error) {
	if env == nil {
		return nil, errors.New("viewsync: envelope is nil")
	}
	target := p.baseURL + p.path

	buf, err := json.Marshal(env)
	if err != nil {
		perr := &PushError{Kind: "transport_error", Cause: fmt.Errorf("marshal envelope: %w", err)}
		p.emitFailure(ctx, env, target, perr)
		return nil, perr
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(buf))
	if err != nil {
		perr := &PushError{Kind: "transport_error", Cause: fmt.Errorf("build request: %w", err)}
		p.emitFailure(ctx, env, target, perr)
		return nil, perr
	}
	req.Header.Set("Content-Type", "application/json")
	if p.authToken != "" {
		req.Header.Set("Authorization", "Bearer "+p.authToken)
	}

	resp, err := p.httpClient.Do(req)
	if err != nil {
		perr := &PushError{Kind: "transport_error", Cause: fmt.Errorf("http do: %w", err)}
		p.emitFailure(ctx, env, target, perr)
		return nil, perr
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		var ack PushAck
		raw, rerr := io.ReadAll(resp.Body)
		if rerr != nil {
			perr := &PushError{Kind: "transport_error", Cause: fmt.Errorf("read body: %w", rerr)}
			p.emitFailure(ctx, env, target, perr)
			return nil, perr
		}
		// An empty body is allowed — the server may ack with bare 200.
		// In that case fill in message_id from the envelope so callers
		// always observe a non-empty MessageID on success.
		if len(bytes.TrimSpace(raw)) == 0 {
			return &PushAck{MessageID: env.ID}, nil
		}
		if uerr := json.Unmarshal(raw, &ack); uerr != nil {
			perr := &PushError{Kind: "transport_error", Cause: fmt.Errorf("decode ack: %w", uerr)}
			p.emitFailure(ctx, env, target, perr)
			return nil, perr
		}
		if ack.MessageID == "" {
			ack.MessageID = env.ID
		}
		return &ack, nil
	}

	// Non-2xx: capture a bounded slice of the body for diagnostics.
	bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, p.maxBody))
	perr := &PushError{
		Kind:       "http_status",
		HTTPStatus: resp.StatusCode,
		Body:       string(bodyBytes),
	}
	p.emitFailure(ctx, env, target, perr)
	return nil, perr
}

// emitFailure routes a PushError through the FailureSink. Sink errors
// are swallowed (logging-tier observability loss; the caller already
// receives the canonical failure via the returned PushError).
func (p *HTTPPusher) emitFailure(ctx context.Context, env *v4types.Envelope, target string, perr *PushError) {
	if p.failure == nil {
		return
	}
	detail := perr.Body
	if perr.Kind == "transport_error" && perr.Cause != nil {
		detail = perr.Cause.Error()
	}
	_ = p.failure.EmitViewSyncFailed(ctx, FailureParams{
		ChannelID:  env.ChannelID,
		MessageID:  env.ID,
		TargetURL:  target,
		Kind:       perr.Kind,
		HTTPStatus: perr.HTTPStatus,
		Detail:     detail,
		OccurredAt: p.clock(),
	})
}

// -----------------------------------------------------------------------------
// HarnessFailureSink — default sink wired to a pkg/harness.Deps bundle
// -----------------------------------------------------------------------------

// HarnessWriter is the local-binding sink — same shape as
// adapter.HarnessWriter but redeclared here so internal/viewsync does
// not depend on pkg/adapter (cyclic-import prevention). Production wiring
// uses DefaultHarnessWriter(deps); tests inject a recording stub.
type HarnessWriter interface {
	Write(ctx context.Context, env *v4types.Envelope, callerCtx pkgharness.CallerCtx) (pkgharness.WriteResult, error)
}

// HarnessWriterFunc adapts a closure to HarnessWriter.
type HarnessWriterFunc func(ctx context.Context, env *v4types.Envelope, callerCtx pkgharness.CallerCtx) (pkgharness.WriteResult, error)

// Write satisfies HarnessWriter.
func (f HarnessWriterFunc) Write(ctx context.Context, env *v4types.Envelope, cc pkgharness.CallerCtx) (pkgharness.WriteResult, error) {
	return f(ctx, env, cc)
}

// DefaultHarnessWriter returns a HarnessWriter that calls
// harness.InWorkerBus with the supplied Deps. Production callers wire
// this once per channel.
func DefaultHarnessWriter(deps pkgharness.Deps) HarnessWriter {
	return HarnessWriterFunc(func(ctx context.Context, env *v4types.Envelope, cc pkgharness.CallerCtx) (pkgharness.WriteResult, error) {
		return pkgharness.InWorkerBus(ctx, deps, env, cc)
	})
}

// HarnessFailureSink emits the L1 §8.1.1 监控建议 row
// `system.event payload.kind=view_sync_failed` through harness.InWorkerBus
// so the local channel store records the failure exactly the same way
// any other actor would emit. Multiple failures for the same envelope
// id dedupe naturally via harness Step 0.5 because the envelope id is
// deterministic ("view_sync_failed:<channel>:<message>:<kind>").
type HarnessFailureSink struct {
	// Writer is the harness binding. Required.
	Writer HarnessWriter

	// SystemActorID is the sender.id stamped on the emitted system.event.
	// Defaults to DefaultSystemActorID ("system") — the actor_registry
	// MUST have this id registered with kind=system (set up by channel
	// bootstrap saga, L2 §1.4.7).
	SystemActorID string
}

// EmitViewSyncFailed builds the system.event envelope and writes it via
// the harness. Sink errors are returned so the Pusher can log them, but
// the Pusher swallows them — observability loss is preferred over
// shadowing the canonical PushError.
func (s *HarnessFailureSink) EmitViewSyncFailed(ctx context.Context, params FailureParams) error {
	if s.Writer == nil {
		return errors.New("viewsync: HarnessFailureSink.Writer is nil")
	}
	actorID := s.SystemActorID
	if actorID == "" {
		actorID = DefaultSystemActorID
	}
	env, err := buildFailureEvent(params, actorID)
	if err != nil {
		return fmt.Errorf("viewsync: build failure event: %w", err)
	}
	res, err := s.Writer.Write(ctx, env, pkgharness.CallerCtx{
		Authenticated: true,
		ActorID:       actorID,
	})
	if err != nil {
		return fmt.Errorf("viewsync: emit view_sync_failed: %w", err)
	}
	if res.IsReject() {
		// Harness rejected — record the reason in the returned error so
		// operators can see it in logs. We do NOT retry: the sink is
		// best-effort, and a reject typically means the channel is in
		// a degraded state (e.g. system actor deregistered) which
		// retries cannot fix.
		return fmt.Errorf("viewsync: emit view_sync_failed rejected reason=%s detail=%s",
			res.Error.Reason, res.Error.Detail)
	}
	return nil
}

// buildFailureEvent constructs the deterministic system.event envelope
// the L1 §8.1.1 monitoring contract calls for. The envelope id is
// derived from (channel, message, kind) so re-emitting the same failure
// dedupes at harness Step 0.5 — operators see one row per failed push.
func buildFailureEvent(params FailureParams, actorID string) (*v4types.Envelope, error) {
	if params.OccurredAt == 0 {
		params.OccurredAt = time.Now().UnixMilli()
	}
	payload := map[string]any{
		"kind":        "view_sync_failed",
		"severity":    "warn",
		"channel_id":  params.ChannelID,
		"message_id":  params.MessageID,
		"target_url":  params.TargetURL,
		"failure":     params.Kind,
		"http_status": params.HTTPStatus,
		"detail":      params.Detail,
		"emitted_at":  params.OccurredAt,
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal payload: %w", err)
	}
	// Deterministic id keeps duplicate emits idempotent via harness
	// Step 0.5; include the failure kind so transport_error vs
	// http_status fan out (operator can tell a flaky network apart
	// from a real 4xx).
	envID := "view_sync_failed:" + params.ChannelID + ":" + params.MessageID + ":" + params.Kind
	return &v4types.Envelope{
		ID:         envID,
		TS:         params.OccurredAt,
		ChannelID:  params.ChannelID,
		Sender:     v4types.Sender{Kind: v4types.SenderSystem, ID: actorID},
		Kind:       v4types.KindEvent,
		Type:       "system.event",
		Payload:    raw,
		Visibility: v4types.VisibilitySystem,
		Audience:   []string{"*"},
	}, nil
}
