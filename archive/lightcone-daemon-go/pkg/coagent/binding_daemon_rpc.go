package coagent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/coagent-ai/daemon-go/pkg/v4types"
)

// daemonRPCBinding is the HTTP client for daemon_rpc (L2 §3.6.1).
// It posts MessageSendRequest-equivalent bodies to BaseURL + Path
// and translates 200 / 4xx into SendResult / *RejectError.
//
// Both the binding and the daemon-side handler share the same wire
// schema (defined here as messageSendRequest / messageSendSuccess /
// messageSendError) — we intentionally re-declare the wire types
// locally so the public package does not depend on internal/harness.
type daemonRPCBinding struct {
	baseURL    string
	rpcPath    string
	authToken  string
	httpClient *http.Client
}

// DaemonRPCOptions tunes the HTTP binding.
type DaemonRPCOptions struct {
	// BaseURL is the daemon RPC host (e.g. "http://127.0.0.1:38117"). Required.
	BaseURL string

	// RPCPath overrides the message.send path. Defaults to
	// "/api/rpc/message.send" matching internal/harness.RPCPath. Tests
	// can override for routing experiments.
	RPCPath string

	// AuthToken is sent as `Authorization: Bearer <token>`. Empty
	// token → daemon AuthFunc returns auth_failed and the binding
	// surfaces a *RejectError{Reason: auth_failed}.
	AuthToken string

	// HTTPClient overrides the http client. Defaults to http.DefaultClient
	// (tests pass httptest.NewServer().Client() so TLS cert pinning is
	// not in the way).
	HTTPClient *http.Client
}

// NewDaemonRPCBinding constructs the HTTP binding. Returns a Binding
// interface so callers can swap implementations without recompiling.
//
// Authoritative wire shape per L2 §3.6.1:
//
//   - POST $BaseURL$RPCPath
//   - Authorization: Bearer <AuthToken>
//   - Content-Type: application/json
//   - body: { params: <envelope>, declared_sender_kind?, fencing_token?,
//     trigger_correlation_id?, explicit_correlation_id? }
//   - response 200: { id, correlation_id, kind, dedupe? }
//   - response 4xx: { error: { reason, detail, message_id_if_partial?,
//     dedupe_response_id? } }
func NewDaemonRPCBinding(opts DaemonRPCOptions) Binding {
	if opts.RPCPath == "" {
		opts.RPCPath = "/api/rpc/message.send"
	}
	client := opts.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	return &daemonRPCBinding{
		baseURL:    strings.TrimRight(opts.BaseURL, "/"),
		rpcPath:    opts.RPCPath,
		authToken:  opts.AuthToken,
		httpClient: client,
	}
}

// messageSendRequest mirrors internal/harness.MessageSendRequest. We
// re-declare it here so the public package does not import internal/*.
type messageSendRequest struct {
	Params                v4types.Envelope   `json:"params"`
	DeclaredSenderKind    v4types.SenderKind `json:"declared_sender_kind,omitempty"`
	FencingToken          int64              `json:"fencing_token,omitempty"`
	TriggerCorrelationID  string             `json:"trigger_correlation_id,omitempty"`
	ExplicitCorrelationID string             `json:"explicit_correlation_id,omitempty"`
}

type messageSendSuccess struct {
	ID            string       `json:"id"`
	CorrelationID string       `json:"correlation_id"`
	Kind          v4types.Kind `json:"kind"`
	Dedupe        bool         `json:"dedupe,omitempty"`
}

type messageSendError struct {
	Error messageSendErrorBody `json:"error"`
}

type messageSendErrorBody struct {
	Reason             v4types.HarnessRejectReason `json:"reason"`
	Detail             string                      `json:"detail,omitempty"`
	MessageIDIfPartial string                      `json:"message_id_if_partial,omitempty"`
	DedupeResponseID   string                      `json:"dedupe_response_id,omitempty"`
}

// Send posts the envelope and decodes the response into the binding
// contract. Returns *RejectError on harness-mapped 4xx; a plain
// error on transport / decode failures.
func (b *daemonRPCBinding) Send(ctx context.Context, env *v4types.Envelope, opts SendOptions) (*SendResult, error) {
	body := messageSendRequest{
		Params:                *env,
		DeclaredSenderKind:    opts.DeclaredSenderKind,
		FencingToken:          opts.FencingToken,
		TriggerCorrelationID:  opts.TriggerCorrelationID,
		ExplicitCorrelationID: opts.ExplicitCorrelationID,
	}
	buf, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("daemon_rpc: marshal body: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, b.baseURL+b.rpcPath, bytes.NewReader(buf))
	if err != nil {
		return nil, fmt.Errorf("daemon_rpc: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if b.authToken != "" {
		req.Header.Set("Authorization", "Bearer "+b.authToken)
	}

	resp, err := b.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("daemon_rpc: http do: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	raw, rerr := io.ReadAll(resp.Body)
	if rerr != nil {
		return nil, fmt.Errorf("daemon_rpc: read body: %w", rerr)
	}

	if resp.StatusCode == http.StatusOK {
		var success messageSendSuccess
		if err := json.Unmarshal(raw, &success); err != nil {
			return nil, fmt.Errorf("daemon_rpc: decode success body (status=%d): %w", resp.StatusCode, err)
		}
		return &SendResult{
			ID:            success.ID,
			CorrelationID: success.CorrelationID,
			Kind:          success.Kind,
			Dedupe:        success.Dedupe,
		}, nil
	}

	// Non-2xx → expect MessageSendError shape. Some 5xx infra errors
	// may not match the shape — surface them as plain error so the
	// CLI exit code is exitInfra (not exitReject).
	var e messageSendError
	if uerr := json.Unmarshal(raw, &e); uerr == nil && e.Error.Reason != "" {
		return nil, &RejectError{
			Reason:             e.Error.Reason,
			Detail:             e.Error.Detail,
			MessageIDIfPartial: e.Error.MessageIDIfPartial,
			DedupeResponseID:   e.Error.DedupeResponseID,
			HTTPStatus:         resp.StatusCode,
		}
	}
	return nil, fmt.Errorf("daemon_rpc: unexpected status %d: %s", resp.StatusCode, string(raw))
}

// LookupRequest is unsupported by the HTTP binding — the daemon does
// not expose a read-side RPC in M1.3 baseline (channel sqlite reads
// happen out-of-band). Returns (nil, false, nil) so the CLI falls
// back to caller-supplied --type / --audience.
func (b *daemonRPCBinding) LookupRequest(_ context.Context, _ string) (*v4types.Envelope, bool, error) {
	return nil, false, nil
}

// ResolveHandlerActorID is unsupported by the HTTP binding for the
// same reason — type_registry rows live in the daemon's channel
// sqlite and there is no read-side RPC in M1.3 baseline. The CLI
// surfaces request_audience_invalid with a hint to pass --audience.
func (b *daemonRPCBinding) ResolveHandlerActorID(_ context.Context, _ string) (string, bool, error) {
	return "", false, nil
}
