package harness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	pkgharness "github.com/coagent-ai/daemon-go/pkg/harness"
	"github.com/coagent-ai/daemon-go/pkg/v4types"
)

// RPCPath is the canonical HTTP path for the message-send endpoint per
// L1 §10.2 (M1.3 binding entry point). The handler also accepts a
// trailing slash variant — the mux registration adds both.
const RPCPath = "/api/rpc/message.send"

// AuthFunc verifies the bearer token attached to an incoming HTTP request
// and returns the corresponding CallerCtx (Authenticated=true on success).
// On token failure the binding sends HTTP 401 with `{error:
// {reason:"auth_failed", detail:"..."}}` without invoking the harness.
//
// Implementations are channel-specific (the daemon's bootstrap saga
// owns the token store); tests inject a stub that accepts a constant
// "alice"-style token.
type AuthFunc func(ctx context.Context, token string, body *MessageSendRequest) (pkgharness.CallerCtx, error)

// MessageSendRequest is the wire shape of the daemon_rpc body. The
// envelope sits under the `params` key per L2 §3 RPC convention
// (consistent with `params: envelope` in L2 §3.4 sample). Extra trigger
// fields live alongside so the binding can build the CallerCtx without
// stuffing them inside `params`.
type MessageSendRequest struct {
	Params                v4types.Envelope   `json:"params"`
	DeclaredSenderKind    v4types.SenderKind `json:"declared_sender_kind,omitempty"`
	FencingToken          int64              `json:"fencing_token,omitempty"`
	TriggerCorrelationID  string             `json:"trigger_correlation_id,omitempty"`
	ExplicitCorrelationID string             `json:"explicit_correlation_id,omitempty"`
}

// MessageSendSuccess is the HTTP 200 body shape per L2 §3.6.1 ("成功
// response: HTTP 200 + {id, correlation_id, kind}").
type MessageSendSuccess struct {
	ID            string       `json:"id"`
	CorrelationID string       `json:"correlation_id"`
	Kind          v4types.Kind `json:"kind"`
	// Dedupe is informational — tells the caller the success came from
	// the idempotent dedupe path rather than a fresh insert. Not in the
	// spec body but harmless extra field per L2 §3.6 "structured body".
	Dedupe bool `json:"dedupe,omitempty"`
}

// MessageSendError is the HTTP 4xx body shape per L2 §3.6.1.
//
//	{error: {reason, detail, message_id_if_partial?, dedupe_response_id?}}
type MessageSendError struct {
	Error MessageSendErrorBody `json:"error"`
}

// MessageSendErrorBody mirrors the L2 §3.6.1 `error.*` object.
type MessageSendErrorBody struct {
	Reason             v4types.HarnessRejectReason `json:"reason"`
	Detail             string                      `json:"detail,omitempty"`
	MessageIDIfPartial string                      `json:"message_id_if_partial,omitempty"`
	DedupeResponseID   string                      `json:"dedupe_response_id,omitempty"`
}

// HTTPHandlerOptions tunes the message-send HTTP handler. Required:
// Deps + Auth.
type HTTPHandlerOptions struct {
	// Deps is the harness dependency bundle wired to channel sqlite.
	Deps pkgharness.Deps

	// Auth verifies the bearer token and returns CallerCtx. The handler
	// rejects HTTP 401 when Auth returns an error or
	// Authenticated=false.
	Auth AuthFunc

	// MaxBodyBytes optionally caps request body size (default 1 MiB).
	// Bindings can override per channel.
	MaxBodyBytes int64
}

// defaultMaxBody is the default request body cap (1 MiB) — keeps a
// runaway client from exhausting memory.
const defaultMaxBody = 1 << 20

// NewHTTPHandler returns an http.Handler implementing POST RPCPath.
// Every non-POST or wrong-path request is rejected by the caller's
// mux setup — the handler itself only validates method.
func NewHTTPHandler(opts HTTPHandlerOptions) http.Handler {
	if opts.MaxBodyBytes == 0 {
		opts.MaxBodyBytes = defaultMaxBody
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serveMessageSend(w, r, opts)
	})
}

func serveMessageSend(w http.ResponseWriter, r *http.Request, opts HTTPHandlerOptions) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, MessageSendError{
			Error: MessageSendErrorBody{Reason: "method_not_allowed", Detail: "only POST is accepted"},
		})
		return
	}
	if opts.Deps.Store == nil || opts.Auth == nil {
		writeError(w, http.StatusInternalServerError, MessageSendError{
			Error: MessageSendErrorBody{Reason: "server_misconfigured", Detail: "harness deps or auth not wired"},
		})
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, opts.MaxBodyBytes+1))
	if err != nil {
		writeError(w, http.StatusBadRequest, MessageSendError{
			Error: MessageSendErrorBody{Reason: v4types.HarnessMissingRequiredField, Detail: fmt.Sprintf("read body: %v", err)},
		})
		return
	}
	if int64(len(body)) > opts.MaxBodyBytes {
		writeError(w, http.StatusRequestEntityTooLarge, MessageSendError{
			Error: MessageSendErrorBody{Reason: v4types.HarnessMissingRequiredField, Detail: fmt.Sprintf("body exceeds %d bytes", opts.MaxBodyBytes)},
		})
		return
	}

	var req MessageSendRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeError(w, http.StatusBadRequest, MessageSendError{
			Error: MessageSendErrorBody{Reason: v4types.HarnessMissingRequiredField, Detail: fmt.Sprintf("invalid JSON body: %v", err)},
		})
		return
	}

	// Auth: pull the bearer token + delegate to the channel-specific
	// AuthFunc. Empty / malformed Authorization header → 401 with
	// auth_failed before any harness invocation.
	token, terr := extractBearer(r.Header.Get("Authorization"))
	if terr != nil {
		writeError(w, http.StatusUnauthorized, MessageSendError{
			Error: MessageSendErrorBody{Reason: v4types.HarnessAuthFailed, Detail: terr.Error()},
		})
		return
	}
	callerCtx, aerr := opts.Auth(r.Context(), token, &req)
	if aerr != nil || !callerCtx.Authenticated {
		detail := "token invalid"
		if aerr != nil {
			detail = aerr.Error()
		}
		writeError(w, http.StatusUnauthorized, MessageSendError{
			Error: MessageSendErrorBody{Reason: v4types.HarnessAuthFailed, Detail: detail},
		})
		return
	}

	// Bind optional fields from the request body into the callerCtx so
	// the shared Write body sees one authoritative source. We do NOT
	// trust the body for ActorID / Authenticated — those come from the
	// AuthFunc.
	if callerCtx.DeclaredSenderKind == "" {
		callerCtx.DeclaredSenderKind = req.DeclaredSenderKind
	}
	if callerCtx.FencingToken == 0 {
		callerCtx.FencingToken = req.FencingToken
	}
	if callerCtx.Trigger == nil && req.TriggerCorrelationID != "" {
		callerCtx.Trigger = &pkgharness.TriggerCtx{CorrelationID: req.TriggerCorrelationID}
	}
	if callerCtx.ExplicitCorrelationID == "" {
		callerCtx.ExplicitCorrelationID = req.ExplicitCorrelationID
	}

	env := req.Params
	result, werr := pkgharness.Write(r.Context(), opts.Deps, &env, callerCtx)
	if werr != nil {
		var rerr *pkgharness.RejectError
		if errors.As(werr, &rerr) {
			writeReject(w, rerr)
			return
		}
		// Real infrastructure error (sql / driver / ctx).
		writeError(w, http.StatusInternalServerError, MessageSendError{
			Error: MessageSendErrorBody{Reason: "internal_error", Detail: werr.Error()},
		})
		return
	}

	writeJSON(w, http.StatusOK, MessageSendSuccess{
		ID:            result.ID,
		CorrelationID: result.CorrelationID,
		Kind:          result.Kind,
		Dedupe:        result.Dedupe,
	})
}

// writeReject maps a RejectError to its L2 §3.6.1 HTTP status + body.
// The reason → status mapping lives in v4types.HarnessRejectReason.HTTPStatus().
func writeReject(w http.ResponseWriter, rerr *pkgharness.RejectError) {
	status := rerr.Reason.HTTPStatus()
	if status == 0 {
		// Defensive: unknown reason → 400 with the reason text intact.
		status = http.StatusBadRequest
	}
	writeError(w, status, MessageSendError{
		Error: MessageSendErrorBody{
			Reason:             rerr.Reason,
			Detail:             rerr.Detail,
			MessageIDIfPartial: rerr.MessageIDIfPartial,
			DedupeResponseID:   rerr.DedupeResponseID,
		},
	})
}

func writeError(w http.ResponseWriter, status int, body any) {
	writeJSON(w, status, body)
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if body == nil {
		return
	}
	_ = json.NewEncoder(w).Encode(body)
}

// extractBearer parses an `Authorization: Bearer <token>` header.
// Returns the token (non-empty) on success, or an error explaining
// what's wrong.
func extractBearer(header string) (string, error) {
	if header == "" {
		return "", errors.New("missing Authorization header")
	}
	const prefix = "Bearer "
	if !strings.HasPrefix(header, prefix) {
		return "", errors.New("authorization header is not a Bearer token")
	}
	tok := strings.TrimSpace(strings.TrimPrefix(header, prefix))
	if tok == "" {
		return "", errors.New("empty bearer token")
	}
	return tok, nil
}
