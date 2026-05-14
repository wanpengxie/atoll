package viewsync

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/coagent-ai/daemon-go/pkg/v4types"
)

// ResyncRPCPath is the canonical HTTP path the server hits to ask the
// daemon for a channel resync. server-to-daemon RPC per L1 §8.1.3.
const ResyncRPCPath = "/api/rpc/view.resync_channel"

// DefaultResyncLimit caps the number of envelopes returned per call.
// Keeps a server warm-start from streaming millions of rows in one
// response. Server callers iterate (since_seq = last returned seq)
// until HasMore == false to drain the channel.
const DefaultResyncLimit = 500

// MaxResyncLimit hard-caps the per-call envelope count regardless of
// what the caller asks for. Prevents an upstream from overriding
// DefaultResyncLimit and exhausting daemon memory.
const MaxResyncLimit = 5000

// ResyncRequest is the wire body for POST ResyncRPCPath. SinceSeq=0 (or
// omitted) means "from the start" — server caller uses this for cold
// start / full backfill (L1 §8.1.3 触发 "server cold start" 行).
type ResyncRequest struct {
	ChannelID string `json:"channel_id"`
	SinceSeq  int64  `json:"since_seq,omitempty"`
	Limit     int    `json:"limit,omitempty"`
}

// ResyncResponse is the wire body for HTTP 200. Envelopes are ordered
// by messages.seq ASC; HasMore indicates whether the daemon has further
// rows beyond NextSeq (i.e. caller should issue another request with
// since_seq=NextSeq).
type ResyncResponse struct {
	Envelopes []v4types.Envelope `json:"envelopes"`
	NextSeq   int64              `json:"next_seq"`
	HasMore   bool               `json:"has_more"`
}

// ResyncErrorBody is the wire body for non-2xx HTTP responses. Mirrors
// the harness reject shape so server callers can decode uniformly.
type ResyncErrorBody struct {
	Error ResyncErrorPayload `json:"error"`
}

// ResyncErrorPayload is the structured error body. Reason values are
// resync-specific (not in the L1 §10.3 closed sets) — they describe
// transport / lookup failures the harness reject set does not cover.
type ResyncErrorPayload struct {
	Reason string `json:"reason"`
	Detail string `json:"detail,omitempty"`
}

// Resync error reasons (closed set, daemon-side).
const (
	ResyncReasonAuthFailed    = "auth_failed"
	ResyncReasonBadRequest    = "bad_request"
	ResyncReasonChannelMissing = "channel_missing"
	ResyncReasonInternal      = "internal_error"
)

// ResyncStore is the channel-local read interface the resync handler
// needs. The single method returns envelopes ordered by seq ASC where
// seq > sinceSeq, capped at limit. Returning a nil slice on empty
// result is allowed; lastSeq is the largest seq in the returned slice
// (0 when slice is empty).
type ResyncStore interface {
	ListSince(ctx context.Context, sinceSeq int64, limit int) (envelopes []v4types.Envelope, lastSeq int64, hasMore bool, err error)
}

// ResyncStoreResolver maps a channel id to its ResyncStore. The daemon
// hosts many channels concurrently; the resolver picks the right
// store given a request body's `channel_id`.
//
// Returns (nil, false, nil) when the channel id is unknown — the
// handler then replies 404 channel_missing. A non-nil error means a
// real infrastructure failure → 500 internal_error.
type ResyncStoreResolver interface {
	ForChannel(ctx context.Context, channelID string) (ResyncStore, bool, error)
}

// ResyncStoreResolverFunc adapts a closure to ResyncStoreResolver.
type ResyncStoreResolverFunc func(ctx context.Context, channelID string) (ResyncStore, bool, error)

// ForChannel satisfies ResyncStoreResolver.
func (f ResyncStoreResolverFunc) ForChannel(ctx context.Context, channelID string) (ResyncStore, bool, error) {
	return f(ctx, channelID)
}

// StaticResolver is a tiny resolver matching exactly one channel id.
// Use it when the daemon hosts a single channel (tests, single-tenant).
func StaticResolver(channelID string, store ResyncStore) ResyncStoreResolver {
	return ResyncStoreResolverFunc(func(_ context.Context, id string) (ResyncStore, bool, error) {
		if id != channelID {
			return nil, false, nil
		}
		return store, true, nil
	})
}

// ResyncAuthFunc verifies the bearer token attached to the incoming
// resync request and returns nil on success (any non-nil error → 401
// auth_failed). The handler runs auth BEFORE looking up the store so
// an unauthorized caller cannot fingerprint channel ids.
type ResyncAuthFunc func(ctx context.Context, token string, req *ResyncRequest) error

// ResyncHandlerOptions wires the resync HTTP handler. Resolver + Auth
// are required.
type ResyncHandlerOptions struct {
	// Resolver maps channel ids to per-channel stores.
	Resolver ResyncStoreResolver

	// Auth verifies the bearer token. Must be non-nil.
	Auth ResyncAuthFunc

	// MaxBodyBytes caps the request body. Default 64 KiB — request body
	// only carries {channel_id, since_seq, limit}, so this is generous.
	MaxBodyBytes int64

	// DefaultLimit overrides DefaultResyncLimit when set.
	DefaultLimit int
}

const defaultResyncMaxBody = 64 << 10

// NewResyncHandler returns the daemon-side http.Handler implementing
// POST ResyncRPCPath. Caller registers it on a mux:
//
//	mux.Handle(viewsync.ResyncRPCPath, viewsync.NewResyncHandler(opts))
func NewResyncHandler(opts ResyncHandlerOptions) http.Handler {
	if opts.MaxBodyBytes <= 0 {
		opts.MaxBodyBytes = defaultResyncMaxBody
	}
	if opts.DefaultLimit <= 0 {
		opts.DefaultLimit = DefaultResyncLimit
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serveResync(w, r, opts)
	})
}

func serveResync(w http.ResponseWriter, r *http.Request, opts ResyncHandlerOptions) {
	if r.Method != http.MethodPost {
		writeResyncError(w, http.StatusMethodNotAllowed, "method_not_allowed", "only POST is accepted")
		return
	}
	if opts.Resolver == nil || opts.Auth == nil {
		writeResyncError(w, http.StatusInternalServerError, ResyncReasonInternal, "resync handler not wired")
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, opts.MaxBodyBytes+1))
	if err != nil {
		writeResyncError(w, http.StatusBadRequest, ResyncReasonBadRequest, "read body: "+err.Error())
		return
	}
	if int64(len(body)) > opts.MaxBodyBytes {
		writeResyncError(w, http.StatusRequestEntityTooLarge, ResyncReasonBadRequest, fmt.Sprintf("body exceeds %d bytes", opts.MaxBodyBytes))
		return
	}
	var req ResyncRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeResyncError(w, http.StatusBadRequest, ResyncReasonBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if strings.TrimSpace(req.ChannelID) == "" {
		writeResyncError(w, http.StatusBadRequest, ResyncReasonBadRequest, "channel_id is required")
		return
	}
	if req.SinceSeq < 0 {
		writeResyncError(w, http.StatusBadRequest, ResyncReasonBadRequest, "since_seq must be >= 0")
		return
	}
	limit := req.Limit
	if limit <= 0 {
		limit = opts.DefaultLimit
	}
	if limit > MaxResyncLimit {
		limit = MaxResyncLimit
	}

	token, terr := extractBearer(r.Header.Get("Authorization"))
	if terr != nil {
		writeResyncError(w, http.StatusUnauthorized, ResyncReasonAuthFailed, terr.Error())
		return
	}
	if aerr := opts.Auth(r.Context(), token, &req); aerr != nil {
		writeResyncError(w, http.StatusUnauthorized, ResyncReasonAuthFailed, aerr.Error())
		return
	}

	store, ok, err := opts.Resolver.ForChannel(r.Context(), req.ChannelID)
	if err != nil {
		writeResyncError(w, http.StatusInternalServerError, ResyncReasonInternal, "resolve channel: "+err.Error())
		return
	}
	if !ok || store == nil {
		writeResyncError(w, http.StatusNotFound, ResyncReasonChannelMissing, "channel not hosted by this daemon")
		return
	}

	envelopes, lastSeq, hasMore, lerr := store.ListSince(r.Context(), req.SinceSeq, limit)
	if lerr != nil {
		writeResyncError(w, http.StatusInternalServerError, ResyncReasonInternal, "list since: "+lerr.Error())
		return
	}
	if envelopes == nil {
		envelopes = []v4types.Envelope{}
	}
	resp := ResyncResponse{
		Envelopes: envelopes,
		NextSeq:   lastSeq,
		HasMore:   hasMore,
	}
	writeResyncJSON(w, http.StatusOK, resp)
}

// extractBearer mirrors internal/harness.extractBearer — kept local so
// internal/viewsync does not depend on internal/harness (cyclic-import
// avoidance; the harness package may import viewsync in M1.x).
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

func writeResyncError(w http.ResponseWriter, status int, reason, detail string) {
	writeResyncJSON(w, status, ResyncErrorBody{Error: ResyncErrorPayload{Reason: reason, Detail: detail}})
}

func writeResyncJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if body == nil {
		return
	}
	_ = json.NewEncoder(w).Encode(body)
}

// -----------------------------------------------------------------------------
// SQLiteResyncStore — channel-local sqlite reader
// -----------------------------------------------------------------------------

// SQLiteResyncStore is the production ResyncStore implementation. It
// reads the channel-local `messages` table ordered by `seq ASC`. The
// store is read-only — no INSERT / UPDATE / DELETE.
type SQLiteResyncStore struct {
	db *sql.DB
}

// NewSQLiteResyncStore wraps a channel-local *sql.DB (built via
// internal/store.OpenChannel) as a ResyncStore.
func NewSQLiteResyncStore(db *sql.DB) *SQLiteResyncStore {
	return &SQLiteResyncStore{db: db}
}

// ListSince queries up to `limit+1` rows with seq > sinceSeq ordered by
// seq ASC. The extra row probes whether more exist beyond `limit` so
// HasMore round-trips honestly. Returns (envelopes, lastSeq, hasMore,
// err) — `lastSeq` is the seq of the last envelope in the returned
// slice (or sinceSeq when empty so the caller can keep iterating).
func (s *SQLiteResyncStore) ListSince(ctx context.Context, sinceSeq int64, limit int) ([]v4types.Envelope, int64, bool, error) {
	if limit <= 0 {
		limit = DefaultResyncLimit
	}
	if limit > MaxResyncLimit {
		limit = MaxResyncLimit
	}
	probe := limit + 1
	rows, err := s.db.QueryContext(ctx,
		`SELECT seq, id, ts, ts_received, channel_id, sender_kind, sender_id, sender_name,
		        kind, type, payload, parent_id, correlation_id, doc_refs,
		        visibility, audience, not_before, expires_at,
		        delivered_at, delivery_failed_at, last_error, attempts, is_terminal
		   FROM messages
		  WHERE seq > ?
		  ORDER BY seq ASC
		  LIMIT ?`,
		sinceSeq, probe,
	)
	if err != nil {
		return nil, sinceSeq, false, fmt.Errorf("sqlite_resync_store: query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	envelopes := make([]v4types.Envelope, 0, limit)
	var lastSeq = sinceSeq
	for rows.Next() {
		env, seq, scanErr := scanResyncRow(rows)
		if scanErr != nil {
			return nil, sinceSeq, false, scanErr
		}
		envelopes = append(envelopes, env)
		lastSeq = seq
	}
	if err := rows.Err(); err != nil {
		return nil, sinceSeq, false, fmt.Errorf("sqlite_resync_store: rows.Err: %w", err)
	}

	hasMore := false
	if len(envelopes) > limit {
		// We over-fetched by 1 to detect HasMore. Drop the probe row from
		// the returned slice (caller should pick up where lastSeq stops
		// being the trimmed-to-limit last element).
		envelopes = envelopes[:limit]
		lastSeq = envelopes[len(envelopes)-1].Seq
		hasMore = true
	}
	return envelopes, lastSeq, hasMore, nil
}

// scanResyncRow scans one row using the same column layout as
// internal/harness/sqlite_store.go's findOne, plus the leading seq
// column. Kept local to viewsync because internal/harness's scanner
// is unexported and we deliberately do not want a dependency edge.
func scanResyncRow(rows *sql.Rows) (v4types.Envelope, int64, error) {
	var (
		env            v4types.Envelope
		seq            int64
		senderName     sql.NullString
		parentID       sql.NullString
		correlationID  sql.NullString
		docRefs        sql.NullString
		notBefore      sql.NullInt64
		expiresAt      sql.NullInt64
		deliveredAt    sql.NullInt64
		deliveryFailed sql.NullInt64
		lastErr        sql.NullString
		audienceRaw    string
		isTerminalInt  int64
		payloadRaw     string
		senderKindStr  string
		kindStr        string
		visibilityStr  string
	)
	if err := rows.Scan(
		&seq, &env.ID, &env.TS, &env.TSReceived, &env.ChannelID,
		&senderKindStr, &env.Sender.ID, &senderName,
		&kindStr, &env.Type, &payloadRaw,
		&parentID, &correlationID, &docRefs,
		&visibilityStr, &audienceRaw, &notBefore, &expiresAt,
		&deliveredAt, &deliveryFailed, &lastErr, &env.Attempts, &isTerminalInt,
	); err != nil {
		return env, 0, fmt.Errorf("sqlite_resync_store: scan: %w", err)
	}
	env.Seq = seq
	env.Sender.Kind = v4types.SenderKind(senderKindStr)
	if senderName.Valid {
		env.Sender.Name = senderName.String
	}
	env.Kind = v4types.Kind(kindStr)
	env.Visibility = v4types.Visibility(visibilityStr)
	env.Payload = json.RawMessage(payloadRaw)
	if parentID.Valid {
		env.ParentID = parentID.String
	}
	if correlationID.Valid {
		env.CorrelationID = correlationID.String
	}
	if docRefs.Valid {
		var refs []string
		if err := json.Unmarshal([]byte(docRefs.String), &refs); err != nil {
			return env, 0, fmt.Errorf("sqlite_resync_store: parse doc_refs: %w", err)
		}
		env.DocRefs = &refs
	}
	if err := json.Unmarshal([]byte(audienceRaw), &env.Audience); err != nil {
		return env, 0, fmt.Errorf("sqlite_resync_store: parse audience: %w", err)
	}
	if notBefore.Valid {
		v := notBefore.Int64
		env.NotBefore = &v
	}
	if expiresAt.Valid {
		v := expiresAt.Int64
		env.ExpiresAt = &v
	}
	if deliveredAt.Valid {
		v := deliveredAt.Int64
		env.DeliveredAt = &v
	}
	if deliveryFailed.Valid {
		v := deliveryFailed.Int64
		env.DeliveryFailedAt = &v
	}
	if lastErr.Valid {
		env.LastError = lastErr.String
	}
	env.IsTerminal = isTerminalInt == 1
	return env, seq, nil
}

// -----------------------------------------------------------------------------
// ResyncClient — server-side HTTP caller
// -----------------------------------------------------------------------------

// ResyncClientOptions tunes the server-side HTTP client. BaseURL is
// required; every other field has a sensible default.
type ResyncClientOptions struct {
	// BaseURL is the daemon origin (e.g. "http://127.0.0.1:38117"). Required.
	BaseURL string

	// Path overrides ResyncRPCPath. Tests + routing experiments only.
	Path string

	// AuthToken is sent as `Authorization: Bearer <token>`.
	AuthToken string

	// HTTPClient overrides the http client. Default = http.DefaultClient.
	HTTPClient *http.Client
}

// ResyncClient calls the daemon's resync RPC and decodes the response
// into ResyncResponse. Used by the server-side cache when it needs to
// rebuild from daemon truth (L1 §8.1.3 触发场景).
type ResyncClient struct {
	baseURL   string
	path      string
	authToken string
	client    *http.Client
}

// NewResyncClient builds a server-side caller.
func NewResyncClient(opts ResyncClientOptions) (*ResyncClient, error) {
	if strings.TrimSpace(opts.BaseURL) == "" {
		return nil, errors.New("viewsync: ResyncClientOptions.BaseURL is required")
	}
	path := opts.Path
	if path == "" {
		path = ResyncRPCPath
	}
	client := opts.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	return &ResyncClient{
		baseURL:   strings.TrimRight(opts.BaseURL, "/"),
		path:      path,
		authToken: opts.AuthToken,
		client:    client,
	}, nil
}

// Resync issues one POST ResyncRPCPath call and returns the decoded
// response. Server callers iterate (since_seq=NextSeq) until HasMore=false.
//
// Returns a ResyncError when the daemon replied with a structured error
// body; a plain error for transport / decode failures.
func (c *ResyncClient) Resync(ctx context.Context, req ResyncRequest) (*ResyncResponse, error) {
	if strings.TrimSpace(req.ChannelID) == "" {
		return nil, errors.New("viewsync: ResyncRequest.ChannelID is required")
	}
	buf, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("viewsync: marshal request: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+c.path, strings.NewReader(string(buf)))
	if err != nil {
		return nil, fmt.Errorf("viewsync: build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if c.authToken != "" {
		httpReq.Header.Set("Authorization", "Bearer "+c.authToken)
	}
	resp, err := c.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("viewsync: http do: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, rerr := io.ReadAll(resp.Body)
	if rerr != nil {
		return nil, fmt.Errorf("viewsync: read body: %w", rerr)
	}
	if resp.StatusCode == http.StatusOK {
		var out ResyncResponse
		if err := json.Unmarshal(raw, &out); err != nil {
			return nil, fmt.Errorf("viewsync: decode response: %w", err)
		}
		return &out, nil
	}
	var errBody ResyncErrorBody
	if uerr := json.Unmarshal(raw, &errBody); uerr == nil && errBody.Error.Reason != "" {
		return nil, &ResyncError{
			HTTPStatus: resp.StatusCode,
			Reason:     errBody.Error.Reason,
			Detail:     errBody.Error.Detail,
		}
	}
	return nil, fmt.Errorf("viewsync: unexpected status %d: %s", resp.StatusCode, string(raw))
}

// ResyncAll is a convenience wrapper that drains the channel by
// calling Resync in a loop until HasMore=false. Returns the
// concatenated envelopes. Used by server cold-start path where the
// caller wants the full backfill in one go.
//
// IMPORTANT: server callers handling large channels SHOULD prefer
// Resync + their own checkpointing — ResyncAll loads everything into
// memory.
func (c *ResyncClient) ResyncAll(ctx context.Context, channelID string, perCall int) ([]v4types.Envelope, error) {
	if strings.TrimSpace(channelID) == "" {
		return nil, errors.New("viewsync: channelID is required")
	}
	var all []v4types.Envelope
	since := int64(0)
	for {
		out, err := c.Resync(ctx, ResyncRequest{
			ChannelID: channelID,
			SinceSeq:  since,
			Limit:     perCall,
		})
		if err != nil {
			return all, err
		}
		all = append(all, out.Envelopes...)
		if !out.HasMore {
			return all, nil
		}
		// Defensive: if NextSeq did not advance the loop would spin.
		// Treat that as an upstream bug and break with what we have.
		if out.NextSeq <= since {
			return all, fmt.Errorf("viewsync: NextSeq did not advance (since=%d next=%d)", since, out.NextSeq)
		}
		since = out.NextSeq
	}
}

// ResyncError is the structured error returned by ResyncClient when the
// daemon replied with a non-2xx + ResyncErrorBody body.
type ResyncError struct {
	HTTPStatus int
	Reason     string
	Detail     string
}

// Error implements the error interface.
func (e *ResyncError) Error() string {
	return fmt.Sprintf("viewsync: resync rejected status=%d reason=%s detail=%s", e.HTTPStatus, e.Reason, e.Detail)
}
