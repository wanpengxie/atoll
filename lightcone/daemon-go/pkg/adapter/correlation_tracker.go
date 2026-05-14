package adapter

// correlation_tracker.go implements L2 §8.2 — the request-scoped
// pending tracker the framework hands every adapter via ctx.Correlation.
//
// Persistence model:
//
//   - Channel-local sqlite table `adapter_correlation` (DDL below). The
//     framework runs `CREATE TABLE IF NOT EXISTS` on Install so the
//     table appears the first time an adapter Manager touches a
//     channel.
//   - In-memory cache (map[external_id]) is populated on Track and
//     lazily on Recover. On daemon restart the cache starts empty;
//     Recover queries sqlite on miss and populates it before returning.
//   - GC scans by `deadline + grace < now` and deletes both the row +
//     the cache entry. The grace period defaults to 5 minutes per
//     L2 §8.2.
//   - Each GC eviction emits one
//     `system.event payload.kind=correlation_gc adapter=<name>
//     request_id=<X>` row so operators can grep failures. The id is
//     deterministic so duplicate GC sweeps dedupe via harness Step 0.5.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	pkgharness "github.com/coagent-ai/daemon-go/pkg/harness"
	"github.com/coagent-ai/daemon-go/pkg/v4types"
)

// DefaultGCGraceMs is the L2 §8.2 baseline grace period (5 minutes
// after a tracked entry's deadline elapses before it is purged).
const DefaultGCGraceMs int64 = 5 * 60 * 1000

// DefaultGCPeriod is the wall-clock interval Manager.RunGC walks the
// table on. Chosen well below DefaultGCGraceMs so a stuck callback
// surfaces within ~30 s of becoming garbage.
const DefaultGCPeriod = 30 * time.Second

// CorrelationTrackerDDL is the channel-sqlite DDL the framework runs
// on Install. Idempotent (`CREATE TABLE IF NOT EXISTS`); applying it
// to an existing channel is a no-op when the table already exists.
//
// Composite primary key (adapter_name, external_id) prevents two
// adapters from clobbering each other's correlation entries when they
// happen to mint identical external ids.
const CorrelationTrackerDDL = `
CREATE TABLE IF NOT EXISTS adapter_correlation (
  adapter_name  TEXT NOT NULL,
  external_id   TEXT NOT NULL,
  request_id    TEXT NOT NULL,
  deadline_ms   INTEGER NOT NULL,
  created_at_ms INTEGER NOT NULL,
  PRIMARY KEY (adapter_name, external_id)
);
CREATE INDEX IF NOT EXISTS ix_adapter_correlation_request
  ON adapter_correlation(adapter_name, request_id);
CREATE INDEX IF NOT EXISTS ix_adapter_correlation_deadline
  ON adapter_correlation(adapter_name, deadline_ms);
`

// CorrelationTracker is the F2 surface adapters call from Handle (to
// register an external_id ↔ request_id pair) and OnExternalCallback
// (Recover the request_id from the external_id).
//
// All methods are safe for concurrent use; the implementation
// serialises access via an internal mutex + sqlite IMMEDIATE writes.
type CorrelationTracker interface {
	// Track records the mapping (request_id ↔ external_id) with a
	// deadline (wall-clock ms). Repeated Track for the same
	// (adapter, external_id) overwrites the row — adapters that
	// re-issue an external call after a retry get a fresh deadline.
	Track(ctx context.Context, requestID, externalID string, deadlineMs int64) error

	// Recover maps an external_id back to its request_id. Returns
	// ("", false, nil) when no entry exists (the caller treats the
	// callback as orphan per L1 §6.5). Errors are infrastructure-level
	// (sql / driver).
	Recover(ctx context.Context, externalID string) (string, bool, error)

	// Forget drops any entries indexed by request_id. The framework
	// calls it from Respond after a terminal write commits so subsequent
	// late callbacks fall through to the orphan path instead of being
	// recovered into a closed request.
	Forget(ctx context.Context, requestID string) error
}

// correlationTracker is the sqlite-backed CorrelationTracker the
// Manager hands to every adapter. One instance per adapter — the
// `adapter_name` column scopes every query so adapters share the
// table without collisions.
type correlationTracker struct {
	db            *sql.DB
	adapterName   string
	channelID     string
	systemActorID string
	writer        HarnessWriter
	logger        Logger
	clock         func() int64

	mu        sync.Mutex
	cache     map[string]*correlationEntry // external_id -> entry
	byRequest map[string]string            // request_id -> external_id (for Forget)
}

// correlationEntry is the in-memory mirror of one
// adapter_correlation row.
type correlationEntry struct {
	requestID string
	deadline  int64
}

// newCorrelationTracker constructs a tracker bound to db + adapter
// identity. Required fields: db, adapterName, channelID,
// systemActorID, writer, clock. logger defaults to noopLogger.
func newCorrelationTracker(
	db *sql.DB,
	adapterName, channelID, systemActorID string,
	writer HarnessWriter,
	clock func() int64,
	logger Logger,
) *correlationTracker {
	if logger == nil {
		logger = noopLogger{}
	}
	return &correlationTracker{
		db:            db,
		adapterName:   adapterName,
		channelID:     channelID,
		systemActorID: systemActorID,
		writer:        writer,
		clock:         clock,
		logger:        logger,
		cache:         map[string]*correlationEntry{},
		byRequest:     map[string]string{},
	}
}

// Track upserts the (request, external, deadline) row + cache entry.
func (t *correlationTracker) Track(ctx context.Context, requestID, externalID string, deadlineMs int64) error {
	if strings.TrimSpace(requestID) == "" {
		return errors.New("adapter: Track requestID is required")
	}
	if strings.TrimSpace(externalID) == "" {
		return errors.New("adapter: Track externalID is required")
	}
	if deadlineMs <= 0 {
		return fmt.Errorf("adapter: Track deadlineMs must be > 0, got %d", deadlineMs)
	}
	now := t.clock()
	_, err := t.db.ExecContext(ctx,
		`INSERT INTO adapter_correlation
		   (adapter_name, external_id, request_id, deadline_ms, created_at_ms)
		 VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT(adapter_name, external_id) DO UPDATE SET
		   request_id = excluded.request_id,
		   deadline_ms = excluded.deadline_ms,
		   created_at_ms = excluded.created_at_ms`,
		t.adapterName, externalID, requestID, deadlineMs, now,
	)
	if err != nil {
		return fmt.Errorf("adapter: track upsert: %w", err)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	// Clear any stale byRequest mapping pointing at a different
	// external_id (rare — adapter re-tracks the same request with a
	// new external id).
	if prevExt, ok := t.byRequest[requestID]; ok && prevExt != externalID {
		delete(t.cache, prevExt)
	}
	t.cache[externalID] = &correlationEntry{
		requestID: requestID,
		deadline:  deadlineMs,
	}
	t.byRequest[requestID] = externalID
	return nil
}

// Recover returns the request_id mapped to externalID, hitting the
// in-memory cache first and falling back to sqlite on miss. Sets the
// cache entry on a hit so future Recover calls stay in memory.
func (t *correlationTracker) Recover(ctx context.Context, externalID string) (string, bool, error) {
	if strings.TrimSpace(externalID) == "" {
		return "", false, nil
	}
	t.mu.Lock()
	if entry, ok := t.cache[externalID]; ok {
		out := entry.requestID
		t.mu.Unlock()
		return out, true, nil
	}
	t.mu.Unlock()

	row := t.db.QueryRowContext(ctx,
		`SELECT request_id, deadline_ms FROM adapter_correlation
		  WHERE adapter_name = ? AND external_id = ?`,
		t.adapterName, externalID,
	)
	var requestID string
	var deadline int64
	if err := row.Scan(&requestID, &deadline); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", false, nil
		}
		return "", false, fmt.Errorf("adapter: recover scan: %w", err)
	}
	t.mu.Lock()
	t.cache[externalID] = &correlationEntry{
		requestID: requestID,
		deadline:  deadline,
	}
	t.byRequest[requestID] = externalID
	t.mu.Unlock()
	return requestID, true, nil
}

// Forget deletes every adapter_correlation row pointing at requestID
// + clears the cache entry. Idempotent: missing rows are not an error.
func (t *correlationTracker) Forget(ctx context.Context, requestID string) error {
	if strings.TrimSpace(requestID) == "" {
		return nil
	}
	_, err := t.db.ExecContext(ctx,
		`DELETE FROM adapter_correlation
		  WHERE adapter_name = ? AND request_id = ?`,
		t.adapterName, requestID,
	)
	if err != nil {
		return fmt.Errorf("adapter: forget: %w", err)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if ext, ok := t.byRequest[requestID]; ok {
		delete(t.cache, ext)
		delete(t.byRequest, requestID)
	}
	return nil
}

// gcStats summarises one GC sweep. Tests check Evicted to verify entry
// counts; production wiring logs the stats.
type gcStats struct {
	Scanned int
	Evicted int
}

// gc walks every adapter_correlation row whose `deadline + grace < now`
// and deletes it. For each evicted entry it emits a
// `system.event payload.kind=correlation_gc` row so operators can grep
// adapter health.
//
// graceMs defaults to DefaultGCGraceMs when 0. now defaults to the
// tracker's clock when 0 — exposed for tests that want to walk forward
// in deterministic steps.
func (t *correlationTracker) gc(ctx context.Context, now, graceMs int64) (gcStats, error) {
	if now == 0 {
		now = t.clock()
	}
	if graceMs == 0 {
		graceMs = DefaultGCGraceMs
	}

	// Pull every expired (external_id, request_id) up front so we can
	// emit one event per row without holding the rowset open through
	// the harness write loop.
	rows, err := t.db.QueryContext(ctx,
		`SELECT external_id, request_id, deadline_ms
		   FROM adapter_correlation
		  WHERE adapter_name = ? AND deadline_ms + ? < ?`,
		t.adapterName, graceMs, now,
	)
	if err != nil {
		return gcStats{}, fmt.Errorf("adapter: gc scan: %w", err)
	}
	type expired struct {
		externalID string
		requestID  string
		deadline   int64
	}
	var batch []expired
	for rows.Next() {
		var e expired
		if err := rows.Scan(&e.externalID, &e.requestID, &e.deadline); err != nil {
			_ = rows.Close()
			return gcStats{}, fmt.Errorf("adapter: gc scan row: %w", err)
		}
		batch = append(batch, e)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return gcStats{}, fmt.Errorf("adapter: gc rows: %w", err)
	}
	_ = rows.Close()

	stats := gcStats{Scanned: len(batch)}
	if len(batch) == 0 {
		return stats, nil
	}

	// Stable ordering so log + emit chains are deterministic across
	// runs (helps test assertions + ops grep workflows).
	sort.Slice(batch, func(i, j int) bool { return batch[i].externalID < batch[j].externalID })

	for _, e := range batch {
		if err := t.evictAndEmit(ctx, e.externalID, e.requestID, e.deadline, now); err != nil {
			// Skip the row this round; next GC tick retries. We log
			// + continue rather than abort the whole sweep so one
			// bad row does not stall GC for the whole channel.
			t.logger.Warn("adapter.correlation.gc.error",
				"adapter", t.adapterName,
				"external_id", e.externalID,
				"err", err.Error(),
			)
			continue
		}
		stats.Evicted++
	}
	return stats, nil
}

// evictAndEmit deletes one expired row + emits the correlation_gc
// system event. The envelope id is deterministic so duplicate GC
// sweeps dedupe at harness Step 0.5.
func (t *correlationTracker) evictAndEmit(ctx context.Context, externalID, requestID string, deadline, now int64) error {
	if _, err := t.db.ExecContext(ctx,
		`DELETE FROM adapter_correlation
		  WHERE adapter_name = ? AND external_id = ?`,
		t.adapterName, externalID,
	); err != nil {
		return fmt.Errorf("delete: %w", err)
	}

	t.mu.Lock()
	delete(t.cache, externalID)
	if t.byRequest[requestID] == externalID {
		delete(t.byRequest, requestID)
	}
	t.mu.Unlock()

	if t.writer == nil {
		// Production wiring always supplies a writer; tests that skip
		// it just observe the delete + skip the emit.
		return nil
	}
	envelope, err := t.buildGCEvent(externalID, requestID, deadline, now)
	if err != nil {
		return fmt.Errorf("build event: %w", err)
	}
	res, err := t.writer.Write(ctx, envelope, pkgharness.CallerCtx{
		Authenticated: true,
		ActorID:       t.systemActorID,
	})
	if err != nil {
		return fmt.Errorf("emit: %w", err)
	}
	if !res.OK {
		// Harness rejected — log + treat as observability loss.
		reason := ""
		detail := ""
		if res.Error != nil {
			reason = string(res.Error.Reason)
			detail = res.Error.Detail
		}
		t.logger.Warn("adapter.correlation.gc.reject",
			"adapter", t.adapterName,
			"external_id", externalID,
			"reason", reason,
			"detail", detail,
		)
	}
	return nil
}

// buildGCEvent materialises one correlation_gc system.event envelope.
// id = "correlation_gc:<adapter>:<external_id>" — stable across reruns.
func (t *correlationTracker) buildGCEvent(externalID, requestID string, deadline, now int64) (*v4types.Envelope, error) {
	payload := map[string]any{
		"kind":        "correlation_gc",
		"adapter":     t.adapterName,
		"request_id":  requestID,
		"external_id": externalID,
		"deadline_ms": deadline,
		"emitted_at":  now,
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal payload: %w", err)
	}
	return &v4types.Envelope{
		ID:         "correlation_gc:" + t.adapterName + ":" + externalID,
		TS:         now,
		ChannelID:  t.channelID,
		Sender:     v4types.Sender{Kind: v4types.SenderSystem, ID: t.systemActorID},
		Kind:       v4types.KindEvent,
		Type:       "system.event",
		Payload:    raw,
		Visibility: v4types.VisibilitySystem,
		Audience:   []string{"*"},
	}, nil
}
