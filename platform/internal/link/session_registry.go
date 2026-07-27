package link

import (
	"errors"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
)

// SessionGeneration is the one UUIDv7 coordinate minted by the home when a
// carrier is accepted. The daemon only echoes the value it receives at attach.
type SessionGeneration string

type SessionState string

const (
	SessionCandidate SessionState = "candidate"
	SessionActive    SessionState = "active"
	SessionClosing   SessionState = "closing"
	SessionClosed    SessionState = "closed"
)

// SessionEndReason is deliberately closed. Operational detail belongs in
// SessionSnapshot.Detail; it must never create another lifecycle vocabulary.
type SessionEndReason string

const (
	SessionCarrierLost       SessionEndReason = "carrier_lost"
	SessionLivenessExpired   SessionEndReason = "liveness_expired"
	SessionSpineLost         SessionEndReason = "spine_lost"
	SessionProtocolViolation SessionEndReason = "protocol_violation"
	SessionLocalFault        SessionEndReason = "local_fault"
	SessionRevoked           SessionEndReason = "revoked"
	SessionAdmissionRejected SessionEndReason = "admission_rejected"
	SessionHandshakeTimeout  SessionEndReason = "handshake_timeout"
)

const (
	defaultCandidateTTL      = 30 * time.Second
	defaultDiagnosticTTL     = time.Hour
	defaultProbeInterval     = 10 * time.Second
	defaultLivenessTTL       = 30 * time.Second
	defaultSettlementWindow  = 2 * time.Second
	defaultSessionJoinWindow = 5 * time.Second
)

// SessionSnapshot is a diagnostic value. It is never accepted back as
// authority.
type SessionSnapshot struct {
	Generation SessionGeneration
	Key        string
	State      SessionState
	Reason     SessionEndReason
	Detail     string
	LastSeen   time.Time
	MintedAt   time.Time
	AttachedAt time.Time
	ClosedAt   time.Time
	Abandoned  int64
}

type sessionValue struct {
	generation SessionGeneration
	key        string
	state      SessionState
	reason     SessionEndReason
	detail     string
	lastSeen   time.Time
	mintedAt   time.Time
	attachedAt time.Time
	closedAt   time.Time
	abandoned  int64
}

type sessionRecord struct {
	registry   *sessionRegistry
	generation SessionGeneration
	key        string

	// sealed closes when beginSeal commits this record's verdict. Evidence is
	// never queued in a channel: report writes the verdict into the ledger
	// under its lock, and sealed is only the collection wake-up level.
	sealed         chan struct{}
	doneOnce       sync.Once
	done           chan struct{}
	candidateTimer *time.Timer

	handleMu     sync.Mutex
	handle       *linkHandle
	physicalDone <-chan struct{}
}

type sessionEvidence struct {
	reason SessionEndReason
	detail string
	err    error
}

// report IS the verdict: it performs the locked decision write immediately, so
// evidence is never parked where later evidence could displace or lose it.
// Stale candidate-only evidence is refused inside beginSeal; a later real
// reason still lands because nothing was consumed. The supervising loop only
// observes the sealed level and collects.
func (r *sessionRecord) report(reason SessionEndReason, detail string, err error) {
	if r == nil || r.registry == nil {
		return
	}
	_ = r.registry.beginSeal(r, sessionEvidence{reason: reason, detail: detail, err: err})
}

func (r *sessionRecord) setHandle(handle *linkHandle) {
	r.handleMu.Lock()
	r.handle = handle
	r.handleMu.Unlock()
}

func (r *sessionRecord) linkHandle() *linkHandle {
	r.handleMu.Lock()
	defer r.handleMu.Unlock()
	return r.handle
}

func (r *sessionRecord) setPhysicalDone(done <-chan struct{}) {
	r.handleMu.Lock()
	r.physicalDone = done
	r.handleMu.Unlock()
}

func (r *sessionRecord) physicalJoin() <-chan struct{} {
	r.handleMu.Lock()
	defer r.handleMu.Unlock()
	return r.physicalDone
}

func (r *sessionRecord) finish() {
	r.doneOnce.Do(func() { close(r.done) })
}

type sessionRegistry struct {
	mu      sync.Mutex
	live    map[SessionGeneration]*sessionValue
	closed  map[SessionGeneration]*sessionValue
	records map[SessionGeneration]*sessionRecord
	current map[string]SessionGeneration
	logger  *slog.Logger

	candidateTTL  time.Duration
	diagnosticTTL time.Duration
	probeInterval time.Duration
	livenessTTL   time.Duration
}

func newSessionRegistry(logger *slog.Logger) *sessionRegistry {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &sessionRegistry{
		live:          make(map[SessionGeneration]*sessionValue),
		closed:        make(map[SessionGeneration]*sessionValue),
		records:       make(map[SessionGeneration]*sessionRecord),
		current:       make(map[string]SessionGeneration),
		logger:        logger,
		candidateTTL:  defaultCandidateTTL,
		diagnosticTTL: defaultDiagnosticTTL,
		probeInterval: defaultProbeInterval,
		livenessTTL:   defaultLivenessTTL,
	}
}

func mintSessionGeneration() (SessionGeneration, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return "", err
	}
	return SessionGeneration(id.String()), nil
}

func (r *sessionRegistry) mint(key string) (*sessionRecord, error) {
	if r == nil || key == "" {
		return nil, errors.New("link: session key is required")
	}
	generation, err := mintSessionGeneration()
	if err != nil {
		return nil, err
	}
	record, err := r.insert(generation, key, SessionCandidate)
	if err != nil {
		return nil, err
	}
	r.armCandidateTimer(record, "attach_not_received_before_ttl")
	return record, nil
}

// armCandidateTimer installs the TTL under the ledger lock — the same lock
// every reader (activate, beginSeal) holds when stopping it.
func (r *sessionRegistry) armCandidateTimer(record *sessionRecord, detail string) {
	r.mu.Lock()
	record.candidateTimer = time.AfterFunc(r.candidateTTL, func() {
		r.reportIfCandidate(record, SessionHandshakeTimeout, detail)
	})
	r.mu.Unlock()
}

// reportIfCandidate offers candidate-only evidence. The state check narrows
// the race with activate; the authoritative conditional write is beginSeal's
// candidate guard, which refuses stale candidate-only reasons.
func (r *sessionRegistry) reportIfCandidate(
	record *sessionRecord,
	reason SessionEndReason,
	detail string,
) {
	r.mu.Lock()
	value := r.live[record.generation]
	isCandidate := value != nil && value.state == SessionCandidate
	r.mu.Unlock()
	if isCandidate {
		record.report(reason, detail, nil)
	}
}

// adopt installs the generation minted by the home into the daemon's local
// ledger. It never mints or rewrites that coordinate.
func (r *sessionRegistry) adopt(generation SessionGeneration, key string) (*sessionRecord, error) {
	if r == nil || generation == "" || key == "" {
		return nil, errors.New("link: adopted session coordinate is incomplete")
	}
	if parsed, err := uuid.Parse(string(generation)); err != nil ||
		parsed.Version() != 7 || parsed.Variant() != uuid.RFC4122 ||
		parsed.String() != string(generation) {
		return nil, errors.New("link: session generation must be canonical UUIDv7")
	}
	record, err := r.insert(generation, key, SessionCandidate)
	if err != nil {
		return nil, err
	}
	r.armCandidateTimer(record, "adopted_session_not_activated_before_ttl")
	if _, err := r.activate(record); err != nil {
		return nil, err
	}
	return record, nil
}

func (r *sessionRegistry) insert(
	generation SessionGeneration,
	key string,
	state SessionState,
) (*sessionRecord, error) {
	now := time.Now()
	record := &sessionRecord{
		registry: r, generation: generation, key: key,
		sealed: make(chan struct{}), done: make(chan struct{}),
	}
	r.mu.Lock()
	if r.live[generation] != nil || r.closed[generation] != nil {
		r.mu.Unlock()
		return nil, errors.New("link: session generation already exists")
	}
	r.live[generation] = &sessionValue{
		generation: generation, key: key, state: state,
		mintedAt: now, lastSeen: now,
	}
	r.records[generation] = record
	r.mu.Unlock()
	r.logger.Info("link.session_state",
		"generation", generation, "key", key, "state", state, "at", now)
	return record, nil
}

// activate commits candidate→active and the current pointer in one lock hold.
// It returns the displaced record for observability only; displacement does
// not alter or close that still-active session.
func (r *sessionRegistry) activate(record *sessionRecord) (*sessionRecord, error) {
	if r == nil || record == nil || record.registry != r {
		return nil, errors.New("link: foreign session record")
	}
	now := time.Now()
	r.mu.Lock()
	value := r.live[record.generation]
	if value == nil || value.state != SessionCandidate {
		r.mu.Unlock()
		return nil, errors.New("link: session is not an attach candidate")
	}
	var displaced *sessionRecord
	if old := r.current[record.key]; old != "" && old != record.generation {
		displaced = r.records[old]
	}
	value.state = SessionActive
	if record.candidateTimer != nil {
		record.candidateTimer.Stop()
	}
	value.attachedAt = now
	value.lastSeen = now
	r.current[record.key] = record.generation
	r.mu.Unlock()
	r.logger.Info("link.session_state",
		"generation", record.generation, "key", record.key,
		"state", SessionActive, "at", now)
	if displaced != nil {
		r.logger.Info("link.session_displaced",
			"generation", displaced.generation, "key", displaced.key,
			"by_generation", record.generation)
	}
	return displaced, nil
}

// sealVerdict is beginSeal's locked outcome. Only sealCommitted transfers
// teardown obligation to the caller; sealStaleEvidence means the session is
// still healthy and supervision must continue.
type sealVerdict int

const (
	sealCommitted sealVerdict = iota
	sealAlreadyDecided
	sealStaleEvidence
)

// beginSeal is the decision write. Every authority reads the same locked
// value, so returning from this method means every admission/current gate sees
// the cut. Candidate sessions move directly to closed as required by the
// canonical four-state model. Candidate-only reasons (handshake timeout,
// admission rejection) are refused as stale once the session is no longer a
// candidate: the verdict a reason presupposes is confirmed under the same lock
// that writes it.
func (r *sessionRegistry) beginSeal(record *sessionRecord, evidence sessionEvidence) sealVerdict {
	if r == nil || record == nil || record.registry != r {
		return sealAlreadyDecided
	}
	now := time.Now()
	if !validSessionEndReason(evidence.reason) {
		evidence.detail = "invalid_session_end_reason: " + string(evidence.reason)
		evidence.reason = SessionLocalFault
	}
	r.mu.Lock()
	if record.candidateTimer != nil {
		record.candidateTimer.Stop()
	}
	value := r.live[record.generation]
	if value == nil || value.state == SessionClosing || value.state == SessionClosed {
		r.mu.Unlock()
		return sealAlreadyDecided
	}
	if candidateOnlyReason(evidence.reason) && value.state != SessionCandidate {
		r.mu.Unlock()
		r.logger.Info("link.session_stale_candidate_evidence",
			"generation", record.generation, "key", record.key,
			"reason", evidence.reason, "detail", evidence.detail)
		return sealStaleEvidence
	}
	value.reason = evidence.reason
	value.detail = evidence.detail
	if value.state == SessionCandidate {
		value.state = SessionClosed
		value.closedAt = now
	} else {
		value.state = SessionClosing
	}
	if r.current[record.key] == record.generation {
		delete(r.current, record.key)
	}
	state := value.state
	r.mu.Unlock()
	close(record.sealed)
	r.logger.Warn("link.session_state",
		"generation", record.generation, "key", record.key,
		"state", state, "reason", evidence.reason,
		"detail", evidence.detail, "error", evidence.err, "at", now)
	return sealCommitted
}

func candidateOnlyReason(reason SessionEndReason) bool {
	return reason == SessionHandshakeTimeout || reason == SessionAdmissionRejected
}

func validSessionEndReason(reason SessionEndReason) bool {
	switch reason {
	case SessionCarrierLost, SessionLivenessExpired, SessionSpineLost,
		SessionProtocolViolation, SessionLocalFault, SessionRevoked,
		SessionAdmissionRejected, SessionHandshakeTimeout:
		return true
	default:
		return false
	}
}

func (r *sessionRegistry) completeSeal(record *sessionRecord, abandoned int64) {
	if r == nil || record == nil || record.registry != r {
		return
	}
	now := time.Now()
	r.mu.Lock()
	value := r.live[record.generation]
	if value == nil {
		r.mu.Unlock()
		return
	}
	if value.state == SessionClosing {
		value.state = SessionClosed
		value.closedAt = now
	}
	value.abandoned += abandoned
	delete(r.live, record.generation)
	r.closed[record.generation] = value
	snapshot := snapshotOf(value)
	retention := r.diagnosticTTL
	r.mu.Unlock()
	record.finish()
	r.logger.Warn("link.session_state",
		"generation", snapshot.Generation, "key", snapshot.Key,
		"state", snapshot.State, "reason", snapshot.Reason,
		"abandoned", snapshot.Abandoned, "at", now)
	time.AfterFunc(retention, func() { r.expireDiagnostic(record.generation) })
}

func (r *sessionRegistry) expireDiagnostic(generation SessionGeneration) {
	r.mu.Lock()
	value := r.closed[generation]
	if value != nil && value.state == SessionClosed &&
		!value.closedAt.IsZero() && time.Since(value.closedAt) >= r.diagnosticTTL {
		delete(r.closed, generation)
		delete(r.records, generation)
	}
	r.mu.Unlock()
}

func (r *sessionRegistry) touch(record *sessionRecord, at time.Time) bool {
	if r == nil || record == nil || record.registry != r {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	value := r.live[record.generation]
	if value == nil || value.state != SessionActive {
		return false
	}
	if at.After(value.lastSeen) {
		value.lastSeen = at
	}
	return true
}

func (r *sessionRegistry) lastSeen(record *sessionRecord) (time.Time, bool) {
	if r == nil || record == nil || record.registry != r {
		return time.Time{}, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	value := r.live[record.generation]
	if value == nil || value.state != SessionActive {
		return time.Time{}, false
	}
	return value.lastSeen, true
}

type admitAuthority struct {
	registry   *sessionRegistry
	generation SessionGeneration
	key        string
}

func (r *sessionRegistry) admit(record *sessionRecord) admitAuthority {
	if r == nil || record == nil || record.registry != r {
		return admitAuthority{}
	}
	return admitAuthority{registry: r, generation: record.generation, key: record.key}
}

func (a admitAuthority) allows() bool {
	if a.registry == nil || a.generation == "" || a.key == "" {
		return false
	}
	a.registry.mu.Lock()
	defer a.registry.mu.Unlock()
	value := a.registry.live[a.generation]
	return value != nil && value.key == a.key && value.state == SessionActive
}

type currentAuthority struct {
	registry   *sessionRegistry
	generation SessionGeneration
	key        string
}

// SessionAuthority is an opaque pair of live credentials. Its coordinates are
// immutable and its registry reference is private, so callers can ask but
// cannot manufacture or rewrite a verdict.
type SessionAuthority struct {
	admit      admitAuthority
	current    currentAuthority
	generation SessionGeneration
	key        string
}

func authorityPair(registry *sessionRegistry, record *sessionRecord) SessionAuthority {
	if registry == nil || record == nil || record.registry != registry {
		return SessionAuthority{}
	}
	return SessionAuthority{
		admit: registry.admit(record), current: registry.authority(record),
		generation: record.generation, key: record.key,
	}
}

func (a SessionAuthority) registerPhysicalDone(done <-chan struct{}) {
	if a.admit.registry == nil || done == nil {
		return
	}
	if record := a.admit.registry.record(a.generation); record != nil && record.key == a.key {
		record.setPhysicalDone(done)
	}
}

func (a SessionAuthority) admits() bool    { return a.admit.allows() }
func (a SessionAuthority) isCurrent() bool { return a.current.allows() }

func (r *sessionRegistry) authority(record *sessionRecord) currentAuthority {
	if r == nil || record == nil || record.registry != r {
		return currentAuthority{}
	}
	return currentAuthority{registry: r, generation: record.generation, key: record.key}
}

func (a currentAuthority) allows() bool {
	if a.registry == nil || a.generation == "" || a.key == "" {
		return false
	}
	a.registry.mu.Lock()
	defer a.registry.mu.Unlock()
	value := a.registry.live[a.generation]
	return value != nil && value.key == a.key && value.state == SessionActive &&
		a.registry.current[a.key] == a.generation
}

func (r *sessionRegistry) currentRecord(key string) *sessionRecord {
	if r == nil || key == "" {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	generation := r.current[key]
	value := r.live[generation]
	if value == nil || value.state != SessionActive {
		return nil
	}
	return r.records[generation]
}

func (r *sessionRegistry) record(generation SessionGeneration) *sessionRecord {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.live[generation] == nil {
		return nil
	}
	return r.records[generation]
}

func (r *sessionRegistry) snapshots() []SessionSnapshot {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	out := make([]SessionSnapshot, 0, len(r.live)+len(r.closed))
	for _, value := range r.live {
		out = append(out, snapshotOf(value))
	}
	for _, value := range r.closed {
		out = append(out, snapshotOf(value))
	}
	r.mu.Unlock()
	sort.Slice(out, func(i, j int) bool { return out[i].MintedAt.Before(out[j].MintedAt) })
	return out
}

func (r *sessionRegistry) currentKeys() []string {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	out := make([]string, 0, len(r.current))
	for key, generation := range r.current {
		if value := r.live[generation]; value != nil && value.state == SessionActive {
			out = append(out, key)
		}
	}
	r.mu.Unlock()
	sort.Strings(out)
	return out
}

func snapshotOf(value *sessionValue) SessionSnapshot {
	if value == nil {
		return SessionSnapshot{}
	}
	return SessionSnapshot{
		Generation: value.generation,
		Key:        value.key,
		State:      value.state,
		Reason:     value.reason,
		Detail:     value.detail,
		LastSeen:   value.lastSeen,
		MintedAt:   value.mintedAt,
		AttachedAt: value.attachedAt,
		ClosedAt:   value.closedAt,
		Abandoned:  value.abandoned,
	}
}
