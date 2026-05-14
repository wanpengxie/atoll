package bootstrap

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"time"
)

// Bootstrap status enum values mirror the bootstrap_registry CHECK
// constraint (L2 §1.4.7). Exported as constants so HTTP / reconcile
// callers do not duplicate the literal strings.
const (
	StatusInProgress = "in_progress"
	StatusCompleted  = "completed"
	StatusRolledBack = "rolled_back"
)

// DefaultChannelAgentName is the convention from L1 §3.2:
// `<channel_id>:channel-agent`. Saga uses it whenever
// CreateParams.ChannelAgent.ActorID is empty.
const DefaultChannelAgentName = "channel-agent"

// Sentinel errors callers (HTTP handler / server reconcile) inspect to
// translate to L2 §3.6.1 daemon_rpc reason / HTTP status code.
//
//	ErrBootstrapInProgress  -> 409 bootstrap_in_progress
//	ErrBootstrapRolledBack  -> 409 (caller MUST switch create_request_id)
//	ErrParamsInvalid        -> 400 + per-field reason
var (
	ErrBootstrapInProgress = errors.New("bootstrap_in_progress")
	ErrBootstrapRolledBack = errors.New("bootstrap_rolled_back")
	ErrParamsInvalid       = errors.New("params_invalid")
)

// CreateParams carries every input the daemon needs to run the 9-step
// saga. channel_id and create_request_id are supplied by the server
// (L1 §3.1.2 — daemon never generates channel_id).
//
// NOTE (T102 FIX-2): the legacy `workdir_path` field has been removed
// from the wire contract. The server no longer dictates a filesystem
// path; the daemon derives `<channelRoot>/<channel_id>` internally and
// validates that the derived path stays within the configured
// channelRoot. This closes the codex t87 critical (an attacker who
// could craft a CreateParams could cause `os.RemoveAll` on an arbitrary
// daemon-readable directory during compensate).
type CreateParams struct {
	// CreateRequestID is the server-side UUID idempotency key. Same id
	// re-sent to daemon returns the cached status (completed → existing
	// channel_id; in_progress → ErrBootstrapInProgress; rolled_back →
	// ErrBootstrapRolledBack).
	CreateRequestID string `json:"create_request_id"`

	// ChannelID is the server-allocated id. The daemon stores it in
	// bootstrap_registry.channel_id (UNIQUE) and uses it as
	// messages.channel_id for the seed channel_created event. It is
	// also the only segment the daemon joins onto its configured
	// channelRoot to derive the per-channel workdir path; therefore the
	// saga validates that ChannelID contains no path separators and no
	// "..".
	ChannelID string `json:"channel_id"`

	// HumanMembers are the channel's human user ids (L0 §2.3 sender.id
	// for sender.kind=human). May be empty (channel without humans —
	// e.g. system-only diagnostic channel).
	HumanMembers []HumanMember `json:"human_members,omitempty"`

	// ChannelAgent specifies the agent actor seeded in step 5. When
	// ActorID is empty the saga derives `<channel_id>:channel-agent`.
	ChannelAgent ChannelAgentSpec `json:"channel_agent"`

	// ToolAdapters are the step-6 adapter installs. For each entry the
	// saga writes one actor_registry row + every TypeRow under it. The
	// L2 §3.5 install-order contract is honored (actor before types).
	ToolAdapters []ToolAdapterSpec `json:"tool_adapters,omitempty"`

	// BusinessTypes are the step-7 template snapshot rows. Each row's
	// handler_actor_id (when non-empty) MUST point at an actor seeded in
	// step 3-6; the saga validates this before INSERT.
	BusinessTypes []TypeRegistryRow `json:"business_types,omitempty"`
}

// HumanMember is the minimal subset of L1 user metadata the saga needs
// to seed actor_registry. The server holds the canonical user record;
// the daemon stores the id only.
type HumanMember struct {
	ActorID string `json:"actor_id"`
}

// ChannelAgentSpec lets the server pin a custom agent actor_id. When
// blank the saga uses `<channel_id>:channel-agent` (L1 §3.2 convention).
type ChannelAgentSpec struct {
	ActorID string `json:"actor_id,omitempty"`
}

// ToolAdapterSpec wires a single tool adapter into the new channel.
// Binding is normative ('daemon_rpc' or 'in_worker_bus') and must match
// every TypeRow.HandlerBinding under this adapter (validated by the
// saga before INSERT).
type ToolAdapterSpec struct {
	ActorID  string            `json:"actor_id"`
	Binding  string            `json:"binding"`
	TypeRows []TypeRegistryRow `json:"type_rows,omitempty"`
}

// TypeRegistryRow models one row of the channel-local type_registry
// table (L2 §1.4.2). The saga writes the row as-is — full type validation
// (Ad-2 max_pending_ms / fallback schema / handler binding match) is the
// concern of T5 (Type Registry Install Validation). The saga performs
// only the minimal checks needed to keep the spec invariant intact:
//
//   - HandlerBinding ∈ {'daemon_rpc','in_worker_bus'}
//   - When HandlerActorID is set, the actor must exist in actor_registry
//     within the same transaction.
type TypeRegistryRow struct {
	Type               string          `json:"type"`
	AllowedKinds       []string        `json:"allowed_kinds"`
	SchemasByKind      json.RawMessage `json:"schemas_by_kind"`
	HandlerBinding     string          `json:"handler_binding"`
	TerminalConvention string          `json:"terminal_convention,omitempty"`
	MaxPendingMs       *int64          `json:"max_pending_ms,omitempty"`
	HandlerActorID     string          `json:"handler_actor_id,omitempty"`
	Domain             string          `json:"domain,omitempty"`
}

// Result is the success return of ChannelCreate. Status is one of the
// Status* constants above; for completed responses ChannelID matches
// params.ChannelID, for in_progress / rolled_back responses ChannelID
// reflects the previously-recorded value from bootstrap_registry.
type Result struct {
	ChannelID string `json:"channel_id"`
	Status    string `json:"status"`
}

// ChannelInfo is one row returned by ListChannels. It is the daemon
// truth view of a completed channel (used by the server reconcile API
// to rebuild its informational cache, L2 §1.4.7 step 9).
type ChannelInfo struct {
	ChannelID       string `json:"channel_id"`
	WorkdirPath     string `json:"workdir_path"`
	CreateRequestID string `json:"create_request_id"`
	CompletedAt     int64  `json:"completed_at"`
}

// ReconcileReport summarises what Saga.Reconcile did during a single
// daemon startup pass. The numbers are derived from bootstrap_registry
// state changes (not just inspected rows) so callers can log a one-line
// summary.
type ReconcileReport struct {
	Scanned    int      // rows seen with status='in_progress'
	RolledBack int      // pushed in_progress → rolled_back (integrity fail)
	Completed  int      // pushed in_progress → completed (retry step 8-9)
	Failures   []string // create_request_id list where reconcile itself errored
}

// openChannelFn matches store.OpenChannel. Injection point so tests can
// simulate a "channel sqlite open" failure without touching disk.
type openChannelFn func(ctx context.Context, path string) (*sql.DB, error)

// Saga is the L2 §1.4.7 9-step driver. The struct is stateless across
// calls beyond the *sql.DB pool + injected hooks; one Saga instance per
// daemon process is sufficient.
type Saga struct {
	daemonDB *sql.DB

	// channelRoot is the absolute directory under which every channel's
	// workdir lives. The saga derives each channel workdir as
	// filepath.Join(channelRoot, channel_id) and validates the result
	// stays within channelRoot (T102 FIX-2 containment). New() requires
	// a non-empty channelRoot; ChannelCreate refuses params_invalid
	// otherwise so production wiring cannot accidentally skip the check.
	channelRoot string

	now    func() int64
	openCh openChannelFn
	mkdir  func(path string, perm os.FileMode) error
	rmAll  func(path string) error
	stat   func(path string) (fs.FileInfo, error)

	// fileWriter creates the per-workdir compensate marker. Tests can
	// stub it; production uses os.WriteFile. The marker lets compensate
	// distinguish "directory the saga created" from "directory that
	// happened to already exist" — only the former is safe to rm.
	fileWriter func(path string, data []byte, perm os.FileMode) error

	// failpoints is an optional per-step error injection map used only
	// by tests. Keys are the failPoint constants below. When the saga
	// reaches a step whose key is in the map it returns the mapped
	// error (causing the documented compensation path to run).
	failpoints map[string]error
}

// failpoint keys — exported via package-private constants so tests in
// the same package can request a specific compensation path without
// touching real sqlite / OS state.
const (
	fpStep1Insert    = "step1_insert"
	fpStep2Mkdir     = "step2_mkdir"
	fpStep2OpenCh    = "step2_open_channel"
	fpStep3System    = "step3_system_actor"
	fpStep4Human     = "step4_human_member"
	fpStep5Agent     = "step5_channel_agent"
	fpStep6Adapter   = "step6_adapter"
	fpStep7Type      = "step7_business_type"
	fpStep8aEmit     = "step8a_emit"
	fpStep8bComplete = "step8b_complete"
)

// Option tunes Saga construction. Production callers normally need
// none; tests inject clocks / filesystem / failpoints through them.
type Option func(*Saga)

// WithNow replaces the wall-clock used for bootstrap_registry
// started_at / completed_at and message.ts. Default: time.Now().Unix().
func WithNow(fn func() int64) Option {
	return func(s *Saga) {
		if fn != nil {
			s.now = fn
		}
	}
}

// WithOpenChannel replaces the OpenChannel hook. Default: a thin
// closure around store.OpenChannel (wired in saga.go to avoid a doc.go
// import cycle with internal/store tests).
func WithOpenChannel(fn openChannelFn) Option {
	return func(s *Saga) {
		if fn != nil {
			s.openCh = fn
		}
	}
}

// WithFilesystem replaces mkdir / rmAll / stat. Default: os package.
func WithFilesystem(
	mkdir func(string, os.FileMode) error,
	rmAll func(string) error,
	stat func(string) (fs.FileInfo, error),
) Option {
	return func(s *Saga) {
		if mkdir != nil {
			s.mkdir = mkdir
		}
		if rmAll != nil {
			s.rmAll = rmAll
		}
		if stat != nil {
			s.stat = stat
		}
	}
}

// WithChannelRoot configures the directory under which channel workdirs
// are derived. Required for ChannelCreate (T102 FIX-2): the saga refuses
// to run with an empty channelRoot so production wiring cannot bypass
// the containment check. Path must be absolute; the saga calls
// filepath.Clean on it once at construction time.
func WithChannelRoot(root string) Option {
	return func(s *Saga) {
		s.channelRoot = root
	}
}

// WithFileWriter replaces the compensate marker writer. Default:
// os.WriteFile. Tests inject a stub to assert the marker file is
// written exactly once per workdir.
func WithFileWriter(fn func(path string, data []byte, perm os.FileMode) error) Option {
	return func(s *Saga) {
		if fn != nil {
			s.fileWriter = fn
		}
	}
}

// withFailpoints is an internal-only option for table-driven tests.
// Exposed via testing helpers in saga_test.go; keep unexported so
// production builds cannot accidentally inject errors.
func withFailpoints(fp map[string]error) Option {
	return func(s *Saga) {
		s.failpoints = fp
	}
}

// nowUnix is the production wall-clock. Kept package-private so tests
// always inject WithNow rather than monkey-patching.
func nowUnix() int64 { return time.Now().Unix() }
