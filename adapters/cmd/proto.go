// Package cmd is a thin "wrap a CLI binary as an actor" adapter — the
// minimal living example of actor-cli-pattern §19.3 (cli-adapter as
// substrate ecosystem lever). It demonstrates the full
// `actor-adapter.md` exposed surface (A-H informational + R1-R6
// reliability) for a brand-new embedded adapter with ~200 LOC.
//
// Wire model:
//
//	envelope(kind=request, type=cmd.exec, payload={binary, args, ...})
//	  → Module.Handle
//	    → os/exec.CommandContext + capture stdout/stderr/exit_code
//	      → Respond(kind=response, status=completed/failed,
//	                payload={stdout, stderr, exit_code, duration_ms})
//
// Binding is embedded — adapter runs in-process inside daemon. No
// DeviceTransit, no upstream probe; framework baseline marks it ready
// once Init succeeds.
//
// Safety: by default the adapter accepts ONLY the binaries listed in
// `DefaultAllowedBinaries` (echo / true / false / date / printf / pwd /
// ls / cat / sh-with-c). Composition-root callers may pass a wider /
// narrower allowlist via Config.AllowedBinaries. An empty allowlist
// rejects every request — fail-closed.
package cmd

import (
	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// AdapterName is the framework module name; routing key for
// Manager.OnExternalCallback (cmd adapter has no external callbacks but
// the field is required by Module.Declares).
const AdapterName = "cmd"

// DefaultAdapterActorID is the canonical actor_registry id.
const DefaultAdapterActorID actor.ActorID = "tool:cmd"

// Binding is the protocol binding — embedded: adapter Module runs in
// the daemon process and synchronously exec's the requested binary.
const Binding = actor.BindingEmbedded

// DefaultMaxPendingMs is the per-request budget. Tight 30s default
// matches R5 invariant; long-running commands MUST raise it via
// TypeDeclaration override or accept fast-fail.
const DefaultMaxPendingMs int64 = 30_000

// DefaultBinaryTimeoutMs is the per-exec timeout when the request
// payload omits timeout_ms. Tight by intent — cli-adapter pattern
// favors short, repeatable invocations.
const DefaultBinaryTimeoutMs int64 = 10_000

// Type names — closed set. Wire convention `<adapter>.<verb>`
// (impl-vocabulary §3.0 #1).
const (
	// TypeExec runs a binary and returns stdout/stderr/exit_code.
	TypeExec = "cmd.exec"

	// TypeWhich locates a binary in PATH. Cheap, no exec. Useful for
	// pre-flight before TypeExec or for capability probes.
	TypeWhich = "cmd.which"
)

// RequestResponseTypes is the closed set Declares() exposes.
var RequestResponseTypes = []string{TypeExec, TypeWhich}

// AllTypes is the full closed set (no event-only types in v1).
var AllTypes = append([]string{}, RequestResponseTypes...)

// DefaultAllowedBinaries is the conservative starting allowlist —
// deterministic, side-effect-free utilities that exist on every POSIX
// system. Composition-root callers extend this with project-specific
// tools (`git`, `kubectl`, `ffmpeg`, ...) when the deployment justifies
// the wider surface.
var DefaultAllowedBinaries = []string{
	"echo",
	"true",
	"false",
	"date",
	"printf",
	"pwd",
	"ls",
	"cat",
}

// ExecRequest mirrors the cmd.exec payload schema.
type ExecRequest struct {
	Binary    string            `json:"binary"`
	Args      []string          `json:"args,omitempty"`
	Stdin     string            `json:"stdin,omitempty"`
	Cwd       string            `json:"cwd,omitempty"`
	Env       map[string]string `json:"env,omitempty"`
	TimeoutMs int64             `json:"timeout_ms,omitempty"`
}

// ExecResponse mirrors the cmd.exec success payload schema. Failure
// branches use the standard {status:"failed", reason, error_code,
// detail} terminal shape from impl-vocabulary §3.1.
type ExecResponse struct {
	Stdout     string `json:"stdout"`
	Stderr     string `json:"stderr"`
	ExitCode   int    `json:"exit_code"`
	DurationMs int64  `json:"duration_ms"`
}

// WhichRequest mirrors cmd.which payload.
type WhichRequest struct {
	Binary string `json:"binary"`
}

// WhichResponse mirrors cmd.which success payload.
type WhichResponse struct {
	Binary  string `json:"binary"`
	Path    string `json:"path"`
	Allowed bool   `json:"allowed"`
}

// DeclarationTypeDeclarations returns the kernel/adapter.TypeDeclaration
// map. Both v1 types are request/response with payload_status terminal
// convention. typeMeta (describe.go) supplies the A-F informational
// fields.
func DeclarationTypeDeclarations() map[string]adapter.TypeDeclaration {
	allowed := []message.Kind{message.KindRequest, message.KindResponse}
	out := make(map[string]adapter.TypeDeclaration, len(AllTypes))
	for _, t := range AllTypes {
		row := typeMeta[t]
		row.AllowedKinds = allowed
		row.TerminalConvention = string(adapter.TerminalPayloadStatus)
		out[t] = row
	}
	return out
}
