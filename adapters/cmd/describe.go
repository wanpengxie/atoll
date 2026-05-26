package cmd

import (
	"encoding/json"

	"github.com/wanpengxie/ActOS/kernel/adapter"
)

// actorDescription — actor-CLI §9 one-line positioning for list_actors.
const actorDescription = "Wrap a POSIX CLI binary as an actor. The minimal cli-adapter (actor-cli-pattern §19.3). Subject to a binary allowlist set at install time."

// actorSkillDoc — describe_actor markdown. Concrete enough for an
// agent to plan + recover; small enough to live within token budgets.
const actorSkillDoc = "" +
	"# cmd — generic CLI wrapper\n" +
	"\n" +
	"Runs a POSIX binary in the daemon process and returns its `{stdout, " +
	"stderr, exit_code, duration_ms}`. The substrate-native realisation " +
	"of the actor-cli-pattern §19.3 cli-adapter lever — wrap any tool, " +
	"keep the envelope discipline.\n" +
	"\n" +
	"## Tool surface\n" +
	"\n" +
	"- `cmd.exec` — run a binary with arguments / stdin / cwd / env / timeout.\n" +
	"- `cmd.which` — locate a binary in `$PATH` and report whether it is allowed.\n" +
	"\n" +
	"## Typical workflow\n" +
	"\n" +
	"1. `cmd.which {binary}` — pre-flight check that the binary exists + is allowed.\n" +
	"2. `cmd.exec {binary, args, ...}` — run it; inspect `exit_code` and `stdout`.\n" +
	"3. Non-zero `exit_code` is NOT a failed terminal — the binary ran, the " +
	"caller decides whether to treat it as success / failure.\n" +
	"\n" +
	"## Failure modes\n" +
	"\n" +
	"- `binary_not_allowed` — binary is outside the install-time allowlist.\n" +
	"- `binary_not_found` — binary not in `$PATH`.\n" +
	"- `payload_decode_failed` — payload missing required fields.\n" +
	"- `exec_timeout` — process did not finish within timeout_ms (default 10s).\n" +
	"- `exec_failed` — fork/exec syscall itself failed.\n" +
	"\n" +
	"## Constraints\n" +
	"\n" +
	"- `binary` is exec'd directly via `os/exec.CommandContext` — NOT through a shell. " +
	"Use `sh` + `-c` explicitly when you need shell features (only when sh is allowed).\n" +
	"- `cwd`, when present, must be an absolute path; relative paths reject.\n" +
	"- `timeout_ms` caps at the actor's `max_pending_ms` budget (30s default); " +
	"longer runs need a TypeDeclaration override at install time.\n"

// typeMeta — actor-cli describe_type convention payload. Filled per
// type so agents see concrete examples + recovery hints without having
// to read source.
var typeMeta = map[string]adapter.TypeDeclaration{
	TypeExec: {
		Description:    "Run a binary with arguments; return stdout, stderr, exit_code, duration_ms.",
		PayloadExample: json.RawMessage(`{"binary":"echo","args":["hello","world"]}`),
		PayloadFields: []adapter.FieldDoc{
			{Name: "binary", Required: true, Description: "Executable name (looked up in $PATH) or absolute path. Must be in the install-time allowlist.", Example: "echo"},
			{Name: "args", Description: "Argv (excluding binary). Each element passed as a separate argument; not shell-expanded.", Example: []string{"hello", "world"}},
			{Name: "stdin", Description: "Optional standard input. UTF-8 string.", Example: ""},
			{Name: "cwd", Description: "Optional working directory (absolute path).", Example: "/tmp"},
			{Name: "env", Description: "Optional extra environment variables. Adapter inherits daemon env; entries here append/override.", Example: map[string]string{"FOO": "bar"}},
			{Name: "timeout_ms", Description: "Optional per-exec timeout in milliseconds. Default 10000. Capped by max_pending_ms.", Example: 5000},
		},
		ErrorCodes: []adapter.ErrorDoc{
			{Code: "binary_not_allowed", Description: "Requested binary is outside the install-time allowlist.", Recovery: "Choose a binary from cmd.which output where allowed=true, or ask the operator to widen the allowlist."},
			{Code: "binary_not_found", Description: "Binary not found in $PATH on the daemon machine.", Recovery: "Use cmd.which to confirm availability; install the binary on the daemon host."},
			{Code: "payload_decode_failed", Description: "Payload JSON missing required fields or malformed.", Recovery: "Call describe_type cmd.exec to see payload_example."},
			{Code: "exec_timeout", Description: "Process did not finish within timeout_ms.", Recovery: "Raise timeout_ms (subject to max_pending_ms cap) or break the work into smaller commands."},
			{Code: "exec_failed", Description: "fork/exec syscall failed; see detail.", Recovery: "Check cwd exists, binary is executable, and the daemon process has permission."},
		},
		Notes: "Response is success-shaped even on non-zero exit_code — the binary ran. Failure terminal is reserved for adapter-side errors (allowlist / syscall / timeout).",
	},

	TypeWhich: {
		Description:    "Locate a binary in $PATH; report path + whether it is in the install allowlist.",
		PayloadExample: json.RawMessage(`{"binary":"echo"}`),
		PayloadFields: []adapter.FieldDoc{
			{Name: "binary", Required: true, Description: "Executable name to look up.", Example: "echo"},
		},
		ErrorCodes: []adapter.ErrorDoc{
			{Code: "payload_decode_failed", Description: "Missing or malformed binary field.", Recovery: "Pass {\"binary\":\"<name>\"}."},
		},
		Notes: "Cheap, no exec — pre-flight for cmd.exec. `allowed=false` means cmd.exec will reject with binary_not_allowed; `path` is still populated if the binary exists on disk (informational only).",
	},
}
