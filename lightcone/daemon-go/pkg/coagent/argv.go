package coagent

import "strings"

// splitFlagArgs separates argv into (flagArgs, positionalArgs) so the
// subcommand can call fs.Parse(flagArgs) regardless of how the caller
// interleaved flags and positional text. Required because the L2 §3.2
// canonical forms place positional text first:
//
//	coagent emit "调研做完了..."
//	coagent emit "同意 @A 的方案" --parent <msg_id>
//	coagent answer <request_id> "搞定了" --type biz.foo
//
// Standard Go `flag` parsing stops at the first non-flag token; this
// helper preserves caller-friendly ordering without dragging in pflag.
//
// Rules:
//
//   - Tokens starting with "--<name>" or "-<name>" are flag tokens.
//   - "--<name>=<value>" is a single self-contained flag token.
//   - "--<name>" without "=" consumes the next token as its value
//     UNLESS the flag name is in boolFlags (then the flag is standalone).
//   - "--" by itself terminates flag parsing — everything after is
//     positional (POSIX convention; lets callers pass "--" to send
//     "-something" as text).
//   - Everything else is positional.
//
// The function is intentionally permissive: unknown flag names still
// take a value argument (because we don't yet know if Go's flag set
// will accept them). The FlagSet's own Parse then rejects unknown
// flags with a friendly error.
func splitFlagArgs(argv []string, boolFlags map[string]bool) ([]string, []string) {
	var (
		flagArgs []string
		posArgs  []string
		i        = 0
	)
	for i < len(argv) {
		tok := argv[i]
		if tok == "--" {
			// Terminator: everything after is positional.
			posArgs = append(posArgs, argv[i+1:]...)
			return flagArgs, posArgs
		}
		if !isFlagToken(tok) {
			posArgs = append(posArgs, tok)
			i++
			continue
		}
		// It's a flag token.
		name, hasEq := flagName(tok)
		if hasEq {
			// Self-contained.
			flagArgs = append(flagArgs, tok)
			i++
			continue
		}
		// Bare flag form. Consume next token as value unless the flag
		// is a known bool flag.
		if boolFlags != nil && boolFlags[name] {
			flagArgs = append(flagArgs, tok)
			i++
			continue
		}
		// Default: pair with the next token as the value.
		flagArgs = append(flagArgs, tok)
		if i+1 < len(argv) {
			flagArgs = append(flagArgs, argv[i+1])
			i += 2
			continue
		}
		// No value follows — let flag.Parse surface the error.
		i++
	}
	return flagArgs, posArgs
}

// isFlagToken reports whether tok looks like a flag (starts with `-`
// and has at least one more character). The single dash `-` alone is
// NOT a flag (callers sometimes use it as a positional stand-in).
func isFlagToken(tok string) bool {
	if len(tok) < 2 || tok[0] != '-' {
		return false
	}
	return true
}

// flagName extracts the flag name from a "--name" or "-n" or
// "--name=value" token. Returns (name, hasEqualsValue).
func flagName(tok string) (string, bool) {
	body := strings.TrimLeft(tok, "-")
	if eq := strings.IndexByte(body, '='); eq >= 0 {
		return body[:eq], true
	}
	return body, false
}

// boolFlagNames returns the closed set of bool flags the CLI accepts.
// Tested in argv_test.go alongside splitFlagArgs.
func boolFlagNames() map[string]bool {
	return map[string]bool{
		"private": true,
		"system":  true,
	}
}
