// Package logger is a thin zerolog facade reused by cmd/server,
// cmd/daemon and cmd/worker. The goal (M1.6-T7 acceptance B) is one
// JSON line per event written to stdout so an operator can pipe the
// coagent binaries' output into jq / loki / etc. without first
// untangling stdlib's text format.
//
// Why a facade and not zerolog directly:
//   - we want a single place to stamp `component` + `version` so a
//     multi-binary cvmax deploy stays grep-able;
//   - we want gin / stdlib `log` to interoperate (they default to
//     `log.SetOutput(os.Stderr)`, which would split the stream); by
//     calling RedirectStdlib we route the stdlib logger into zerolog
//     too, so legacy log.Printf callers we have not migrated yet still
//     come out as JSON.
//
// What this package is NOT: a domain abstraction layer. zerolog's
// chain-style API is good enough to leak through; callers can call
// `.Z()` to get the underlying *zerolog.Logger and use the full API.
package logger

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog"
)

// Logger wraps a zerolog.Logger so we can stamp project-wide context
// fields and avoid leaking the vendor type into every signature.
type Logger struct {
	z zerolog.Logger
}

// Config drives New(). All fields optional; sensible defaults applied.
type Config struct {
	// Component is the binary name stamped on every line ("server",
	// "daemon", "worker", …). When empty, "coagent" is used so the
	// downstream parser still has a label to group by.
	Component string

	// Version is an optional build tag stamped on every line. Pass the
	// cmd/* `version` ldflag so prod logs carry the release tag.
	Version string

	// Writer is where JSON lines go. Defaults to os.Stdout (so logs go
	// to the same place gin's release-mode handler logs would —
	// docker / systemd capture them as application output, not error
	// output).
	Writer io.Writer

	// Pretty, when true, switches to zerolog.ConsoleWriter (human
	// readable colored output). Use for dev only; production MUST stay
	// on the JSON path so log shipping works.
	Pretty bool

	// Level overrides the global log level ("trace" / "debug" / "info"
	// / "warn" / "error"). Empty string = info.
	Level string
}

// New builds a Logger. Safe to call multiple times in the same process;
// the returned Logger is independent of the zerolog global.
func New(cfg Config) Logger {
	if cfg.Component == "" {
		cfg.Component = "coagent"
	}
	if cfg.Writer == nil {
		cfg.Writer = os.Stdout
	}

	// Use unix-ms timestamps — the rest of the v4 protocol speaks
	// unix-ms (see kernel/message.Envelope.TS); JSON consumers can
	// align log events to envelope timestamps without unit conversion.
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMs

	out := cfg.Writer
	if cfg.Pretty {
		out = zerolog.ConsoleWriter{Out: cfg.Writer, TimeFormat: time.RFC3339}
	}

	z := zerolog.New(out).With().
		Timestamp().
		Str("component", cfg.Component).
		Logger()
	if cfg.Version != "" && cfg.Version != "dev" {
		z = z.With().Str("version", cfg.Version).Logger()
	}

	if lvl := strings.TrimSpace(strings.ToLower(cfg.Level)); lvl != "" {
		if parsed, err := zerolog.ParseLevel(lvl); err == nil {
			z = z.Level(parsed)
		}
	}

	return Logger{z: z}
}

// Z returns the underlying *zerolog.Logger so callers can use the full
// zerolog chain API. Stable pointer — safe to keep around.
func (l Logger) Z() *zerolog.Logger { return &l.z }

// Info / Warn / Error / Debug are thin shortcuts so the common case
// (one-shot info line) stays terse at call sites.

// Info emits one info-level line.
func (l Logger) Info(msg string) { l.z.Info().Msg(msg) }

// Warn emits one warn-level line.
func (l Logger) Warn(msg string) { l.z.Warn().Msg(msg) }

// Error emits one error-level line.
func (l Logger) Error(msg string) { l.z.Error().Msg(msg) }

// Debug emits one debug-level line.
func (l Logger) Debug(msg string) { l.z.Debug().Msg(msg) }

// Infof / Warnf / Errorf are printf-style shortcuts for one-off legacy
// migrations. New code should prefer the zerolog chain API (.Str /
// .Int / .Err) for structured fields.

// Infof emits a printf-formatted info line.
func (l Logger) Infof(format string, args ...any) {
	l.z.Info().Msg(sprintf(format, args...))
}

// Warnf emits a printf-formatted warn line.
func (l Logger) Warnf(format string, args ...any) {
	l.z.Warn().Msg(sprintf(format, args...))
}

// Errorf emits a printf-formatted error line.
func (l Logger) Errorf(format string, args ...any) {
	l.z.Error().Msg(sprintf(format, args...))
}

// Fatalf emits a fatal line then calls os.Exit(1). Mirrors log.Fatalf
// at the boot path so cmd/* swap-in stays minimal.
func (l Logger) Fatalf(format string, args ...any) {
	l.z.Error().Msg(sprintf(format, args...))
	// zerolog's .Fatal() helper calls os.Exit; we want one canonical
	// shape so callers see the same exit code (1) regardless of which
	// logger they imported.
	os.Exit(1)
}

func sprintf(format string, args ...any) string {
	if len(args) == 0 {
		return format
	}
	return fmt.Sprintf(format, args...)
}

// RedirectStdlib points the stdlib `log` package at this logger so any
// log.Printf calls that haven't been migrated yet still emit JSON.
// Returns a cleanup function that restores the previous stdlib output;
// most callers can ignore it (process lifetime).
//
// IMPORTANT: must be called once per process, after New(). Calling it
// twice in the same process is harmless but the second call's cleanup
// restores the FIRST call's writer, not the original stdlib default.
func (l Logger) RedirectStdlib() func() {
	prevFlags := log.Flags()
	prevPrefix := log.Prefix()
	// zerolog ships an adapter that satisfies io.Writer and emits each
	// stdlib log line as an info-level JSON record. Suppress stdlib's
	// own timestamp/prefix so we don't double-stamp.
	log.SetFlags(0)
	log.SetPrefix("")
	log.SetOutput(stdlibAdapter{l: l.z})
	return func() {
		log.SetFlags(prevFlags)
		log.SetPrefix(prevPrefix)
		log.SetOutput(os.Stderr)
	}
}

// stdlibAdapter routes stdlib log writes into a zerolog logger as
// info-level entries. The stdlib log package writes one entry per call
// already; we just trim the trailing newline so the JSON message field
// stays clean.
type stdlibAdapter struct {
	l zerolog.Logger
}

// Write implements io.Writer.
func (a stdlibAdapter) Write(p []byte) (int, error) {
	msg := strings.TrimRight(string(p), "\r\n")
	if msg == "" {
		return len(p), nil
	}
	a.l.Info().Str("source", "stdlib_log").Msg(msg)
	return len(p), nil
}
