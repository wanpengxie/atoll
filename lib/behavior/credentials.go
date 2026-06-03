package behavior

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// CredentialStore is the contract an adapter uses to fetch secrets (api
// keys, tokens, signing material). The composition root wires a concrete
// backend.
//
// Implementations MUST:
//
//   - Treat Get / Put as concurrent-safe.
//   - Refuse to return values for unknown keys (return ok=false).
//   - Never log the value through their own logger (callers redact
//     before logging via the package-level Redact helper).
type CredentialStore interface {
	Get(ctx context.Context, key string) (value string, ok bool, err error)
	Put(ctx context.Context, key, value string) error
	Delete(ctx context.Context, key string) error
}

// ErrCredentialMissing is wrapped by adapters when a required credential
// is absent. Distinct from infrastructure errors so callers can decide
// whether to retry vs fail-fast.
var ErrCredentialMissing = errors.New("behavior: credential missing")

// Redact returns a log-safe rendering of `secret`. The transformation
// keeps the first and last MinKeepEdge characters and replaces the
// middle with `***`, so short secrets are still fully masked. Empty
// strings stay empty. Inputs shorter than 2*MinKeepEdge+1 are masked
// entirely as `***`.
//
// MUST be called before passing any credential-bearing string to a
// logger / error wrapper. Use RedactError to scrub a wrapped error
// message in a single call.
func Redact(secret string) string {
	if secret == "" {
		return ""
	}
	const edge = MinKeepEdge
	if len(secret) <= 2*edge {
		return "***"
	}
	return secret[:edge] + "***" + secret[len(secret)-edge:]
}

// MinKeepEdge is the number of leading + trailing characters Redact
// preserves when the input is long enough.
const MinKeepEdge = 2

// RedactSubstrings returns msg with every occurrence of each `secret`
// (non-empty) replaced by its Redact rendering. Used to scrub error
// payloads that may have embedded the raw secret.
func RedactSubstrings(msg string, secrets ...string) string {
	out := msg
	for _, s := range secrets {
		if s == "" {
			continue
		}
		out = strings.ReplaceAll(out, s, Redact(s))
	}
	return out
}

// RedactError returns a new error whose Error() text has every secret
// substring replaced by Redact(secret). The error keeps Unwrap chain
// intact so callers can still errors.Is / errors.As against the
// original.
func RedactError(err error, secrets ...string) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	red := RedactSubstrings(msg, secrets...)
	if red == msg {
		return err
	}
	return &redactedError{wrapped: err, msg: red}
}

type redactedError struct {
	wrapped error
	msg     string
}

func (e *redactedError) Error() string { return e.msg }
func (e *redactedError) Unwrap() error { return e.wrapped }

// MissingCredentialError formats a friendly error message wrapping
// ErrCredentialMissing. Callers receive both the key (in the message)
// AND the ability to errors.Is against ErrCredentialMissing.
func MissingCredentialError(key string) error {
	return fmt.Errorf("%w: %s", ErrCredentialMissing, key)
}
