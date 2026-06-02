package behavior

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

)

// CredentialStore is the F8 contract every adapter uses to fetch
// secrets (api keys, tokens, signing material). Production wires a
// secure backend; tests use NewMemoryCredentialStore.
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

// CredentialStoreReceiver is implemented by modules that need credentials.
// Manager injects a scoped CredentialStore after Declaration validation and
// before Init, so adapters keep using logical keys such as "feishu.app_secret".
type CredentialStoreReceiver interface {
	SetCredentialStore(CredentialStore)
}

// ErrCredentialMissing is wrapped by adapters when a required credential
// is absent. Distinct from infrastructure errors so callers can decide
// whether to retry vs fail-fast.
var ErrCredentialMissing = errors.New("framework: credential missing")

// MemoryCredentialStore is the default in-memory CredentialStore. It is
// safe for concurrent use and zero-value-usable via NewMemoryCredentialStore.
type MemoryCredentialStore struct {
	mu   sync.RWMutex
	data map[string]string
}

// NewMemoryCredentialStore returns an empty MemoryCredentialStore.
func NewMemoryCredentialStore() *MemoryCredentialStore {
	return &MemoryCredentialStore{data: map[string]string{}}
}

// Get returns the stored value for key. Returns ok=false when absent.
func (s *MemoryCredentialStore) Get(_ context.Context, key string) (string, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	return v, ok, nil
}

// Put writes value at key.
func (s *MemoryCredentialStore) Put(_ context.Context, key, value string) error {
	if key == "" {
		return errors.New("framework: credential key required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
	return nil
}

// Delete removes a credential by key. No-op when absent.
func (s *MemoryCredentialStore) Delete(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
	return nil
}

// ScopedCredentialStore wraps a CredentialStore and prefixes every key with
// an adapter-specific namespace. The namespace is invisible to adapters.
type ScopedCredentialStore struct {
	Inner CredentialStore
	Scope string
}

// NewScopedCredentialStore returns a CredentialStore scoped under scope.
func NewScopedCredentialStore(inner CredentialStore, scope string) *ScopedCredentialStore {
	return &ScopedCredentialStore{Inner: inner, Scope: scope}
}

// NewScopedCredentialStoreForDeclaration derives the adapter credential
// scope from framework-owned declaration metadata.
func NewScopedCredentialStoreForDeclaration(inner CredentialStore, decl Declaration) *ScopedCredentialStore {
	return NewScopedCredentialStore(inner, CredentialScopeForDeclaration(decl))
}

// CredentialScopeForDeclaration is the physical namespace Manager uses for
// adapter credentials. It intentionally combines name and actor id so two
// installed adapters cannot share credentials by colliding on logical keys.
func CredentialScopeForDeclaration(decl Declaration) string {
	if decl.Name == "" && decl.ActorID == "" {
		return ""
	}
	return "adapter:" + decl.Name + ":actor:" + string(decl.ActorID)
}

func (s *ScopedCredentialStore) Get(ctx context.Context, key string) (string, bool, error) {
	return s.Inner.Get(ctx, s.scope(key))
}

func (s *ScopedCredentialStore) Put(ctx context.Context, key, value string) error {
	return s.Inner.Put(ctx, s.scope(key), value)
}

func (s *ScopedCredentialStore) Delete(ctx context.Context, key string) error {
	return s.Inner.Delete(ctx, s.scope(key))
}

func (s *ScopedCredentialStore) scope(key string) string {
	if s.Scope == "" {
		return key
	}
	return s.Scope + ":" + key
}

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
