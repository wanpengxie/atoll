package behavior

import (
	"log/slog"
	"time"
)

// Deps is the construction bundle a composition root injects into an adapter
// Module constructor — only the seams a Module actually needs (HTTP client /
// credential store / state / observability / clock). All fields optional;
// adapters tolerate nils for what they don't use.
type Deps struct {
	HTTPClient      *HTTPClient
	CredentialStore CredentialStore
	StateStore      StateStore
	Logger          *slog.Logger
	Metrics         Metrics
	Tracer          Tracer
	Clock           func() time.Time
}

// Factory is the adapter constructor contract the composition root (cmd)
// registers and invokes with a Deps bundle.
type Factory func(deps Deps) Module
