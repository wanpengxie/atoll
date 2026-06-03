package behavior

import (
	"log/slog"
	"time"
)

// Deps is the construction bundle a composition root injects into an adapter
// Module constructor — only the seams a Module actually needs (HTTP client /
// credential store / observability / clock). All fields optional; adapters
// tolerate nils for what they don't use.
type Deps struct {
	HTTPClient      *HTTPClient
	CredentialStore CredentialStore
	Logger          *slog.Logger
	Metrics         Metrics
	Clock           func() time.Time
}
