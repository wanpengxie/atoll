package storespec

import "context"

// AdapterState is the channel-local adapter key/value store (opaque binary
// values) — channel-local persistence for adapter runtime state. Valid v2
// (server-truth-side channel-local persistence; unlike the deleted daemon-local
// store it does not violate daemon=no-truth). Forward-derived (§4.5); the
// adapter-runtime consumer (lib/adapters) adapts to it.
type AdapterState interface {
	Get(ctx context.Context, key string) ([]byte, bool, error)
	Put(ctx context.Context, key string, value []byte) error
	Delete(ctx context.Context, key string) error
	List(ctx context.Context, prefix string) ([]string, error)
}

// AdapterCredentials is the channel-local sealed credential store — values are
// encrypted at rest via the store's injected SecretBox; callers see plaintext
// strings.
type AdapterCredentials interface {
	Get(ctx context.Context, key string) (string, bool, error)
	Put(ctx context.Context, key, value string) error
	Delete(ctx context.Context, key string) error
}
