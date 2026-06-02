package store_test

import (
	"context"
	"database/sql"

	"github.com/wanpengxie/ActOS/runtime/fence"
	"github.com/wanpengxie/ActOS/runtime/internal/store"
)

// fakeFence is a pure-substrate WriteFence that accepts ONLY the configured
// (token, epoch) tuple and returns *store.FencingStaleError otherwise. It
// mirrors the daemon-side ChannelLock gate WITHOUT importing the
// release-specific framework layer, so runtime/store tests stay extractable
// (substrate must compile its own test suite with kernel + store only).
//
//nolint:unused // retained (reaper/correlation will wire; xhs allowlist schema)
func fakeFence(token fence.FencingToken, epoch fence.DaemonEpoch) store.WriteFence {
	return store.WriteFenceFunc(func(_ context.Context, _ *sql.Tx, tok fence.FencingToken, ep fence.DaemonEpoch) error {
		if tok != token || ep != epoch {
			return &store.FencingStaleError{HaveToken: token, GotToken: tok, HaveEpoch: epoch, GotEpoch: ep}
		}
		return nil
	})
}
