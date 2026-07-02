package store

import (
	"context"

	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// requestLookupStore is the sqlite-backed implementation of
// runtime/storespec.RequestLookup over a channel's messages table. It is a
// thin wrapper around store.messages.FindByID that adapts the value-typed
// Envelope return to the pointer-typed contract callers expect (so they can
// mutate the envelope when constructing response patches).
//
// One *requestLookupStore per channel sqlite. Safe for concurrent use. No
// channelID is held: the messages store is already bound to one channel's
// sqlite, so a plumbed channelID would be a pseudo-field (the binding is the
// scope, not a per-call argument).
type requestLookupStore struct {
	messages *messages
}

// newRequestLookup wraps a messages store.
func newRequestLookup(messages *messages) *requestLookupStore {
	return &requestLookupStore{messages: messages}
}

// FindByID satisfies runtime/storespec.RequestLookup. Returns nil envelope
// + ok=false when the row is missing.
func (r *requestLookupStore) FindByID(ctx context.Context, id message.ID) (*message.Envelope, bool, error) {
	env, ok, err := r.messages.FindByID(ctx, id)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}
	return &env.Envelope, true, nil
}

// Compile-time interface check.
var _ storespec.RequestLookup = (*requestLookupStore)(nil)
