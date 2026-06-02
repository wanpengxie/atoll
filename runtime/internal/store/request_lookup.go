package store

import (
	"context"

	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/runtime/storespec"
)

// requestLookupStore is the sqlite-backed implementation of
// kernel/storespec.RequestLookup over a channel's messages table. It is a
// thin wrapper around store.messages.FindByID that adapts the
// value-typed Envelope return to the pointer-typed contract Manager
// expects (so callers can mutate the envelope when constructing
// response patches).
//
// One *requestLookupStore per channel sqlite. Safe for concurrent use.
type requestLookupStore struct {
	messages  *messages
	channelID channel.ID
}

// NewRequestLookup wraps a messages store. channelID is plumbed for
// safety even though messages already scopes by per-channel sqlite —
// keeping the parameter makes the cross-channel invariant explicit and
// future-proofs the wiring should the same messages instance ever
// service multiple channels.
func newRequestLookup(messages *messages, channelID channel.ID) *requestLookupStore {
	return &requestLookupStore{messages: messages, channelID: channelID}
}

// FindByID satisfies kernel/storespec.RequestLookup. Returns nil envelope
// + ok=false when the row is missing.
func (r *requestLookupStore) FindByID(ctx context.Context, id message.ID) (*message.Envelope, bool, error) {
	env, ok, err := r.messages.FindByID(ctx, r.channelID, id)
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
