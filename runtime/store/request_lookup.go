package store

import (
	"context"

	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// RequestLookup is the sqlite-backed implementation of
// kernel/adapter.RequestLookup over a channel's messages table. It is a
// thin wrapper around store.Messages.FindByID that adapts the
// value-typed Envelope return to the pointer-typed contract Manager
// expects (so callers can mutate the envelope when constructing
// response patches).
//
// One *RequestLookup per channel sqlite. Safe for concurrent use.
type RequestLookup struct {
	messages  *Messages
	channelID channel.ID
}

// NewRequestLookup wraps a Messages store. channelID is plumbed for
// safety even though Messages already scopes by per-channel sqlite —
// keeping the parameter makes the cross-channel invariant explicit and
// future-proofs the wiring should the same Messages instance ever
// service multiple channels.
func NewRequestLookup(messages *Messages, channelID channel.ID) *RequestLookup {
	return &RequestLookup{messages: messages, channelID: channelID}
}

// FindByID satisfies kernel/adapter.RequestLookup. Returns nil envelope
// + ok=false when the row is missing.
func (r *RequestLookup) FindByID(ctx context.Context, id string) (*message.Envelope, bool, error) {
	env, ok, err := r.messages.FindByID(ctx, r.channelID, id)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}
	cp := env
	return &cp, true, nil
}

// Compile-time interface check.
var _ adapter.RequestLookup = (*RequestLookup)(nil)
