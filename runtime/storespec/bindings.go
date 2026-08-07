package storespec

import (
	"context"
)

// DaemonID names one channel↔daemon wiring row. The binding row belongs to the
// wiring domain alone: attaching or detaching a daemon never touches an actor
// record (a dangling placement is a legal state, §5.6).
type DaemonID string

// DaemonBindingStore is the wiring domain's bare typed write face. Both verbs
// are last-write-wins settings, so they carry no dedup ceremony.
type DaemonBindingStore interface {
	AttachDaemon(ctx context.Context, id DaemonID, at int64) (created bool, err error)
	DetachDaemon(ctx context.Context, id DaemonID) (removed bool, err error)
	IsBound(context.Context, DaemonID) (bool, error)
	ListBoundDaemons(context.Context) ([]DaemonID, error)
}

// DaemonBindingReader is the read-only half handed to Platform projections.
type DaemonBindingReader interface {
	IsBound(context.Context, DaemonID) (bool, error)
	ListBoundDaemons(context.Context) ([]DaemonID, error)
}
