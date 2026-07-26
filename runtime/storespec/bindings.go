package storespec

import (
	"context"

	"github.com/wanpengxie/atoll/protocol/actor"
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

// ChannelRouting owns the channel default-agent pointer. The pointer is channel
// configuration, not a dead actor's belonging: terminal never clears it, and a
// pointer at a deregistered actor simply reads as unconfigured.
type ChannelRouting interface {
	DefaultAgent(context.Context) (actor.ActorID, bool, error)
	SetDefaultAgent(context.Context, actor.ActorID) error
}
