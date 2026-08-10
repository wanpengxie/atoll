package storespec

import "context"

// ChannelGenesis is the immutable self-description stored inside one channel
// database. Names intentionally do not appear here: they are space routing
// metadata, not channel identity.
type ChannelGenesis struct {
	ChannelID          string
	Type               string
	OwnerPrincipal     string
	ParentChannelID    string
	InitiatorPrincipal string
	CreatedAt          int64
}

type GenesisStore interface {
	CreateGenesis(context.Context, ChannelGenesis) error
	ReadGenesis(context.Context) (ChannelGenesis, bool, error)
}
