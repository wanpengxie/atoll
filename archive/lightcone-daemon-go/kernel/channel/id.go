// Package channel holds the ChannelId scalar and ChannelRef (federation
// addressing). v5 channels are the atomic unit of harness + ledger +
// message-log; cross-channel addressing flows through ChannelRef.
//
// kernel/channel is IO-free. Channel-local sqlite stores live in
// runtime/store per T3.
package channel

// ChannelId is the channel naming scalar (uuid v7 or
// hexadecimal-encoded random per spec).
//
// TODO(T1): finalize naming convention + validator.
type ChannelId string

// String returns the wire form of c.
func (c ChannelId) String() string { return string(c) }
