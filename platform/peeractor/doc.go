// Package peeractor implements a channel-local member representing one remote
// channel. It turns local requests into protocol/channel peer frames, using
// EffectiveCaller as the frame origin, and forwards them through the channel's
// service port without exposing transport details to ordinary actors.
//
// Each request has an independent forwarding task. Provisional peer progress
// is relayed to the local pending call; the final peer result becomes exactly
// one local reply or failure.
package peeractor
