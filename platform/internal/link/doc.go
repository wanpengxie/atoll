// Package link is the byte-level carrier/lane mechanism shared by the space
// daemon host and device runtime. One authenticated WS owns one yamux session,
// one carrier spine, any number of server-opened lane_control streams, and
// device-opened actor streams stamped with an immutable (channel,laneGen)
// header. Lane termination is always local retirement; only session or carrier
// spine failure terminates the carrier.
package link
