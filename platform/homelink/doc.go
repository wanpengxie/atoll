// Package homelink connects an attached compute (daemon) to its channel home
// (server) over a computebus WebSocket: attach with the api-key (lightcone
// pattern: one key, one URL), keep the placement lease fresh via Heartbeat,
// receive DispatchFrames for hosted cells, and send EmitFrames + DeathFrames up.
//
// EmitID correlation (which EmitAck matches which EmitFrame) and timeout are
// owned here, not by UplinkWriter. UplinkWriter blocks on homelink.Emit and
// receives the resolved EmitAck.
//
// Depends on wire/computebus + kernel/actor. MUST NOT import server.
package homelink
