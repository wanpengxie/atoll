// Package homelink connects an attached compute (daemon) to its channel home
// (server) over wire/computebus: attach with the api-key (lightcone-style, one
// key + one URL), keep the placement lease fresh via Heartbeat, receive
// DispatchFrames for hosted cells, and send EmitFrames + DeathFrames up.
//
// Port from: adapters/proxy/daemon connection logic + server/daemonbus client
// side. v2: cloud daemon and user/proxy daemon are the SAME binary/concept.
//
// Depends on runtime + lib + wire. MUST NOT import server.
package homelink

// Port anchors (read 2026-06-02):
//   - adapters/proxy/daemon/transport.go:21  Dial(serverWS, apiKey) → WSConnection
//   - adapters/proxy/daemon/transport.go:86  ReadFrame / :98 WriteFrame (gorilla/ws)
//   - adapters/proxy/daemon/daemon.go        attach + heartbeat loop
// Adapt the device-frame protocol to computebus.{AttachRequest, Heartbeat,
// DispatchFrame(down), EmitFrame(up), DeathFrame}.
