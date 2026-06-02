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
