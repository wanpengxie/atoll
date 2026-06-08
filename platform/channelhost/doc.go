// Package channelhost is the v2 channel-home business layer: it assembles
// stores + channelkit (actorrt + sysactor + death edge wiring) into the
// per-channel truth holder. It receives a harness.Writer via Config (injected
// by the assembly root) and does NOT own the write-fanout path or client push.
//
// Assembly:
//  1. Assembly root opens store via runtime.OpenChannel -> storespec interfaces,
//     passes them into channelhost.Config.Stores.
//  2. Assembly root creates the write chain (harness + postCommitWriter) and
//     injects it as Config.Writer.
//  3. channelhost.New bootstraps system actor membership, creates sysactor and
//     channelkit around the injected Writer.
//  4. Assembly root reads back Runtime() and Deliverer() to wire fleet and the
//     postCommitWriter.
//
// Depends on runtime + lib. MUST NOT import fleet, wire, gateway, daemon, or
// concrete adapters.
package channelhost
