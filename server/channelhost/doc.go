// Package channelhost is the v2 channel-home: it COMPOSES the deployment-agnostic
// core (storespec interfaces + runtime/harness + lib/channelkit) into a process
// that HOLDS channel truth. This is the v2 truth-flip: truth physically lives at
// the server, not the daemon; the server is no longer a view-cache.
//
// Assembly:
//  1. Caller opens store via runtime.OpenChannel -> storespec interfaces, passes
//     them into channelhost.Config.Stores.
//  2. harness.New(Deps{Log, ActorRegistry, ChannelID}) -> *Chain (9-step write path).
//  3. fanoutWriter wraps Chain: Write success -> Deliverer.Deliver(local) +
//     remoteDispatch(wire) + pushHub.notify(client).
//  4. channelkit.New(Config{System, Writer, OpenRequests}) -> *Channel
//     (assembles actorrt + sysactor + death edge wiring).
//  5. channel genesis: register system actor.
//
// Depends on runtime + lib + wire. MUST NOT import daemon or concrete adapters.
package channelhost
