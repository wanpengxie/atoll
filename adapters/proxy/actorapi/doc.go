// Package actorapi declares the local proxy daemon ActorModule interface.
//
// It is a contract package only. User-side proxy daemon implementations host
// modules through this interface; server-side code must continue to observe
// actor state through envelope-visible calls or events rather than by importing
// module implementations.
package actorapi
