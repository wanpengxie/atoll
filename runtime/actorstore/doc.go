// Package actorstore is the sole keeper of actor RECORDS — who an actor is and
// what it is. Belongings (state, timers, grants, resources, message history)
// stay with their own domains, keyed by ActorID; a record verb here touches
// nothing but the record.
//
// There is only one kind of managed actor. Durable and entry are not actor
// kinds, worlds or lifecycle types — they are two places the SAME ActorRecord
// can live, chosen by the operation that created it (declaration birth →
// durable, fork → entry). The classification fact has exactly one physical
// existence: membership of the process entry table. It is never a field on a
// record, never travels on the wire, and has exactly one reader (the state
// organ's backing route, via IsEntry).
package actorstore
