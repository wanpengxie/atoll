// Package actorstore is the sole keeper of actor RECORDS — who an actor is and
// what it is. Belongings (state, timers, grants, resources, message history)
// stay with their own domains, keyed by ActorID; a record verb here touches
// nothing but the record.
//
// There is only one kind of managed actor and one durable record path.
package actorstore
