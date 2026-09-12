// Package ledgerview binds the channel ledger's read face to an actor
// authority. A minted handle checks that live authority at every read before
// delegating to the platform-owned ledger implementation.
package ledgerview
