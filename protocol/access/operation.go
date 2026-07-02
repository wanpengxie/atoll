package access

// Operation is the substrate-meaningful verb set of the access plane — the CLOSED SET
// that falls out of the resource lifecycle (§1.3) + content access: an object is created /
// mutated / granted / destroyed, and its bytes are read. The kernel must distinguish these
// because each carries a distinct lifecycle/authorization meaning the door enforces (create
// is CONTAINER-gated and IS ownership, write mutates existing content, set replaces a grant,
// non-lossy delete, side-effect-free read). It is NOT the driver's FINE verb (sign/hmac/
// select ride in Args, opaque — 守结构不守词汇). Closed set; adding a verb is a protocol
// revision (use is the one KNOWN additive extension, §9 — not pre-reserved; transfer is NOT
// an op, it is set with control-level Ops).
type Operation string

const (
	// OpCreate — birth: allocate a NEW ResourceID with initial bytes (Args); creation IS
	// ownership — the creator atomically becomes controller (full grant in R), §1.3. Authorized
	// at the CONTAINER (channel membership: member ⟹ create-own), NOT by R: the object/R does
	// not exist yet, so there is nothing object-level to check (POSIX checks the directory to
	// create, the file to access; K8s create is namespace-scoped). Distinct from OpWrite
	// precisely because the authz locus differs. Create on an existing id → already_exists
	// (§3.3): atomic test-and-set on existence, no silent re-create / controller grab. DAY-1 the
	// caller PROPOSES the ResourceID (deterministic, like a path/key). Driver-GENERATED-and-
	// returned id is DEFERRED (零预留): when added, the created id rides the ipc success ack
	// (transport), not proto — §3.1/§3.5.
	OpCreate Operation = "create"

	// OpRead — the object's bytes flow OUT to the caller. Side-effect-free, idempotent,
	// the safe-retry baseline (forward §12.8). What the caller then does with the bytes
	// (feed an LLM or not = lock③) is the actor's concern, not the door's.
	OpRead Operation = "read"

	// OpWrite — caller bytes flow IN, mutating EXISTING content (birth is OpCreate, not write).
	// Write on a non-existent id → resource_not_found (you cannot update what is not there).
	// Side-effecting; retry safety is the driver's call (PUT idempotent, append not — §12.8).
	OpWrite Operation = "write"

	// OpSet — SET (replace) a subject's grant in the object's authorization relation R
	// (§2.4): chmod/setfacl semantics, NOT seL4 add-only — Grant.Ops is the grantee's NEW
	// grant, and Ops=∅ REVOKES (so revoke needs no separate op). Idempotent (retry-safe).
	// Governed: the door requires the caller hold set-right (day-1 the controller). The
	// grant spec is the proto type access.Grant in the typed Invocation.Grant field (§3.2.1) —
	// NOT opaque Args — because the substrate authz manager decodes it to write R and both
	// wire ends must agree on its shape (认证判准 + envelope/payload rule). Set's executor is
	// the substrate, not a driver. DAY-1 Grant.Ops ⊆ {read,write} (granting set/delete =
	// delegating control = §11). TRANSFER (chown) is NOT a separate op: with no separate
	// owner field (control = full grant in R), transfer = set(Y, full) + set(self, ∅), a §11
	// Ops-policy widening.
	OpSet Operation = "set"

	// OpDelete — the object's explicit death. NON-LOSSY (§1.2 后果③): an access-plane
	// object dies ONLY by an explicit op=delete (a full-rights holder) or when its
	// owning scope dies — channel-scoped objects with the channel destroy, actor-scoped
	// objects with their owner's deregister (the scope law, forward §12.1.5). Scope death
	// is not a lossy auto-destroy: the object outlives everything short of its scope, and
	// no living scope ever silently drops it. Deletes by id — it has NO operand.
	// Side-effecting.
	OpDelete Operation = "delete"
)

// allOperations backs ParseOperation. UNEXPORTED: the closed-set contract is the
// ParseOperation predicate, not a mutable enumeration slice (an exported slice
// would let an importer rewrite the protocol closed set at runtime).
var allOperations = []Operation{OpCreate, OpRead, OpWrite, OpSet, OpDelete}

// String returns the wire form.
func (o Operation) String() string { return string(o) }

// ParseOperation resolves a canonical wire-form string against the closed set.
// Deserialization (wire / dispatch) MUST go through ParseOperation rather than a
// bare access.Operation(string) cast so an out-of-set value cannot enter the ADT.
func ParseOperation(raw string) (Operation, bool) {
	for _, o := range allOperations {
		if string(o) == raw {
			return o, true
		}
	}
	return "", false
}
