package access

// Operation is the substrate-meaningful verb set of the access plane — the CLOSED SET
// that falls out of the resource lifecycle plus content access: an object is created /
// mutated / destroyed, and its bytes are read. The kernel must distinguish these
// because each carries a distinct lifecycle/authorization meaning the door enforces (create
// is CONTAINER-gated and IS ownership, write mutates existing content,
// non-lossy delete, side-effect-free read). It is NOT the driver's FINE verb (sign/hmac/
// select ride in Args, opaque — the substrate guards structure, not vocabulary). Closed
// set; adding a verb is a protocol revision (use is a known additive extension, not
// pre-reserved). There is NO per-object grant verb: the membrane is a uniform
// trust phase (PM-D1) — read/write authorization is membership itself, delete
// additionally distinguishes the creator (PM-D3) — so R and its set verb have
// no place in the model.
type Operation string

const (
	// OpCreate — birth: allocate a NEW ResourceID with initial bytes (Args); creation IS
	// ownership — the creator atomically becomes controller (full grant in R). Authorized
	// at the CONTAINER (channel membership: member ⟹ create-own), NOT by R: the object/R does
	// not exist yet, so there is nothing object-level to check (POSIX checks the directory to
	// create, the file to access; K8s create is namespace-scoped). Distinct from OpWrite
	// precisely because the authz locus differs. Create on an existing id → already_exists:
	// atomic test-and-set on existence, no silent re-create / controller grab. DAY-1 the
	// caller PROPOSES the ResourceID (deterministic, like a path/key). Driver-GENERATED-and-
	// returned id is DEFERRED, not pre-reserved: when added, the created id rides the ipc
	// success ack (transport), not the protocol.
	OpCreate Operation = "create"

	// OpRead — the object's bytes flow OUT to the caller. Side-effect-free, idempotent,
	// the safe-retry baseline. What the caller then does with the bytes (feed an LLM or
	// not) is the actor's concern, not the door's.
	OpRead Operation = "read"

	// OpWrite — caller bytes flow IN, mutating EXISTING content (birth is OpCreate, not write).
	// Write on a non-existent id → resource_not_found (you cannot update what is not there).
	// Side-effecting; retry safety is the driver's call (PUT idempotent, append not).
	OpWrite Operation = "write"

	// OpDelete — the object's explicit death. NON-LOSSY: an access-plane object dies
	// ONLY by an explicit op=delete (channel-scoped: the creator or the channel
	// owner, PM-D3) or when its owning scope dies — channel-scoped objects with the
	// channel destroy, actor-scoped objects with
	// their owner's deregister (the scope law). Scope death is not a lossy auto-destroy:
	// the object outlives everything short of its scope, and no living scope ever
	// silently drops it. Deletes by id — it has NO operand. Side-effecting.
	OpDelete Operation = "delete"
)

// allOperations backs ParseOperation. UNEXPORTED: the closed-set contract is the
// ParseOperation predicate, not a mutable enumeration slice (an exported slice
// would let an importer rewrite the protocol closed set at runtime).
var allOperations = []Operation{OpCreate, OpRead, OpWrite, OpDelete}

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
