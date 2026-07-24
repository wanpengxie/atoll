// Package humancell is the home-side human actor's body (platform 拓扑批
// T2): the frame interpreter + mailbox serve loop that answers a human
// subject's own requests and drives the person's own actions onto the cell's
// welded caps.
//
// It is 受养驱动 (domain 件), not substrate law: the cell lives INSIDE the
// membrane because it holds a 受养特权 (a per-identity slot — the在场与递交
// 接头盒 the gateway drives frames through), but its request-type table
// (human.message/human.approve/…), its error codes, and its verb vocabulary
// are all DOMAIN words — the "human subject" convention, not a substrate
// invariant every actor must obey. platform/subjectgate is the
// substrate seam (registry + slot + wire frame contract) this body drives;
// platform/home is the assembly root that wires a live cell per admitted
// member (humancell_wiring.go: humanCellFactory/runHumanCell) — this
// package holds none of that assembly, only the body Home hands a Sys to
// run.
//
// Exported surface (the wiring seam, five names): Deps (the interpreter's
// injected read-only face), RequestLookup (Deps' from-log recovery
// interface), InterpretFrames (the frame-interpreter goroutine loop),
// HumanServe (the mailbox serve loop), WirePresenceSelfReport (the
// device-presence self-report wiring against a slot). Everything else stays
// unexported — a private interpreter/verb-mapping implementation platform's
// wiring shell never reaches into.
package humancell
