// Package actorcaps defines Caps — the substrate-injected capability bundle an
// actor cell is born holding. It is the plane-of-birth contract between the
// platform assembly root (which mints and wires the capabilities) and the actor
// implementation (which receives them and does nothing to obtain them itself):
// the factory an actor author writes is `func(Caps) actorrt.Actor`.
//
// WHY A LEAF PACKAGE (not platform root, not runtime):
//
//   - Caps is pure composition over runtime capability VOCABULARY
//     (harness.Pen / accessdoor.AccessHandle / schedule.ScheduleHandle /
//     actorcaps.LifecycleHandle). It carries no platform-assembly machinery (no link,
//     no tap, no WS transport, no sysactor). An actor implementation must be
//     able to NAME its birth capabilities without importing the heavy platform
//     assembly root — so this type does not live in package platform.
//   - It cannot live in runtime/actorrt (actorrt must never import
//     runtime/harness) nor in harness/accessdoor/schedule (none of
//     those may reach across to the others). Caps sits ABOVE all four, which is
//     a downstream (lib) concern, not a runtime one — placed here rather than
//     inventing a new runtime package.
//   - As a lib leaf it is importable by both the platform assembly root and actor
//     implementations without either side importing the other's assembly code.
package actorcaps
