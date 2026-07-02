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
//     actorrt.SpawnHandle). It carries no platform-assembly machinery (no link,
//     no tap, no WS transport, no sysactor). An actor implementation must be
//     able to NAME its birth capabilities without importing the heavy platform
//     assembly root — so this type does not live in package platform.
//   - It cannot live in runtime/actorrt (actorrt must never import
//     runtime/harness — fork.go) nor in harness/accessdoor/schedule (none of
//     those may reach across to the others). Caps sits ABOVE all four, which is
//     a downstream (lib) concern, not a runtime one — the wiring期 places it
//     here rather than inventing a new runtime package.
//   - As a lib leaf it is importable by BOTH the platform assembly root and by
//     channelkit (which cannot import platform — platform imports channelkit,
//     so a Caps in platform root would be a cycle the moment channelkit needs
//     to name a participant factory).
package actorcaps
