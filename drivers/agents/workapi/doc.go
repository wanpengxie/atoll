// Package workapi owns the work-aware interface advertised by Agent drivers.
//
// It deliberately does not describe a driver, provider, process, or scheduler.
// A native message Agent and an adapter around a black-box Agent speak the same
// words when they advertise the same protocol capability. Implementations may
// have one execution lane or many; capability discovery says which is true.
package workapi
