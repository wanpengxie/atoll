// Package supervisor will host the worker_locks CAS + supervisor main
// loop + backlog scan introduced by ticket T6
// (.dalek/pm/m1.3-tickets.md §T6).
//
// T6 is the most complex M1.3 ticket (L sized): worker spawn / steal /
// heartbeat / fencing + turn-replay semantics all live here.
package supervisor
