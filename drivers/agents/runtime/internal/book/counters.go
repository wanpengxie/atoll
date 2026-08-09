// Package book contains Runtime's pure, handle-free transition primitives.
package book

// Counters owns every monotonic, incarnation-local Runtime identity. Zero is
// never returned and overflow is a terminal contract fault for the caller.
type Counters struct {
	generation uint64
	attempt    uint64
	action     uint64
	revision   uint64
}

func next(value *uint64) (uint64, bool) {
	*value = *value + 1
	if *value == 0 {
		return 0, false
	}
	return *value, true
}
func (c *Counters) Generation() (uint64, bool) { return next(&c.generation) }
func (c *Counters) Attempt() (uint64, bool)    { return next(&c.attempt) }
func (c *Counters) Action() (uint64, bool)     { return next(&c.action) }
func (c *Counters) Revision() (uint64, bool)   { return next(&c.revision) }
