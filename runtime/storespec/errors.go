package storespec

import "errors"

// ErrConflictExists reports that an enforced singleton already has an active
// member with the same kind and seed.
var ErrConflictExists = errors.New("conflict_exists")
