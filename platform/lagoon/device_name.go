package lagoon

import (
	"errors"
	"regexp"
)

var deviceNamePattern = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)

// ValidateDeviceName is the single mint/claim/boot gate for human-readable
// daemon names. Names are single-label and are never normalized.
func ValidateDeviceName(name string) error {
	if !deviceNamePattern.MatchString(name) {
		return errors.New("lagoon: invalid device name")
	}
	return nil
}
