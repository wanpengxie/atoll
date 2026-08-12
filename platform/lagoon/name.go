package lagoon

import (
	"errors"
	"strings"
)

// The registry is the only place a device or channel name is ever minted, so
// the name law lives here with the mint. One law serves both: a device name and
// one channel label are the same kind of word, and there is no second copy of
// this function anywhere else in the tree.

// ValidateName accepts one lowercase DNS-style label: 1-63 ASCII lowercase
// letters, digits, or hyphens, with a letter or digit at both ends.
func ValidateName(name string) error {
	if len(name) < 1 || len(name) > 63 {
		return errors.New("lagoon: invalid name")
	}
	for i := 0; i < len(name); i++ {
		b := name[i]
		if b >= 'a' && b <= 'z' || b >= '0' && b <= '9' {
			continue
		}
		if b == '-' && i > 0 && i < len(name)-1 {
			continue
		}
		return errors.New("lagoon: invalid name")
	}
	return nil
}

// maxQualifiedName bounds the whole dotted name, because that whole name is
// also one directory name on every daemon that holds the channel. 255 is the
// single-component limit on the filesystems we run on; a name longer than this
// would mint fine and then never be able to exist on disk.
const maxQualifiedName = 255

// ValidateQualifiedName accepts the sole dotted spelling of a channel name.
// The c0 root is always explicit and every component obeys the same name law.
func ValidateQualifiedName(name string) error {
	if len(name) > maxQualifiedName {
		return errors.New("lagoon: qualified channel name too long")
	}
	parts := strings.Split(name, ".")
	if parts[0] != "c0" {
		return errors.New("lagoon: invalid qualified channel name")
	}
	for _, part := range parts {
		if ValidateName(part) != nil {
			return errors.New("lagoon: invalid qualified channel name")
		}
	}
	return nil
}

// JoinName constructs a child's sole qualified spelling from its parent and one
// already-normalized channel label.
func JoinName(parent, name string) (string, error) {
	if ValidateQualifiedName(parent) != nil || ValidateName(name) != nil {
		return "", errors.New("lagoon: invalid qualified channel name")
	}
	joined := parent + "." + name
	if err := ValidateQualifiedName(joined); err != nil {
		return "", err
	}
	return joined, nil
}
