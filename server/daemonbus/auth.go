package daemonbus

import (
	"crypto/sha256"
	"crypto/subtle"
)

func sharedSecretEqual(presented, expected string) bool {
	presentedHash := sha256.Sum256([]byte(presented))
	expectedHash := sha256.Sum256([]byte(expected))
	return subtle.ConstantTimeCompare(presentedHash[:], expectedHash[:]) == 1
}
