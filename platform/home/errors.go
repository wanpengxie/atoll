package home

import (
	"errors"
	"time"
)

const introductionResolveTimeout = 2 * time.Second

var (
	ErrEndSystemForbidden = errors.New("platform: system actor cannot use managed terminal lifecycle")
	ErrEndNotSponsor      = errors.New("platform: caller is not target sponsor")
	ErrEndNotMember       = errors.New("platform: actor is not active")
)
