package actorbase

import (
	"context"
	"time"
)

// IdleArbiter receives a non-blocking idle request. Approval may be returned
// directly by simple hosts or delivered later through IdleApproved; production
// Home uses the latter so the approval shares the carrier's ordered ingress.
type IdleArbiter interface {
	RequestIdle(context.Context) (approved bool, err error)
}

type Options struct {
	IdleTimeout time.Duration
	IdleArbiter IdleArbiter
}
