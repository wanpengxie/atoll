package actorbase

import (
	"context"
	"time"
)

// IdleArbiter receives a non-blocking idle request. Approval is always delivered
// later through IdleApproved so it shares the carrier's ordered ingress.
type IdleArbiter interface {
	RequestIdle(context.Context) error
}

type Options struct {
	IdleTimeout time.Duration
	IdleArbiter IdleArbiter
}
