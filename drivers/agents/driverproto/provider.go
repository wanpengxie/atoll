// Package driverproto defines the provider-neutral contract between the
// shared agent runtime and provider adapters.
package driverproto

import (
	"context"

	"github.com/wanpengxie/atoll/lib/introspect"
)

type Provider interface {
	Spec() ProviderSpec
	NewAdapter() (Adapter, error)
}

// Adapter is an immutable, resource-free worker factory.
type Adapter interface {
	NewWorker(WorkerHost) (Worker, error)
}

// Worker owns every physical resource in one runtime generation.
type Worker interface {
	Open(context.Context, OpenRequest) OpenResult
	Start(context.Context, StartRequest) StartResult
	Control(context.Context, ControlRequest) ControlResult
	Retire()
	Reaped() <-chan struct{}
}

type ProviderSpec struct {
	Name         string
	Capabilities Capabilities
	Describe     introspect.Describe
}

type Capabilities struct {
	Steer     bool
	Interrupt bool
	Resume    bool
}
