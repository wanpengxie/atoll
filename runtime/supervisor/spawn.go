package supervisor

import "context"

// Spawner abstracts how the supervisor brings a worker subprocess up.
// cmd/daemon plugs in runtime/workerhost.ExecSpawner; tests use
// PipeSpawner. Declared here (rather than referencing workerhost.Spawner)
// so supervisor stays independent of workerhost's sqlite-ladened
// implementation.
type Spawner interface {
	Spawn(ctx context.Context, leaseID string) (WorkerProc, error)
}

// WorkerProc is the runtime/workerhost.WorkerProc shape, re-declared
// here as an interface alias for the same reason as Spawner.
type WorkerProc interface {
	GetLeaseID() string
	Wait() error
	Kill() error
}
