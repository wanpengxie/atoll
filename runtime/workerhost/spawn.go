package workerhost

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
)

// Spawner abstracts how a worker subprocess is started. cmd/daemon uses
// ExecSpawner which spawns ./bin/coagent-worker via os/exec; tests can
// inject InProcessSpawner which runs a goroutine.
type Spawner interface {
	// Spawn starts a worker subprocess and returns the bidirectional
	// IPC stream + a Wait func to await exit + a Kill func to force
	// termination.
	Spawn(ctx context.Context, leaseID string) (WorkerProc, error)
}

// WorkerProc is the daemon-side handle on a running worker.
type WorkerProc struct {
	LeaseID string
	Stdin   io.WriteCloser // daemon writes here
	Stdout  io.ReadCloser  // daemon reads from here
	Wait    func() error
	Kill    func() error
}

// ExecSpawner spawns ./bin/coagent-worker (path configurable).
type ExecSpawner struct {
	BinaryPath string
	Env        []string
	Args       []string
}

// Spawn starts the worker binary.
func (s *ExecSpawner) Spawn(ctx context.Context, leaseID string) (WorkerProc, error) {
	if s.BinaryPath == "" {
		return WorkerProc{}, errors.New("workerhost: ExecSpawner.BinaryPath empty")
	}
	cmd := exec.CommandContext(ctx, s.BinaryPath, s.Args...) //nolint:gosec
	cmd.Env = append(os.Environ(), s.Env...)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return WorkerProc{}, fmt.Errorf("workerhost: stdin pipe: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return WorkerProc{}, fmt.Errorf("workerhost: stdout pipe: %w", err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return WorkerProc{}, fmt.Errorf("workerhost: start %s: %w", s.BinaryPath, err)
	}
	return WorkerProc{
		LeaseID: leaseID,
		Stdin:   stdin,
		Stdout:  stdout,
		Wait:    func() error { return cmd.Wait() },
		Kill:    func() error { return cmd.Process.Kill() },
	}, nil
}

// PipeSpawner is an in-process Spawner used by tests. It runs a
// user-supplied worker function in a goroutine with io.Pipe streams
// connecting it to the daemon side.
type PipeSpawner struct {
	// WorkerFunc is invoked in a goroutine for each Spawn. The function
	// reads from in (daemon→worker) and writes to out (worker→daemon).
	// When the function returns the goroutine ends; out is closed to
	// signal Wait().
	WorkerFunc func(ctx context.Context, leaseID string, in io.Reader, out io.Writer) error
}

// Spawn starts a goroutine pair simulating a worker subprocess.
func (s *PipeSpawner) Spawn(ctx context.Context, leaseID string) (WorkerProc, error) {
	if s.WorkerFunc == nil {
		return WorkerProc{}, errors.New("workerhost: PipeSpawner.WorkerFunc nil")
	}
	d2wR, d2wW := io.Pipe() // daemon stdin → worker reads from d2wR
	w2dR, w2dW := io.Pipe() // worker writes to w2dW → daemon reads from w2dR
	done := make(chan error, 1)
	var killOnce sync.Once

	go func() {
		err := s.WorkerFunc(ctx, leaseID, d2wR, w2dW)
		// Close write-side so daemon reader sees EOF.
		_ = w2dW.Close()
		// Close read-side too so subsequent writes by daemon fail loudly.
		_ = d2wR.Close()
		done <- err
	}()

	return WorkerProc{
		LeaseID: leaseID,
		Stdin:   d2wW, // daemon writes here → worker reads
		Stdout:  w2dR, // worker writes here → daemon reads
		Wait: func() error {
			err, ok := <-done
			if !ok {
				return nil
			}
			return err
		},
		Kill: func() error {
			killOnce.Do(func() {
				_ = d2wW.Close()
				_ = w2dR.Close()
			})
			return nil
		},
	}, nil
}
