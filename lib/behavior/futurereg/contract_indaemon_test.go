package futurereg_test

import (
	"context"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/lib/behavior/futurereg"
	"github.com/wanpengxie/ActOS/lib/behavior/futurereg/contract"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// inDaemonTransport binds the shared contract scenarios to the in-daemon path:
// the framework response router drives a bare futurereg.FutureRegistry, so this
// transport exercises the registry directly (Register / Deliver / Await over a
// Handle / Watch / Cancel). It is "transport A" in the cross-transport
// contract harness (spec §7).
type inDaemonTransport struct {
	reg *futurereg.FutureRegistry
}

func (tr *inDaemonTransport) Register(id message.ID) { tr.reg.Register(id) }

func (tr *inDaemonTransport) Deliver(env *message.Envelope) futurereg.Disposition {
	return tr.reg.Deliver(env)
}

func (tr *inDaemonTransport) Await(ctx context.Context, id message.ID, timeout time.Duration) (*message.Envelope, bool, error) {
	h := tr.reg.Register(id) // idempotent rebind to the existing set
	env, err := h.Await(ctx, timeout)
	if err != nil {
		if err == context.DeadlineExceeded {
			// window elapsed without a final — not a hard error
			return nil, false, nil
		}
		return nil, false, err
	}
	return env, true, nil
}

func (tr *inDaemonTransport) Watch(id message.ID) (futurereg.Watcher, error) {
	return tr.reg.Register(id).Watch()
}

func (tr *inDaemonTransport) Abandon(id message.ID) { tr.reg.Cancel(id) }

func (tr *inDaemonTransport) Pending() []message.ID { return tr.reg.Pending() }

// TestContractInDaemonTransport runs the shared futurereg contract scenario
// table against the in-daemon (direct registry) transport. The kimi worker
// helper runs the SAME table in adapters/llm/kimi; matching results across both
// prove the futurereg semantics do not drift between transports.
func TestContractInDaemonTransport(t *testing.T) {
	contract.Run(t, func(t *testing.T) contract.Transport {
		return &inDaemonTransport{reg: futurereg.New()}
	})
}
