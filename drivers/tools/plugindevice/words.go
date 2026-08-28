package plugindevice

import (
	"context"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/resource"
)

// The two words every plugin adapter answers about its own endpoint. They are
// named per adapter (xhs.listen.set, kimi.listen.set, …) because the endpoint
// belongs to that adapter, not to a shared service — this package supplies the
// behaviour, each adapter supplies the name.

// StateKey is where the desired address is kept so it survives a restart.
//
// Deliberately actor STATE and not declaration config: a config change mints a
// new incarnation, which would mean a `set` restarts the very actor whose
// listener it was moving — and the guarantee that a failed bind leaves the old
// listener untouched cannot survive being restarted. State survives a restart
// without minting a term, and the `set` request and its terminal are already
// the attributable record of the change in the ledger. So the ledger says who
// changed it, and state says what it is now; neither is a hidden file.
const StateKey resource.ResourceID = "plugindevice.listen-addr"

// SetPayload is the set word's body.
type SetPayload struct {
	ListenAddr string `json:"listen_addr"`
}

// Status is the get word's answer: what was asked for, what is actually bound,
// and whether a plugin is attached right now. It carries no frames and no
// credentials — this reports the endpoint, not what goes through it.
type Status struct {
	DesiredAddr string `json:"desired_addr"`
	ActualAddr  string `json:"actual_addr,omitempty"`
	Online      bool   `json:"online"`
	Loopback    bool   `json:"loopback"`
}

// Status reads the endpoint's current state.
func (d *Device) Status() Status {
	desired := d.Desired()
	return Status{
		DesiredAddr: desired,
		ActualAddr:  d.Addr(),
		Online:      d.Online(),
		Loopback:    IsLoopbackAddr(desired),
	}
}

// HandleSet moves the endpoint and remembers where. Order matters and is the
// contract: bind first, persist only once the bind actually landed. Persisting
// first would leave a restart chasing an address that was never reachable.
//
// A persist failure after a successful rebind is reported rather than rolled
// back: the endpoint really did move, and telling the caller it did not would
// be worse than telling them it may not survive a restart.
func (d *Device) HandleSet(ctx context.Context, sys actorbase.Sys, msg actorbase.Msg) {
	var payload SetPayload
	if err := actorbase.DecodeStrict(msg.Payload, &payload); err != nil {
		_, _ = sys.Fail(msg, "invalid_args", err.Error())
		return
	}
	if payload.ListenAddr == "" {
		_, _ = sys.Fail(msg, "invalid_args", "listen_addr required")
		return
	}
	if err := ValidateAddr(payload.ListenAddr); err != nil {
		_, _ = sys.Fail(msg, "invalid_args", err.Error())
		return
	}
	actual, err := d.Rebind(ctx, payload.ListenAddr)
	if err != nil {
		// The old listener is untouched — say so, because the caller's next
		// question is always "did I just lose the endpoint I was using".
		_, _ = sys.Fail(msg, "bind_failed", err.Error()+"; the previous listener is still serving")
		return
	}
	persisted := true
	if out, perr := sys.State().Put(StateKey, []byte(actual)); perr != nil || !out.Accepted() {
		persisted = false
		d.deps.Logger.Warn(d.deps.Tool+".device.desired_persist_failed", "addr", actual)
	}
	status := d.Status()
	_, _ = sys.Reply(msg, map[string]any{
		"desired_addr": status.DesiredAddr,
		"actual_addr":  status.ActualAddr,
		"online":       status.Online,
		"loopback":     status.Loopback,
		"persisted":    persisted,
	})
}

// HandleGet answers with the endpoint's current state.
func (d *Device) HandleGet(sys actorbase.Sys, msg actorbase.Msg) {
	var empty struct{}
	if err := actorbase.DecodeStrictEmpty(msg.Payload, &empty); err != nil {
		_, _ = sys.Fail(msg, "invalid_args", err.Error())
		return
	}
	status := d.Status()
	_, _ = sys.Reply(msg, map[string]any{
		"desired_addr": status.DesiredAddr,
		"actual_addr":  status.ActualAddr,
		"online":       status.Online,
		"loopback":     status.Loopback,
	})
}

// startAddrBudget and startAddrRetry mirror the agent driver's catch-up read
// (drivers/agents/base/persist.go): long enough for a daemon link to come up,
// short enough that an unreachable one never delays boot past it.
const (
	startAddrBudget = 5 * time.Second
	startAddrRetry  = 100 * time.Millisecond
)

// StartAddr picks the address this incarnation should bind: what a previous
// `set` asked for, else the adapter's default.
//
// It RETRIES while the state door cannot yet say, and that is the whole reason
// this is not one call. A daemon-hosted actor boots exactly while its outbound
// link is coming up, and until it is the door honestly answers outcome_unknown
// rather than faking either answer. Reading once there reads "unknown", falls
// back to the default, and silently undoes an address the operator set — which
// is how a setting that persisted correctly still failed to survive a restart.
// The agent driver already learned this; the shape is copied from it.
//
// A RESOLVED answer is definitive either way and returns at once: a value is
// used, and resource_not_found means nothing was ever set — the ordinary first
// boot, not something to wait out. A stored address that no longer validates is
// ignored with a loud line rather than taking the actor down: an actor that
// refuses to start is far harder to recover than one listening somewhere
// unexpected.
func StartAddr(ctx context.Context, sys actorbase.Sys, fallback string, logger Logger) string {
	deadline := time.Now().Add(startAddrBudget)
	for attempt := 0; ; attempt++ {
		addr, resolved := readStartAddr(sys, fallback, logger, attempt)
		if resolved {
			return addr
		}
		if time.Now().After(deadline) {
			logger.Warn("plugindevice.stored_addr_unreadable", "using", fallback, "attempts", attempt+1)
			return fallback
		}
		select {
		case <-ctx.Done():
			return fallback
		case <-time.After(startAddrRetry):
		}
	}
}

// readStartAddr does one read. resolved=false means the door could not say yet
// and the caller should ask again.
func readStartAddr(sys actorbase.Sys, fallback string, logger Logger, attempt int) (string, bool) {
	out, err := sys.State().Get(StateKey)
	// Every arm says which one it was. They all end at the same fallback, and
	// folding them into one silent return makes the difference between "nothing
	// was ever set" and "what was set could not be read" invisible — which is
	// exactly the question being asked when an address does not survive a
	// restart.
	switch {
	case err != nil:
		// Not resolved: an error here is as likely to be the link as the store.
		logger.Warn("plugindevice.stored_addr_read_error", "err", err.Error(), "attempt", attempt+1)
		return "", false
	case out.Accepted() && (!out.Found || len(out.Value) == 0):
		logger.Info("plugindevice.stored_addr_absent", "using", fallback)
		return fallback, true
	case !out.Accepted() && out.RejectReason == access.ResourceNotFound:
		// Nothing was ever set. Definitive, and the ordinary first boot.
		logger.Info("plugindevice.stored_addr_absent", "using", fallback)
		return fallback, true
	case !out.Accepted():
		// outcome_unknown and friends: the door cannot say YET. Ask again.
		logger.Warn("plugindevice.stored_addr_unresolved", "reason", string(out.RejectReason), "attempt", attempt+1)
		return "", false
	}
	stored := string(out.Value)
	if verr := ValidateAddr(stored); verr != nil {
		logger.Warn("plugindevice.stored_addr_rejected", "addr", stored, "err", verr.Error())
		return fallback, true
	}
	if attempt > 0 {
		logger.Info("plugindevice.stored_addr_restored_after_link", "addr", stored, "attempts", attempt+1)
	} else {
		logger.Info("plugindevice.stored_addr_restored", "addr", stored)
	}
	return stored, true
}

// Logger is the narrow face StartAddr needs. Error and Warn are different
// verdicts here and the caller should be able to tell them apart: a read that
// failed is a fault, an address that never existed is a first boot.
type Logger interface {
	Info(string, ...any)
	Warn(string, ...any)
	Error(string, ...any)
}
