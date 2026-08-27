package plugindevice

import (
	"context"

	"github.com/wanpengxie/atoll/lib/actorbase"
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

// StartAddr picks the address an incarnation should bind: what a previous
// `set` asked for, else the adapter's default. A cold read (nothing stored yet)
// is the ordinary first boot, never an error to die on; a stored address that
// no longer validates is ignored with a loud line rather than taking the actor
// down, because an actor that refuses to start is much harder to fix than one
// listening somewhere unexpected.
func StartAddr(sys actorbase.Sys, fallback string, logger interface {
	Warn(string, ...any)
}) string {
	out, err := sys.State().Get(StateKey)
	if err != nil || !out.Accepted() || !out.Found || len(out.Value) == 0 {
		return fallback
	}
	stored := string(out.Value)
	if verr := ValidateAddr(stored); verr != nil {
		logger.Warn("plugindevice.stored_addr_rejected", "addr", stored, "err", verr.Error())
		return fallback
	}
	return stored
}
