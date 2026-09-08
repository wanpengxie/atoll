package plugindevice

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"sync"
)

// shared.go is what makes "one browser, many channels" ordinary rather than a
// feature.
//
// A plugin device is ONE physical thing: one Chrome extension dialling one
// port. But a channel seats its own tool actor, so N channels mean N actors —
// and every one of them used to construct its own Device and bind its own
// listener. The second seat lost the race for the port and died at birth. The
// class comment said two instances on different addresses were legal, which was
// true and useless: the extension dials exactly one port, so a second listener
// has nobody on the other end.
//
// The fix is not a bigger Device. It is that the Device stopped being half an
// actor (see Deps): once the in-flight table holds a continuation instead of
// somebody's Msg, sharing needs no arbitration at all — replies find their way
// home by correlation id, and each caller keeps its own books.
//
// So this file only does the small remaining thing: hand the same Device to
// everyone who asks for the same address, and close it when the last one lets
// go.

var (
	sharedMu sync.Mutex
	shared   = map[string]*sharedEntry{}
)

type sharedEntry struct {
	dev  *Device
	refs int
}

// Handle is one caller's grip on a shared Device. It carries the caller's
// session — the grouping key — because that is the one thing that MUST differ
// between callers of the same device; putting it on the Device would herd
// everyone back into a single tab group.
//
// The handle knows nothing about actors either. It holds an opaque string it
// was handed and stamps it on every call, so no call site has to remember: a
// single forgotten session is a tab escaping into the wrong group.
type Handle struct {
	dev     *Device
	addr    string
	session string

	mu       sync.Mutex
	released bool
}

// Attach returns a handle onto the process-wide Device for addr, binding it on
// the first attach. Later attaches to the same address reuse the live listener
// and connection — that IS the sharing.
//
// deps of the second and later attaches are ignored: the Device already exists
// and its transport is already configured. Only the session differs, and that
// travels on the handle.
func Attach(addr, session string, deps Deps) (*Handle, error) {
	if err := ValidateAddr(addr); err != nil {
		return nil, err
	}
	sharedMu.Lock()
	defer sharedMu.Unlock()

	// Port 0 means "give me ANY free port", so two callers asking for it want
	// two different ports — the request string is not an identity. Sharing on it
	// hands the second caller the first one's endpoint, which is not a subtle
	// failure: it silently returns an address somebody else is already serving.
	// So an ephemeral request always mints its own device and is then filed
	// under the address it actually got, where a later caller naming that
	// concrete address will share it.
	entry, ok := shared[addr]
	if ephemeralPort(addr) {
		ok = false
	}
	key := addr
	if !ok {
		dev := New(deps)
		if err := dev.Bind(addr); err != nil {
			return nil, err
		}
		entry = &sharedEntry{dev: dev}
		if resolved := dev.Addr(); resolved != "" {
			key = resolved
		}
		shared[key] = entry
	}
	entry.refs++
	return &Handle{dev: entry.dev, addr: key, session: session}, nil
}

// ephemeralPort reports whether addr asks the OS to choose the port.
func ephemeralPort(addr string) bool {
	_, port, err := net.SplitHostPort(addr)
	return err == nil && port == "0"
}

// Release drops this caller's grip. The last one out stops the Device, which
// closes the listener and the connection; earlier ones must not, because the
// other callers are still using them.
func (h *Handle) Release(ctx context.Context) error {
	h.mu.Lock()
	if h.released {
		h.mu.Unlock()
		return nil
	}
	h.released = true
	h.mu.Unlock()

	sharedMu.Lock()
	entry, ok := shared[h.addr]
	if !ok {
		sharedMu.Unlock()
		return nil
	}
	entry.refs--
	last := entry.refs <= 0
	if last {
		delete(shared, h.addr)
	}
	sharedMu.Unlock()
	if !last {
		return nil
	}
	return entry.dev.Stop(ctx)
}

// Call sends one command, stamped with this handle's session, and hands the
// answer back through done. done is called exactly once — on the device's
// reply, or by the reaper when the deadline passes.
func (h *Handle) Call(correlationID string, spec Spec, params json.RawMessage, done func(Inbound)) error {
	if done == nil {
		return errors.New(h.dev.deps.Tool + ": a call needs somewhere to put the answer")
	}
	return h.dev.Call(correlationID, h.session, spec, params, done)
}

// Session is the grouping key this handle stamps. Exposed for describe/status
// text, so an operator can see which group a seat writes into.
func (h *Handle) Session() string { return h.session }

// Online reports whether the plugin is attached. It is a QUERY, not a
// subscription: presence used to be pushed through a device-level callback that
// published an obs in one actor's name, which is meaningless once the device is
// shared. Each caller reads this on its own maintenance tick and publishes into
// its own channel.
func (h *Handle) Online() bool { return h.dev.Online() }

// Addr, Desired, Status and Rebind pass through to the shared Device — they are
// DEVICE-level facts and operations. Rebind in particular moves the endpoint
// for every caller at once, which is correct (there is one endpoint) but worth
// saying out loud: one channel's kimi.listen.set relocates everyone's browser
// connection.
func (h *Handle) Addr() string    { return h.dev.Addr() }
func (h *Handle) Desired() string { return h.dev.Desired() }
func (h *Handle) Status() Status  { return h.dev.Status() }

func (h *Handle) Rebind(ctx context.Context, addr string) (string, error) {
	if err := ValidateAddr(addr); err != nil {
		return "", err
	}
	sharedMu.Lock()
	defer sharedMu.Unlock()
	entry, ok := shared[h.addr]
	if !ok {
		return "", errors.New(h.dev.deps.Tool + ": device is no longer attached")
	}
	resolved, err := entry.dev.Rebind(ctx, addr)
	if err != nil {
		return "", err
	}
	// Re-key so later attaches find the device where it now lives.
	if resolved != h.addr {
		delete(shared, h.addr)
		shared[resolved] = entry
		h.addr = resolved
	}
	return resolved, nil
}

// Sweep runs the shared reaper. Any caller may drive it; expired entries are
// rung out through their own continuations, so a sweep driven by one actor
// still fails the right requests in the right channels.
func (h *Handle) Sweep() { h.dev.Sweep() }
