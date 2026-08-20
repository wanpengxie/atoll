// Package dataplane owns daemon file tickets, byte routing and cut-through
// pumps. It has no principal or authorization vocabulary; the access door is
// the only component allowed to mint through Issuer.
package dataplane

import (
	"context"
	"errors"
	"io"
	"net/url"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/resource"
)

const TicketTTL = 10 * time.Minute

var (
	ErrClosed        = errors.New("dataplane: closed")
	ErrNotBound      = errors.New("dataplane: host stream opener not bound")
	ErrAlreadyBound  = errors.New("dataplane: host stream opener already bound")
	ErrInvalidTicket = errors.New("dataplane: invalid or expired ticket")
	ErrHostOffline   = errors.New("dataplane: host offline")
)

type HostOfflineError struct{ Host string }

func (e *HostOfflineError) Error() string { return "dataplane: host offline: " + e.Host }
func (e *HostOfflineError) Unwrap() error { return ErrHostOffline }

func NewHostOfflineError(host string) error { return &HostOfflineError{Host: host} }

type Ticket struct {
	ChannelID channel.ID
	Address   resource.ResourceID
	// Path is the file's location inside that channel's directory on the host
	// machine — the address minus its host and channel segments. It is resolved
	// once at issue time so the redeeming side never re-parses the address.
	Path   string
	Mode   access.Operation
	HostID string
	// Caller is whose transfer this is. A ticket's scope is exactly
	// (ChannelID, Caller) and neither half is optional: there are no bytes that
	// belong to no channel — bytes outside one are not addressable by any actor
	// on this plane at all — and there is no access without an actor as the
	// subject doing the reading or writing. The door decides on one connection
	// and the bytes move on another; these two fields are what makes the second
	// connection the same operation as the first.
	Caller  actor.ActorID
	Expires time.Time
}

type IssueSpec struct {
	Address   resource.ResourceID
	Path      string
	ChannelID channel.ID
	Mode      access.Operation
	HostID    string
	HostName  string
	Caller    actor.ActorID
}

type Grant struct {
	Ticket string
}

type Issuer interface {
	Issue(context.Context, IssueSpec) (Grant, error)
}

type Redeemer interface {
	Resolve(ch channel.ID, caller actor.ActorID, token string) (Ticket, error)
	ServeExchange(context.Context, channel.ID, io.ReadWriteCloser)
	ServeHTTP(ctx context.Context, ch channel.ID, caller actor.ActorID, token string, mode access.Operation, dst io.Writer, src io.Reader) error
	TicketFile(ch channel.ID, caller actor.ActorID, token string) (string, bool)
	OpenTransfer(ctx context.Context, ch channel.ID, caller actor.ActorID, token string, mode access.Operation) (io.ReadWriteCloser, error)
}

type Binder interface {
	BindHostStreamOpener(HostStreamOpener) error
	UnbindHostStreamOpener()
}

// HostStreamOpener is supplied once by daemonhost after both organs exist.
// OpenHost returns a stream whose host-leg request header is already written.
type HostStreamOpener interface {
	Online(string, channel.ID) bool
	OpenHost(context.Context, Ticket) (io.ReadWriteCloser, error)
}

type plane struct {
	now func() time.Time

	mu        sync.Mutex
	closed    bool
	boundEver bool
	opener    HostStreamOpener
	tickets   map[string]Ticket
	wg        sync.WaitGroup
}

type issuer struct{ p *plane }
type redeemer struct{ p *plane }
type binder struct{ p *plane }

// New returns structurally separate capabilities. No exported value can both
// mint and redeem tickets.
func New() (Issuer, Redeemer, Binder, func(context.Context) error) {
	p := &plane{now: time.Now, tickets: make(map[string]Ticket)}
	return issuer{p}, redeemer{p}, binder{p}, p.close
}

func (b binder) BindHostStreamOpener(opener HostStreamOpener) error {
	if opener == nil {
		return errors.New("dataplane: nil host stream opener")
	}
	b.p.mu.Lock()
	defer b.p.mu.Unlock()
	if b.p.closed {
		return ErrClosed
	}
	if b.p.boundEver {
		return ErrAlreadyBound
	}
	b.p.boundEver = true
	b.p.opener = opener
	return nil
}

func (b binder) UnbindHostStreamOpener() {
	b.p.mu.Lock()
	b.p.opener = nil
	b.p.mu.Unlock()
}

func (i issuer) Issue(_ context.Context, spec IssueSpec) (Grant, error) {
	// Caller sits in this list beside ChannelID because the two together ARE the
	// ticket's scope. A ticket with no actor on it is an access with no subject,
	// which is not a weaker ticket — it is not a ticket.
	if spec.Address == "" || spec.Path == "" || spec.ChannelID == "" || spec.Caller == "" ||
		spec.HostID == "" || spec.HostName == "" ||
		(spec.Mode != access.OpRead && spec.Mode != access.OpWrite) {
		return Grant{}, errors.New("dataplane: invalid issue spec")
	}
	i.p.mu.Lock()
	defer i.p.mu.Unlock()
	if i.p.closed {
		return Grant{}, ErrClosed
	}
	if i.p.opener == nil {
		return Grant{}, ErrNotBound
	}
	if !i.p.opener.Online(spec.HostID, spec.ChannelID) {
		return Grant{}, NewHostOfflineError(spec.HostName)
	}
	now := i.p.now()
	i.p.sweepLocked(now)
	token := uuid.NewString()
	i.p.tickets[token] = Ticket{Address: spec.Address, Path: spec.Path, ChannelID: spec.ChannelID, Mode: spec.Mode,
		HostID: spec.HostID, Caller: spec.Caller, Expires: now.Add(TicketTTL)}
	return Grant{Ticket: token}, nil
}

func (r redeemer) Resolve(ch channel.ID, caller actor.ActorID, token string) (Ticket, error) {
	return r.p.resolve(ch, caller, token)
}

// resolve is the ONLY way a ticket comes back out of this table, and it always
// asks for the ticket's whole scope. There is deliberately no channel-blind or
// actor-blind variant: a lookup that omits half the scope does not check it, so
// the scope would hold only for whoever remembered to pass it.
func (p *plane) resolve(ch channel.ID, caller actor.ActorID, token string) (Ticket, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return Ticket{}, ErrClosed
	}
	now := p.now()
	ticket, ok := p.tickets[token]
	if !ok || ticket.ChannelID != ch || ticket.Caller != caller || !now.Before(ticket.Expires) {
		if ok && !now.Before(ticket.Expires) {
			delete(p.tickets, token)
		}
		return Ticket{}, ErrInvalidTicket
	}
	return ticket, nil
}

func (p *plane) openerFor(ticket Ticket) (HostStreamOpener, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, ErrClosed
	}
	if p.opener == nil {
		return nil, ErrNotBound
	}
	if !p.opener.Online(ticket.HostID, ticket.ChannelID) {
		return nil, NewHostOfflineError(ticketHost(ticket))
	}
	return p.opener, nil
}

func ticketHost(ticket Ticket) string {
	u, err := url.Parse(string(ticket.Address))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(u.Host)
}

func (r redeemer) ServeExchange(ctx context.Context, ch channel.ID, caller io.ReadWriteCloser) {
	if caller == nil {
		return
	}
	if !r.p.begin() {
		_ = caller.Close()
		return
	}
	defer r.p.wg.Done()
	defer caller.Close()
	fail := func(code string, err error) {
		detail := ""
		if err != nil {
			detail = err.Error()
		}
		_ = link.WriteExchangeControl(caller, link.ExchangeStatus{OK: false, Code: code, Detail: detail})
	}
	var head link.ExchangeTicketHeader
	if err := link.ReadExchangeControl(caller, &head); err != nil || head.Ticket == "" || head.Caller == "" {
		fail("protocol_error", err)
		return
	}
	ticket, err := r.Resolve(ch, actor.ActorID(head.Caller), head.Ticket)
	if err != nil {
		fail("invalid_ticket", err)
		return
	}
	opener, err := r.p.openerFor(ticket)
	if err != nil {
		if errors.Is(err, ErrHostOffline) {
			_ = link.WriteExchangeControl(caller, link.ExchangeStatus{OK: false, Code: "host_offline", Detail: ticketHost(ticket)})
			return
		}
		fail(errorCode(err), err)
		return
	}
	host, err := opener.OpenHost(ctx, ticket)
	if err != nil {
		if errors.Is(err, ErrHostOffline) {
			_ = link.WriteExchangeControl(caller, link.ExchangeStatus{OK: false, Code: "host_offline", Detail: ticketHost(ticket)})
			return
		}
		fail(errorCode(err), err)
		return
	}
	defer host.Close()
	if ticket.Mode == access.OpRead {
		if err := link.RelayExchangeBytes(caller, host); err != nil {
			var terminal *link.ExchangeTerminalError
			if errors.As(err, &terminal) {
				_ = link.WriteExchangeControl(caller, link.ExchangeStatus{OK: false, Code: terminal.Code, Detail: terminal.Detail})
				return
			}
			fail("transfer_failed", err)
			return
		}
		var status link.ExchangeStatus
		if err := link.ReadExchangeControl(host, &status); err != nil {
			fail("transfer_failed", err)
			return
		}
		_ = link.WriteExchangeControl(caller, status)
		return
	}
	// WRITE is duplex even though its normal state machine looks sequential:
	// the host may reject before the caller finishes producing bytes. Observe
	// the host terminal concurrently, forward it immediately, and close both
	// legs so the upload stops without half-close semantics.
	var segmentEnded atomic.Bool
	terminalDone := make(chan struct{}, 1)
	go func() {
		var status link.ExchangeStatus
		err := link.ReadExchangeControl(host, &status)
		if err != nil {
			status = link.ExchangeStatus{OK: false, Code: "transfer_failed", Detail: err.Error()}
		} else if status.OK && !segmentEnded.Load() {
			status = link.ExchangeStatus{OK: false, Code: "protocol_error", Detail: "host sent success before the byte-segment terminator"}
		}
		if writeErr := link.WriteExchangeControl(caller, status); err == nil {
			err = writeErr
		}
		_ = caller.Close()
		_ = host.Close()
		terminalDone <- struct{}{}
	}()
	relayErr := link.RelayExchangeBytesNotifyEnd(host, caller, func() { segmentEnded.Store(true) })
	if relayErr != nil {
		// Closing the host unblocks the terminal observer when the caller
		// disappeared. Leave the caller leg open until that observer has had
		// the chance to forward an already-received early host status.
		_ = host.Close()
	}
	// Joining this observer is part of the exchange lifetime: ServeExchange
	// never returns with a status reader left behind on a retired lane.
	<-terminalDone
}

// OpenTransfer hands back the live byte stream for one ticket instead of
// pumping it somewhere. This is what an actor running in this process redeems
// with: it is the same table, the same scope check and the same stream to the
// same machine that a browser's transfer uses — the server has always been the
// one opening these streams, it just never had a way to keep one.
//
// The stream's host header is already written, so the caller reads or writes
// exchange frames on it directly and closes it when done.
func (r redeemer) OpenTransfer(ctx context.Context, ch channel.ID, caller actor.ActorID, token string, mode access.Operation) (io.ReadWriteCloser, error) {
	if !r.p.begin() {
		return nil, ErrClosed
	}
	defer r.p.wg.Done()
	ticket, err := r.p.resolve(ch, caller, token)
	if err != nil {
		return nil, err
	}
	if ticket.Mode != mode {
		return nil, ErrInvalidTicket
	}
	opener, err := r.p.openerFor(ticket)
	if err != nil {
		return nil, err
	}
	host, err := opener.OpenHost(ctx, ticket)
	if err != nil {
		if errors.Is(err, ErrHostOffline) {
			return nil, NewHostOfflineError(ticketHost(ticket))
		}
		return nil, err
	}
	return host, nil
}

// TicketFile answers the file name a live ticket will move, so an entrance can
// name a download before the first byte goes out. It opens nothing: the ticket
// still has to survive redemption in ServeHTTP.
func (r redeemer) TicketFile(ch channel.ID, caller actor.ActorID, token string) (string, bool) {
	ticket, err := r.p.resolve(ch, caller, token)
	if err != nil {
		return "", false
	}
	return path.Base(ticket.Path), true
}

// ServeHTTP moves one transfer's bytes. The ticket fixes the machine, the path
// and the direction, all decided at issue time by the access door; the channel
// and the actor are its scope, and the entrance supplies both — an entrance
// facing outward has resolved an outside claim into that actor by the time it
// gets here. The direction it believes it is in is checked too: a read grant is
// not a licence to write.
func (r redeemer) ServeHTTP(ctx context.Context, ch channel.ID, caller actor.ActorID, token string, mode access.Operation, dst io.Writer, src io.Reader) error {
	if !r.p.begin() {
		return ErrClosed
	}
	defer r.p.wg.Done()
	ticket, err := r.p.resolve(ch, caller, token)
	if err != nil {
		return err
	}
	if ticket.Mode != mode {
		return ErrInvalidTicket
	}
	opener, err := r.p.openerFor(ticket)
	if err != nil {
		return err
	}
	host, err := opener.OpenHost(ctx, ticket)
	if err != nil {
		// The lane can retire between openerFor's liveness check and this open.
		// Name the machine here, where the ticket is still in hand, so the
		// entrance never needs an address to say who went away.
		if errors.Is(err, ErrHostOffline) {
			return NewHostOfflineError(ticketHost(ticket))
		}
		return err
	}
	defer host.Close()
	if mode == access.OpRead {
		if err := link.ReadExchangeBytes(dst, host); err != nil {
			return err
		}
		return readSuccessfulStatus(host)
	}
	var segmentEnded atomic.Bool
	type statusResult struct {
		status link.ExchangeStatus
		err    error
	}
	statusDone := make(chan statusResult, 1)
	go func() {
		var status link.ExchangeStatus
		err := link.ReadExchangeControl(host, &status)
		if err == nil && status.OK && !segmentEnded.Load() {
			err = errors.New("dataplane: host sent success before the byte-segment terminator")
		}
		statusDone <- statusResult{status: status, err: err}
		if err != nil || !status.OK {
			_ = host.Close()
			if closer, ok := src.(io.Closer); ok {
				_ = closer.Close()
			}
		}
	}()
	writeErr := link.WriteExchangeBytesNotifyEnd(host, src, func() { segmentEnded.Store(true) })
	if writeErr != nil {
		_ = host.Close()
	}
	result := <-statusDone
	if result.err != nil {
		return result.err
	}
	if !result.status.OK {
		return &link.ExchangeTerminalError{Code: result.status.Code, Detail: result.status.Detail}
	}
	if writeErr != nil {
		return writeErr
	}
	return nil
}

func readSuccessfulStatus(r io.Reader) error {
	var status link.ExchangeStatus
	if err := link.ReadExchangeControl(r, &status); err != nil {
		return err
	}
	if !status.OK {
		return &link.ExchangeTerminalError{Code: status.Code, Detail: status.Detail}
	}
	return nil
}

func errorCode(err error) string {
	if errors.Is(err, ErrHostOffline) {
		return "host_offline"
	}
	return "unavailable"
}

func (p *plane) begin() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return false
	}
	p.wg.Add(1)
	return true
}

func (p *plane) sweepLocked(now time.Time) {
	for token, ticket := range p.tickets {
		if !now.Before(ticket.Expires) {
			delete(p.tickets, token)
		}
	}
}

func (p *plane) close(ctx context.Context) error {
	p.mu.Lock()
	p.closed = true
	p.opener = nil
	p.tickets = make(map[string]Ticket)
	p.mu.Unlock()
	done := make(chan struct{})
	go func() { p.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
