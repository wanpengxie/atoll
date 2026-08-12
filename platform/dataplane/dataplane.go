// Package dataplane owns daemon file tickets, byte routing and cut-through
// pumps. It has no principal or authorization vocabulary; the access door is
// the only component allowed to mint through Issuer.
package dataplane

import (
	"context"
	"errors"
	"io"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/protocol/access"
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
	Mode      access.Operation
	HostID    string
	Expires   time.Time
}

type IssueSpec struct {
	Address   resource.ResourceID
	ChannelID channel.ID
	Mode      access.Operation
	HostID    string
	HostName  string
}

type Grant struct {
	Ticket string
}

type Issuer interface {
	Issue(context.Context, IssueSpec) (Grant, error)
}

type Redeemer interface {
	Resolve(channel.ID, string) (Ticket, error)
	ServeExchange(context.Context, channel.ID, io.ReadWriteCloser)
	ServeHTTP(context.Context, resource.ResourceID, string, access.Operation, io.Writer, io.Reader) error
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
	if spec.Address == "" || spec.ChannelID == "" || spec.HostID == "" || spec.HostName == "" ||
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
	i.p.tickets[token] = Ticket{Address: spec.Address, ChannelID: spec.ChannelID, Mode: spec.Mode,
		HostID: spec.HostID, Expires: now.Add(TicketTTL)}
	return Grant{Ticket: token}, nil
}

func (r redeemer) Resolve(ch channel.ID, token string) (Ticket, error) {
	return r.p.resolve(ch, token, "")
}

func (p *plane) resolve(ch channel.ID, token, _ string) (Ticket, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return Ticket{}, ErrClosed
	}
	now := p.now()
	ticket, ok := p.tickets[token]
	if !ok || ticket.ChannelID != ch || !now.Before(ticket.Expires) {
		if ok && !now.Before(ticket.Expires) {
			delete(p.tickets, token)
		}
		return Ticket{}, ErrInvalidTicket
	}
	return ticket, nil
}

func (p *plane) resolveAny(token string) (Ticket, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return Ticket{}, ErrClosed
	}
	now := p.now()
	ticket, ok := p.tickets[token]
	if !ok || !now.Before(ticket.Expires) {
		if ok {
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
	if err := link.ReadExchangeControl(caller, &head); err != nil || head.Ticket == "" {
		fail("protocol_error", err)
		return
	}
	ticket, err := r.Resolve(ch, head.Ticket)
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

func (r redeemer) ServeHTTP(ctx context.Context, address resource.ResourceID, token string, mode access.Operation, dst io.Writer, src io.Reader) error {
	if !r.p.begin() {
		return ErrClosed
	}
	defer r.p.wg.Done()
	ticket, err := r.p.resolveAny(token)
	if err != nil {
		return err
	}
	if ticket.Address != address || ticket.Mode != mode {
		return ErrInvalidTicket
	}
	opener, err := r.p.openerFor(ticket)
	if err != nil {
		return err
	}
	host, err := opener.OpenHost(ctx, ticket)
	if err != nil {
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
