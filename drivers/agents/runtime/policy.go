package runtime

import "time"

// Policy contains only bounds consumed by the production Runtime.
type Policy struct {
	CommandCapacity     int
	IngressCapacity     int
	CallbackCapacity    int
	EventCapacity       int
	OpenFactDeadline    time.Duration
	StartFactDeadline   time.Duration
	ControlFactDeadline time.Duration
	InterruptEnded      time.Duration
	Watchdog            time.Duration
	ReapedDemand        time.Duration
	MethodCall          time.Duration
	InputMaxItems       int
	InputMaxBytes       int
}

func DefaultPolicy() Policy {
	return Policy{
		CommandCapacity: 32, IngressCapacity: 256, CallbackCapacity: 32, EventCapacity: 256,
		OpenFactDeadline: 45 * time.Second, StartFactDeadline: 45 * time.Second,
		ControlFactDeadline: 45 * time.Second, InterruptEnded: 45 * time.Second,
		Watchdog: 10 * time.Minute, ReapedDemand: 30 * time.Second,
		MethodCall: 30 * time.Second, InputMaxItems: 256, InputMaxBytes: 8 << 20,
	}
}

func (p Policy) normalized() Policy {
	d := DefaultPolicy()
	if p.CommandCapacity <= 0 {
		p.CommandCapacity = d.CommandCapacity
	}
	if p.IngressCapacity <= 0 {
		p.IngressCapacity = d.IngressCapacity
	}
	if p.CallbackCapacity <= 0 {
		p.CallbackCapacity = d.CallbackCapacity
	}
	if p.EventCapacity <= 0 {
		p.EventCapacity = d.EventCapacity
	}
	if p.OpenFactDeadline <= 0 {
		p.OpenFactDeadline = d.OpenFactDeadline
	}
	if p.StartFactDeadline <= 0 {
		p.StartFactDeadline = d.StartFactDeadline
	}
	if p.ControlFactDeadline <= 0 {
		p.ControlFactDeadline = d.ControlFactDeadline
	}
	if p.InterruptEnded <= 0 {
		p.InterruptEnded = d.InterruptEnded
	}
	if p.Watchdog <= 0 {
		p.Watchdog = d.Watchdog
	}
	if p.ReapedDemand <= 0 {
		p.ReapedDemand = d.ReapedDemand
	}
	if p.MethodCall <= 0 {
		p.MethodCall = d.MethodCall
	}
	if p.InputMaxItems <= 0 {
		p.InputMaxItems = d.InputMaxItems
	}
	if p.InputMaxBytes <= 0 {
		p.InputMaxBytes = d.InputMaxBytes
	}
	return p
}
