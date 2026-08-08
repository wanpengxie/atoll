package runtime

import "time"

type Policy struct {
	CommandAdmission time.Duration
	OpenCall         time.Duration
	StartCall        time.Duration
	Started          time.Duration
	ControlCall      time.Duration
	InterruptEnded   time.Duration
	Watchdog         time.Duration
	SafetyInterrupt  time.Duration
	TerminalDrain    time.Duration
	Reap             time.Duration
	InputMaxItems    int
	InputMaxBytes    int
}

func DefaultPolicy() Policy {
	return Policy{
		CommandAdmission: 2 * time.Second,
		OpenCall:         30 * time.Second, StartCall: 45 * time.Second,
		Started: 45 * time.Second, ControlCall: 45 * time.Second,
		InterruptEnded: 45 * time.Second, Watchdog: 10 * time.Minute,
		SafetyInterrupt: 5 * time.Second, TerminalDrain: 5 * time.Second,
		Reap: 30 * time.Second, InputMaxItems: 256, InputMaxBytes: 8 << 20,
	}
}

func (p Policy) normalized() Policy {
	d := DefaultPolicy()
	if p.CommandAdmission <= 0 {
		p.CommandAdmission = d.CommandAdmission
	}
	if p.OpenCall <= 0 {
		p.OpenCall = d.OpenCall
	}
	if p.StartCall <= 0 {
		p.StartCall = d.StartCall
	}
	if p.Started <= 0 {
		p.Started = d.Started
	}
	if p.ControlCall <= 0 {
		p.ControlCall = d.ControlCall
	}
	if p.InterruptEnded <= 0 {
		p.InterruptEnded = d.InterruptEnded
	}
	if p.Watchdog <= 0 {
		p.Watchdog = d.Watchdog
	}
	if p.SafetyInterrupt <= 0 {
		p.SafetyInterrupt = d.SafetyInterrupt
	}
	if p.TerminalDrain <= 0 {
		p.TerminalDrain = d.TerminalDrain
	}
	if p.Reap <= 0 {
		p.Reap = d.Reap
	}
	if p.InputMaxItems <= 0 {
		p.InputMaxItems = d.InputMaxItems
	}
	if p.InputMaxBytes <= 0 {
		p.InputMaxBytes = d.InputMaxBytes
	}
	return p
}
