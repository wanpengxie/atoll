package base

import (
	"errors"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/actor"
)

// NewEngine builds one provider incarnation. The event port is stable for the
// incarnation; providers fence process generations internally.
type NewEngine func(sys actorbase.Sys, resumeSeed []byte, events EventPort) (Engine, error)

type Config struct {
	NewEngine         NewEngine
	SupportedControls []string
	BufferMaxCount    int
	BufferMaxBytes    int
	BatchMaxCount     int
}

type definition struct {
	cfg      Config
	controls map[string]struct{}
}

func Def(doc string, cfg Config) (actorbase.Def, error) {
	if cfg.NewEngine == nil {
		return actorbase.Def{}, errors.New("agent/base: Config.NewEngine required")
	}
	d := definition{cfg: cfg, controls: map[string]struct{}{
		TypeQueue: {}, TypeStop: {},
	}}
	for _, typ := range cfg.SupportedControls {
		if !isEngineControl(typ) {
			return actorbase.Def{}, errors.New("agent/base: unknown supported control " + typ)
		}
		d.controls[typ] = struct{}{}
	}
	d.cfg.SupportedControls = append([]string(nil), cfg.SupportedControls...)
	if d.cfg.BufferMaxCount <= 0 {
		d.cfg.BufferMaxCount = defaultBufferMaxCount
	}
	if d.cfg.BufferMaxBytes <= 0 {
		d.cfg.BufferMaxBytes = defaultBufferMaxBytes
	}
	if d.cfg.BatchMaxCount <= 0 {
		d.cfg.BatchMaxCount = defaultBatchMaxCount
	}
	return actorbase.Def{Doc: doc, New: func() (actorbase.Proc, error) {
		return d.run, nil
	}}, nil
}

func isEngineControl(typ string) bool {
	switch typ {
	case TypeSteer, TypeInterrupt, TypeTerminate, TypeRestart:
		return true
	default:
		return false
	}
}

const (
	TypeSteer     = "agent.steer"
	TypeInterrupt = "agent.interrupt"
	TypeQueue     = "agent.queue"
	TypeStop      = "agent.stop"
	TypeTerminate = "agent.terminate"
	TypeRestart   = "agent.restart"
)

func engineControlTypes() []string {
	return []string{TypeSteer, TypeInterrupt, TypeTerminate, TypeRestart}
}

func (d definition) supports(typ string) bool {
	_, ok := d.controls[typ]
	return ok
}

func actorKind() actor.Kind { return actor.KindAgent }
