package base

import (
	"errors"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/actor"
)

type Config struct {
	NewRuntime      NewRuntime
	Runtime         RuntimeSpec
	BufferMaxCount  int
	BufferMaxBytes  int
	BatchMaxCount   int
	ReceiptDeadline time.Duration
}

type Definition = actorbase.Def

type definition struct {
	cfg      Config
	controls map[string]struct{}
}

func Def(doc string, cfg Config) (actorbase.Def, error) {
	if cfg.NewRuntime == nil {
		return actorbase.Def{}, errors.New("agent/base: Config.NewRuntime required")
	}
	if cfg.Runtime.Describe.Description == "" {
		cfg.Runtime.Describe.Description = doc
	}
	if cfg.Runtime.Describe.SkillDoc == "" {
		cfg.Runtime.Describe.SkillDoc = doc
	}
	d := definition{cfg: cfg, controls: map[string]struct{}{TypeQueue: {}, TypeStop: {}, TypeTerminate: {}, TypeRestart: {}}}
	if cfg.Runtime.Capabilities.Steer {
		d.controls[TypeSteer] = struct{}{}
	}
	if cfg.Runtime.Capabilities.Interrupt {
		d.controls[TypeInterrupt] = struct{}{}
	}
	if d.cfg.BufferMaxCount <= 0 {
		d.cfg.BufferMaxCount = defaultBufferMaxCount
	}
	if d.cfg.BufferMaxBytes <= 0 {
		d.cfg.BufferMaxBytes = defaultBufferMaxBytes
	}
	if d.cfg.BatchMaxCount <= 0 {
		d.cfg.BatchMaxCount = defaultBatchMaxCount
	}
	if d.cfg.ReceiptDeadline <= 0 {
		d.cfg.ReceiptDeadline = 20 * time.Minute
	}
	return actorbase.Def{Doc: doc, New: func() (actorbase.Proc, error) { return d.run, nil }}, nil
}

const (
	TypeSteer     = "agent.steer"
	TypeInterrupt = "agent.interrupt"
	TypeQueue     = "agent.queue"
	TypeStop      = "agent.stop"
	TypeTerminate = "agent.terminate"
	TypeRestart   = "agent.restart"
)

func (d definition) supports(typ string) bool { _, ok := d.controls[typ]; return ok }
func actorKind() actor.Kind                   { return actor.KindAgent }
