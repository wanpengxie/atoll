package gateway

import "sync"

// poke 缝 (连接模型勘误期 §3.2 / 表② 逐符号迁移): the old RevocationSource +
// SubscribeRevoked/onRevoked machinery is reborn as a membership-change poke —
// its性质 changes from "撤销执行" to pure及时性 (the resolver/sweep is the
// correctness正门; a lost poke only delays convergence, never breaks it).
//
// The gateway exposes Gateway.Poke(principal). The two本期写入点 (Admit 入籍,
// Remove 注销 — both platform-side) and the future ACL point emit through this
// hub so the assembly root (cmd/server) wires them WITHOUT drivers importing app:
// the emit points call hub.Poke(principal); the gateway subscribes hub with
// gw.Poke. The bridge (this hub) is the retained注入桥 (表② "SetRevokeSink 注入桥
// 改形保留"), only its key changed from (channel, subject actor_id) to principal.
type PokeHub struct {
	mu   sync.Mutex
	subs []func(principal string)
}

// NewPokeHub builds an empty hub.
func NewPokeHub() *PokeHub { return &PokeHub{} }

// Subscribe registers a poke handler (the gateway's Poke).
func (h *PokeHub) Subscribe(fn func(principal string)) {
	h.mu.Lock()
	h.subs = append(h.subs, fn)
	h.mu.Unlock()
}

// Poke fans one membership-change poke out to every subscriber. The emit points
// (Admit / Remove / ACL) call this with the affected principal (a Remove must
// resolve principal BEFORE its dereg cascade — actor_id → PrincipalOf — since the
// row is gone after; §3.2 poke 缝).
func (h *PokeHub) Poke(principal string) {
	h.mu.Lock()
	subs := make([]func(string), len(h.subs))
	copy(subs, h.subs)
	h.mu.Unlock()
	for _, fn := range subs {
		fn(principal)
	}
}
