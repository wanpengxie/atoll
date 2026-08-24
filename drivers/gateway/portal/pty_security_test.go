package portal

import (
	"net/http"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
)

// 终端是一条能执行任意命令的路。判权粒度恒是「开不开会话」（design §7），
// 所以那几道门必须真的在，且必须在 Upgrade 之前——一旦升级成功，后面就没有
// 拒绝的机会了。本组把每一道单独钉住。

func TestOnlyHumansMayOpenATerminal(t *testing.T) {
	// 口子恒单向（design §5）：给 agent 一个 PTY 等于让它绕过逐次判权与逐次
	// 落账，与「把人拉进账本」的理由方向相反。
	for _, tc := range []struct {
		id   actor.ActorID
		want bool
	}{
		{"human:root:1787570189318", true},
		{"agent:codex:1787570378227", false},
		{"agent:claude:1787570189039", false},
		{"tool:device:1787570189039", false},
		{"peer:svcactor:1787570189039", false},
		{"system", false},
		// 形不对的 id 恒不得被当成人：kind 是结构位，恒不是自称。
		{"human", false},
		{"human:root", false},
		{"human:root:1:extra", false},
		{"", false},
	} {
		if got := isHuman(tc.id); got != tc.want {
			t.Errorf("isHuman(%q) = %v, want %v", tc.id, got, tc.want)
		}
	}
}

func TestPTYRefusesCrossOriginUpgrade(t *testing.T) {
	// Cookie 认证 + WebSocket = CSWSH 的经典面。跨源必须拒，否则任意站点都能
	// 用受害者的 cookie 开一个终端。
	p := &Portal{}
	check := p.ptyUpgrader().CheckOrigin
	for _, tc := range []struct {
		name, origin, host string
		want               bool
	}{
		{"同源放行", "https://node.example", "node.example", true},
		{"跨源拒绝", "https://evil.example", "node.example", false},
		{"子域也算跨源", "https://a.node.example", "node.example", false},
		{"畸形 Origin 拒绝", "://", "node.example", false},
		// 非浏览器客户端（curl / 原生）恒不带 Origin；浏览器恒会带，
		// 故放行空 Origin 恒不构成 CSWSH 面。与 /ws 同一约定。
		{"无 Origin 放行", "", "node.example", true},
	} {
		r := newOriginRequest(tc.origin, tc.host)
		if got := check(r); got != tc.want {
			t.Errorf("%s: CheckOrigin(origin=%q host=%q) = %v, want %v", tc.name, tc.origin, tc.host, got, tc.want)
		}
	}
}

func newOriginRequest(origin, host string) *http.Request {
	r := &http.Request{Header: http.Header{}, Host: host}
	if origin != "" {
		r.Header.Set("Origin", origin)
	}
	return r
}
