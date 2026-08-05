package archtest

// ============================================================================
// 第一组 · 层方向墙（import direction）
//
// 准入三问：
//  1. 不变量：上游恒不依赖下游。一级包的依赖箭头是一张封闭的图（见
//     layerAllowlist），每个包只许 import 表里正向枚举的那几个，表外即违规。
//  2. 后果：一条反向 import 落地，基座从此为下游的编译负责——下游现状开始
//     决定上游形状，分层塌方且不可逆。
//  3. 强度：Go 只有 internal/ 一种子树可见性语法，没有跨树的方向语法，
//     编译器表达不了"runtime 不许 import platform"，只能走 import 墙。
//     internal/ 已保证的（树外 ↛ */internal）恒不在此重复检查。
// ============================================================================

import (
	"fmt"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
)

const moduleRoot = "github.com/wanpengxie/atoll"

type srcFile struct {
	path    string   // 仓库相对路径，如 "runtime/actorrt/unit.go"
	dir     string   // 所在包目录，如 "runtime/actorrt"
	imports []string // import path 列表
}

var (
	corpusOnce sync.Once
	corpus     []srcFile
	corpusErr  error
)

// productionFiles 单次遍历全仓生产 .go 文件（跳过测试文件；跳过 "."/"_" 开头
// 目录与 node_modules/bin/testdata/archtest），只解析 import 表，全组墙共用。
func productionFiles(t *testing.T) []srcFile {
	t.Helper()
	corpusOnce.Do(func() {
		fset := token.NewFileSet()
		corpusErr = filepath.WalkDir("..", func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			name := d.Name()
			if d.IsDir() {
				if path == ".." {
					return nil
				}
				if strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_") ||
					name == "node_modules" || name == "bin" ||
					name == "testdata" || name == "archtest" {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				return nil
			}
			f, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
			if err != nil {
				return err
			}
			rel := filepath.ToSlash(strings.TrimPrefix(path, "../"))
			sf := srcFile{path: rel, dir: filepath.ToSlash(filepath.Dir(rel))}
			for _, imp := range f.Imports {
				p, err := strconv.Unquote(imp.Path.Value)
				if err != nil {
					return err
				}
				sf.imports = append(sf.imports, p)
			}
			corpus = append(corpus, sf)
			return nil
		})
	})
	if corpusErr != nil {
		t.Fatal(corpusErr)
	}
	return corpus
}

// atollPath 返回模块内相对路径（"runtime/actorrt"；模块根包返回 ""）。
func atollPath(importPath string) (string, bool) {
	if importPath == moduleRoot {
		return "", true
	}
	return strings.CutPrefix(importPath, moduleRoot+"/")
}

func isStdlib(importPath string) bool {
	first, _, _ := strings.Cut(importPath, "/")
	return !strings.Contains(first, ".")
}

func hasPathPrefix(path, prefix string) bool {
	return path == prefix || strings.HasPrefix(path, prefix+"/")
}

func topDir(rel string) string {
	top, _, _ := strings.Cut(rel, "/")
	return top
}

func failWall(t *testing.T, violations []string, rule, fix string) {
	t.Helper()
	if len(violations) > 0 {
		t.Fatalf("%s\n违规：\n  %s\n改法：%s", rule, strings.Join(violations, "\n  "), fix)
	}
}

// layerAllowlist 是一级包依赖图的正向枚举（owner 2026-07-30 拍板的层模型）：
// 每个一级包对项目内只许 import 这里列出的一级包（自身恒许；外部模块不限）。
// 表外的一级目录出现（无论作为 importer 还是被 import）一律红——新目录必须
// 显式入表，入表即过 review。
//
//	protocol ◄─ runtime ◄─ lib ◄─ platform ◄─ registry ◄─ {drivers, app} ◄─ cmd
var layerAllowlist = map[string][]string{
	"protocol": {},
	"runtime":  {"protocol"},
	"lib":      {"protocol", "runtime"},
	"platform": {"protocol", "runtime", "lib"},
	"registry": {"protocol", "platform"},
	"drivers":  {"protocol", "runtime", "lib", "platform", "registry"},
	"app":      {"protocol", "platform", "registry"},
	"cmd":      {"protocol", "runtime", "lib", "platform", "registry", "app", "drivers"},
	"e2e":      {}, // 纯测试目录，无生产文件；占位使其入表
	// scripts = demo/工具脚本层，恒零内部依赖：demo 就是"第一个第三方壳"，
	// 只许说契约语言（HTTP/ws + 生成 schema）——它 import 不到内部包这件事
	// 本身就是"契约面足以封壳"的结构证明。
	"scripts": {},
}

// TestLayerGraphIsExactlyTheDeclaredArrows —— 一级包依赖图恒等于 layerAllowlist
// 声明的那张图：正向枚举，不在名单上的箭头不存在。
func TestLayerGraphIsExactlyTheDeclaredArrows(t *testing.T) {
	var bad []string
	for _, f := range productionFiles(t) {
		from := topDir(f.dir)
		allowed, known := layerAllowlist[from]
		if !known {
			bad = append(bad, fmt.Sprintf("%s：一级目录 %q 不在 layerAllowlist 里——新目录必须显式入表", f.path, from))
			continue
		}
		for _, p := range f.imports {
			rel, ok := atollPath(p)
			if !ok {
				continue
			}
			to := topDir(rel)
			if rel == "" { // 模块根包 = 装配根，按 "runtime" 归层（它就是 runtime 的开箱面）
				to = "runtime"
			}
			if to == from {
				continue
			}
			permitted := false
			for _, a := range allowed {
				if to == a {
					permitted = true
					break
				}
			}
			if !permitted {
				bad = append(bad, fmt.Sprintf("%s imports %q（%s → %s 不在图上）", f.path, p, from, to))
			}
		}
	}
	failWall(t, bad,
		"一级包依赖图只有 layerAllowlist 正向枚举的箭头。",
		"下游适配上游：把需要的词汇下放到更低层，或（确属层模型变更时）改这张表并过 review。")
}

// TestProtocolTakesNoSeamOrExternalImports —— protocol 除层图约束（不 import
// 任何项目包）外，还恒不接状态/IO/传输缝：stdlib 里的 context / database/sql /
// net/http 及一切外部模块禁入。
func TestProtocolTakesNoSeamOrExternalImports(t *testing.T) {
	var bad []string
	for _, f := range productionFiles(t) {
		if !hasPathPrefix(f.dir, "protocol") {
			continue
		}
		for _, p := range f.imports {
			if _, ok := atollPath(p); ok {
				continue // 层图墙管
			}
			if isStdlib(p) {
				for _, banned := range []string{"context", "database/sql", "net/http"} {
					if hasPathPrefix(p, banned) {
						bad = append(bad, fmt.Sprintf("%s imports %q（protocol 不接状态/IO/传输缝）", f.path, p))
					}
				}
				continue
			}
			bad = append(bad, fmt.Sprintf("%s imports 外部模块 %q", f.path, p))
		}
	}
	failWall(t, bad,
		"protocol 只许 stdlib（去 context/sql/http），无外部模块。",
		"把带状态/IO 的依赖上移进 runtime，或把该类型从 protocol 下沉出去。")
}

// TestIpcIsAPureWireLeaf —— runtime/ipc 的 import 面正向枚举：stdlib +
// protocol/* + 自身，仅此三类。混入兄弟包或第三方依赖，wire 格式就和实现
// 耦死，不再可独立演化。
func TestIpcIsAPureWireLeaf(t *testing.T) {
	var bad []string
	for _, f := range productionFiles(t) {
		if !hasPathPrefix(f.dir, "runtime/ipc") {
			continue
		}
		for _, p := range f.imports {
			if isStdlib(p) {
				continue
			}
			if rel, ok := atollPath(p); ok && (hasPathPrefix(rel, "protocol") || hasPathPrefix(rel, "runtime/ipc")) {
				continue
			}
			bad = append(bad, fmt.Sprintf("%s imports %q", f.path, p))
		}
	}
	failWall(t, bad,
		"runtime/ipc 的 import 面 = stdlib + protocol/* + 自身。",
		"wire 层需要的类型下放 protocol；需要状态的逻辑不属于 ipc。")
}

// TestSubstrateInternalLayering —— runtime 树内三个敏感叶的内部依赖面正向
// 枚举（protocol/* 与自身恒许，此处枚举的是允许的 runtime 兄弟包）。断的是
// "被管理者反向抓管理者"——一旦成立，代际权威就有第二个说话的人。
func TestSubstrateInternalLayering(t *testing.T) {
	rules := []struct {
		scope   string
		allowed []string // 允许 import 的 runtime 兄弟包
	}{
		// actorrt 是精确单元叶：runtime 树内谁也不认识——被 actorctl/actorhost
		// 管理，恒不认识管理者，也不认识 wire（ipc）与存储契约（storespec）。
		// （它 import 的 runtime/debug 是 Go 标准库，不在本表射程内。）
		{"runtime/actorrt", nil},
		// actorhost 持有并驱动 actorrt；恒不认识装配它的 actorctl。
		{"runtime/actorhost", []string{"runtime/actorrt"}},
		// actorcaps 是能力捆词汇：坐在三条臂之上，恒不认识控制面与存储。
		{"runtime/actorcaps", []string{"runtime/accessdoor", "runtime/harness", "runtime/schedule"}},
	}
	var bad []string
	for _, f := range productionFiles(t) {
		for _, r := range rules {
			if !hasPathPrefix(f.dir, r.scope) {
				continue
			}
			for _, p := range f.imports {
				rel, ok := atollPath(p)
				if !ok || !hasPathPrefix(rel, "runtime") || hasPathPrefix(rel, r.scope) {
					continue
				}
				permitted := false
				for _, a := range r.allowed {
					if hasPathPrefix(rel, a) {
						permitted = true
						break
					}
				}
				if !permitted {
					bad = append(bad, fmt.Sprintf("%s imports %q（%s 的允许面：%v）", f.path, p, r.scope, r.allowed))
				}
			}
		}
	}
	failWall(t, bad,
		"substrate 敏感叶的 runtime 内部依赖面被突破（叶反向抓管理者/wire/存储）。",
		"管理者持有叶并驱动它；叶需要向上说话恒走回调/契约类型。确属新合法依赖时改本表并过 review。")
}

// TestKernelContractLeavesConfined —— 三个 kernel-only 契约叶与装配根的
// importer 白名单（正向枚举谁可以 import 它）。谁能拿到这些句柄 = 谁能绕过
// accessdoor/pen 直连底座，所以 importer 集合本身就是权限面。
func TestKernelContractLeavesConfined(t *testing.T) {
	rules := []struct {
		leaf    string // 模块内相对路径；"" = 模块根包（六路裸写捆的开箱点）
		allowed func(dir string) bool
		why     string
	}{
		{"runtime/timerspec", func(d string) bool { return hasPathPrefix(d, "runtime") },
			"树外 import 它 = 下游可自实现 TimerStore、绕 pen 开延迟伪造作者的写路径（可爬升：挪进 runtime/internal/ 后本条即删）"},
		{"runtime/resourcespec", func(d string) bool { return hasPathPrefix(d, "runtime") || d == "platform/home" },
			"kernel-only 的 R+driver 契约叶，树外拿到即可绕 accessdoor 自实现资源面"},
		{"runtime/internal/store", func(d string) bool { return d == "runtime" },
			"runtime 树内也只许根包碰 sqlite 具体实现，兄弟包恒走 storespec/resourcespec/timerspec 契约叶（树外已由 internal/ 编译器拦截）"},
		{"", func(d string) bool { return d == "platform/home" },
			"模块根包暴露六路裸写捆（OpenChannel→ChannelStores），只许装配缝 platform/home 开箱，其余人写 truth 恒经 harness.Pen"},
	}
	var bad []string
	for _, f := range productionFiles(t) {
		for _, p := range f.imports {
			rel, ok := atollPath(p)
			if !ok {
				continue
			}
			for _, r := range rules {
				if rel == r.leaf && !r.allowed(f.dir) {
					bad = append(bad, fmt.Sprintf("%s imports %q —— %s", f.path, p, r.why))
				}
			}
		}
	}
	failWall(t, bad,
		"kernel 契约叶/装配根的 importer 白名单被突破。",
		"经 accessdoor/harness.Pen/契约叶走，恒不直连底座；白名单确需扩时在本表登记并说明谁批的。")
}

// TestDriversUsePlatformExportFaceOnly —— drivers 消费 platform 恒经四个出口
// 脸（正向枚举）：platform 根、subjectgate、channelhost、channelspec。
// platform/internal 树外本由编译器拦，此处防的是出口脸清单被悄悄扩大。
func TestDriversUsePlatformExportFaceOnly(t *testing.T) {
	platformFace := map[string]bool{
		"platform":             true,
		"platform/subjectgate": true,
		"platform/channelhost": true,
		"platform/channelspec": true,
	}
	var bad []string
	for _, f := range productionFiles(t) {
		if !hasPathPrefix(f.dir, "drivers") {
			continue
		}
		for _, p := range f.imports {
			rel, ok := atollPath(p)
			if !ok || !hasPathPrefix(rel, "platform") {
				continue
			}
			if !platformFace[rel] {
				bad = append(bad, fmt.Sprintf("%s imports %q", f.path, p))
			}
		}
	}
	failWall(t, bad,
		"drivers 只许经 platform 四出口脸（platform 根 / subjectgate / channelhost / channelspec）。",
		"要新的 platform 能力就在出口脸上加导出方法；确需扩脸时在本表登记。")
}

// TestResourceAccessNeverSelfSigns —— 资源访问票据恒是"服务端追踪、单次兑换"
// 的不透明值：accessdoor 全树与 link 的三个票据文件恒不 import 签名/MAC 原语。
// 一旦引入 hmac/ed25519，票据就能变成离线自验证、可重放的东西——泄漏一个
// token 从"下一次兑换失败"升级成"永久通行证"。
func TestResourceAccessNeverSelfSigns(t *testing.T) {
	inScope := func(f srcFile) bool {
		if hasPathPrefix(f.dir, "runtime/accessdoor") {
			return true
		}
		switch f.path {
		case "platform/internal/link/filebytes.go",
			"platform/internal/link/lanecontrol.go",
			"platform/internal/link/storagecontrol.go":
			return true
		}
		return false
	}
	banned := []string{"crypto/hmac", "crypto/ed25519", "crypto/rsa", "crypto/ecdsa", "golang.org/x/crypto"}
	var bad []string
	for _, f := range productionFiles(t) {
		if !inScope(f) {
			continue
		}
		for _, p := range f.imports {
			for _, b := range banned {
				if hasPathPrefix(p, b) {
					bad = append(bad, fmt.Sprintf("%s imports %q", f.path, p))
				}
			}
		}
	}
	failWall(t, bad,
		"资源访问路径恒不引入签名/MAC 原语，票据恒为服务端追踪的不透明单次值。",
		"要防伪造，靠服务端记账与单次兑换，恒不靠把票据变成可离线验证的自证物。")
}
