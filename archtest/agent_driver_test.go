package archtest

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"os"
	"regexp"
	"strings"
	"testing"
)

func TestAgentDriverDependencyWalls(t *testing.T) {
	var bad []string
	for _, f := range productionFiles(t) {
		for _, p := range f.imports {
			rel, ok := atollPath(p)
			if !ok {
				continue
			}
			switch {
			case hasPathPrefix(f.dir, "drivers/agents/provider"):
				// 一个 provider 自己的子包（internal/…）在它自身边界之内；
				// 墙拦的是跨层与跨 provider 依赖。
				own := f.dir
				if parts := strings.SplitN(f.dir, "/", 5); len(parts) >= 4 {
					own = strings.Join(parts[:4], "/")
				}
				if hasPathPrefix(rel, "drivers/agents") && !hasPathPrefix(rel, "drivers/agents/driverproto") && !hasPathPrefix(rel, own) {
					bad = append(bad, fmt.Sprintf("%s imports %s", f.path, p))
				}
				if hasPathPrefix(rel, "lib/actorbase") || hasPathPrefix(rel, "runtime/actorhost") || hasPathPrefix(rel, "runtime/actorrt") {
					bad = append(bad, fmt.Sprintf("%s imports %s", f.path, p))
				}
			case hasPathPrefix(f.dir, "drivers/agents/base"):
				if hasPathPrefix(rel, "drivers/agents/runtime") && !hasPathPrefix(rel, "drivers/agents/runtimeproto") || hasPathPrefix(rel, "drivers/agents/driverproto") || hasPathPrefix(rel, "drivers/agents/provider") || hasPathPrefix(rel, "drivers/agents/all") {
					bad = append(bad, fmt.Sprintf("%s imports %s", f.path, p))
				}
			case hasPathPrefix(f.dir, "drivers/agents/runtime") && !hasPathPrefix(f.dir, "drivers/agents/runtimeproto"):
				if hasPathPrefix(rel, "drivers/agents/base") || hasPathPrefix(rel, "drivers/agents/provider") || hasPathPrefix(rel, "lib/actorbase") || hasPathPrefix(rel, "runtime/actorhost") || hasPathPrefix(rel, "runtime/actorrt") {
					bad = append(bad, fmt.Sprintf("%s imports %s", f.path, p))
				}
			case hasPathPrefix(f.dir, "drivers/agents/driverproto"), hasPathPrefix(f.dir, "drivers/agents/runtimeproto"), hasPathPrefix(f.dir, "drivers/agents/effectcap"):
				if hasPathPrefix(rel, "drivers/agents/base") || hasPathPrefix(rel, "drivers/agents/runtime") && !hasPathPrefix(rel, "drivers/agents/runtimeproto") || hasPathPrefix(rel, "drivers/agents/provider") || hasPathPrefix(rel, "lib/actorbase") {
					bad = append(bad, fmt.Sprintf("%s imports %s", f.path, p))
				}
			}
		}
		if hasPathPrefix(f.dir, "drivers/agents/provider") && (strings.HasSuffix(f.path, "/engine.go") || strings.Contains(f.path, "legacy") || strings.Contains(f.path, "compat")) {
			bad = append(bad, f.path+": forbidden provider lifecycle/legacy file")
		}
	}
	if len(bad) > 0 {
		t.Fatalf("agent driver dependency wall violations:\n  %s", strings.Join(bad, "\n  "))
	}
}

func TestAgentDriverObsoleteSurfaceIsAbsent(t *testing.T) {
	forbidden := regexp.MustCompile(`\b(Adapter|OpenResult|StartResult|ControlResult|Certainty|Ambiguous|runtimeFuse|unboundedQueue|persistCoordinator|EffectScope)\b|NewAdapter|NewEffectScope`)
	var bad []string
	for _, f := range productionFiles(t) {
		if !hasPathPrefix(f.dir, "drivers/agents") {
			continue
		}
		raw, err := os.ReadFile("../" + f.path)
		if err != nil {
			t.Fatal(err)
		}
		if hit := forbidden.Find(raw); hit != nil {
			bad = append(bad, fmt.Sprintf("%s contains obsolete %q", f.path, hit))
		}
		if bytesContains(raw, []byte("registry.Register(")) && !hasPathPrefix(f.dir, "drivers/agents/all") {
			bad = append(bad, f.path+": provider-side registration is forbidden")
		}
	}
	if len(bad) > 0 {
		t.Fatalf("obsolete agent surface remains:\n  %s", strings.Join(bad, "\n  "))
	}
}

func TestAgentDriverProductionWriteMouths(t *testing.T) {
	var bad []string
	retireCalls := 0
	for _, f := range productionFiles(t) {
		raw, err := os.ReadFile("../" + f.path)
		if err != nil {
			t.Fatal(err)
		}
		text := string(raw)
		if hasPathPrefix(f.dir, "drivers/agents/base") && (strings.Contains(text, ".Reply(") || strings.Contains(text, ".Fail(")) && !strings.HasSuffix(f.path, "/exec.go") {
			bad = append(bad, f.path+": terminal write outside Base executor")
		}
		if hasPathPrefix(f.dir, "drivers/agents/runtime") && strings.Contains(text, ".Retire()") {
			retireCalls += strings.Count(text, ".Retire()")
			if !strings.HasSuffix(f.path, "/slot.go") {
				bad = append(bad, f.path+": Worker.Retire outside workerSlot")
			}
		}
		if hasPathPrefix(f.dir, "drivers/agents/runtime") && !strings.HasSuffix(f.path, "/executor.go") {
			for _, mouth := range []string{"e.events.", "e.provider.NewWorker(", "w.Open(", "w.Start(", "w.Control(", "e.deps.Tools.Invoke(", "e.deps.Resources.Invoke("} {
				if strings.Contains(text, mouth) {
					bad = append(bad, f.path+": Runtime cross-boundary effect outside executor: "+mouth)
				}
			}
		}
		if strings.Contains(f.dir, "/internal/book") {
			for _, forbidden := range []string{"context.", "time.", "go func", "actorbase", "driverproto.Worker"} {
				if strings.Contains(text, forbidden) {
					bad = append(bad, f.path+": transition book contains "+forbidden)
				}
			}
		}
	}
	if retireCalls != 1 {
		bad = append(bad, fmt.Sprintf("Runtime physical Retire callsites=%d want 1", retireCalls))
	}
	if len(bad) > 0 {
		t.Fatalf("agent production write-mouth violations:\n  %s", strings.Join(bad, "\n  "))
	}
}

func TestRuntimeGenerationHasSingleWipeMouth(t *testing.T) {
	type wipe struct {
		file, function string
		line           int
	}
	var wipes []wipe
	for _, source := range productionFiles(t) {
		if !hasPathPrefix(source.dir, "drivers/agents/runtime") || hasPathPrefix(source.dir, "drivers/agents/runtimeproto") {
			continue
		}
		fset := token.NewFileSet()
		parsed, err := parser.ParseFile(fset, "../"+source.path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, declaration := range parsed.Decls {
			fn, ok := declaration.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				assignment, ok := node.(*ast.AssignStmt)
				if !ok || assignment.Tok != token.ASSIGN || len(assignment.Lhs) != len(assignment.Rhs) {
					return true
				}
				for i, left := range assignment.Lhs {
					selector, ok := left.(*ast.SelectorExpr)
					if !ok || selector.Sel.Name != "generation" {
						continue
					}
					literal, ok := assignment.Rhs[i].(*ast.CompositeLit)
					if !ok || len(literal.Elts) != 0 {
						continue
					}
					typeName, ok := literal.Type.(*ast.Ident)
					if !ok || typeName.Name != "generationState" {
						continue
					}
					wipes = append(wipes, wipe{file: source.path, function: fn.Name.Name, line: fset.Position(assignment.Pos()).Line})
				}
				return true
			})
		}
	}
	if len(wipes) != 1 || wipes[0].file != "drivers/agents/runtime/engine.go" || wipes[0].function != "wipeSettledGeneration" {
		t.Fatalf("Runtime generation zero writes=%+v; want exactly engine.wipeSettledGeneration", wipes)
	}
}

// TestAgentDriverPackagesHoldNoMutableGlobalState 钉的不变量：agent driver 各层
// （base/runtime/协议叶包/provider）的一切可变状态必须有唯一属主——挂在 worker/
// engine/owner loop 或由组装根显式注入的对象上。包级可变全局是无主状态，
// 设计档 §9 明文"不得用 package global 偷渡"。
//
// 违反的后果（真实拦截对象：spawnGates，2026-08 整删）：一个 actor 的进程拉起
// 失败通过包级退避表罚睡同键的另一个 actor（跨 actor 传染），静默睡眠把真实
// 错误加工成 deadline 超时，且无人能重置该状态——生命周期脱离所有权体系。
//
// 为什么钉在语法结构层而非更高：Go 编译器不禁止包级 var，import 方向与此
// 无关；可变性在类型面上可静态判定（map/chan/sync/atomic/make），故扫包级
// var 声明的类型与初值文本。词表类全局（错误哨兵、编译正则、字符串切片、
// 接口断言）不含这些类型面，天然放行。
func TestAgentDriverPackagesHoldNoMutableGlobalState(t *testing.T) {
	mutable := regexp.MustCompile(`\bmap\[|\bchan\b|\bsync\.|\batomic\.|\bmake\(`)
	fset := token.NewFileSet()
	var bad []string
	for _, f := range productionFiles(t) {
		if !hasPathPrefix(f.dir, "drivers/agents") {
			continue
		}
		parsed, err := parser.ParseFile(fset, "../"+f.path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range parsed.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.VAR {
				continue
			}
			for _, spec := range gen.Specs {
				vs, ok := spec.(*ast.ValueSpec)
				if !ok {
					continue
				}
				named := false
				for _, n := range vs.Names {
					if n.Name != "_" {
						named = true
					}
				}
				if !named {
					continue // 接口实现断言不携带状态
				}
				var buf bytes.Buffer
				if vs.Type != nil {
					_ = printer.Fprint(&buf, fset, vs.Type)
				}
				for _, v := range vs.Values {
					buf.WriteByte(' ')
					_ = printer.Fprint(&buf, fset, v)
				}
				if hit := mutable.FindString(buf.String()); hit != "" {
					bad = append(bad, fmt.Sprintf("%s: 包级 var %s 带可变类型面 %q——状态必须有属主：挂到构造出的对象上，由组装根注入", f.path, vs.Names[0].Name, hit))
				}
			}
		}
	}
	if len(bad) > 0 {
		t.Fatalf("agent driver 出现包级可变全局（无主状态）:\n  %s", strings.Join(bad, "\n  "))
	}
}

func bytesContains(haystack, needle []byte) bool {
	return strings.Contains(string(haystack), string(needle))
}
