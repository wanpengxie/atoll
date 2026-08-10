package archtest

// ============================================================================
// 第二组 · 闭集词汇墙（golden closed sets）
//
// 准入三问：
//  1. 不变量：三个跨界词汇表是闭集——wire 帧类型、kernel 协议包导出面、
//     actor 能力面动词表。加词/删词必须显式改本文件的登记表，逼过 review。
//  2. 后果：wire 词表漏登记 = 对端解不了帧；kernel 协议包混入业务类型 =
//     协议层被域污染；Sys 长新方法 = 每个 Proc 无声扩权。
//  3. 强度：编译器不承诺"全集恒等于登记表"——它只看用到的。全集比对只能
//     由 AST/反射枚举真实全集来做。这类墙无绕路：比对对象是机器完整可见的
//     全集，藏着加一个是不存在的写法。
// ============================================================================

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/wanpengxie/atoll/lib/actorbase"
)

// parsePackageDir 解析一个包目录的全部非测试 .go 文件。
func parsePackageDir(t *testing.T, dir string) map[string]*ast.File {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	files := map[string]*ast.File{}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		path := filepath.Join(dir, name)
		f, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		files[path] = f
	}
	return files
}

// diffSets 报告 actual 相对 golden 的新增与消失。
func diffSets(actual, golden []string) (extra, missing []string) {
	inGolden := map[string]bool{}
	for _, g := range golden {
		inGolden[g] = true
	}
	inActual := map[string]bool{}
	for _, a := range actual {
		inActual[a] = true
		if !inGolden[a] {
			extra = append(extra, a)
		}
	}
	for _, g := range golden {
		if !inActual[g] {
			missing = append(missing, g)
		}
	}
	sort.Strings(extra)
	sort.Strings(missing)
	return extra, missing
}

// ipcKindVocabulary —— runtime/ipc 的 wire 帧类型全集。新增帧必须在此登记：
// 这张表就是"哪些帧存在"的评审记录。
var ipcKindVocabulary = []string{
	"KindHandshake", "KindDeliver", "KindDeliverResult", "KindEmit", "KindEmitAck",
	"KindDown", "KindCancel", "KindCancelRequest", "KindObs",
	"KindAccess", "KindAccessAck", "KindSchedule", "KindScheduleAck",
	"KindDetach", "KindSpawn", "KindSpawnAck", "KindEnd", "KindEndAck",
}

// TestIPCKindVocabularyIsRegistered —— 从 AST 源头枚举 runtime/ipc 里所有显式
// 标注为 Kind 类型的 const 声明，与登记表恒等比对。包内自己的 len 自洽测试
// 堵不住"加了 Kind 忘补手写 map"（两边一起忘），源头枚举堵得住。
func TestIPCKindVocabularyIsRegistered(t *testing.T) {
	var actual []string
	for _, f := range parsePackageDir(t, "../runtime/ipc") {
		for _, decl := range f.Decls {
			gd, ok := decl.(*ast.GenDecl)
			if !ok || gd.Tok != token.CONST {
				continue
			}
			for _, spec := range gd.Specs {
				vs, ok := spec.(*ast.ValueSpec)
				if !ok {
					continue
				}
				if id, ok := vs.Type.(*ast.Ident); !ok || id.Name != "Kind" {
					continue
				}
				for _, name := range vs.Names {
					actual = append(actual, name.Name)
				}
			}
		}
	}
	extra, missing := diffSets(actual, ipcKindVocabulary)
	if len(extra)+len(missing) > 0 {
		t.Fatalf("runtime/ipc 的 Kind 帧词表是闭集。\n未登记的新帧: %v\n登记过但已消失: %v\n改法：真要增删帧，把本表和对端处理一起改，一次 review 过。", extra, missing)
	}
}

// channelProtocolExportSurface —— protocol/channel（kernel 协议叶）的导出面
// 全集：13 个 kernel 类型 + 2 个规范化函数 + 2 个错误 + 4 个枚举成员。
// 业务/space/膜类型恒不出现在这里。
var channelProtocolExportSurface = []string{
	// types
	"ID", "Reader", "ReaderMode", "AdmitResult", "IntroduceResult", "RemoveResult",
	"Placement", "PlacementKind", "ResourceListQuery", "ResourceMeta", "ResourcePage",
	"ResourceRef", "ResourceFetch",
	// funcs
	"CanonicalJSON", "Digest",
	// vars
	"ErrInvalidPlacement", "ErrInvalidRequest",
	// enum consts
	"PlacementServer", "PlacementDaemon", "ReaderMember", "ReaderObserver",
}

// TestChannelProtocolExportSurfaceIsRegistered —— 枚举 protocol/channel 全部
// 导出顶层标识符（类型/函数/变量/常量），与登记表恒等比对。
func TestChannelProtocolExportSurfaceIsRegistered(t *testing.T) {
	var actual []string
	for _, f := range parsePackageDir(t, "../protocol/channel") {
		for _, decl := range f.Decls {
			switch d := decl.(type) {
			case *ast.FuncDecl:
				if d.Recv == nil && d.Name.IsExported() {
					actual = append(actual, d.Name.Name)
				}
			case *ast.GenDecl:
				for _, spec := range d.Specs {
					switch s := spec.(type) {
					case *ast.TypeSpec:
						if s.Name.IsExported() {
							actual = append(actual, s.Name.Name)
						}
					case *ast.ValueSpec:
						for _, name := range s.Names {
							if name.IsExported() {
								actual = append(actual, name.Name)
							}
						}
					}
				}
			}
		}
	}
	extra, missing := diffSets(actual, channelProtocolExportSurface)
	if len(extra)+len(missing) > 0 {
		t.Fatalf("protocol/channel 是 kernel 协议叶，导出面是闭集。\n未登记的新导出: %v\n登记过但已消失: %v\n改法：kernel 词汇变更改本表过 review；业务类型恒不进 protocol。", extra, missing)
	}
}

// sysVerbTable —— actorbase.Sys 的动词全集：能力面本身。一列一个动词，
// 恒无并行孪生（RespondEnvelope 之于 Reply 那种"同一列两张嘴"）。
var sysVerbTable = []string{
	// Pen —— 应答写
	"Reply", "Fail", "Progress",
	// Pen —— 主动写
	"Emit", "Post", "Call",
	// 访问面两柄
	"Resource", "State",
	// 定时
	"After", "CancelTimer",
	// 生命周期
	"Fork", "End",
	// 上下文/观测
	"Self", "PublishObs",
	// 输入流与进程生命
	"Recv", "Life",
}

// TestSysVerbTableIsExact —— 反射枚举 actorbase.Sys 的真实方法全集，与登记表
// 恒等比对。加动词 = 给每个 Proc 扩权，必须显式改表过 review。
func TestSysVerbTableIsExact(t *testing.T) {
	sysType := reflect.TypeOf((*actorbase.Sys)(nil)).Elem()
	var actual []string
	for i := 0; i < sysType.NumMethod(); i++ {
		actual = append(actual, sysType.Method(i).Name)
	}
	extra, missing := diffSets(actual, sysVerbTable)
	if len(extra)+len(missing) > 0 {
		t.Fatalf("actorbase.Sys 的动词表是能力面闭集。\n未登记的新动词: %v\n登记过但已消失: %v\n改法：真长新原子就改本表过 review；同一列的第二张嘴（后缀孪生）恒不许。", extra, missing)
	}
}
