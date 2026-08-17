package codex

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

func TestInitializeOptsOutAllDeltaNotificationMethods(t *testing.T) {
	want := []string{"item/agentMessage/delta", "item/commandExecution/outputDelta", "item/fileChange/outputDelta", "item/plan/delta", "item/reasoning/summaryTextDelta", "item/reasoning/textDelta", "command/exec/outputDelta", "process/outputDelta", "thread/realtime/outputAudio/delta", "thread/realtime/transcript/delta"}
	if got := deltaNotificationMethods(); !reflect.DeepEqual(got, want) {
		t.Fatalf("methods=%v", got)
	}
}

func TestRequiredMethodsAndFieldsGolden(t *testing.T) {
	want := map[string][]string{
		"initialize":                            {"capabilities.optOutNotificationMethods", "clientInfo.name", "clientInfo.title", "clientInfo.version", "result.userAgent"},
		"initialized":                           {},
		"thread/start":                          {"approvalPolicy", "cwd", "model", "result.thread.id", "sandbox"},
		"thread/resume":                         {"excludeTurns", "model", "result.thread.id", "threadId"},
		"thread/compact/start":                  {"threadId"},
		"thread/tokenUsage/updated":             {"threadId", "tokenUsage.last.totalTokens", "tokenUsage.modelContextWindow", "turnId"},
		"turn/start":                            {"effort", "input", "model", "threadId"},
		"turn/steer":                            {"expectedTurnId", "input", "threadId"},
		"turn/interrupt":                        {"threadId", "turnId"},
		"turn/started":                          {"threadId", "turn.id"},
		"turn/completed":                        {"threadId", "turn.error.additionalDetails", "turn.error.message", "turn.id", "turn.status"},
		"item/started":                          {"item.id", "item.status", "item.tool", "item.type", "threadId", "turnId"},
		"item/completed":                        {"item.aggregatedOutput", "item.command", "item.id", "item.status", "item.text", "item.tool", "item.type", "threadId", "turnId"},
		"error":                                 {"error.message", "threadId", "turnId", "willRetry"},
		"currentTime/read":                      {},
		"item/commandExecution/requestApproval": {"result.decision"},
		"item/fileChange/requestApproval":       {"result.decision"},
		"item/permissions/requestApproval":      {"error.code", "error.message"},
		"execCommandApproval":                   {"result.decision"},
		"applyPatchApproval":                    {"result.decision"},
	}
	methods, declarations := productionProtocolSurface(t)
	wantMethods := map[string]bool{}
	for method := range want {
		wantMethods[method] = true
	}
	if !reflect.DeepEqual(methods, wantMethods) {
		t.Fatalf("production methods=\n%#v\nwant=\n%#v", methods, wantMethods)
	}
	for method, fields := range want {
		jsonTokens := protocolTokensForMethod(t, method, declarations)
		for _, field := range fields {
			for _, segment := range strings.Split(field, ".") {
				// result is the JSON-RPC envelope owned by rpcClient. The
				// method-specific decoder starts at the result body.
				if segment == "result" {
					continue
				}
				if !jsonTokens[segment] {
					t.Fatalf("%s dependency %q is absent from its production request/decoder path", method, field)
				}
			}
		}
	}
}

// productionProtocolSurface reads the production provider, not a second test
// literal. Removing or renaming a method/JSON field therefore breaks this
// wall even when the expected fixture remains untouched.
func productionProtocolSurface(t *testing.T) (map[string]bool, map[string]ast.Node) {
	t.Helper()
	_, thisFile, _, _ := runtime.Caller(0)
	dir := filepath.Dir(thisFile)
	methods := map[string]bool{}
	declarations := map[string]ast.Node{}
	for _, name := range []string{"worker.go", "output.go", "rpc.go"} {
		file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(dir, name), nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, declaration := range file.Decls {
			switch declaration := declaration.(type) {
			case *ast.FuncDecl:
				declarations[declaration.Name.Name] = declaration
			case *ast.GenDecl:
				for _, spec := range declaration.Specs {
					if typ, ok := spec.(*ast.TypeSpec); ok {
						declarations[typ.Name.Name] = typ
					}
				}
			}
		}
		ast.Inspect(file, func(node ast.Node) bool {
			literal, ok := node.(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING {
				return true
			}
			value, err := strconv.Unquote(literal.Value)
			if err != nil {
				return true
			}
			if isRPCMethodLiteral(value) {
				methods[value] = true
			}
			return true
		})
	}
	return methods, declarations
}

func protocolTokensForMethod(t *testing.T, method string, declarations map[string]ast.Node) map[string]bool {
	t.Helper()
	contexts := map[string][]string{
		"initialize": {"Open", "afterInitialize"}, "initialized": {"afterInitialize"},
		"thread/start": {"afterInitialize", "afterSession", "threadIDFrom"}, "thread/resume": {"afterInitialize", "afterSession", "threadIDFrom"},
		"thread/compact/start": {"Start"}, "thread/tokenUsage/updated": {"notification", "tokenUsageNotice"},
		"turn/start": {"Start"}, "turn/steer": {"Control"}, "turn/interrupt": {"Control"},
		"turn/started": {"notification", "turnNotice", "turnWire"}, "turn/completed": {"notification", "turnNotice", "turnWire"},
		"item/started": {"notification", "itemNotice", "itemWire"}, "item/completed": {"notification", "itemNotice", "itemWire"},
		"error": {"notification"}, "currentTime/read": {"handleServerRequest"},
		"item/commandExecution/requestApproval": {"handleServerRequest"}, "item/fileChange/requestApproval": {"handleServerRequest"},
		"item/permissions/requestApproval": {"handleServerRequest", "handleRequest", "rpcError"}, "execCommandApproval": {"handleServerRequest"},
		"applyPatchApproval": {"handleServerRequest"},
	}
	tokens := map[string]bool{}
	for _, name := range contexts[method] {
		node := declarations[name]
		if node == nil {
			t.Fatalf("production declaration %q for %s is missing", name, method)
		}
		ast.Inspect(node, func(node ast.Node) bool {
			literal, ok := node.(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING {
				return true
			}
			value, err := strconv.Unquote(literal.Value)
			if err != nil {
				return true
			}
			if strings.HasPrefix(value, "json:") {
				value = strings.Split(reflect.StructTag(value).Get("json"), ",")[0]
			}
			if value != "" && value != "-" {
				tokens[value] = true
			}
			return true
		})
	}
	return tokens
}

func isRPCMethodLiteral(value string) bool {
	if strings.Contains(strings.ToLower(value), "delta") {
		return false
	}
	for _, prefix := range []string{"thread/", "turn/", "item/", "currentTime/"} {
		if strings.HasPrefix(value, prefix) {
			return true
		}
	}
	return value == "initialize" || value == "initialized" || value == "error" || value == "execCommandApproval" || value == "applyPatchApproval"
}
