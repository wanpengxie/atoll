package main

import (
	"reflect"
	"testing"
)

// restart 只需要认得 --dir（为了找 pid 文件），其余参数必须原样交给 start。
// 漏传一个 --addr 或 --root-password，重启出来的节点就跟停掉的那个不是同一
// 个配置——而人以为自己只是"重启了一下"。
func TestRestartExtractsOnlyDirAndPassesEverythingElseThrough(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want []string
	}{
		{"分离形", []string{"--addr", "127.0.0.1:9000", "--dir", "/tmp/n"}, []string{"--dir", "/tmp/n"}},
		{"等号形", []string{"--dir=/tmp/n", "--open-registration"}, []string{"--dir=/tmp/n"}},
		{"单横线", []string{"-dir", "/tmp/n"}, []string{"-dir", "/tmp/n"}},
		{"没有 dir", []string{"--addr", "127.0.0.1:9000"}, nil},
		{"空参数", nil, nil},
		// --dir 在末尾且没有值：恒不得把它当成有值而吞掉下一个（不存在的）参数
		{"dir 缺值", []string{"--addr", "x", "--dir"}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := restartDirArgs(tc.args); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("restartDirArgs(%q) = %q, want %q", tc.args, got, tc.want)
			}
		})
	}
}
