package coagent

import (
	"reflect"
	"testing"
)

func TestSplitFlagArgs_FlagsBeforePositional(t *testing.T) {
	flagArgs, pos := splitFlagArgs(
		[]string{"--type", "agent.text", "hello", "world"},
		boolFlagNames(),
	)
	wantFlag := []string{"--type", "agent.text"}
	wantPos := []string{"hello", "world"}
	if !reflect.DeepEqual(flagArgs, wantFlag) {
		t.Fatalf("flagArgs = %v, want %v", flagArgs, wantFlag)
	}
	if !reflect.DeepEqual(pos, wantPos) {
		t.Fatalf("pos = %v, want %v", pos, wantPos)
	}
}

func TestSplitFlagArgs_FlagsAfterPositional(t *testing.T) {
	flagArgs, pos := splitFlagArgs(
		[]string{"hello", "world", "--type", "agent.text", "--parent", "m-1"},
		boolFlagNames(),
	)
	wantFlag := []string{"--type", "agent.text", "--parent", "m-1"}
	wantPos := []string{"hello", "world"}
	if !reflect.DeepEqual(flagArgs, wantFlag) {
		t.Fatalf("flagArgs = %v, want %v", flagArgs, wantFlag)
	}
	if !reflect.DeepEqual(pos, wantPos) {
		t.Fatalf("pos = %v, want %v", pos, wantPos)
	}
}

func TestSplitFlagArgs_BoolFlagsConsumeNoValue(t *testing.T) {
	flagArgs, pos := splitFlagArgs(
		[]string{"note", "--private", "--type", "agent.text"},
		boolFlagNames(),
	)
	wantFlag := []string{"--private", "--type", "agent.text"}
	wantPos := []string{"note"}
	if !reflect.DeepEqual(flagArgs, wantFlag) {
		t.Fatalf("flagArgs = %v, want %v", flagArgs, wantFlag)
	}
	if !reflect.DeepEqual(pos, wantPos) {
		t.Fatalf("pos = %v, want %v", pos, wantPos)
	}
}

func TestSplitFlagArgs_EqualsSyntax(t *testing.T) {
	flagArgs, pos := splitFlagArgs(
		[]string{"--type=agent.text", "hi"},
		boolFlagNames(),
	)
	wantFlag := []string{"--type=agent.text"}
	wantPos := []string{"hi"}
	if !reflect.DeepEqual(flagArgs, wantFlag) {
		t.Fatalf("flagArgs = %v, want %v", flagArgs, wantFlag)
	}
	if !reflect.DeepEqual(pos, wantPos) {
		t.Fatalf("pos = %v, want %v", pos, wantPos)
	}
}

func TestSplitFlagArgs_DoubleDashTerminator(t *testing.T) {
	flagArgs, pos := splitFlagArgs(
		[]string{"--type", "agent.text", "--", "--this", "is", "positional"},
		boolFlagNames(),
	)
	wantFlag := []string{"--type", "agent.text"}
	wantPos := []string{"--this", "is", "positional"}
	if !reflect.DeepEqual(flagArgs, wantFlag) {
		t.Fatalf("flagArgs = %v, want %v", flagArgs, wantFlag)
	}
	if !reflect.DeepEqual(pos, wantPos) {
		t.Fatalf("pos = %v, want %v", pos, wantPos)
	}
}

func TestSplitFlagArgs_LoneDashIsPositional(t *testing.T) {
	flagArgs, pos := splitFlagArgs(
		[]string{"-", "--type", "agent.text"},
		boolFlagNames(),
	)
	wantFlag := []string{"--type", "agent.text"}
	wantPos := []string{"-"}
	if !reflect.DeepEqual(flagArgs, wantFlag) {
		t.Fatalf("flagArgs = %v, want %v", flagArgs, wantFlag)
	}
	if !reflect.DeepEqual(pos, wantPos) {
		t.Fatalf("pos = %v, want %v", pos, wantPos)
	}
}
