package sysactor

import (
	"testing"

	"github.com/wanpengxie/atoll/protocol/message"
)

func TestSystemManifestUsesTheClosedSystemVocabulary(t *testing.T) {
	manifest := systemManifest()
	if manifest.Class != "membrane" || len(manifest.Interfaces) != 1 || manifest.Interfaces[0] != "actor" {
		t.Fatalf("manifest=%+v", manifest)
	}
	for _, entry := range message.SystemEntries() {
		_, present := manifest.Words[entry.Name]
		if present != (entry.Kind == message.KindRequest) {
			t.Fatalf("word %q present=%v kind=%q", entry.Name, present, entry.Kind)
		}
	}
}
