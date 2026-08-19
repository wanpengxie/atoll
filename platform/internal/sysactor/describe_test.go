package sysactor

import (
	"strings"
	"testing"

	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/protocol/message"
)

func TestSystemManifestUsesTheClosedSystemVocabulary(t *testing.T) {
	manifest := systemManifest()
	docs := introspect.SystemWordSpecs()
	if len(docs) != len(message.SystemEntries()) {
		t.Fatalf("documented words=%d closed entries=%d", len(docs), len(message.SystemEntries()))
	}
	if manifest.Class != "sysactor" || len(manifest.Interfaces) != 1 || manifest.Interfaces[0] != "actor" {
		t.Fatalf("manifest=%+v", manifest)
	}
	for _, entry := range message.SystemEntries() {
		_, present := manifest.Words[entry.Name]
		if present != (entry.Kind == message.KindRequest) {
			t.Fatalf("word %q present=%v kind=%q", entry.Name, present, entry.Kind)
		}
		doc, documented := docs[entry.Name]
		if !documented || strings.Count(doc.Description, "\n") != 1 || len(doc.InputSchema) == 0 {
			t.Fatalf("word %q documentation=%+v", entry.Name, doc)
		}
	}
}
