// Package boundedjson builds a deterministic, valid JSON observation of an
// arbitrarily large JSON value. It preserves small facts and replaces large
// values with typed head/tail summaries until the requested byte budget fits.
package boundedjson

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"
	"unicode/utf8"
)

const (
	sampleBytes       = 128
	smallEntryBytes   = 512
	containerOverhead = 512
	maxLargeEntries   = 8
)

type Metadata struct {
	OriginalBytes int
	SHA256        string
	Projected     bool
}

// Project returns valid JSON no larger than budget. JSON already within the
// budget is returned byte-for-byte; projected JSON is deterministic.
func Project(raw []byte, budget int) (json.RawMessage, Metadata, error) {
	meta := Metadata{OriginalBytes: len(raw), SHA256: digest(raw)}
	if budget <= 0 {
		return nil, meta, errors.New("boundedjson: positive budget required")
	}
	if !json.Valid(raw) {
		return nil, meta, errors.New("boundedjson: invalid JSON")
	}
	if len(raw) <= budget {
		return append(json.RawMessage(nil), raw...), meta, nil
	}

	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, meta, err
	}
	meta.Projected = true

	projected := projectValue(value, budget)
	encoded, err := json.Marshal(projected)
	if err != nil {
		return nil, meta, err
	}
	if len(encoded) <= budget {
		return encoded, meta, nil
	}
	return minimalProjection(meta, budget)
}

func projectValue(value any, budget int) any {
	encoded, err := json.Marshal(value)
	if err == nil && len(encoded) <= budget {
		return value
	}
	if err != nil {
		return map[string]any{"$atoll_cut": true}
	}
	switch node := value.(type) {
	case string:
		return summarizeString(node, encoded, budget)
	case []any:
		return summarizeArray(node, encoded, budget)
	case map[string]any:
		return projectObject(node, encoded, budget)
	default:
		return cutScalar(encoded, budget)
	}
}

type objectEntry struct {
	key   string
	value any
	size  int
}

func projectObject(node map[string]any, encoded []byte, budget int) any {
	if budget < containerOverhead {
		return cutContainer("object", len(node), "original_keys", encoded, budget)
	}
	entries := make([]objectEntry, 0, len(node))
	for key, value := range node {
		valueBytes, err := json.Marshal(value)
		if err != nil {
			continue
		}
		keyBytes, _ := json.Marshal(key)
		entries = append(entries, objectEntry{key: key, value: value, size: len(keyBytes) + 1 + len(valueBytes) + 1})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entryPriority(entries[i].key) != entryPriority(entries[j].key) {
			return entryPriority(entries[i].key) < entryPriority(entries[j].key)
		}
		if entries[i].size != entries[j].size {
			return entries[i].size < entries[j].size
		}
		return entries[i].key < entries[j].key
	})

	out := map[string]any{}
	used := 2
	reserve := min(containerOverhead, budget/3)
	smallLimit := max(0, budget/2)
	selected := map[string]bool{}
	for _, entry := range entries {
		if entry.size > smallEntryBytes || used+entry.size > smallLimit {
			continue
		}
		out[entry.key] = entry.value
		selected[entry.key] = true
		used += entry.size
	}

	large := make([]objectEntry, 0, len(entries))
	for _, entry := range entries {
		if !selected[entry.key] {
			large = append(large, entry)
		}
	}
	sort.Slice(large, func(i, j int) bool {
		if large[i].size != large[j].size {
			return large[i].size > large[j].size
		}
		return large[i].key < large[j].key
	})
	if len(large) > maxLargeEntries {
		large = large[:maxLargeEntries]
	}
	for index, entry := range large {
		remainingSlots := len(large) - index
		available := budget - reserve - used
		if available <= 64 || remainingSlots <= 0 {
			break
		}
		valueBudget := max(64, available/remainingSlots-entryKeyCost(entry.key))
		candidate := projectValue(entry.value, valueBudget)
		cost := entryCost(entry.key, candidate)
		if used+cost+reserve > budget {
			continue
		}
		out[entry.key] = candidate
		selected[entry.key] = true
		used += cost
	}

	omitted := len(node) - len(selected)
	markerKey := uniqueMarkerKey(node)
	marker := map[string]any{
		"type": "object", "original_bytes": len(encoded), "original_keys": len(node),
		"omitted_keys": omitted, "sha256": digest(encoded),
	}
	out[markerKey] = marker
	if fitted, ok := fitObject(out, markerKey, budget); ok {
		return fitted
	}
	return cutContainer("object", len(node), "original_keys", encoded, budget)
}

func entryPriority(key string) int {
	switch key {
	case "status", "reason", "error_code", "detail":
		return 0
	case "turn_id", "kind", "phase", "outcome", "tool_call_id", "tool":
		return 1
	default:
		return 2
	}
}

func fitObject(out map[string]any, markerKey string, budget int) (map[string]any, bool) {
	for {
		encoded, err := json.Marshal(out)
		if err != nil {
			return nil, false
		}
		if len(encoded) <= budget {
			return out, true
		}
		var largest string
		largestSize := -1
		for key, value := range out {
			if key == markerKey {
				continue
			}
			size := entryCost(key, value)
			if size > largestSize {
				largest, largestSize = key, size
			}
		}
		if largest == "" {
			return nil, false
		}
		delete(out, largest)
		if marker, ok := out[markerKey].(map[string]any); ok {
			marker["omitted_keys"] = marker["omitted_keys"].(int) + 1
		}
	}
}

func summarizeString(node string, encoded []byte, budget int) any {
	marker := map[string]any{
		"type": "string", "original_bytes": len(node), "sha256": digest([]byte(node)),
	}
	if budget > containerOverhead {
		head := utf8Head(node, sampleBytes)
		tail := utf8Tail(node, sampleBytes)
		marker["head"], marker["tail"] = head, tail
		marker["omitted_bytes"] = max(0, len(node)-len(head)-len(tail))
	}
	return fitCut(marker, encoded, budget)
}

func summarizeArray(node []any, encoded []byte, budget int) any {
	marker := map[string]any{
		"type": "array", "original_bytes": len(encoded), "original_items": len(node),
		"sha256": digest(encoded),
	}
	if budget > containerOverhead && len(node) > 0 {
		itemBudget := max(64, (budget-containerOverhead)/4)
		headCount := min(2, len(node))
		tailCount := min(2, max(0, len(node)-headCount))
		head := make([]any, 0, headCount)
		for i := 0; i < headCount; i++ {
			head = append(head, projectValue(node[i], itemBudget))
		}
		tail := make([]any, 0, tailCount)
		for i := len(node) - tailCount; i < len(node); i++ {
			tail = append(tail, projectValue(node[i], itemBudget))
		}
		marker["head"], marker["tail"] = head, tail
		marker["omitted_items"] = max(0, len(node)-headCount-tailCount)
	}
	return fitCut(marker, encoded, budget)
}

func cutScalar(encoded []byte, budget int) any {
	marker := map[string]any{"type": "scalar", "original_bytes": len(encoded), "sha256": digest(encoded)}
	if budget > containerOverhead {
		marker["head"] = utf8Head(string(encoded), sampleBytes)
		marker["tail"] = utf8Tail(string(encoded), sampleBytes)
	}
	return fitCut(marker, encoded, budget)
}

func cutContainer(kind string, count int, countKey string, encoded []byte, budget int) any {
	marker := map[string]any{
		"type": kind, "original_bytes": len(encoded), countKey: count,
		"sha256": digest(encoded), "reason": "projection_budget_exhausted",
	}
	return fitCut(marker, encoded, budget)
}

func fitCut(marker map[string]any, _ []byte, budget int) any {
	wrapped := map[string]any{"$atoll_cut": marker}
	encoded, _ := json.Marshal(wrapped)
	if len(encoded) <= budget {
		return wrapped
	}
	delete(marker, "head")
	delete(marker, "tail")
	delete(marker, "omitted_bytes")
	delete(marker, "omitted_items")
	encoded, _ = json.Marshal(wrapped)
	if len(encoded) <= budget {
		return wrapped
	}
	return map[string]any{"$atoll_cut": true}
}

func minimalProjection(meta Metadata, budget int) (json.RawMessage, Metadata, error) {
	minimal := map[string]any{"$atoll_cut": map[string]any{
		"type": "json", "reason": "projection_budget_exhausted",
		"original_bytes": meta.OriginalBytes, "sha256": meta.SHA256,
	}}
	encoded, err := json.Marshal(minimal)
	if err != nil {
		return nil, meta, err
	}
	if len(encoded) <= budget {
		return encoded, meta, nil
	}
	last := json.RawMessage(`{"$atoll_cut":true}`)
	if len(last) > budget {
		return nil, meta, errors.New("boundedjson: budget too small for fallback")
	}
	return last, meta, nil
}

func uniqueMarkerKey(node map[string]any) string {
	key := "$atoll_cut"
	for {
		if _, exists := node[key]; !exists {
			return key
		}
		key += "_"
	}
}

func entryKeyCost(key string) int {
	encoded, _ := json.Marshal(key)
	return len(encoded) + 2
}

func entryCost(key string, value any) int {
	keyBytes, _ := json.Marshal(key)
	valueBytes, _ := json.Marshal(value)
	return len(keyBytes) + 1 + len(valueBytes) + 1
}

func utf8Head(value string, budget int) string {
	if len(value) <= budget {
		return value
	}
	end := budget
	for end > 0 && !utf8.RuneStart(value[end]) {
		end--
	}
	return value[:end]
}

func utf8Tail(value string, budget int) string {
	if len(value) <= budget {
		return value
	}
	start := len(value) - budget
	for start < len(value) && !utf8.RuneStart(value[start]) {
		start++
	}
	return value[start:]
}

func digest(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}
