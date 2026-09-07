package storagehost

import (
	"io"
	"io/fs"
	"mime"
	"net/http"
	"path/filepath"
	"strings"
)

// The device decides a file's media type because the device is the only party
// holding the bytes. Three sources, in order of trust:
//
//  1. a curated extension table for the files this product actually sees —
//     source code, configs, docs — where the OS mime table is unreliable
//     (".ts" is TypeScript here, not a Qt translation file) or absent;
//  2. the OS mime table (mime.TypeByExtension), which knows the long tail;
//  3. the first 512 bytes' magic (http.DetectContentType), the same signatures
//     libmagic uses for the common binary formats, plus a UTF-8/text check.
//
// Parameters are stripped ("; charset=utf-8" says nothing a client wants),
// and "application/octet-stream" is the honest answer for "no idea".

const unknownMediaType = "application/octet-stream"

var curatedMediaTypes = map[string]string{
	".md": "text/markdown", ".markdown": "text/markdown", ".mdown": "text/markdown",
	".txt": "text/plain", ".text": "text/plain", ".log": "text/plain",
	".go": "text/plain", ".rs": "text/plain", ".py": "text/plain", ".rb": "text/plain", ".php": "text/plain",
	".java": "text/plain", ".kt": "text/plain", ".kts": "text/plain", ".swift": "text/plain", ".scala": "text/plain",
	".c": "text/plain", ".cc": "text/plain", ".cpp": "text/plain", ".cxx": "text/plain", ".h": "text/plain", ".hpp": "text/plain", ".m": "text/plain",
	".js": "text/javascript", ".mjs": "text/javascript", ".cjs": "text/javascript", ".jsx": "text/javascript",
	".ts": "text/plain", ".tsx": "text/plain", ".mts": "text/plain", ".cts": "text/plain",
	".sh": "text/plain", ".bash": "text/plain", ".zsh": "text/plain", ".fish": "text/plain", ".ps1": "text/plain", ".bat": "text/plain",
	".css": "text/css", ".scss": "text/plain", ".less": "text/plain", ".html": "text/html", ".htm": "text/html",
	".xml": "text/xml", ".svg": "image/svg+xml",
	".yaml": "text/yaml", ".yml": "text/yaml", ".toml": "text/plain", ".ini": "text/plain", ".cfg": "text/plain", ".conf": "text/plain",
	".env": "text/plain", ".properties": "text/plain", ".editorconfig": "text/plain", ".gitignore": "text/plain",
	".sql": "text/plain", ".graphql": "text/plain", ".proto": "text/plain", ".diff": "text/plain", ".patch": "text/plain",
	".tex": "text/plain", ".rst": "text/plain", ".org": "text/plain",
	".json": "application/json", ".jsonl": "application/json", ".ndjson": "application/json",
	".csv": "text/csv", ".tsv": "text/tab-separated-values",
	".pdf": "application/pdf",
	".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".gif": "image/gif", ".webp": "image/webp", ".bmp": "image/bmp", ".ico": "image/x-icon", ".avif": "image/avif",
	".mp3": "audio/mpeg", ".wav": "audio/wav", ".ogg": "audio/ogg", ".m4a": "audio/mp4", ".flac": "audio/flac",
	".mp4": "video/mp4", ".webm": "video/webm", ".mov": "video/quicktime",
	".zip": "application/zip", ".gz": "application/gzip", ".tar": "application/x-tar",
	".doc": "application/msword", ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
	".xls": "application/vnd.ms-excel", ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
	".ppt": "application/vnd.ms-powerpoint", ".pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
}

// Well-known text files that carry no extension.
var curatedBareNames = map[string]string{
	"makefile": "text/plain", "dockerfile": "text/plain", "license": "text/plain", "readme": "text/plain",
	"changelog": "text/plain", "authors": "text/plain", "todo": "text/plain", "gemfile": "text/plain", "rakefile": "text/plain",
}

const sniffWindow = 512

// mediaType answers for regular files only; a directory has no type.
func (h *Host) mediaType(path string, info fs.FileInfo) string {
	if !info.Mode().IsRegular() {
		return ""
	}
	if byName := mediaTypeByName(filepath.Base(path)); byName != "" {
		return byName
	}
	f, err := h.cr.root.Open(path)
	if err != nil {
		return unknownMediaType
	}
	defer f.Close()
	head := make([]byte, sniffWindow)
	n, err := io.ReadFull(f, head)
	if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
		return unknownMediaType
	}
	return sniffMediaType(head[:n])
}

func mediaTypeByName(base string) string {
	lower := strings.ToLower(base)
	if t, ok := curatedBareNames[lower]; ok {
		return t
	}
	ext := filepath.Ext(lower)
	if ext == "" {
		return ""
	}
	if t, ok := curatedMediaTypes[ext]; ok {
		return t
	}
	return stripParams(mime.TypeByExtension(ext))
}

// sniffMediaType classifies bytes the name could not. An empty file is plain
// text: there is nothing in it to be anything else.
func sniffMediaType(head []byte) string {
	if len(head) == 0 {
		return "text/plain"
	}
	return stripParams(http.DetectContentType(head))
}

func stripParams(value string) string {
	if i := strings.IndexByte(value, ';'); i >= 0 {
		value = value[:i]
	}
	return strings.TrimSpace(value)
}
