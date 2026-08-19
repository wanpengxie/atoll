package xhs

import "github.com/wanpengxie/atoll/lib/introspect"

// describe.go is the actor.describe self-answer catalog — discovery is the
// actor answering live (no external catalog). The shape mirrors echo's, scaled
// to the four xhs business types.

const actorDescription = "Xiaohongshu adapter: publish notes, search, and fetch notes/recent posts through a connected browser extension. Send it xhs.* requests; it translates to the device and returns business results."

const actorSkillDoc = "" +
	"# xhs\n" +
	"\n" +
	"Device-backed adapter for Xiaohongshu (小红书). Drives a connected browser\n" +
	"extension; one extension == one device.\n" +
	"\n" +
	"## Tool surface\n" +
	"\n" +
	"- `xhs.publish` — publish a note `{title, content, images, tags}`; long-running.\n" +
	"- `xhs.search` — keyword search `{keyword, limit?}`.\n" +
	"- `xhs.note.fetch` — fetch one note by `url` or `note_id`+`xsec_token`.\n" +
	"- `xhs.recent.fetch` — fetch the account's recent notes `{limit?}`.\n" +
	"\n" +
	"## Errors\n" +
	"\n" +
	"- `device_offline` — no extension is connected; retry once a device attaches.\n" +
	"- `timeout` — the device did not reply within the type's budget.\n" +
	"\n" +
	"## Describe surface\n" +
	"\n" +
	"- `actor.describe` — returns the actor id, this skill doc, and the four type entries.\n"

func manifest() introspect.Manifest {
	return introspect.Manifest{
		Class: "xhs", Interfaces: []string{"actor"},
		Words: map[string]introspect.WordSpec{
			TypePublish: {
				Description: "Publish a note to Xiaohongshu. Long-running (image upload + post).",
				PayloadFields: []introspect.FieldDoc{
					{Name: "title", Required: true, Description: "Note title."},
					{Name: "content", Required: true, Description: "Note body text."},
					{Name: "images", Description: "Image paths (workdir-relative) or urls.", Example: []string{"img/cover.png"}},
					{Name: "tags", Description: "Topic tags.", Example: []string{"旅行"}},
				},
				ErrorCodes: []string{"device_offline", "timeout"},
				Notes:      "out: {status, note_id, url}",
			},
			TypeSearch: {
				Description: "Search Xiaohongshu by keyword.",
				PayloadFields: []introspect.FieldDoc{
					{Name: "keyword", Required: true, Description: "Search keyword."},
					{Name: "limit", Description: "Max results."},
				},
				Notes: "out: {results: []object}",
			},
			TypeNoteFetch: {
				Description: "Fetch one note. Locate by url, or by note_id + xsec_token.",
				PayloadFields: []introspect.FieldDoc{
					{Name: "url", Description: "Note url (one valid locator)."},
					{Name: "note_id", Description: "Note id (with xsec_token)."},
					{Name: "xsec_token", Description: "Security token paired with note_id."},
				},
				Notes: "out: {note: object}",
			},
			TypeRecentFetch: {
				Description: "Fetch the connected account's recent notes.",
				PayloadFields: []introspect.FieldDoc{
					{Name: "limit", Description: "Max notes."},
				},
				Notes: "out: {notes: []object}",
			},
		},
	}
}
