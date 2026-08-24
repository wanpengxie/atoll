package e2e

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestHumanFileCreatePutAndGetThroughDataPlane(t *testing.T) {
	h := newHarness(t)
	api, ws := rootClient(t, h, map[string]int64{c0ChannelID: 0})
	registrar := findRegistrar(t, ws)
	device := registrarRequest(t, ws, c0ChannelID, registrar, "system.device.create", map[string]any{"name": "file-host"})
	deviceID := stringField(t, device, "id")
	deviceKey := stringField(t, device, "key")
	registrarRequest(t, ws, c0ChannelID, registrar, "system.device.attach", map[string]any{"channel_id": c0ChannelID, "device_id": deviceID})
	const declarationID = "file-host-readiness"
	registrarRequest(t, ws, c0ChannelID, registrar, "system.actor.template.create", map[string]any{
		// A declaration's name is the word the member it seats is called by, so
		// it obeys the name law; the sentence goes in description.
		"id": declarationID, "name": "file-host-readiness", "class": "echo",
		"description": "File host readiness probe.",
		"config":      map[string]any{}, "visibility": "private",
	})
	introduced := ws.request(c0ChannelID, "system.member.create", systemActor, map[string]any{"decl_id": declarationID})
	echoID := stringField(t, introduced, "member")

	daemonLog := filepath.Join(h.root, "logs", "file-host.log")
	daemon := startProc(t, "file-host", filepath.Join(e2eBinDir, "atoll-daemon"), []string{
		"--server", fmt.Sprintf("ws://127.0.0.1:%d/compute", h.port),
		"--key", deviceKey, "--name", "file-host", "--home", filepath.Join(h.root, "file-host"),
	}, h.env, filepath.Join(h.root, "work"), daemonLog)
	waitActorPresence(t, ws, echoID, true, daemon, daemonLog)

	address := "daemon://file-host/c0/e2e/report.bin"
	created := ws.resource(map[string]any{
		"channel_id": c0ChannelID, "op": "create", "address": address,
		"with_content": true,
	})
	ticket := stringField(t, created, "ticket")
	if created["redeem"] != "http" {
		t.Fatalf("create outcome=%v", created)
	}
	want := bytes.Repeat([]byte("data-plane-e2e\n"), 4096)
	endpoint := h.base + "/files?channel_id=" + url.QueryEscape(c0ChannelID) + "&t=" + url.QueryEscape(ticket)
	req, err := http.NewRequest(http.MethodPut, endpoint, bytes.NewReader(want))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := api.http.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	putBody, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("PUT status=%d body=%s\ndaemon log:\n%s", resp.StatusCode, putBody, tailLog(daemonLog, 100))
	}
	physical := filepath.Join(h.root, "file-host", "daemons", deviceID, "channels", c0ChannelID, "e2e", "report.bin")
	if onDisk, err := os.ReadFile(physical); err != nil || !bytes.Equal(onDisk, want) {
		t.Fatalf("channel tree bytes=%d err=%v, want collaboration bytes=%d", len(onDisk), err, len(want))
	}

	opened := ws.resource(map[string]any{"channel_id": c0ChannelID, "op": "read", "resource_id": address})
	readTicket := stringField(t, opened, "ticket")
	resp, err = api.http.Get(h.base + "/files?channel_id=" + url.QueryEscape(c0ChannelID) + "&t=" + url.QueryEscape(readTicket))
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !bytes.Equal(got, want) {
		t.Fatalf("GET status=%d bytes=%d want=%d\ndaemon log:\n%s", resp.StatusCode, len(got), len(want), tailLog(daemonLog, 100))
	}
}

func TestQualifiedChannelAddressMatchesDiskAndRetirementLeavesBytes(t *testing.T) {
	h := newHarness(t)
	api, ws := rootClient(t, h, map[string]int64{c0ChannelID: 0})
	registrar := findRegistrar(t, ws)
	// parent is never a field: a channel is created from inside its parent, and
	// the registrar in c0 makes c0 the parent.
	createdChannel := createChannelWithRoot(t, ws, c0ChannelID, registrar, "archive")
	channelID := stringField(t, createdChannel, "channel_id")
	channelRow := registrarRequest(t, ws, c0ChannelID, registrar, "system.channel.get", map[string]any{"channel_id": channelID})
	qualified := stringField(t, channelRow, "qualified_name")
	if qualified != "c0.archive" {
		t.Fatalf("qualified channel name=%q", qualified)
	}
	device := registrarRequest(t, ws, c0ChannelID, registrar, "system.device.create", map[string]any{"name": "archive-host"})
	deviceID := stringField(t, device, "id")
	attachDevice(t, ws, channelID, deviceID)
	daemonLog := filepath.Join(h.root, "logs", "archive-host.log")
	daemon := startProc(t, "archive-host", filepath.Join(e2eBinDir, "atoll-daemon"), []string{
		"--server", fmt.Sprintf("ws://127.0.0.1:%d/compute", h.port),
		"--key", stringField(t, device, "key"), "--name", "archive-host", "--home", filepath.Join(h.root, "archive-host"),
	}, h.env, filepath.Join(h.root, "work"), daemonLog)

	address := "daemon://archive-host/c0.archive/docs/report.txt"
	var outcome map[string]any
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if daemon.exited() {
			t.Fatalf("archive daemon exited\n%s", tailLog(daemonLog, 100))
		}
		if opened, err := ws.tryResource(map[string]any{"channel_id": channelID, "op": "create", "address": address, "with_content": true}); err == nil {
			outcome = opened
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if outcome == nil {
		t.Fatalf("qualified channel file route did not become ready\n%s", tailLog(daemonLog, 100))
	}
	want := []byte("qualified-channel-bytes")
	httpPutFile(t, api, h.base, channelID, address, stringField(t, outcome, "ticket"), want)
	channelRoot := filepath.Join(h.root, "archive-host", "daemons", deviceID, "channels", qualified)
	physical := filepath.Join(channelRoot, "docs", "report.txt")
	if got, err := os.ReadFile(physical); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("qualified disk path bytes=%q err=%v", got, err)
	}

	reverse := filepath.Join(channelRoot, "docs", "from-disk.txt")
	if err := os.WriteFile(reverse, []byte("disk-visible"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := httpReadFile(t, api, h.base, ws, channelID, "daemon://archive-host/c0.archive/docs/from-disk.txt"); string(got) != "disk-visible" {
		t.Fatalf("reverse disk read=%q", got)
	}
	registrarRequest(t, ws, c0ChannelID, registrar, "system.channel.delete", map[string]any{"channel_id": channelID})
	if got, err := os.ReadFile(physical); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("retirement changed ordinary file bytes=%q err=%v", got, err)
	}
	// Retirement keeps the bytes and ends the collaboration: the channel stops
	// being a place its members can act in, so nothing can be asked of it any
	// more. The files stay on disk as ordinary files for their owner to take.
	retireDeadline := time.Now().Add(20 * time.Second)
	for {
		if _, _, err := ws.tryRequest(channelID, "system.member.list", systemActor, map[string]any{}); err != nil {
			break
		}
		if time.Now().After(retireDeadline) {
			t.Fatal("retired channel still accepts collaboration")
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func TestParentAndChildChannelsStayFlatOnDifferentDaemons(t *testing.T) {
	h := newHarness(t)
	_, ws := rootClient(t, h, map[string]int64{c0ChannelID: 0})
	registrar := findRegistrar(t, ws)
	parent := createChannelWithRoot(t, ws, c0ChannelID, registrar, "project")
	parentID := stringField(t, parent, "channel_id")
	// The child is created from inside the parent: root (owner, hence member of
	// project) speaks system.channel.create to project's own door, and project
	// becomes the parent because that is where the request was made.
	projectDoor := awaitDoor(t, ws, parentID)
	childReply := ws.request(parentID, "system.channel.create", projectDoor, map[string]any{
		"name": "backend",
		// 从 project 内部建 backend，同样要显式说明谁被带进去——这里带的是
		// project 里的 root（与 c0 里的 root 是同一个人、不同的 actor id）。
		// 这条经频道自己的门走，故用门的形 initial_actor_ids。
		"initial_actor_ids": []any{rootActorID(t, ws, parentID)},
	})
	child, _ := childReply["value"].(map[string]any)
	if child == nil {
		t.Fatalf("system.channel.create through project door omitted value: %v", childReply)
	}
	childID := stringField(t, child, "channel_id")

	type daemonCase struct {
		name, channelID, qualified string
	}
	cases := []daemonCase{{"parent-host", parentID, "c0.project"}, {"child-host", childID, "c0.project.backend"}}
	var childChannels string
	for _, tc := range cases {
		device := registrarRequest(t, ws, c0ChannelID, registrar, "system.device.create", map[string]any{"name": tc.name})
		deviceID := stringField(t, device, "id")
		attachDevice(t, ws, tc.channelID, deviceID)
		logPath := filepath.Join(h.root, "logs", tc.name+".log")
		proc := startProc(t, tc.name, filepath.Join(e2eBinDir, "atoll-daemon"), []string{
			"--server", fmt.Sprintf("ws://127.0.0.1:%d/compute", h.port),
			"--key", stringField(t, device, "key"), "--name", tc.name, "--home", filepath.Join(h.root, tc.name),
		}, h.env, filepath.Join(h.root, "work"), logPath)
		channelsRoot := filepath.Join(h.root, tc.name, "daemons", deviceID, "channels")
		if tc.name == "child-host" {
			childChannels = channelsRoot
		}
		deadline := time.Now().Add(20 * time.Second)
		for time.Now().Before(deadline) {
			if proc.exited() {
				t.Fatalf("%s exited\n%s", tc.name, tailLog(logPath, 100))
			}
			if info, err := os.Stat(filepath.Join(channelsRoot, tc.qualified)); err == nil && info.IsDir() {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		entries, err := os.ReadDir(channelsRoot)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 1 || entries[0].Name() != tc.qualified {
			t.Fatalf("%s channel directories=%v, want only %q", tc.name, entries, tc.qualified)
		}
	}
	if _, err := os.Stat(filepath.Join(childChannels, "c0.project")); !os.IsNotExist(err) {
		t.Fatalf("child daemon contains parent shell: %v", err)
	}
}

func httpPutFile(t *testing.T, api *apiClient, base, channelID, address, ticket string, content []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, base+"/files?channel_id="+url.QueryEscape(channelID)+"&t="+url.QueryEscape(ticket), bytes.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := api.http.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("PUT %s status=%d body=%s", address, resp.StatusCode, body)
	}
}

func httpReadFile(t *testing.T, api *apiClient, base string, ws *wsClient, channelID, address string) []byte {
	t.Helper()
	opened := ws.resource(map[string]any{"channel_id": channelID, "op": "read", "resource_id": address})
	endpoint := base + "/files?channel_id=" + url.QueryEscape(channelID) + "&t=" + url.QueryEscape(stringField(t, opened, "ticket"))
	resp, err := api.http.Get(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s status=%d body=%s", address, resp.StatusCode, body)
	}
	wantName := filepath.Base(strings.TrimPrefix(address, "daemon://storage-node/"))
	if disposition := resp.Header.Get("Content-Disposition"); !strings.Contains(disposition, wantName) {
		t.Fatalf("Content-Disposition=%q, want basename %q", disposition, wantName)
	}
	return body
}
