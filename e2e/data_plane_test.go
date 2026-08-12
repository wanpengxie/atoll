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
	registrar := findTool(t, ws)
	device := registrarRequest(t, ws, registrar, "device.mint", map[string]any{"name": "file-host"})
	deviceID := stringField(t, device, "id")
	deviceKey := stringField(t, device, "key")
	registrarRequest(t, ws, registrar, "device.attach", map[string]any{"channel_id": c0ChannelID, "device_id": deviceID})
	const declarationID = "file-host-readiness"
	registrarRequest(t, ws, registrar, "decl.register", map[string]any{
		"id": declarationID, "name": "file host readiness", "class": "echo",
		"config": map[string]any{}, "visibility": "private",
	})
	introduced := ws.request(c0ChannelID, "channel.introduce_actor", systemActor, map[string]any{"kind": "tool", "decl_id": declarationID})
	echoID := stringField(t, introduced, "instance_id")

	daemonLog := filepath.Join(h.root, "logs", "file-host.log")
	daemon := startProc(t, "file-host", filepath.Join(e2eBinDir, "atoll-daemon"), []string{
		"--server", fmt.Sprintf("ws://127.0.0.1:%d/compute", h.port),
		"--key", deviceKey, "--name", "file-host", "--home", filepath.Join(h.root, "file-host"),
	}, h.env, filepath.Join(h.root, "work"), daemonLog)
	waitActorPresence(t, ws, echoID, true, daemon, daemonLog)

	address := "daemon://file-host/c0/e2e/report.bin"
	created := ws.resource(map[string]any{
		"op": "create", "address": address,
		"with_content": true,
	})
	ticket := stringField(t, created, "ticket")
	if created["redeem"] != "http" {
		t.Fatalf("create outcome=%v", created)
	}
	want := bytes.Repeat([]byte("data-plane-e2e\n"), 4096)
	endpoint := h.base + "/files/" + url.PathEscape(address) + "?t=" + url.QueryEscape(ticket)
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

	opened := ws.resource(map[string]any{"op": "read", "resource_id": address})
	readTicket := stringField(t, opened, "ticket")
	resp, err = api.http.Get(h.base + "/files/" + url.PathEscape(address) + "?t=" + url.QueryEscape(readTicket))
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
	registrar := findTool(t, ws)
	createdChannel := registrarRequest(t, ws, registrar, "channel.create", map[string]any{
		"name": "archive", "parent": c0ChannelID,
	})
	channelID := stringField(t, createdChannel, "id")
	qualified := stringField(t, createdChannel, "qualified_name")
	if qualified != "c0.archive" {
		t.Fatalf("qualified channel name=%q", qualified)
	}
	device := registrarRequest(t, ws, registrar, "device.mint", map[string]any{"name": "archive-host"})
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
		if opened, err := ws.tryResource(map[string]any{"op": "create", "address": address, "with_content": true}); err == nil {
			outcome = opened
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if outcome == nil {
		t.Fatalf("qualified channel file route did not become ready\n%s", tailLog(daemonLog, 100))
	}
	want := []byte("qualified-channel-bytes")
	httpPutFile(t, api, h.base, address, stringField(t, outcome, "ticket"), want)
	channelRoot := filepath.Join(h.root, "archive-host", "daemons", deviceID, "channels", qualified)
	physical := filepath.Join(channelRoot, "docs", "report.txt")
	if got, err := os.ReadFile(physical); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("qualified disk path bytes=%q err=%v", got, err)
	}

	reverse := filepath.Join(channelRoot, "docs", "from-disk.txt")
	if err := os.WriteFile(reverse, []byte("disk-visible"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := httpReadFile(t, api, h.base, ws, "daemon://archive-host/c0.archive/docs/from-disk.txt"); string(got) != "disk-visible" {
		t.Fatalf("reverse disk read=%q", got)
	}
	registrarRequest(t, ws, registrar, "channel.retire", map[string]any{"channel_id": channelID})
	if got, err := os.ReadFile(physical); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("retirement changed ordinary file bytes=%q err=%v", got, err)
	}
	// Retirement keeps the bytes and ends the collaboration: the channel stops
	// being a place its members can act in, so nothing can be asked of it any
	// more. The files stay on disk as ordinary files for their owner to take.
	retireDeadline := time.Now().Add(20 * time.Second)
	for {
		if _, _, err := ws.tryRequest(channelID, "actor.list", systemActor, map[string]any{}); err != nil {
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
	registrar := findTool(t, ws)
	parent := registrarRequest(t, ws, registrar, "channel.create", map[string]any{"name": "project", "parent": c0ChannelID})
	parentID := stringField(t, parent, "id")
	child := registrarRequest(t, ws, registrar, "channel.create", map[string]any{"name": "backend", "parent": parentID})
	childID := stringField(t, child, "id")

	type daemonCase struct {
		name, channelID, qualified string
	}
	cases := []daemonCase{{"parent-host", parentID, "c0.project"}, {"child-host", childID, "c0.project.backend"}}
	var childChannels string
	for _, tc := range cases {
		device := registrarRequest(t, ws, registrar, "device.mint", map[string]any{"name": tc.name})
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

func TestCrossDeviceFileReadWriteCreateAndOfflineSemantics(t *testing.T) {
	h := newHarness(t)
	api, ws := rootClient(t, h, map[string]int64{c0ChannelID: 0})
	registrar := findTool(t, ws)

	// Bind only B while introducing the actor, making its placement
	// deterministic. A joins afterwards and is used solely as the file host.
	actorDevice := registrarRequest(t, ws, registrar, "device.mint", map[string]any{"name": "actor-node"})
	actorDeviceID := stringField(t, actorDevice, "id")
	actorDeviceKey := stringField(t, actorDevice, "key")
	registrarRequest(t, ws, registrar, "device.attach", map[string]any{"channel_id": c0ChannelID, "device_id": actorDeviceID})
	const declarationID = "cross-device-file-probe"
	registrarRequest(t, ws, registrar, "decl.register", map[string]any{
		"id": declarationID, "name": "cross-device file probe", "class": "echo",
		"config": map[string]any{}, "visibility": "private",
	})
	introduced := ws.request(c0ChannelID, "channel.introduce_actor", systemActor, map[string]any{"kind": "tool", "decl_id": declarationID})
	actorID := stringField(t, introduced, "instance_id")
	actorLog := filepath.Join(h.root, "logs", "actor-node.log")
	actorDaemon := startProc(t, "actor-node", filepath.Join(e2eBinDir, "atoll-daemon"), []string{
		"--server", fmt.Sprintf("ws://127.0.0.1:%d/compute", h.port),
		"--key", actorDeviceKey, "--name", "actor-node", "--home", filepath.Join(h.root, "actor-node"),
	}, h.env, filepath.Join(h.root, "work"), actorLog)
	waitActorPresence(t, ws, actorID, true, actorDaemon, actorLog)

	storageDevice := registrarRequest(t, ws, registrar, "device.mint", map[string]any{"name": "storage-node"})
	storageDeviceID := stringField(t, storageDevice, "id")
	storageDeviceKey := stringField(t, storageDevice, "key")
	registrarRequest(t, ws, registrar, "device.attach", map[string]any{"channel_id": c0ChannelID, "device_id": storageDeviceID})
	storageLog := filepath.Join(h.root, "logs", "storage-node.log")
	storageDaemon := startProc(t, "storage-node", filepath.Join(e2eBinDir, "atoll-daemon"), []string{
		"--server", fmt.Sprintf("ws://127.0.0.1:%d/compute", h.port),
		"--key", storageDeviceKey, "--name", "storage-node", "--home", filepath.Join(h.root, "storage-node"),
	}, h.env, filepath.Join(h.root, "work"), storageLog)
	waitStorageReady(t, ws, storageDaemon, storageLog)

	address := "daemon://storage-node/c0/e2e/cross.bin"
	original := "bytes-created-on-A"
	created := ws.resource(map[string]any{"op": "create", "address": address, "with_content": true})
	httpPutFile(t, api, h.base, address, stringField(t, created, "ticket"), []byte(original))

	// A hosts the file; the echo actor is pinned to B and reads through the
	// caller↔server↔A pair of exchange streams.
	readReply := ws.request(c0ChannelID, "echo.file_read", actorID, map[string]any{"address": address})
	if readReply["content"] != original {
		t.Fatalf("cross-device read reply=%v\nA log:\n%s\nB log:\n%s", readReply, tailLog(storageLog, 100), tailLog(actorLog, 100))
	}

	rewritten := "bytes-written-from-B"
	writeReply := ws.request(c0ChannelID, "echo.file_write", actorID, map[string]any{"address": address, "content": rewritten})
	if writeReply["ok"] != true {
		t.Fatalf("cross-device write reply=%v", writeReply)
	}
	if got := httpReadFile(t, api, h.base, ws, address); string(got) != rewritten {
		t.Fatalf("A bytes after B write=%q, want %q", got, rewritten)
	}

	createdByB := "daemon://storage-node/c0/e2e/created-by-B.bin"
	createReply := ws.request(c0ChannelID, "echo.file_create", actorID, map[string]any{"address": createdByB, "content": "created-remotely"})
	if createReply["ok"] != true {
		t.Fatalf("cross-device create reply=%v", createReply)
	}
	if got := httpReadFile(t, api, h.base, ws, createdByB); string(got) != "created-remotely" {
		t.Fatalf("remote create bytes=%q", got)
	}

	// Mint while A is online, then redeem only after it is gone.
	preissued := ws.resource(map[string]any{"op": "read", "resource_id": address})
	preissuedTicket := stringField(t, preissued, "ticket")
	storageDaemon.kill9(t)

	const offlineText = "accessdoor: daemon host offline: storage-node"
	started := time.Now()
	endpoint := h.base + "/files/" + url.PathEscape(address) + "?t=" + url.QueryEscape(preissuedTicket)
	resp, requestErr := api.http.Get(endpoint)
	if requestErr != nil {
		t.Fatal(requestErr)
	}
	offlineBody, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("post-issue offline redemption took %s", elapsed)
	}
	if resp.StatusCode != http.StatusServiceUnavailable || !strings.Contains(string(offlineBody), offlineText) {
		t.Fatalf("post-issue offline status=%d body=%s", resp.StatusCode, offlineBody)
	}

	started = time.Now()
	_, issueFailure, err := ws.tryRequest(c0ChannelID, "echo.file_read", actorID, map[string]any{"address": address})
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("issue-time offline failure took %s", elapsed)
	}
	if err == nil || !strings.Contains(fmt.Sprint(issueFailure), offlineText) {
		t.Fatalf("issue-time offline result=%v err=%v", issueFailure, err)
	}
}

func TestColocatedActorFileCreateReadWriteUsesLocalRedemption(t *testing.T) {
	h := newHarness(t)
	_, ws := rootClient(t, h, map[string]int64{c0ChannelID: 0})
	registrar := findTool(t, ws)
	device := registrarRequest(t, ws, registrar, "device.mint", map[string]any{"name": "colocated-node"})
	deviceID := stringField(t, device, "id")
	deviceKey := stringField(t, device, "key")
	registrarRequest(t, ws, registrar, "device.attach", map[string]any{"channel_id": c0ChannelID, "device_id": deviceID})
	const declarationID = "colocated-file-probe"
	registrarRequest(t, ws, registrar, "decl.register", map[string]any{
		"id": declarationID, "name": "colocated file probe", "class": "echo",
		"config": map[string]any{}, "visibility": "private",
	})
	introduced := ws.request(c0ChannelID, "channel.introduce_actor", systemActor, map[string]any{"kind": "tool", "decl_id": declarationID})
	actorID := stringField(t, introduced, "instance_id")
	daemonLog := filepath.Join(h.root, "logs", "colocated-node.log")
	daemon := startProc(t, "colocated-node", filepath.Join(e2eBinDir, "atoll-daemon"), []string{
		"--server", fmt.Sprintf("ws://127.0.0.1:%d/compute", h.port),
		"--key", deviceKey, "--name", "colocated-node", "--home", filepath.Join(h.root, "colocated-node"),
	}, h.env, filepath.Join(h.root, "work"), daemonLog)
	waitActorPresence(t, ws, actorID, true, daemon, daemonLog)

	address := "daemon://colocated-node/c0/e2e/local.bin"
	created := ws.request(c0ChannelID, "echo.file_create", actorID, map[string]any{"address": address, "content": "created-locally"})
	if created["ok"] != true || created["redeem"] != "local" {
		t.Fatalf("colocated create reply=%v\ndaemon log:\n%s", created, tailLog(daemonLog, 100))
	}
	read := ws.request(c0ChannelID, "echo.file_read", actorID, map[string]any{"address": address})
	if read["content"] != "created-locally" || read["redeem"] != "local" {
		t.Fatalf("colocated read reply=%v", read)
	}
	written := ws.request(c0ChannelID, "echo.file_write", actorID, map[string]any{"address": address, "content": "rewritten-locally"})
	if written["ok"] != true || written["redeem"] != "local" {
		t.Fatalf("colocated write reply=%v", written)
	}
	read = ws.request(c0ChannelID, "echo.file_read", actorID, map[string]any{"address": address})
	if read["content"] != "rewritten-locally" || read["redeem"] != "local" {
		t.Fatalf("colocated final read reply=%v", read)
	}
	channelRoot := filepath.Join(h.root, "colocated-node", "daemons", deviceID, "channels", c0ChannelID)
	physical := filepath.Join(channelRoot, "e2e", "local.bin")
	if got, err := os.ReadFile(physical); err != nil || string(got) != "rewritten-locally" {
		t.Fatalf("agent workspace file=%q err=%v", got, err)
	}
	if err := os.WriteFile(filepath.Join(channelRoot, "e2e", "from-bash.txt"), []byte("bash-visible"), 0o600); err != nil {
		t.Fatal(err)
	}
	read = ws.request(c0ChannelID, "echo.file_read", actorID, map[string]any{"address": "daemon://colocated-node/c0/e2e/from-bash.txt"})
	if read["content"] != "bash-visible" || read["redeem"] != "local" {
		t.Fatalf("file written in agent workdir was not the collaboration file: %v", read)
	}
}

func waitStorageReady(t *testing.T, ws *wsClient, daemon *proc, logPath string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for attempt := 0; ; attempt++ {
		if daemon.exited() {
			t.Fatalf("storage daemon exited while waiting for its lane\n%s", tailLog(logPath, 100))
		}
		address := fmt.Sprintf("daemon://storage-node/c0/e2e/readiness-%d", attempt)
		if _, err := ws.tryResource(map[string]any{"op": "create", "address": address}); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("storage daemon did not become ready\n%s", tailLog(logPath, 100))
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func httpPutFile(t *testing.T, api *apiClient, base, address, ticket string, content []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, base+"/files/"+url.PathEscape(address)+"?t="+url.QueryEscape(ticket), bytes.NewReader(content))
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

func httpReadFile(t *testing.T, api *apiClient, base string, ws *wsClient, address string) []byte {
	t.Helper()
	opened := ws.resource(map[string]any{"op": "read", "resource_id": address})
	endpoint := base + "/files/" + url.PathEscape(address) + "?t=" + url.QueryEscape(stringField(t, opened, "ticket"))
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
