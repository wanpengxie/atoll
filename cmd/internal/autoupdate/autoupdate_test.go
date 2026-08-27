package autoupdate

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
)

func TestStatusChecksOSSAndPersistsAvailability(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/releases/latest" {
			t.Fatalf("path=%s", r.URL.Path)
		}
		_, _ = w.Write([]byte("v0.07\n"))
	}))
	defer server.Close()
	home := t.TempDir()
	m, err := New(Config{Home: home, CurrentVersion: "v0.06", Executable: os.Args[0], ParentPID: os.Getpid(), OSSBase: server.URL, GitHubBase: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	got, err := m.Status(context.Background(), true)
	if err != nil {
		t.Fatal(err)
	}
	s := got.(Snapshot)
	if !s.Available || s.LatestVersion != "v0.07" || s.CurrentVersion != "v0.06" {
		t.Fatalf("snapshot=%+v", s)
	}
	if _, err := os.Stat(filepath.Join(home, "update.json")); err != nil {
		t.Fatal(err)
	}
}

func TestDevelopmentBuildNeverOffersAutomaticUpdate(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("v9.9")) }))
	defer server.Close()
	m, err := New(Config{Home: t.TempDir(), CurrentVersion: "dev", Executable: os.Args[0], ParentPID: os.Getpid(), OSSBase: server.URL, GitHubBase: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	got, err := m.Status(context.Background(), true)
	if err != nil {
		t.Fatal(err)
	}
	if s := got.(Snapshot); s.Available || s.Detail == "" {
		t.Fatalf("snapshot=%+v", s)
	}
}

func TestWorkerSpawnFailureIsRetryableInsteadOfStuckActive(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("v0.07")) }))
	defer server.Close()
	home := t.TempDir()
	m, err := New(Config{Home: home, CurrentVersion: "v0.06", Executable: filepath.Join(home, "missing-atoll"), ParentPID: os.Getpid(), OSSBase: server.URL, GitHubBase: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.Status(context.Background(), true); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Start(context.Background()); err == nil {
		t.Fatal("expected worker spawn error")
	}
	s, err := readState(home)
	if err != nil {
		t.Fatal(err)
	}
	if s.Status != "failed" || !s.Available {
		t.Fatalf("snapshot=%+v", s)
	}
}

func TestStartReturnsTheDetachedWorkerPIDBeforeReleasingProcess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("v0.07")) }))
	defer server.Close()
	home := t.TempDir()
	executable := filepath.Join(home, "worker-stub")
	if err := os.WriteFile(executable, []byte("#!/bin/sh\nexec sleep 30\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	m, err := New(Config{Home: home, CurrentVersion: "v0.06", Executable: executable, ParentPID: os.Getpid(), OSSBase: server.URL, GitHubBase: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.Status(context.Background(), true); err != nil {
		t.Fatal(err)
	}
	got, err := m.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	s := got.(Snapshot)
	if s.WorkerPID <= 0 {
		t.Fatalf("worker pid=%d", s.WorkerPID)
	}
	_ = syscall.Kill(s.WorkerPID, syscall.SIGKILL)
}

func TestLatestFallsBackToGitHubRedirectTag(t *testing.T) {
	github := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/releases/latest" {
			http.Redirect(w, r, "/releases/tag/v0.08", http.StatusFound)
			return
		}
		_, _ = w.Write([]byte("release"))
	}))
	defer github.Close()
	broken := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "no", http.StatusBadGateway) }))
	defer broken.Close()
	got, err := latestVersion(context.Background(), github.Client(), broken.URL, github.URL)
	if err != nil || got != "v0.08" {
		t.Fatalf("version=%q err=%v", got, err)
	}
}

func TestReleaseDownloadFallsBackFromOSSToGitHub(t *testing.T) {
	archive := releaseArchive(t, "v0.07", fakeAtollScript("v0.07", false))
	hash := sha256.Sum256(archive)
	name := "atoll_v0.07_" + runtime.GOOS + "_" + runtime.GOARCH + ".tar.gz"
	ossCalls := 0
	oss := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		ossCalls++
		http.Error(w, "mirror unavailable", http.StatusBadGateway)
	}))
	defer oss.Close()
	github := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/checksums.txt") {
			_, _ = w.Write([]byte(hex.EncodeToString(hash[:]) + "  " + name + "\n"))
			return
		}
		if strings.HasSuffix(r.URL.Path, "/"+name) {
			_, _ = w.Write(archive)
			return
		}
		http.NotFound(w, r)
	}))
	defer github.Close()
	dir := t.TempDir()
	w := worker{cfg: Config{Executable: filepath.Join(dir, "atoll"), HTTPClient: github.Client(), OSSBase: oss.URL, GitHubBase: github.URL}, target: "v0.07"}
	path, err := w.downloadRelease()
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(path)
	if ossCalls == 0 {
		t.Fatal("OSS was not attempted before fallback")
	}
	got, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(got, archive) {
		t.Fatalf("download err=%v equal=%v", err, bytes.Equal(got, archive))
	}
}

func TestExtractBinaryReadsOnlyExpectedReleaseMember(t *testing.T) {
	dir := t.TempDir()
	archive := filepath.Join(dir, "release.tar.gz")
	f, err := os.Create(archive)
	if err != nil {
		t.Fatal(err)
	}
	gz := gzip.NewWriter(f)
	tw := tar.NewWriter(gz)
	want := "atoll_v0.07_" + runtime.GOOS + "_" + runtime.GOARCH + "/atoll"
	content := []byte("binary")
	if err := tw.WriteHeader(&tar.Header{Name: "../../outside", Mode: 0o755, Size: 3, Typeflag: tar.TypeReg}); err != nil {
		t.Fatal(err)
	}
	_, _ = tw.Write([]byte("bad"))
	if err := tw.WriteHeader(&tar.Header{Name: want, Mode: 0o755, Size: int64(len(content)), Typeflag: tar.TypeReg}); err != nil {
		t.Fatal(err)
	}
	_, _ = tw.Write(content)
	_ = tw.Close()
	_ = gz.Close()
	_ = f.Close()
	stage, err := extractBinary(archive, filepath.Join(dir, "atoll"), "v0.07")
	if err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile(stage)
	if string(got) != string(content) {
		t.Fatalf("content=%q", got)
	}
	if _, err := os.Stat(filepath.Join(dir, "outside")); !os.IsNotExist(err) {
		t.Fatalf("unexpected outside file: %v", err)
	}
}

func TestWorkerReplacesBinaryAndRestarts(t *testing.T) {
	testWorkerInstall(t, false)
}

func TestWorkerRestoresPreviousBinaryWhenNewVersionCannotStart(t *testing.T) {
	testWorkerInstall(t, true)
}

func testWorkerInstall(t *testing.T, failNewStart bool) {
	t.Helper()
	dir := t.TempDir()
	executable := filepath.Join(dir, "atoll")
	oldBinary := fakeAtollScript("v0.06", false)
	newBinary := fakeAtollScript("v0.07", failNewStart)
	if err := os.WriteFile(executable, oldBinary, 0o755); err != nil {
		t.Fatal(err)
	}
	archive := releaseArchive(t, "v0.07", newBinary)
	hash := sha256.Sum256(archive)
	name := "atoll_v0.07_" + runtime.GOOS + "_" + runtime.GOARCH + ".tar.gz"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/checksums.txt"):
			_, _ = w.Write([]byte(hex.EncodeToString(hash[:]) + "  " + name + "\n"))
		case strings.HasSuffix(r.URL.Path, "/"+name):
			_, _ = w.Write(archive)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	parent := exec.Command("sleep", "30")
	if err := parent.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { _ = parent.Wait(); close(done) }()
	t.Cleanup(func() {
		_ = parent.Process.Kill()
		<-done
	})
	w := worker{cfg: Config{
		Home: dir, CurrentVersion: "v0.06", Executable: executable,
		ParentPID: parent.Process.Pid, HTTPClient: server.Client(),
		OSSBase: server.URL, GitHubBase: server.URL,
	}, target: "v0.07"}
	err := w.run()
	installed, readErr := os.ReadFile(executable)
	if readErr != nil {
		t.Fatal(readErr)
	}
	state, stateErr := readState(dir)
	if stateErr != nil {
		t.Fatal(stateErr)
	}
	if failNewStart {
		if err == nil || string(installed) != string(oldBinary) || state.Status != "failed" || !state.Available {
			t.Fatalf("err=%v restored=%v state=%+v", err, string(installed) == string(oldBinary), state)
		}
		return
	}
	if err != nil || string(installed) != string(newBinary) || state.Status != "succeeded" {
		t.Fatalf("err=%v installed=%v state=%+v", err, string(installed) == string(newBinary), state)
	}
	previous, err := os.ReadFile(executable + ".previous")
	if err != nil || string(previous) != string(oldBinary) {
		t.Fatalf("previous err=%v content=%q", err, previous)
	}
}

func fakeAtollScript(version string, failStart bool) []byte {
	startExit := "0"
	if failStart {
		startExit = "1"
	}
	return []byte("#!/bin/sh\nif [ \"$1\" = version ]; then echo 'atoll " + version + " (web test)'; exit 0; fi\nif [ \"$1\" = start ]; then exit " + startExit + "; fi\nexit 2\n")
}

func releaseArchive(t *testing.T, version string, binary []byte) []byte {
	t.Helper()
	var out bytes.Buffer
	gz := gzip.NewWriter(&out)
	tw := tar.NewWriter(gz)
	name := "atoll_" + version + "_" + runtime.GOOS + "_" + runtime.GOARCH + "/atoll"
	if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o755, Size: int64(len(binary)), Typeflag: tar.TypeReg}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write(binary); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}
