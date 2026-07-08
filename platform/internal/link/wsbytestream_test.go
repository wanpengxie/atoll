package link

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// wsbytestream_test.go is 期11 spec §5.2 片①'s own independent proof: it
// stands up a REAL pair of gorilla WS connections over an actual TCP loop
// (httptest.Server + websocket.DefaultDialer, same idiom link_test.go's own
// wsURL rig uses) and drives wsByteStream — never a fake/in-memory
// io.ReadWriteCloser — because the thing under test is specifically the
// message-boundary state machine bridging gorilla's per-message Next
// Reader/Writer API down to a flat byte stream, which only a real WS wire
// protocol round trip can exercise honestly.

// wsBytePair upgrades one httptest connection and hands back both ends
// already wrapped as wsByteStream, ready for the test body to drive.
type wsBytePair struct {
	client *wsByteStream
	server *wsByteStream
	srv    *httptest.Server
}

func (p *wsBytePair) Close() {
	_ = p.client.Close()
	_ = p.server.Close()
	p.srv.Close()
}

func newWSBytePair(t *testing.T) *wsBytePair {
	t.Helper()
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	serverConnCh := make(chan *websocket.Conn, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("server upgrade: %v", err)
			return
		}
		serverConnCh <- ws
	}))

	wsURL := "ws" + srv.URL[len("http"):]
	clientWS, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		srv.Close()
		t.Fatalf("client dial: %v", err)
	}

	var serverWS *websocket.Conn
	select {
	case serverWS = <-serverConnCh:
	case <-time.After(5 * time.Second):
		srv.Close()
		t.Fatal("timed out waiting for server-side upgrade")
	}

	return &wsBytePair{
		client: newWSByteStream(clientWS),
		server: newWSByteStream(serverWS),
		srv:    srv,
	}
}

// TestWSByteStream_SingleMessageRoundTrip is the smallest possible sanity
// check: one Write, one Read, exact bytes back.
func TestWSByteStream_SingleMessageRoundTrip(t *testing.T) {
	p := newWSBytePair(t)
	defer p.Close()

	want := []byte("hello yamux carrier")
	if _, err := p.client.Write(want); err != nil {
		t.Fatalf("client write: %v", err)
	}
	got := make([]byte, len(want))
	if _, err := io.ReadFull(p.server, got); err != nil {
		t.Fatalf("server read: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("round trip mismatch: got %q want %q", got, want)
	}
}

// TestWSByteStream_MessageBoundaryReassembly writes many independently-sized
// WS messages (each its own Write call — gorilla's NextWriter/Close makes
// every Write exactly one binary message) and reads the WHOLE concatenation
// back through io.ReadFull with a total-size buffer that spans dozens of
// underlying messages — proving Read transparently crosses message
// boundaries (the "读完该 message EOF≠连接EOF 再 NextReader" state machine)
// and reassembles a byte-exact stream, not a per-message chunked one.
func TestWSByteStream_MessageBoundaryReassembly(t *testing.T) {
	p := newWSBytePair(t)
	defer p.Close()

	// Precompute every chunk (and the total length) up front, single
	// threaded — a concurrent writer racing against a reader sizing its
	// buffer off "bytes written so far" would be a TOCTOU in the TEST, not
	// the thing under test. Reading exactly the known total via io.ReadFull
	// avoids leaning on connection-close-as-EOF (a raw ws.Close() is an
	// abnormal closure at the WS protocol level, not a clean stream EOF —
	// orthogonal to what this test is proving).
	rng := rand.New(rand.NewSource(1))
	const numMessages = 200
	chunks := make([][]byte, numMessages)
	var want bytes.Buffer
	for i := range chunks {
		n := rng.Intn(500) + 1 // 1..500 bytes, deliberately smaller AND larger than typical read buffers
		chunk := make([]byte, n)
		if _, err := rng.Read(chunk); err != nil {
			t.Fatalf("gen chunk %d: %v", i, err)
		}
		chunks[i] = chunk
		want.Write(chunk)
	}

	errCh := make(chan error, 1)
	go func() {
		for _, chunk := range chunks {
			if _, err := p.client.Write(chunk); err != nil {
				errCh <- err
				return
			}
		}
		errCh <- nil
	}()

	got := make([]byte, want.Len())
	readErrCh := make(chan error, 1)
	go func() {
		_, err := io.ReadFull(p.server, got)
		readErrCh <- err
	}()

	if err := <-errCh; err != nil {
		t.Fatalf("writer: %v", err)
	}
	select {
	case err := <-readErrCh:
		if err != nil {
			t.Fatalf("reader: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for reader to drain all messages")
	}

	if !bytes.Equal(got, want.Bytes()) {
		t.Fatalf("reassembly mismatch: got %d bytes, want %d bytes", len(got), want.Len())
	}
}

// TestWSByteStream_BidirectionalIOCopy is the task's own required stress
// shape: a pair of in-memory WS ends, both wrapped as wsByteStream, driven
// by io.Copy pumping RANDOM-length byte streams in BOTH directions
// simultaneously (client→server and server→client concurrently) — exercising
// simultaneous Read+Write on the SAME stream instance (the real yamux usage
// pattern: one session reads and writes its carrier concurrently), asserting
// byte-exact, order-preserved, lossless delivery via a content hash compare.
// Run with -race to confirm the rmu/wmu split makes this safe.
func TestWSByteStream_BidirectionalIOCopy(t *testing.T) {
	p := newWSBytePair(t)
	defer p.Close()

	const payloadSize = 512 * 1024 // large enough to span MANY underlying WS messages at any writer chunk size
	clientToServer := randomBytes(t, 1, payloadSize)
	serverToClient := randomBytes(t, 2, payloadSize)

	var wg sync.WaitGroup
	errs := make(chan error, 4)

	// client -> server
	wg.Add(1)
	go func() {
		defer wg.Done()
		if _, err := chunkedWrite(p.client, clientToServer, 3); err != nil {
			errs <- fmt.Errorf("client write: %w", err)
		}
	}()
	serverGotCh := make(chan []byte, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		buf := make([]byte, len(clientToServer))
		if _, err := io.ReadFull(p.server, buf); err != nil {
			errs <- fmt.Errorf("server read: %w", err)
			return
		}
		serverGotCh <- buf
	}()

	// server -> client, concurrently on the SAME two stream instances
	wg.Add(1)
	go func() {
		defer wg.Done()
		if _, err := chunkedWrite(p.server, serverToClient, 4); err != nil {
			errs <- fmt.Errorf("server write: %w", err)
		}
	}()
	clientGotCh := make(chan []byte, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		buf := make([]byte, len(serverToClient))
		if _, err := io.ReadFull(p.client, buf); err != nil {
			errs <- fmt.Errorf("client read: %w", err)
			return
		}
		clientGotCh <- buf
	}()

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for bidirectional copy to finish")
	}
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if t.Failed() {
		return
	}

	serverGot := <-serverGotCh
	clientGot := <-clientGotCh
	if sum(serverGot) != sum(clientToServer) {
		t.Fatal("client->server payload corrupted (hash mismatch)")
	}
	if sum(clientGot) != sum(serverToClient) {
		t.Fatal("server->client payload corrupted (hash mismatch)")
	}
}

// TestWSByteStream_ConcurrentWriters drives N goroutines calling Write
// concurrently on the SAME wsByteStream — each message self-describes its
// own length so the reader can verify no interleaving/corruption occurred
// (which is exactly what would happen if wmu did not serialise NextWriter
// calls). Run with -race.
func TestWSByteStream_ConcurrentWriters(t *testing.T) {
	p := newWSBytePair(t)
	defer p.Close()

	const numWriters = 16
	const msgsPerWriter = 25
	total := numWriters * msgsPerWriter

	var wg sync.WaitGroup
	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(w) + 100))
			for i := 0; i < msgsPerWriter; i++ {
				n := rng.Intn(200) + 8
				msg := make([]byte, n)
				// Self-describing payload: every byte equals a fixed marker
				// derived from (w,i) so any cross-writer interleaving shows
				// up as a non-uniform message on read.
				marker := byte((w*msgsPerWriter + i) % 251)
				for j := range msg {
					msg[j] = marker
				}
				if _, err := p.client.Write(msg); err != nil {
					t.Errorf("writer %d: write: %v", w, err)
					return
				}
			}
		}(w)
	}

	readErrCh := make(chan error, 1)
	go func() {
		for i := 0; i < total; i++ {
			_, r, err := p.server.ws.NextReader()
			if err != nil {
				readErrCh <- fmt.Errorf("message %d: NextReader: %w", i, err)
				return
			}
			b, err := io.ReadAll(r)
			if err != nil {
				readErrCh <- fmt.Errorf("message %d: ReadAll: %w", i, err)
				return
			}
			if len(b) == 0 {
				readErrCh <- fmt.Errorf("message %d: empty (a Write got lost/split)", i)
				return
			}
			marker := b[0]
			for _, c := range b {
				if c != marker {
					readErrCh <- fmt.Errorf("message %d: non-uniform bytes — writers interleaved onto one WS message", i)
					return
				}
			}
		}
		readErrCh <- nil
	}()

	wgDone := make(chan struct{})
	go func() { wg.Wait(); close(wgDone) }()
	select {
	case <-wgDone:
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for concurrent writers")
	}

	select {
	case err := <-readErrCh:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for reader to drain all messages")
	}
}

func randomBytes(t *testing.T, seed int64, n int) []byte {
	t.Helper()
	rng := rand.New(rand.NewSource(seed))
	b := make([]byte, n)
	if _, err := rng.Read(b); err != nil {
		t.Fatalf("randomBytes: %v", err)
	}
	return b
}

// chunkedWrite writes all of p to w in pseudo-random-sized chunks seeded by
// seed, so a single logical payload crosses many underlying WS messages.
func chunkedWrite(w io.Writer, p []byte, seed int64) (int, error) {
	rng := rand.New(rand.NewSource(seed))
	total := 0
	for total < len(p) {
		n := rng.Intn(4096) + 1
		if total+n > len(p) {
			n = len(p) - total
		}
		wn, err := w.Write(p[total : total+n])
		total += wn
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

func sum(b []byte) [32]byte { return sha256.Sum256(b) }
