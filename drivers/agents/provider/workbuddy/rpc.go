package workbuddy

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
)

type rpcError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

func (e *rpcError) Error() string {
	return fmt.Sprintf("workbuddy rpc %d: %s", e.Code, redactNative(e.Message))
}

type wireMessage struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}
type rpcReply struct {
	result json.RawMessage
	err    error
}

type rpcClient struct {
	in             io.WriteCloser
	out            io.ReadCloser
	writeMu, mu    sync.Mutex
	pending        map[string]func(rpcReply)
	next           atomic.Uint64
	closed         atomic.Bool
	closeOnce      sync.Once
	onNotification func(string, json.RawMessage)
	onRequest      func(string, json.RawMessage) (any, *rpcError)
	onClose        func(error)
	pumpDone       chan struct{}
}

func newRPC(p *childProcess) *rpcClient {
	return &rpcClient{in: p.stdin, out: p.stdout, pending: map[string]func(rpcReply){}, pumpDone: make(chan struct{})}
}
func (c *rpcClient) start() { go c.readPump() }
func (c *rpcClient) callAsync(method string, params any, done func(json.RawMessage, error)) error {
	if c.closed.Load() {
		return errors.New("workbuddy rpc closed")
	}
	id := c.next.Add(1)
	key := fmt.Sprint(id)
	c.mu.Lock()
	c.pending[key] = func(r rpcReply) { done(r.result, r.err) }
	c.mu.Unlock()
	if err := c.write(map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params}); err != nil {
		c.take(key)
		return err
	}
	return nil
}
func (c *rpcClient) notify(method string, params any) error {
	return c.write(map[string]any{"jsonrpc": "2.0", "method": method, "params": params})
}
func (c *rpcClient) write(v any) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	_, err = c.in.Write(raw)
	return err
}
func (c *rpcClient) take(key string) func(rpcReply) {
	c.mu.Lock()
	defer c.mu.Unlock()
	f := c.pending[key]
	delete(c.pending, key)
	return f
}
func (c *rpcClient) readPump() {
	defer close(c.pumpDone)
	r := bufio.NewReaderSize(c.out, 64<<10)
	for {
		line, err := readBoundedLine(r, maxRPCLineBytes)
		if err != nil {
			c.closeWith(err)
			return
		}
		var m wireMessage
		if err := json.Unmarshal(line, &m); err != nil {
			c.closeWith(fmt.Errorf("workbuddy rpc decode: %w", err))
			return
		}
		if len(m.ID) > 0 && m.Method != "" {
			result, rpcErr := any(nil), (*rpcError)(nil)
			if c.onRequest != nil {
				result, rpcErr = c.onRequest(m.Method, m.Params)
			} else {
				rpcErr = &rpcError{Code: -32601, Message: "method not supported"}
			}
			reply := map[string]any{"jsonrpc": "2.0", "id": json.RawMessage(m.ID)}
			if rpcErr != nil {
				reply["error"] = rpcErr
			} else {
				reply["result"] = result
			}
			_ = c.write(reply)
			continue
		}
		if len(m.ID) > 0 {
			key := string(m.ID)
			if m.ID[0] == '"' {
				_ = json.Unmarshal(m.ID, &key)
			}
			if f := c.take(key); f != nil {
				if m.Error != nil {
					f(rpcReply{err: m.Error})
				} else {
					f(rpcReply{result: m.Result})
				}
			}
			continue
		}
		if m.Method != "" && c.onNotification != nil {
			c.onNotification(m.Method, m.Params)
		}
	}
}
func readBoundedLine(r *bufio.Reader, max int) ([]byte, error) {
	var out []byte
	for {
		frag, err := r.ReadSlice('\n')
		out = append(out, frag...)
		if len(out) > max {
			return nil, errors.New("workbuddy rpc line exceeds 8 MiB")
		}
		if err == nil {
			return bytes.TrimSpace(out), nil
		}
		if !errors.Is(err, bufio.ErrBufferFull) {
			if errors.Is(err, io.EOF) && len(out) > 0 {
				return bytes.TrimSpace(out), nil
			}
			return nil, err
		}
	}
}
func (c *rpcClient) closeWith(err error) {
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		c.mu.Lock()
		pending := c.pending
		c.pending = map[string]func(rpcReply){}
		c.mu.Unlock()
		for _, f := range pending {
			f(rpcReply{err: err})
		}
		if c.onClose != nil {
			c.onClose(err)
		}
	})
}
func (c *rpcClient) retire() {
	c.closeWith(errors.New("workbuddy connection retired"))
	_ = c.in.Close()
	_ = c.out.Close()
}
