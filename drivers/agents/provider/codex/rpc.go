package codex

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/wanpengxie/atoll/drivers/agents/provider/internal/toolsurface"
)

type rpcError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

func (e *rpcError) Error() string { return fmt.Sprintf("codex rpc %d: %s", e.Code, e.Message) }

type rpcReply struct {
	result json.RawMessage
	err    error
}
type wireMessage struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

type rpcClient struct {
	in             io.WriteCloser
	out            io.ReadCloser
	writeMu        sync.Mutex
	mu             sync.Mutex
	pending        map[string]func(rpcReply)
	next           atomic.Uint64
	closed         atomic.Bool
	closeOnce      sync.Once
	onNotification func(string, json.RawMessage)
	onRequest      func(string, json.RawMessage) func() (any, *rpcError)
	onClose        func(error)
	pumpDone       chan struct{}
	requestSlots   chan struct{}
}

func newRPC(p *childProcess) *rpcClient {
	return &rpcClient{in: p.stdin, out: p.stdout, pending: map[string]func(rpcReply){}, pumpDone: make(chan struct{}), requestSlots: make(chan struct{}, 16)}
}
func (c *rpcClient) start() { go c.readPump() }
func (c *rpcClient) callAsync(method string, params any, done func(json.RawMessage, error)) error {
	if c.closed.Load() {
		return errors.New("codex rpc closed")
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
	ch := c.pending[key]
	delete(c.pending, key)
	return ch
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
		var msg wireMessage
		if err := json.Unmarshal(line, &msg); err != nil {
			c.closeWith(fmt.Errorf("codex rpc decode: %w", err))
			return
		}
		if len(msg.ID) > 0 && msg.Method != "" {
			// Capture request ownership while still on the ordered read pump.
			// The returned closure contains the turn snapshot; workers never
			// consult mutable target state after this point.
			var handler func() (any, *rpcError)
			if c.onRequest != nil {
				handler = c.onRequest(msg.Method, msg.Params)
			}
			select {
			case c.requestSlots <- struct{}{}:
				go func() {
					defer func() { <-c.requestSlots }()
					c.handleRequest(msg, handler)
				}()
			default:
				c.rejectOverloaded(msg)
			}
			continue
		}
		if len(msg.ID) > 0 {
			var key string
			if msg.ID[0] == '"' {
				_ = json.Unmarshal(msg.ID, &key)
			} else {
				key = string(msg.ID)
			}
			if ch := c.take(key); ch != nil {
				if msg.Error != nil {
					ch(rpcReply{err: msg.Error})
				} else {
					ch(rpcReply{result: msg.Result})
				}
			}
			continue
		}
		if msg.Method != "" && c.onNotification != nil {
			c.onNotification(msg.Method, msg.Params)
		}
	}
}
func readBoundedLine(r *bufio.Reader, max int) ([]byte, error) {
	var out []byte
	for {
		frag, err := r.ReadSlice('\n')
		out = append(out, frag...)
		if len(out) > max {
			return nil, errors.New("codex rpc line exceeds 8 MiB")
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
func (c *rpcClient) handleRequest(msg wireMessage, handler func() (any, *rpcError)) {
	result, rpcErr := any(nil), (*rpcError)(nil)
	if handler != nil {
		result, rpcErr = handler()
	} else {
		rpcErr = &rpcError{Code: -32601, Message: "method not supported"}
	}
	response := map[string]any{"jsonrpc": "2.0", "id": json.RawMessage(msg.ID)}
	if rpcErr != nil {
		response["error"] = rpcErr
	} else {
		response["result"] = result
	}
	_ = c.write(response)
}

func (c *rpcClient) rejectOverloaded(msg wireMessage) {
	response := map[string]any{"jsonrpc": "2.0", "id": json.RawMessage(msg.ID)}
	if msg.Method == "item/tool/call" {
		response["result"] = dynamicToolResult(toolsurface.ErrorText("internal_error", "tool concurrency limit reached", "Wait for an in-flight tool call to finish, then retry if safe"), true)
	} else {
		response["error"] = &rpcError{Code: -32000, Message: "request concurrency limit reached"}
	}
	_ = c.write(response)
}
func (c *rpcClient) closeWith(err error) {
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		c.mu.Lock()
		pending := c.pending
		c.pending = map[string]func(rpcReply){}
		c.mu.Unlock()
		for _, ch := range pending {
			ch(rpcReply{err: err})
		}
		if c.onClose != nil {
			c.onClose(err)
		}
	})
}
func (c *rpcClient) retire() {
	c.closeWith(errors.New("codex connection retired"))
	_ = c.in.Close()
	_ = c.out.Close()
}
