package claude

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
)

type controlReply struct {
	Success  bool
	Response json.RawMessage
	Error    string
}

type wireEnvelope struct {
	Type        string          `json:"type"`
	Subtype     string          `json:"subtype,omitempty"`
	CommandUUID string          `json:"command_uuid,omitempty"`
	State       string          `json:"state,omitempty"`
	RequestID   string          `json:"request_id,omitempty"`
	Request     json.RawMessage `json:"request,omitempty"`
	Response    json.RawMessage `json:"response,omitempty"`
}

type wireResponse struct {
	Subtype   string          `json:"subtype"`
	RequestID string          `json:"request_id"`
	Response  json.RawMessage `json:"response,omitempty"`
	Error     string          `json:"error,omitempty"`
}

type wireRequest struct {
	Subtype string `json:"subtype"`
}

type wireClient struct {
	in              io.WriteCloser
	out             io.ReadCloser
	writeMu         sync.Mutex
	mu              sync.Mutex
	pending         map[string]func(controlReply)
	next            atomic.Uint64
	closed          atomic.Bool
	retired         atomic.Bool
	closeOnce       sync.Once
	onLifecycle     func(string, string)
	onFrame         func(string, string, json.RawMessage)
	onServerRequest func(string, json.RawMessage) (any, bool)
	onClose         func(error)
	onDebug         func(string, string)
	pumpDone        chan struct{}
}

func newWire(p *childProcess) *wireClient {
	return &wireClient{in: p.stdin, out: p.stdout, pending: map[string]func(controlReply){}, pumpDone: make(chan struct{})}
}

func (c *wireClient) start() { go c.readPump() }

func (c *wireClient) sendControl(subtype string, extra map[string]any, done func(controlReply)) (string, error) {
	if c.closed.Load() {
		return "", errors.New("claude wire closed")
	}
	id := fmt.Sprintf("atoll-%d", c.next.Add(1))
	request := map[string]any{"subtype": subtype}
	for k, v := range extra {
		request[k] = v
	}
	frame := map[string]any{"type": "control_request", "request_id": id, "request": request}
	// Ownership is registered before the write so an immediate response cannot
	// outrun the callback. A failed write removes that ownership again.
	if done != nil {
		c.mu.Lock()
		c.pending[id] = done
		c.mu.Unlock()
	}
	if err := c.writeFrame(frame); err != nil {
		if done != nil {
			c.take(id)
		}
		return "", err
	}
	return id, nil
}

func (c *wireClient) writeFrame(v any) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.closed.Load() {
		return errors.New("claude wire closed")
	}
	_, err = c.in.Write(raw)
	return err
}

func (c *wireClient) take(id string) func(controlReply) {
	c.mu.Lock()
	defer c.mu.Unlock()
	done := c.pending[id]
	delete(c.pending, id)
	return done
}

func (c *wireClient) readPump() {
	defer close(c.pumpDone)
	s := bufio.NewScanner(c.out)
	s.Buffer(make([]byte, 64<<10), maxLineBytes+1)
	for s.Scan() {
		line := append([]byte(nil), s.Bytes()...)
		if len(line) > maxLineBytes {
			c.close(errors.New("claude wire line exceeds 8 MiB"))
			return
		}
		var env wireEnvelope
		if err := json.Unmarshal(line, &env); err != nil {
			c.close(fmt.Errorf("claude wire decode: %w", err))
			return
		}
		switch env.Type {
		case "control_response":
			var response wireResponse
			if err := json.Unmarshal(env.Response, &response); err != nil {
				c.close(fmt.Errorf("claude control response decode: %w", err))
				return
			}
			done := c.take(response.RequestID)
			if done == nil {
				c.debug("unmatched_control_response", response.RequestID)
				continue
			}
			done(controlReply{Success: response.Subtype == "success", Response: response.Response, Error: response.Error})
		case "command_lifecycle":
			if c.onLifecycle != nil {
				c.onLifecycle(env.CommandUUID, env.State)
			}
		case "control_request":
			c.handleServerRequest(env.RequestID, env.Request)
		default:
			if c.onFrame != nil {
				c.onFrame(env.Type, env.Subtype, line)
			}
		}
	}
	err := s.Err()
	if err == nil {
		err = io.EOF
	} else if stringsContains(err.Error(), "token too long") {
		err = errors.New("claude wire line exceeds 8 MiB")
	}
	c.close(err)
}

func stringsContains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func (c *wireClient) handleServerRequest(id string, raw json.RawMessage) {
	var request wireRequest
	if err := json.Unmarshal(raw, &request); err != nil {
		_ = c.writeFrame(map[string]any{"type": "control_response", "response": map[string]any{"subtype": "error", "request_id": id, "error": err.Error()}})
		return
	}
	result, isError := any(nil), true
	if c.onServerRequest != nil {
		result, isError = c.onServerRequest(request.Subtype, raw)
	}
	response := map[string]any{"subtype": "success", "request_id": id, "response": result}
	if isError {
		response = map[string]any{"subtype": "error", "request_id": id, "error": result}
	}
	_ = c.writeFrame(map[string]any{"type": "control_response", "response": response})
}

func (c *wireClient) debug(code, detail string) {
	if c.onDebug != nil {
		c.onDebug(code, detail)
	}
}

func (c *wireClient) close(err error) {
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		c.mu.Lock()
		pending := c.pending
		c.pending = map[string]func(controlReply){}
		c.mu.Unlock()
		for _, done := range pending {
			done(controlReply{Error: "wire closed"})
		}
		if !c.retired.Load() && c.onClose != nil {
			c.onClose(err)
		}
	})
}

func (c *wireClient) retire() {
	c.retired.Store(true)
	_ = c.in.Close()
}
