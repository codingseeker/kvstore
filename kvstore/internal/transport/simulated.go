package transport

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"time"
)

type SimulatedNetwork struct {
	mu         sync.Mutex
	now        time.Time
	handlers   map[string]http.Handler
	blocked    map[string]bool
	drops      map[string]bool
	delays     map[string]time.Duration
	pending    []*SimulatedMessage
	deliverSeq int64
}

type SimulatedMessage struct {
	ID        int64
	From      string
	To        string
	ReadyAt   time.Time
	Request   *http.Request
	Body      []byte
	response  chan simulatedResponse
	delivered bool
	dropped   bool
}

type simulatedResponse struct {
	resp *http.Response
	err  error
}

type SimulatedRoundTripper struct {
	Network *SimulatedNetwork
	From    string
}

func NewSimulatedNetwork(start time.Time) *SimulatedNetwork {
	if start.IsZero() {
		start = time.Unix(0, 0)
	}
	return &SimulatedNetwork{
		now:      start,
		handlers: map[string]http.Handler{},
		blocked:  map[string]bool{},
		drops:    map[string]bool{},
		delays:   map[string]time.Duration{},
	}
}

func (n *SimulatedNetwork) Register(host string, handler http.Handler) {
	n.mu.Lock()
	n.handlers[host] = handler
	n.mu.Unlock()
}

func (n *SimulatedNetwork) Partition(from string, to string) {
	n.mu.Lock()
	n.blocked[from+"->"+to] = true
	n.mu.Unlock()
}

func (n *SimulatedNetwork) Heal(from string, to string) {
	n.mu.Lock()
	delete(n.blocked, from+"->"+to)
	n.mu.Unlock()
}

func (n *SimulatedNetwork) DropNext(from string, to string) {
	n.mu.Lock()
	n.drops[from+"->"+to] = true
	n.mu.Unlock()
}

func (n *SimulatedNetwork) Delay(from string, to string, delay time.Duration) {
	n.mu.Lock()
	n.delays[from+"->"+to] = delay
	n.mu.Unlock()
}

func (n *SimulatedNetwork) Advance(delta time.Duration) {
	n.mu.Lock()
	n.now = n.now.Add(delta)
	n.mu.Unlock()
}

func (n *SimulatedNetwork) Pending() []*SimulatedMessage {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]*SimulatedMessage, 0, len(n.pending))
	for _, msg := range n.pending {
		if !msg.delivered && !msg.dropped {
			out = append(out, msg)
		}
	}
	return out
}

func (n *SimulatedNetwork) DeliverNext() bool {
	msg := n.nextReady()
	if msg == nil {
		return false
	}
	n.deliver(msg)
	return true
}

func (n *SimulatedNetwork) DeliverAll() {
	for n.DeliverNext() {
	}
}

func (n *SimulatedNetwork) RoundTrip(from string, req *http.Request) (*http.Response, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}
	_ = req.Body.Close()
	req.Body = io.NopCloser(bytes.NewReader(body))
	to := req.URL.Host
	n.mu.Lock()
	if n.blocked[from+"->"+to] || n.blocked[from+"->*"] || n.blocked["*->"+to] {
		n.mu.Unlock()
		return nil, errors.New("link partitioned")
	}
	key := from + "->" + to
	if n.drops[key] {
		delete(n.drops, key)
		n.mu.Unlock()
		return nil, errors.New("message dropped")
	}
	clone := req.Clone(req.Context())
	clone.Body = io.NopCloser(bytes.NewReader(body))
	n.deliverSeq++
	msg := &SimulatedMessage{
		ID:       n.deliverSeq,
		From:     from,
		To:       to,
		ReadyAt:  n.now.Add(n.delays[key]),
		Request:  clone,
		Body:     body,
		response: make(chan simulatedResponse, 1),
	}
	n.pending = append(n.pending, msg)
	n.mu.Unlock()
	select {
	case resp := <-msg.response:
		return resp.resp, resp.err
	case <-req.Context().Done():
		return nil, req.Context().Err()
	}
}

func (n *SimulatedNetwork) nextReady() *SimulatedMessage {
	n.mu.Lock()
	defer n.mu.Unlock()
	var selected *SimulatedMessage
	for _, msg := range n.pending {
		if msg.delivered || msg.dropped || msg.ReadyAt.After(n.now) {
			continue
		}
		if selected == nil || msg.ID < selected.ID {
			selected = msg
		}
	}
	if selected != nil {
		selected.delivered = true
	}
	return selected
}

func (n *SimulatedNetwork) deliver(msg *SimulatedMessage) {
	n.mu.Lock()
	handler := n.handlers[msg.To]
	n.mu.Unlock()
	if handler == nil {
		msg.response <- simulatedResponse{err: errors.New("no handler")}
		return
	}
	rec := httptest.NewRecorder()
	req := msg.Request.Clone(context.Background())
	req.Body = io.NopCloser(bytes.NewReader(msg.Body))
	handler.ServeHTTP(rec, req)
	msg.response <- simulatedResponse{resp: rec.Result()}
}

func (t SimulatedRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if t.Network == nil {
		return nil, errors.New("nil simulated network")
	}
	return t.Network.RoundTrip(t.From, req)
}
