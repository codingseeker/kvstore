package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"kvstore/internal/raft"
	"kvstore/internal/store"
)

type Server struct {
	raft    *raft.Node
	kv      *store.KV
	leader  func(string) string
	timeout time.Duration
}

func New(node *raft.Node, kv *store.KV, leaderURL func(string) string, timeout time.Duration) *Server {
	return &Server{raft: node, kv: kv, leader: leaderURL, timeout: timeout}
}

func (s *Server) Register(mux *http.ServeMux) {
	mux.HandleFunc("/", s.handleKey)
	mux.HandleFunc("/status", s.handleStatus)
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/keys", s.handleKeys)
	mux.HandleFunc("/cluster", s.handleCluster)
	mux.HandleFunc("/cluster/add-nodes", s.handleAddNodes)
	mux.HandleFunc("/replication", s.handleReplication)
	mux.HandleFunc("/admin/compact", s.handleCompact)
	mux.HandleFunc("/admin/snapshot", s.handleSnapshot)
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	role, term, leader := s.raft.Status()
	writeJSON(w, http.StatusOK, map[string]any{
		"role":      role,
		"term":      term,
		"leader_id": leader,
	})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	role, term, leader := s.raft.Status()
	writeJSON(w, http.StatusOK, map[string]any{
		"role":            role,
		"term":            term,
		"leader_id":       leader,
		"quorum":          s.raft.QuorumStatus(),
		"replication_lag": s.raft.ReplicationLag(),
	})
}

func (s *Server) handleKeys(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	keys := s.kv.SnapshotKeys()
	writeJSON(w, http.StatusOK, map[string]any{"keys": keys})
}

func (s *Server) handleCluster(w http.ResponseWriter, r *http.Request) {
	role, term, leader := s.raft.Status()
	peers := s.raft.Peers()
	writeJSON(w, http.StatusOK, map[string]any{
		"role":      role,
		"term":      term,
		"leader_id": leader,
		"peers":     peers,
	})
}

func (s *Server) handleAddNodes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	var req struct {
		Nodes []struct {
			ID      string `json:"id"`
			RaftURL string `json:"raft_url"`
		} `json:"nodes"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	for _, node := range req.Nodes {
		s.raft.AddNode(node.ID, node.RaftURL)
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "nodes added"})
}

func (s *Server) handleReplication(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	role, _, leader := s.raft.Status()
	writeJSON(w, http.StatusOK, map[string]any{
		"leader_id":       leader,
		"replication_lag": s.raft.ReplicationLag(),
		"quorum":          s.raft.QuorumStatus(),
		"role":            role,
	})
}

func (s *Server) handleCompact(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	if err := s.raft.CompactAppliedLog(); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "compaction complete"})
}

func (s *Server) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "snapshot triggered"})
}

func (s *Server) handleKey(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/")
	if key == "" || strings.Contains(key, "/") {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "path must be /{key}"})
		return
	}

	switch r.Method {
	case http.MethodPut:
		var body struct {
			Value string `json:"value"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		s.propose(w, store.Command{Op: store.OpPut, Key: key, Value: body.Value})
	case http.MethodDelete:
		s.propose(w, store.Command{Op: store.OpDelete, Key: key})
	case http.MethodGet:
		consistency := r.URL.Query().Get("consistency")
		switch consistency {
		case "", "strong":
			ctx, cancel := context.WithTimeout(r.Context(), s.timeout)
			defer cancel()
			if err := s.raft.ReadIndex(ctx); err != nil {
				s.handleRaftError(w, err)
				return
			}
		case "eventual":
		default:
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "consistency must be strong or eventual"})
			return
		}
		value, ok := s.kv.Get(key)
		if !ok {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"key": key, "value": value})
	default:
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
	}
}

func (s *Server) propose(w http.ResponseWriter, cmd store.Command) {
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()
	if err := s.raft.Propose(ctx, cmd); err != nil {
		s.handleRaftError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "committed"})
}

func (s *Server) handleRaftError(w http.ResponseWriter, err error) {
	if errors.Is(err, raft.ErrNotLeader) {
		leader := s.raft.LeaderID()
		url := ""
		if leader != "" && s.leader != nil {
			url = s.leader(leader)
		}
		if url != "" {
			w.Header().Set("Location", url)
			writeJSON(w, http.StatusTemporaryRedirect, map[string]string{"error": "not leader", "leader": leader, "leader_url": url})
			return
		}
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "leader unknown"})
		return
	}
	if errors.Is(err, context.DeadlineExceeded) {
		writeJSON(w, http.StatusGatewayTimeout, map[string]string{"error": "operation timed out"})
		return
	}
	writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
}

func writeJSON(w http.ResponseWriter, code int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(value)
}
