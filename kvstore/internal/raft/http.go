package raft

import (
	"encoding/json"
	"net/http"
)

func (n *Node) Register(mux *http.ServeMux) {
	mux.HandleFunc("/raft/request-vote", n.handleRequestVoteHTTP)
	mux.HandleFunc("/raft/append-entries", n.handleAppendEntriesHTTP)
	mux.HandleFunc("/raft/read-index", n.handleReadIndexHTTP)
}

func (n *Node) handleRequestVoteHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req RequestVoteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, n.HandleRequestVote(req))
}

func (n *Node) handleReadIndexHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req ReadIndexRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, n.HandleReadIndex(req))
}

func (n *Node) handleAppendEntriesHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req AppendEntriesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, n.HandleAppendEntries(req))
}

func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}
