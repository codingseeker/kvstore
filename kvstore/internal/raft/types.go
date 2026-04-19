package raft

import "kvstore/internal/store"

type Role string

const (
	Follower  Role = "follower"
	Candidate Role = "candidate"
	Leader    Role = "leader"
)

type Peer struct {
	ID      string
	RaftURL string
}

type Entry struct {
	Index   int64         `json:"index"`
	Term    int64         `json:"term"`
	Command store.Command `json:"command"`
}

type RequestVoteRequest struct {
	Term         int64  `json:"term"`
	CandidateID  string `json:"candidate_id"`
	LastLogIndex int64  `json:"last_log_index"`
	LastLogTerm  int64  `json:"last_log_term"`
}

type RequestVoteResponse struct {
	Term        int64 `json:"term"`
	VoteGranted bool  `json:"vote_granted"`
}

type AppendEntriesRequest struct {
	Term         int64   `json:"term"`
	LeaderID     string  `json:"leader_id"`
	PrevLogIndex int64   `json:"prev_log_index"`
	PrevLogTerm  int64   `json:"prev_log_term"`
	Entries      []Entry `json:"entries"`
	LeaderCommit int64   `json:"leader_commit"`
}

type AppendEntriesResponse struct {
	Term       int64 `json:"term"`
	Success    bool  `json:"success"`
	MatchIndex int64 `json:"match_index"`
}

type ReadIndexRequest struct {
	Term     int64  `json:"term"`
	LeaderID string `json:"leader_id"`
}

type ReadIndexResponse struct {
	Term    int64 `json:"term"`
	Success bool  `json:"success"`
}
