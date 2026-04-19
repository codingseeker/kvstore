package raft

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"sort"
	"sync"
	"time"

	"kvstore/internal/observability"
	"kvstore/internal/store"
)

var (
	ErrNotLeader = errors.New("not leader")
	ErrStopped   = errors.New("raft stopped")
)

type Config struct {
	ID              string
	Peers           []Peer
	DataDir         string
	ElectionMin     time.Duration
	ElectionMax     time.Duration
	Heartbeat       time.Duration
	RequestTimeout  time.Duration
	HTTPClient      *http.Client
	Metrics         *observability.Metrics
	Logger          *observability.Logger
	LogSegmentBytes int64
	WALFsyncPolicy  string
}

type Node struct {
	mu sync.Mutex

	id      string
	peers   []Peer
	storage *diskStorage
	kv      *store.KV
	client  *http.Client
	metrics *observability.Metrics
	logger  *observability.Logger

	role          Role
	currentTerm   int64
	votedFor      string
	log           []Entry
	commitIndex   int64
	lastApplied   int64
	leaderID      string
	snapshotIndex int64
	snapshotTerm  int64

	nextIndex  map[string]int64
	matchIndex map[string]int64
	waiters    map[int64]chan error

	electionMin time.Duration
	electionMax time.Duration
	heartbeat   time.Duration
	rpcTimeout  time.Duration

	electionReset time.Time
	stopCh        chan struct{}
	stopped       bool
	applyCond     *sync.Cond
}

func New(cfg Config, kv *store.KV) (*Node, error) {
	if cfg.ElectionMin == 0 {
		cfg.ElectionMin = 350 * time.Millisecond
	}
	if cfg.ElectionMax == 0 {
		cfg.ElectionMax = 700 * time.Millisecond
	}
	if cfg.Heartbeat == 0 {
		cfg.Heartbeat = 100 * time.Millisecond
	}
	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = 2 * time.Second
	}

	ds := newDiskStorage(cfg.DataDir, cfg.LogSegmentBytes, cfg.WALFsyncPolicy)
	st, entries, err := ds.load()
	if err != nil {
		return nil, err
	}
	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{}
	}
	n := &Node{
		id:            cfg.ID,
		peers:         cfg.Peers,
		storage:       ds,
		kv:            kv,
		client:        client,
		metrics:       cfg.Metrics,
		logger:        cfg.Logger,
		role:          Follower,
		currentTerm:   st.CurrentTerm,
		votedFor:      st.VotedFor,
		log:           entries,
		snapshotIndex: st.SnapshotIndex,
		snapshotTerm:  st.SnapshotTerm,
		nextIndex:     map[string]int64{},
		matchIndex:    map[string]int64{},
		waiters:       map[int64]chan error{},
		electionMin:   cfg.ElectionMin,
		electionMax:   cfg.ElectionMax,
		heartbeat:     cfg.Heartbeat,
		rpcTimeout:    cfg.RequestTimeout,
		electionReset: time.Now(),
		stopCh:        make(chan struct{}),
	}
	n.applyCond = sync.NewCond(&n.mu)
	n.lastApplied = n.snapshotIndex
	n.commitIndex = min(st.CommitIndex, n.lastLogIndex())
	if n.commitIndex < n.snapshotIndex {
		n.commitIndex = n.snapshotIndex
	}
	if len(n.peers) == 0 && n.lastLogIndex() > n.commitIndex {
		n.commitIndex = n.lastLogIndex()
	}
	n.validateInvariantsLocked("")
	return n, nil
}

func (n *Node) Start() {
	go n.electionLoop()
	go n.heartbeatLoop()
	go n.applyLoop()
}

func (n *Node) Stop() {
	n.mu.Lock()
	if !n.stopped {
		close(n.stopCh)
		n.stopped = true
		for _, ch := range n.waiters {
			ch <- ErrStopped
			close(ch)
		}
		n.applyCond.Broadcast()
	}
	n.mu.Unlock()
}

func (n *Node) Status() (Role, int64, string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role, n.currentTerm, n.leaderID
}

func (n *Node) LeaderID() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderID
}

func (n *Node) Peers() []Peer {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]Peer(nil), n.peers...)
}

func (n *Node) AddNode(id, raftURL string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, p := range n.peers {
		if p.ID == id {
			return
		}
	}
	n.peers = append(n.peers, Peer{ID: id, RaftURL: raftURL})
	n.nextIndex[id] = n.lastLogIndex() + 1
	n.matchIndex[id] = 0
}

func (n *Node) ReplicationLag() map[string]int64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := map[string]int64{}
	for _, peer := range n.peers {
		lag := n.lastLogIndex() - n.matchIndex[peer.ID]
		if lag < 0 {
			lag = 0
		}
		out[peer.ID] = lag
	}
	return out
}

func (n *Node) QuorumStatus() map[string]any {
	n.mu.Lock()
	defer n.mu.Unlock()
	reachable := 1
	if n.role == Leader {
		for _, peer := range n.peers {
			if n.matchIndex[peer.ID] >= n.commitIndex {
				reachable++
			}
		}
	}
	return map[string]any{
		"size":      n.quorumLocked(),
		"reachable": reachable,
		"available": reachable >= n.quorumLocked(),
	}
}

func (n *Node) AppliedIndex() int64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.lastApplied
}

func (n *Node) LogSpan() (int64, int64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.snapshotIndex, n.lastLogIndex()
}

func (n *Node) IsLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role == Leader && !n.stopped
}

func (n *Node) Propose(ctx context.Context, cmd store.Command) error {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return ErrStopped
	}
	if n.role != Leader {
		n.mu.Unlock()
		return ErrNotLeader
	}
	entry := Entry{Index: n.lastLogIndex() + 1, Term: n.currentTerm, Command: cmd}
	n.log = append(n.log, entry)
	n.validateInvariantsLocked("propose")
	if err := n.storage.appendEntries([]Entry{entry}); err != nil {
		n.mu.Unlock()
		return err
	}
	wait := make(chan error, 1)
	n.waiters[entry.Index] = wait
	n.matchIndex[n.id] = entry.Index
	n.nextIndex[n.id] = entry.Index + 1
	if len(n.peers) == 0 {
		n.commitIndex = entry.Index
		_ = n.persistStateLocked()
		n.applyCond.Broadcast()
	}
	n.mu.Unlock()

	n.replicateAll()

	select {
	case err := <-wait:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-n.stopCh:
		return ErrStopped
	}
}

func (n *Node) HandleRequestVote(req RequestVoteRequest) RequestVoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Term < n.currentTerm {
		return RequestVoteResponse{Term: n.currentTerm}
	}
	if req.Term > n.currentTerm {
		n.becomeFollowerLocked(req.Term, "")
	}
	lastIndex, lastTerm := n.lastLogIndex(), n.lastLogTerm()
	upToDate := req.LastLogTerm > lastTerm || (req.LastLogTerm == lastTerm && req.LastLogIndex >= lastIndex)
	if (n.votedFor == "" || n.votedFor == req.CandidateID) && upToDate {
		n.votedFor = req.CandidateID
		n.electionReset = time.Now()
		_ = n.persistStateLocked()
		n.validateInvariantsLocked("request_vote")
		return RequestVoteResponse{Term: n.currentTerm, VoteGranted: true}
	}
	return RequestVoteResponse{Term: n.currentTerm}
}

func (n *Node) HandleAppendEntries(req AppendEntriesRequest) AppendEntriesResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Term < n.currentTerm {
		return AppendEntriesResponse{Term: n.currentTerm, Success: false, MatchIndex: n.lastLogIndex()}
	}
	if req.Term > n.currentTerm || n.role != Follower {
		n.becomeFollowerLocked(req.Term, req.LeaderID)
	}
	n.leaderID = req.LeaderID
	n.electionReset = time.Now()

	if req.PrevLogIndex > 0 {
		if req.PrevLogIndex < n.snapshotIndex {
			return AppendEntriesResponse{Term: n.currentTerm, Success: true, MatchIndex: n.snapshotIndex}
		}
		if req.PrevLogIndex > n.lastLogIndex() || n.termAtLocked(req.PrevLogIndex) != req.PrevLogTerm {
			return AppendEntriesResponse{Term: n.currentTerm, Success: false, MatchIndex: n.lastLogIndex()}
		}
	}

	changed := false
	for i, incoming := range req.Entries {
		if incoming.Index <= n.snapshotIndex {
			continue
		}
		if incoming.Index <= n.lastLogIndex() {
			if n.termAtLocked(incoming.Index) != incoming.Term {
				n.log = n.log[:n.offsetLocked(incoming.Index)]
				n.log = append(n.log, req.Entries[i:]...)
				changed = true
				break
			}
			continue
		}
		n.log = append(n.log, req.Entries[i:]...)
		changed = true
		break
	}
	if changed {
		_ = n.storage.rewriteLog(n.log)
	}

	if req.LeaderCommit > n.commitIndex {
		n.commitIndex = min(req.LeaderCommit, n.lastLogIndex())
		_ = n.persistStateLocked()
		n.applyCond.Broadcast()
	}
	n.validateInvariantsLocked("append_entries")
	return AppendEntriesResponse{Term: n.currentTerm, Success: true, MatchIndex: n.lastLogIndex()}
}

func (n *Node) HandleReadIndex(req ReadIndexRequest) ReadIndexResponse {
	n.mu.Lock()
	defer n.mu.Unlock()
	if req.Term < n.currentTerm {
		return ReadIndexResponse{Term: n.currentTerm}
	}
	if req.Term > n.currentTerm {
		n.becomeFollowerLocked(req.Term, req.LeaderID)
	}
	if n.leaderID != "" && n.leaderID != req.LeaderID {
		n.recordInvariantViolationLocked("leader_uniqueness")
	}
	n.electionReset = time.Now()
	return ReadIndexResponse{Term: n.currentTerm, Success: true}
}

func (n *Node) ReadIndex(ctx context.Context) error {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return ErrStopped
	}
	if n.role != Leader {
		n.mu.Unlock()
		return ErrNotLeader
	}
	if len(n.peers) == 0 {
		commit := n.commitIndex
		n.mu.Unlock()
		return n.waitForApplied(ctx, commit)
	}
	term := n.currentTerm
	commit := n.commitIndex
	peers := append([]Peer(nil), n.peers...)
	n.mu.Unlock()

	acks := 1
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, peer := range peers {
		wg.Add(1)
		go func(peer Peer) {
			defer wg.Done()
			resp, err := n.readIndex(peer, ReadIndexRequest{Term: term, LeaderID: n.id})
			if err != nil {
				return
			}
			n.mu.Lock()
			if resp.Term > n.currentTerm {
				n.becomeFollowerLocked(resp.Term, "")
				n.mu.Unlock()
				return
			}
			n.mu.Unlock()
			if resp.Success {
				mu.Lock()
				acks++
				mu.Unlock()
			}
		}(peer)
	}
	wg.Wait()
	n.mu.Lock()
	ok := n.role == Leader && n.currentTerm == term && acks >= n.quorumLocked()
	n.mu.Unlock()
	if !ok {
		return ErrNotLeader
	}
	return n.waitForApplied(ctx, commit)
}

func (n *Node) electionLoop() {
	for {
		timeout := n.randomElectionTimeout()
		timer := time.NewTimer(timeout)
		select {
		case <-timer.C:
			n.mu.Lock()
			shouldStart := n.role != Leader && time.Since(n.electionReset) >= timeout
			n.mu.Unlock()
			if shouldStart {
				n.startElection()
			}
		case <-n.stopCh:
			timer.Stop()
			return
		}
	}
}

func (n *Node) heartbeatLoop() {
	ticker := time.NewTicker(n.heartbeat)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if n.IsLeader() {
				n.replicateAll()
			}
		case <-n.stopCh:
			return
		}
	}
}

func (n *Node) applyLoop() {
	for {
		n.mu.Lock()
		for !n.stopped && n.lastApplied >= n.commitIndex {
			n.applyCond.Wait()
		}
		if n.stopped {
			n.mu.Unlock()
			return
		}
		n.lastApplied++
		entry := n.log[n.offsetLocked(n.lastApplied)]
		waiter := n.waiters[entry.Index]
		delete(n.waiters, entry.Index)
		n.mu.Unlock()

		err := n.kv.Apply(entry.Command)
		if waiter != nil {
			waiter <- err
			close(waiter)
		}
	}
}

func (n *Node) startElection() {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return
	}
	n.role = Candidate
	n.currentTerm++
	n.validateInvariantsLocked("election")
	term := n.currentTerm
	n.votedFor = n.id
	n.leaderID = ""
	n.electionReset = time.Now()
	lastIndex, lastTerm := n.lastLogIndex(), n.lastLogTerm()
	_ = n.persistStateLocked()
	peers := append([]Peer(nil), n.peers...)
	n.mu.Unlock()

	votes := 1
	var votesMu sync.Mutex
	var wg sync.WaitGroup
	for _, peer := range peers {
		wg.Add(1)
		go func(peer Peer) {
			defer wg.Done()
			resp, err := n.requestVote(peer, RequestVoteRequest{
				Term:         term,
				CandidateID:  n.id,
				LastLogIndex: lastIndex,
				LastLogTerm:  lastTerm,
			})
			if err != nil {
				return
			}
			n.mu.Lock()
			if resp.Term > n.currentTerm {
				n.becomeFollowerLocked(resp.Term, "")
				n.mu.Unlock()
				return
			}
			n.mu.Unlock()
			if resp.VoteGranted {
				votesMu.Lock()
				votes++
				votesMu.Unlock()
			}
		}(peer)
	}
	wg.Wait()

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role == Candidate && n.currentTerm == term && votes >= n.quorumLocked() {
		n.role = Leader
		n.leaderID = n.id
		if n.metrics != nil {
			n.metrics.RecordLeaderChange()
		}
		if n.logger != nil {
			n.logger.Info("leader_elected", map[string]any{"node": n.id, "term": n.currentTerm})
		}
		next := n.lastLogIndex() + 1
		n.matchIndex = map[string]int64{n.id: n.lastLogIndex()}
		n.nextIndex = map[string]int64{n.id: next}
		for _, peer := range n.peers {
			n.nextIndex[peer.ID] = next
			n.matchIndex[peer.ID] = 0
		}
		go n.replicateAll()
	}
}

func (n *Node) replicateAll() {
	n.mu.Lock()
	if n.role != Leader || n.stopped {
		n.mu.Unlock()
		return
	}
	peers := append([]Peer(nil), n.peers...)
	n.mu.Unlock()

	for _, peer := range peers {
		go n.replicatePeer(peer)
	}
}

func (n *Node) replicatePeer(peer Peer) {
	for {
		n.mu.Lock()
		if n.role != Leader || n.stopped {
			n.mu.Unlock()
			return
		}
		next := n.nextIndex[peer.ID]
		if next == 0 {
			next = n.lastLogIndex() + 1
			n.nextIndex[peer.ID] = next
		}
		if next <= n.snapshotIndex {
			next = n.snapshotIndex + 1
			n.nextIndex[peer.ID] = next
		}
		prevIndex := next - 1
		req := AppendEntriesRequest{
			Term:         n.currentTerm,
			LeaderID:     n.id,
			PrevLogIndex: prevIndex,
			PrevLogTerm:  n.termAtLocked(prevIndex),
			LeaderCommit: n.commitIndex,
		}
		if next <= n.lastLogIndex() {
			req.Entries = append([]Entry(nil), n.log[n.offsetLocked(next):]...)
		}
		n.mu.Unlock()

		resp, err := n.appendEntries(peer, req)
		if err != nil {
			return
		}

		n.mu.Lock()
		if resp.Term > n.currentTerm {
			n.becomeFollowerLocked(resp.Term, "")
			n.mu.Unlock()
			return
		}
		if n.role != Leader || req.Term != n.currentTerm {
			n.mu.Unlock()
			return
		}
		if resp.Success {
			n.matchIndex[peer.ID] = resp.MatchIndex
			n.nextIndex[peer.ID] = resp.MatchIndex + 1
			if n.metrics != nil {
				n.metrics.SetReplicationLag(peer.ID, n.lastLogIndex()-resp.MatchIndex)
			}
			n.advanceCommitLocked()
			n.mu.Unlock()
			return
		}
		if n.nextIndex[peer.ID] > 1 {
			n.nextIndex[peer.ID]--
		}
		n.mu.Unlock()
	}
}

func (n *Node) advanceCommitLocked() {
	var indexes []int64
	indexes = append(indexes, n.lastLogIndex())
	for _, peer := range n.peers {
		indexes = append(indexes, n.matchIndex[peer.ID])
	}
	sort.Slice(indexes, func(i, j int) bool { return indexes[i] > indexes[j] })
	candidate := indexes[n.quorumLocked()-1]
	if candidate > n.commitIndex && n.termAtLocked(candidate) == n.currentTerm {
		n.commitIndex = candidate
		_ = n.persistStateLocked()
		n.applyCond.Broadcast()
	}
}

func (n *Node) requestVote(peer Peer, req RequestVoteRequest) (RequestVoteResponse, error) {
	var resp RequestVoteResponse
	err := n.postJSON(peer.RaftURL+"/raft/request-vote", req, &resp)
	return resp, err
}

func (n *Node) appendEntries(peer Peer, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	var resp AppendEntriesResponse
	err := n.postJSON(peer.RaftURL+"/raft/append-entries", req, &resp)
	return resp, err
}

func (n *Node) readIndex(peer Peer, req ReadIndexRequest) (ReadIndexResponse, error) {
	var resp ReadIndexResponse
	err := n.postJSON(peer.RaftURL+"/raft/read-index", req, &resp)
	return resp, err
}

func (n *Node) postJSON(url string, req any, resp any) error {
	ctx, cancel := context.WithTimeout(context.Background(), n.rpcTimeout)
	defer cancel()
	b, err := json.Marshal(req)
	if err != nil {
		return err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(b))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpResp, err := n.client.Do(httpReq)
	if err != nil {
		return err
	}
	defer func() { _ = httpResp.Body.Close() }()
	if httpResp.StatusCode/100 != 2 {
		return fmt.Errorf("raft rpc %s returned %s", url, httpResp.Status)
	}
	return json.NewDecoder(httpResp.Body).Decode(resp)
}

func (n *Node) becomeFollowerLocked(term int64, leaderID string) {
	if n.role == Leader {
		n.failWaitersLocked(ErrNotLeader)
	}
	previous := n.role
	if term < n.currentTerm {
		n.recordInvariantViolationLocked("term_monotonicity")
	}
	n.role = Follower
	n.currentTerm = term
	n.votedFor = ""
	n.leaderID = leaderID
	n.electionReset = time.Now()
	if previous != Follower && n.logger != nil {
		n.logger.Info("became_follower", map[string]any{"node": n.id, "term": n.currentTerm, "leader": leaderID})
	}
	_ = n.persistStateLocked()
}

func (n *Node) failWaitersLocked(err error) {
	for index, ch := range n.waiters {
		delete(n.waiters, index)
		ch <- err
		close(ch)
	}
}

func (n *Node) persistStateLocked() error {
	return n.storage.saveState(durableState{CurrentTerm: n.currentTerm, VotedFor: n.votedFor, CommitIndex: n.commitIndex, SnapshotIndex: n.snapshotIndex, SnapshotTerm: n.snapshotTerm})
}

func (n *Node) randomElectionTimeout() time.Duration {
	if n.electionMax <= n.electionMin {
		return n.electionMin
	}
	delta := n.electionMax - n.electionMin
	return n.electionMin + time.Duration(rand.Int63n(int64(delta)))
}

func (n *Node) lastLogIndex() int64 {
	if len(n.log) == 0 {
		return n.snapshotIndex
	}
	return n.log[len(n.log)-1].Index
}

func (n *Node) lastLogTerm() int64 {
	if len(n.log) == 0 {
		return n.snapshotTerm
	}
	return n.log[len(n.log)-1].Term
}

func (n *Node) termAtLocked(index int64) int64 {
	if index == 0 {
		return 0
	}
	if index == n.snapshotIndex {
		return n.snapshotTerm
	}
	if index < n.snapshotIndex || index > n.lastLogIndex() {
		return -1
	}
	return n.log[n.offsetLocked(index)].Term
}

func (n *Node) quorumLocked() int {
	return (len(n.peers)+1)/2 + 1
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func (n *Node) offsetLocked(index int64) int64 {
	return index - n.snapshotIndex - 1
}

func (n *Node) CompactAppliedLog() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.lastApplied <= n.snapshotIndex {
		return nil
	}
	term := n.termAtLocked(n.lastApplied)
	if term < 0 {
		n.recordInvariantViolationLocked("snapshot_term")
		return nil
	}
	keep := []Entry{}
	for _, entry := range n.log {
		if entry.Index > n.lastApplied {
			keep = append(keep, entry)
		}
	}
	n.snapshotIndex = n.lastApplied
	n.snapshotTerm = term
	n.log = keep
	if err := n.persistStateLocked(); err != nil {
		return err
	}
	return n.storage.rewriteLog(n.log)
}

func (n *Node) ShouldSnapshot(maxEntries int64) bool {
	if maxEntries <= 0 {
		return false
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.lastApplied-n.snapshotIndex >= maxEntries
}

func (n *Node) waitForApplied(ctx context.Context, index int64) error {
	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()
	for {
		n.mu.Lock()
		if n.stopped {
			n.mu.Unlock()
			return ErrStopped
		}
		if n.lastApplied >= index {
			n.mu.Unlock()
			return nil
		}
		n.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-n.stopCh:
			return ErrStopped
		case <-ticker.C:
		}
	}
}

func (n *Node) validateInvariantsLocked(_ string) {
	if n.commitIndex > n.lastLogIndex() {
		n.recordInvariantViolationLocked("commit_safety")
	}
	if n.commitIndex < n.snapshotIndex {
		n.recordInvariantViolationLocked("commit_safety")
	}
	previousIndex := n.snapshotIndex
	previousTerm := n.snapshotTerm
	for _, entry := range n.log {
		if entry.Index != previousIndex+1 {
			n.recordInvariantViolationLocked("log_matching")
		}
		if entry.Term < previousTerm {
			n.recordInvariantViolationLocked("term_monotonicity")
		}
		previousIndex = entry.Index
		previousTerm = entry.Term
	}
}

func (n *Node) recordInvariantViolationLocked(name string) {
	if n.metrics != nil {
		n.metrics.RecordInvariantViolation(name)
	}
	if n.logger != nil {
		n.logger.Info("raft_invariant_violation", map[string]any{"node": n.id, "invariant": name})
	}
}
