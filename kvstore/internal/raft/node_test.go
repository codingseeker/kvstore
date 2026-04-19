package raft

import (
	"context"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"kvstore/internal/observability"
	"kvstore/internal/store"
	"kvstore/internal/transport"
)

func TestRequestVoteGrantsForUpToDateCandidate(t *testing.T) {
	kv := store.New()
	node, err := New(Config{ID: "n1", DataDir: t.TempDir()}, kv)
	if err != nil {
		t.Fatal(err)
	}
	resp := node.HandleRequestVote(RequestVoteRequest{
		Term:         1,
		CandidateID:  "n2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
	if !resp.VoteGranted {
		t.Fatalf("expected vote granted: %+v", resp)
	}
	if resp.Term != 1 {
		t.Fatalf("term = %d, want 1", resp.Term)
	}
}

func TestAppendEntriesRejectsLogMismatch(t *testing.T) {
	kv := store.New()
	node, err := New(Config{ID: "n1", DataDir: t.TempDir()}, kv)
	if err != nil {
		t.Fatal(err)
	}
	node.log = []Entry{{Index: 1, Term: 1, Command: store.Command{Op: store.OpNoop}}}
	resp := node.HandleAppendEntries(AppendEntriesRequest{
		Term:         2,
		LeaderID:     "n2",
		PrevLogIndex: 1,
		PrevLogTerm:  9,
	})
	if resp.Success {
		t.Fatalf("expected rejection on prev log term mismatch")
	}
}

func TestSingleNodeCommitAndRecovery(t *testing.T) {
	dir := t.TempDir()
	kv := store.New()
	node, err := New(Config{
		ID:          "n1",
		DataDir:     dir,
		ElectionMin: 10 * time.Millisecond,
		ElectionMax: 20 * time.Millisecond,
		Heartbeat:   5 * time.Millisecond,
	}, kv)
	if err != nil {
		t.Fatal(err)
	}
	node.Start()
	defer node.Stop()
	waitLeader(t, node, 2*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := node.Propose(ctx, store.Command{Op: store.OpPut, Key: "a", Value: "1"}); err != nil {
		t.Fatal(err)
	}
	if value, ok := kv.Get("a"); !ok || value != "1" {
		t.Fatalf("value = %q ok=%v", value, ok)
	}
	node.Stop()

	recoveredKV := store.New()
	recovered, err := New(Config{ID: "n1", DataDir: dir}, recoveredKV)
	if err != nil {
		t.Fatal(err)
	}
	recovered.Start()
	defer recovered.Stop()
	waitApplied(t, recovered, 1, time.Second)
	if value, ok := recoveredKV.Get("a"); !ok || value != "1" {
		t.Fatalf("recovered value = %q ok=%v", value, ok)
	}
}

func TestThreeNodeLeaderElectionAndReplication(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.stop()

	leader := cluster.waitLeader(3 * time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := leader.node.Propose(ctx, store.Command{Op: store.OpPut, Key: "k", Value: "v"}); err != nil {
		t.Fatal(err)
	}

	for _, member := range cluster.members {
		waitValue(t, member.kv, "k", "v", 2*time.Second)
	}
}

func TestLeaderFailureReElection(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.stop()

	leader := cluster.waitLeader(3 * time.Second)
	leader.node.Stop()
	_ = leader.server.Close()

	newLeader := cluster.waitLeaderExcept(leader.id, 5*time.Second)
	if newLeader.id == leader.id {
		t.Fatalf("same leader elected after failure")
	}
}

func TestSplitBrainLeaderCannotCommitAndRejoinConsistency(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.stop()

	leader := cluster.waitLeader(3 * time.Second)
	var majority []*testMember
	for _, member := range cluster.members {
		if member.id != leader.id {
			cluster.partition(leader.id, member.id)
			majority = append(majority, member)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	err := leader.node.Propose(ctx, store.Command{Op: store.OpPut, Key: "isolated", Value: "bad"})
	cancel()
	if err == nil {
		t.Fatalf("isolated leader committed without quorum")
	}

	newLeader := cluster.waitLeaderExcept(leader.id, 5*time.Second)
	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	if err := newLeader.node.Propose(ctx, store.Command{Op: store.OpPut, Key: "safe", Value: "ok"}); err != nil {
		cancel()
		t.Fatal(err)
	}
	cancel()
	for _, member := range majority {
		waitValue(t, member.kv, "safe", "ok", 2*time.Second)
	}

	for _, member := range majority {
		cluster.heal(leader.id, member.id)
	}
	waitValue(t, leader.kv, "safe", "ok", 5*time.Second)
	if _, ok := leader.kv.Get("isolated"); ok {
		t.Fatalf("isolated write became visible after heal")
	}
}

func TestDelayedCommitWithFaultInjection(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.stop()
	leader := cluster.waitLeader(3 * time.Second)
	cluster.network.Delay(120*time.Millisecond, 120*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	start := time.Now()
	if err := leader.node.Propose(ctx, store.Command{Op: store.OpPut, Key: "delay", Value: "ok"}); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) < 100*time.Millisecond {
		t.Fatalf("commit was not delayed")
	}
	for _, member := range cluster.members {
		waitValue(t, member.kv, "delay", "ok", 2*time.Second)
	}
}

func TestLeaderChurnMaintainsSafety(t *testing.T) {
	cluster := newTestCluster(t, 5)
	defer cluster.stop()
	seen := map[string]bool{}
	for i := 0; i < 2; i++ {
		leader := cluster.waitLeader(5 * time.Second)
		seen[leader.id] = true
		leader.node.Stop()
		_ = leader.server.Close()
		next := cluster.waitLeaderExcept(leader.id, 5*time.Second)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		if err := next.node.Propose(ctx, store.Command{Op: store.OpPut, Key: "churn", Value: next.id}); err != nil {
			cancel()
			t.Fatal(err)
		}
		cancel()
	}
	if len(seen) < 2 {
		t.Fatalf("expected leader churn")
	}
}

func TestConcurrentStressWithInjectedFaultsPreservesCommittedValues(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.stop()
	metrics := observability.NewMetrics()
	for _, member := range cluster.members {
		member.node.metrics = metrics
	}
	cluster.network.RandomDrop(0.10)
	leader := cluster.waitLeader(3 * time.Second)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = leader.node.Propose(ctx, store.Command{Op: store.OpPut, Key: "stress", Value: string(rune('a' + i%26))})
		}(i)
	}
	wg.Wait()
	cluster.network.RandomDrop(0)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	if err := leader.node.Propose(ctx, store.Command{Op: store.OpPut, Key: "final", Value: "ok"}); err != nil {
		cancel()
		t.Fatal(err)
	}
	cancel()
	for _, member := range cluster.members {
		waitValue(t, member.kv, "final", "ok", 3*time.Second)
	}
	snap := metrics.Snapshot()
	violations := snap["invariant_violations"].(map[string]int64)
	for name, count := range violations {
		if count != 0 {
			t.Fatalf("invariant %s violations=%d", name, count)
		}
	}
}

func TestWALChecksumDetectsCorruption(t *testing.T) {
	dir := t.TempDir()
	storage := newDiskStorage(dir, 0, "always")
	err := storage.appendEntries([]Entry{{Index: 1, Term: 1, Command: store.Command{Op: store.OpPut, Key: "a", Value: "1"}}})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "raft-log.jsonl")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	b[len(b)-5] = '0'
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
	_, _, err = storage.load()
	if err == nil {
		t.Fatalf("expected checksum corruption")
	}
}

func TestWALRecoveryIgnoresPartialTrailingWrite(t *testing.T) {
	dir := t.TempDir()
	storage := newDiskStorage(dir, 0, "always")
	err := storage.appendEntries([]Entry{{Index: 1, Term: 1, Command: store.Command{Op: store.OpPut, Key: "a", Value: "1"}}})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "raft-log.jsonl")
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(`{"entry":{"index":2`); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	_, entries, err := storage.load()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Index != 1 {
		t.Fatalf("entries = %+v", entries)
	}
}

func TestSnapshotCompactionRecovery(t *testing.T) {
	dir := t.TempDir()
	kv := store.New()
	node, err := New(Config{ID: "n1", DataDir: dir, ElectionMin: 10 * time.Millisecond, ElectionMax: 20 * time.Millisecond, Heartbeat: 5 * time.Millisecond}, kv)
	if err != nil {
		t.Fatal(err)
	}
	node.Start()
	waitLeader(t, node, 2*time.Second)
	for i := 0; i < 3; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err := node.Propose(ctx, store.Command{Op: store.OpPut, Key: "k", Value: string(rune('0' + i))})
		cancel()
		if err != nil {
			t.Fatal(err)
		}
	}
	waitApplied(t, node, 3, time.Second)
	if err := node.CompactAppliedLog(); err != nil {
		t.Fatal(err)
	}
	base, last := node.LogSpan()
	if base != 3 || last != 3 {
		t.Fatalf("span = %d,%d want 3,3", base, last)
	}
	node.Stop()

	recoveredKV := store.New()
	recoveredKV.Restore(kv.Snapshot())
	recovered, err := New(Config{ID: "n1", DataDir: dir}, recoveredKV)
	if err != nil {
		t.Fatal(err)
	}
	base, last = recovered.LogSpan()
	if base != 3 || last != 3 {
		t.Fatalf("recovered span = %d,%d want 3,3", base, last)
	}
	if value, ok := recoveredKV.Get("k"); !ok || value != "2" {
		t.Fatalf("snapshot value = %q ok=%v", value, ok)
	}
}

type testMember struct {
	id     string
	node   *Node
	kv     *store.KV
	server *http.Server
	url    string
}

type testCluster struct {
	members []*testMember
	network *transport.Network
}

func newTestCluster(t *testing.T, size int) *testCluster {
	t.Helper()
	listeners := make([]net.Listener, size)
	urls := make([]string, size)
	for i := 0; i < size; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[i] = ln
		urls[i] = "http://" + ln.Addr().String()
	}

	cluster := &testCluster{network: transport.NewNetwork()}
	for i := 0; i < size; i++ {
		id := "n" + string(rune('1'+i))
		var peers []Peer
		for j := 0; j < size; j++ {
			if i == j {
				continue
			}
			peers = append(peers, Peer{ID: "n" + string(rune('1'+j)), RaftURL: urls[j]})
		}
		kv := store.New()
		node, err := New(Config{
			ID:             id,
			Peers:          peers,
			DataDir:        filepath.Join(t.TempDir(), id),
			ElectionMin:    150 * time.Millisecond,
			ElectionMax:    300 * time.Millisecond,
			Heartbeat:      30 * time.Millisecond,
			RequestTimeout: 500 * time.Millisecond,
			HTTPClient: &http.Client{
				Transport: transport.FaultRoundTripper{Network: cluster.network, From: id},
			},
		}, kv)
		if err != nil {
			t.Fatal(err)
		}
		mux := http.NewServeMux()
		node.Register(mux)
		server := &http.Server{Handler: mux}
		member := &testMember{id: id, node: node, kv: kv, server: server, url: urls[i]}
		cluster.members = append(cluster.members, member)
		node.Start()
		go func(ln net.Listener) {
			_ = server.Serve(ln)
		}(listeners[i])
	}
	return cluster
}

func (c *testCluster) stop() {
	for _, member := range c.members {
		member.node.Stop()
		_ = member.server.Close()
	}
}

func (c *testCluster) waitLeader(timeout time.Duration) *testMember {
	return c.waitLeaderExcept("", timeout)
}

func (c *testCluster) waitLeaderExcept(excluded string, timeout time.Duration) *testMember {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, member := range c.members {
			if member.id == excluded {
				continue
			}
			if member.node.IsLeader() {
				return member
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	panic("timed out waiting for leader")
}

func (c *testCluster) partition(a string, b string) {
	c.network.Partition(a, c.host(b))
	c.network.Partition(b, c.host(a))
}

func (c *testCluster) heal(a string, b string) {
	c.network.Heal(a, c.host(b))
	c.network.Heal(b, c.host(a))
}

func (c *testCluster) host(id string) string {
	for _, member := range c.members {
		if member.id == id {
			u, err := url.Parse(member.url)
			if err == nil {
				return u.Host
			}
		}
	}
	return ""
}

func waitLeader(t *testing.T, node *Node, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if node.IsLeader() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for leader")
}

func waitApplied(t *testing.T, node *Node, index int64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		node.mu.Lock()
		applied := node.lastApplied
		node.mu.Unlock()
		if applied >= index {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for apply index %d", index)
}

func waitValue(t *testing.T, kv *store.KV, key, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if got, ok := kv.Get(key); ok && got == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s=%s", key, want)
}
