package raft

import (
	"context"
	"testing"
	"time"

	"kvstore/internal/store"
)

func TestDeterministicRecoveryAfterCrash(t *testing.T) {
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
	waitLeader(t, node, 2*time.Second)

	for i := 0; i < 10; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		if err := node.Propose(ctx, store.Command{Op: store.OpPut, Key: "crash", Value: string(rune('0' + i))}); err != nil {
			cancel()
			t.Fatal(err)
		}
		cancel()
	}
	waitApplied(t, node, 10, 2*time.Second)

	node.Stop()

	recoveredKV := store.New()
	recovered, err := New(Config{ID: "n1", DataDir: dir}, recoveredKV)
	if err != nil {
		t.Fatal(err)
	}
	recovered.Start()
	defer recovered.Stop()

	waitApplied(t, recovered, 10, 2*time.Second)
	if val, ok := recoveredKV.Get("crash"); !ok || val != "9" {
		t.Fatalf("recovered value = %q, ok=%v", val, ok)
	}
}

func TestDeterministicSnapshotDuringWrite(t *testing.T) {
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
	waitLeader(t, node, 2*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	for i := 0; i < 5; i++ {
		if err := node.Propose(ctx, store.Command{Op: store.OpPut, Key: "snap", Value: string(rune('a' + i))}); err != nil {
			cancel()
			t.Fatal(err)
		}
	}
	cancel()

	waitApplied(t, node, 5, 2*time.Second)

	if err := node.CompactAppliedLog(); err != nil {
		t.Fatal(err)
	}

	waitLeader(t, node, 2*time.Second)

	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	if err := node.Propose(ctx2, store.Command{Op: store.OpPut, Key: "after", Value: "post-snapshot"}); err != nil {
		t.Fatal(err)
	}

	waitValue(t, kv, "after", "post-snapshot", 2*time.Second)
	waitValue(t, kv, "snap", "e", 2*time.Second)
}

func TestDeterministicReadIndexLinearizability(t *testing.T) {
	kv := store.New()
	node, err := New(Config{
		ID:          "n1",
		DataDir:     t.TempDir(),
		ElectionMin: 10 * time.Millisecond,
		ElectionMax: 20 * time.Millisecond,
		Heartbeat:   5 * time.Millisecond,
	}, kv)
	if err != nil {
		t.Fatal(err)
	}
	node.Start()
	waitLeader(t, node, 2*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := node.Propose(ctx, store.Command{Op: store.OpPut, Key: "linear", Value: "v1"}); err != nil {
		t.Fatal(err)
	}
	waitValue(t, kv, "linear", "v1", 2*time.Second)

	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	if err := node.ReadIndex(ctx2); err != nil {
		t.Fatalf("read index failed: %v", err)
	}
	readVal, _ := kv.Get("linear")
	if readVal != "v1" {
		t.Fatalf("read returned stale value: %s", readVal)
	}
}
