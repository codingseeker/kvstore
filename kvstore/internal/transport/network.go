package transport

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"
)

type Network struct {
	mu         sync.RWMutex
	blocked    map[string]bool
	minDelay   time.Duration
	maxDelay   time.Duration
	randomDrop float64
}

func NewNetwork() *Network {
	return &Network{blocked: map[string]bool{}}
}

func (n *Network) Partition(from, to string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.blocked[from+"->"+to] = true
}

func (n *Network) Heal(from, to string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.blocked, from+"->"+to)
}

func (n *Network) Delay(minDelay, maxDelay time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.minDelay = minDelay
	n.maxDelay = maxDelay
}

func (n *Network) RandomDrop(probability float64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.randomDrop = probability
}

func (n *Network) BeforeSend(ctx context.Context, from, to string) error {
	n.mu.RLock()
	blocked := n.blocked[from+"->"+to] || n.blocked[from+"->*"] || n.blocked["*->"+to] || n.blocked[to] || n.blocked[from]
	minDelay := n.minDelay
	maxDelay := n.maxDelay
	randomDrop := n.randomDrop
	n.mu.RUnlock()

	if blocked {
		return errors.New("link partitioned")
	}
	if randomDrop > 0 && rand.Float64() < randomDrop {
		return errors.New("message dropped")
	}
	if maxDelay > 0 {
		delay := minDelay
		if maxDelay > minDelay {
			delay += time.Duration(rand.Int63n(int64(maxDelay - minDelay)))
		}
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}
