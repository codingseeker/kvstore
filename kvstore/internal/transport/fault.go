package transport

import (
	"errors"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type FaultConfig struct {
	MinDelay  time.Duration
	MaxDelay  time.Duration
	DropRate  float64
	Partition map[string]bool
}

func FaultConfigFromEnv() FaultConfig {
	cfg := FaultConfig{Partition: map[string]bool{}}
	cfg.MinDelay = durationEnv("KV_FAULT_MIN_DELAY", 0)
	cfg.MaxDelay = durationEnv("KV_FAULT_MAX_DELAY", 0)
	cfg.DropRate = floatEnv("KV_FAULT_DROP_RATE", 0)
	cfg.Partition = ParsePartitions(os.Getenv("KV_FAULT_PARTITIONS"))
	return cfg
}

func NewFaultNetwork(cfg FaultConfig) *Network {
	n := NewNetwork()
	n.Apply(cfg)
	return n
}

type FaultRoundTripper struct {
	Base    http.RoundTripper
	Network *Network
	From    string
}

func (t FaultRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	base := t.Base
	if base == nil {
		base = http.DefaultTransport
	}
	if t.Network != nil {
		if err := t.Network.BeforeSend(req.Context(), t.From, req.URL.Host); err != nil {
			return nil, err
		}
	}
	return base.RoundTrip(req)
}

func (n *Network) Apply(cfg FaultConfig) {
	n.mu.Lock()
	n.minDelay = cfg.MinDelay
	n.maxDelay = cfg.MaxDelay
	n.randomDrop = cfg.DropRate
	if cfg.Partition != nil {
		n.blocked = map[string]bool{}
		for key, value := range cfg.Partition {
			if value {
				n.blocked[key] = true
			}
		}
	}
	n.mu.Unlock()
}

func ParsePartitions(raw string) map[string]bool {
	out := map[string]bool{}
	for _, item := range strings.Split(raw, ",") {
		item = strings.TrimSpace(item)
		if item != "" {
			out[item] = true
		}
	}
	return out
}

func (n *Network) Snapshot() FaultConfig {
	n.mu.RLock()
	defer n.mu.RUnlock()
	partition := map[string]bool{}
	for key, value := range n.blocked {
		partition[key] = value
	}
	return FaultConfig{
		MinDelay:  n.minDelay,
		MaxDelay:  n.maxDelay,
		DropRate:  n.randomDrop,
		Partition: partition,
	}
}

func (n *Network) Validate() error {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if n.randomDrop < 0 || n.randomDrop > 1 {
		return errors.New("drop rate must be between 0 and 1")
	}
	if n.minDelay < 0 || n.maxDelay < 0 {
		return errors.New("delay cannot be negative")
	}
	if n.maxDelay > 0 && n.maxDelay < n.minDelay {
		return errors.New("max delay must be greater than or equal to min delay")
	}
	return nil
}

func durationEnv(key string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func floatEnv(key string, fallback float64) float64 {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return fallback
	}
	return parsed
}
