package store

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
)

type Operation string

const (
	OpPut    Operation = "put"
	OpDelete Operation = "delete"
	OpNoop   Operation = "noop"
)

type Command struct {
	Op    Operation `json:"op"`
	Key   string    `json:"key,omitempty"`
	Value string    `json:"value,omitempty"`
}

type KV struct {
	mu   sync.RWMutex
	data map[string]string
}

func New() *KV {
	return &KV{data: make(map[string]string)}
}

func (k *KV) Apply(cmd Command) error {
	k.mu.Lock()
	defer k.mu.Unlock()

	switch cmd.Op {
	case OpPut:
		if cmd.Key == "" {
			return errors.New("empty key")
		}
		k.data[cmd.Key] = cmd.Value
	case OpDelete:
		if cmd.Key == "" {
			return errors.New("empty key")
		}
		delete(k.data, cmd.Key)
	case OpNoop:
		return nil
	default:
		return errors.New("unknown command")
	}
	return nil
}

func (k *KV) Get(key string) (string, bool) {
	k.mu.RLock()
	defer k.mu.RUnlock()
	v, ok := k.data[key]
	return v, ok
}

func (k *KV) Snapshot() map[string]string {
	k.mu.RLock()
	defer k.mu.RUnlock()
	out := make(map[string]string, len(k.data))
	for key, value := range k.data {
		out[key] = value
	}
	return out
}

func (k *KV) SnapshotKeys() []string {
	k.mu.RLock()
	defer k.mu.RUnlock()
	keys := make([]string, 0, len(k.data))
	for key := range k.data {
		keys = append(keys, key)
	}
	return keys
}

func (k *KV) Restore(data map[string]string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.data = make(map[string]string, len(data))
	for key, value := range data {
		k.data[key] = value
	}
}

func SaveSnapshot(path string, data map[string]string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	b, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer func() { _ = dir.Close() }()
	return dir.Sync()
}

func LoadSnapshot(path string) (map[string]string, error) {
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return map[string]string{}, nil
	}
	if err != nil {
		return nil, err
	}
	var data map[string]string
	if len(b) == 0 {
		return map[string]string{}, nil
	}
	if err := json.Unmarshal(b, &data); err != nil {
		return nil, err
	}
	if data == nil {
		data = map[string]string{}
	}
	return data, nil
}
