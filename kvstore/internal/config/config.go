package config

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Peer struct {
	ID      string
	RaftURL string
}

type Config struct {
	ID              string
	APIAddr         string
	RaftAddr        string
	DataDir         string
	Peers           []Peer
	ElectionMin     time.Duration
	ElectionMax     time.Duration
	Heartbeat       time.Duration
	RequestTimeout  time.Duration
	LeaderLeaseRead bool
	FaultMinDelay   time.Duration
	FaultMaxDelay   time.Duration
	FaultDropRate   float64
	FaultPartitions string
	AuthToken       string
	SigningSecret   string
	RateLimitPerSec int
	RaftTLSCert     string
	RaftTLSKey      string
	RaftTLSCA       string
	LogSegmentBytes int64
	SnapshotEvery   time.Duration
	SnapshotMaxLog  int64
	WALFsyncPolicy  string
}

func Parse() (Config, error) {
	var peerCSV string
	cfg := Config{}
	flag.StringVar(&cfg.ID, "id", stringEnv("KV_ID", ""), "node id")
	flag.StringVar(&cfg.APIAddr, "api", stringEnv("KV_API_ADDR", "127.0.0.1:8080"), "client API listen address")
	flag.StringVar(&cfg.RaftAddr, "raft", stringEnv("KV_RAFT_ADDR", "127.0.0.1:9000"), "Raft RPC listen address")
	flag.StringVar(&cfg.DataDir, "data", stringEnv("KV_DATA_DIR", "data"), "data directory")
	flag.StringVar(&peerCSV, "peers", stringEnv("KV_PEERS", ""), "comma-separated peers id=url, for example n2=http://127.0.0.1:9002")
	flag.DurationVar(&cfg.ElectionMin, "election-min", durationEnv("KV_ELECTION_MIN", 350*time.Millisecond), "minimum election timeout")
	flag.DurationVar(&cfg.ElectionMax, "election-max", durationEnv("KV_ELECTION_MAX", 700*time.Millisecond), "maximum election timeout")
	flag.DurationVar(&cfg.Heartbeat, "heartbeat", durationEnv("KV_HEARTBEAT", 100*time.Millisecond), "leader heartbeat interval")
	flag.DurationVar(&cfg.RequestTimeout, "request-timeout", durationEnv("KV_REQUEST_TIMEOUT", 2*time.Second), "Raft RPC request timeout")
	flag.DurationVar(&cfg.FaultMinDelay, "fault-min-delay", durationEnv("KV_FAULT_MIN_DELAY", 0), "minimum injected Raft message delay")
	flag.DurationVar(&cfg.FaultMaxDelay, "fault-max-delay", durationEnv("KV_FAULT_MAX_DELAY", 0), "maximum injected Raft message delay")
	flag.Float64Var(&cfg.FaultDropRate, "fault-drop-rate", floatEnv("KV_FAULT_DROP_RATE", 0), "injected Raft message drop probability")
	flag.StringVar(&cfg.FaultPartitions, "fault-partitions", stringEnv("KV_FAULT_PARTITIONS", ""), "comma-separated blocked links from->host")
	flag.StringVar(&cfg.AuthToken, "auth-token", stringEnv("KV_AUTH_TOKEN", ""), "optional client API token")
	flag.StringVar(&cfg.SigningSecret, "signing-secret", stringEnv("KV_SIGNING_SECRET", ""), "optional HMAC request signing secret")
	flag.IntVar(&cfg.RateLimitPerSec, "rate-limit-per-sec", intEnv("KV_RATE_LIMIT_PER_SEC", 0), "optional per-client request limit per second")
	flag.StringVar(&cfg.RaftTLSCert, "raft-tls-cert", stringEnv("KV_RAFT_TLS_CERT", ""), "optional Raft TLS certificate")
	flag.StringVar(&cfg.RaftTLSKey, "raft-tls-key", stringEnv("KV_RAFT_TLS_KEY", ""), "optional Raft TLS key")
	flag.StringVar(&cfg.RaftTLSCA, "raft-tls-ca", stringEnv("KV_RAFT_TLS_CA", ""), "optional Raft TLS CA")
	flag.Int64Var(&cfg.LogSegmentBytes, "log-segment-bytes", int64Env("KV_LOG_SEGMENT_BYTES", 0), "optional Raft log segment size in bytes")
	flag.DurationVar(&cfg.SnapshotEvery, "snapshot-every", durationEnv("KV_SNAPSHOT_EVERY", 0), "optional key-value snapshot interval")
	flag.Int64Var(&cfg.SnapshotMaxLog, "snapshot-max-log-entries", int64Env("KV_SNAPSHOT_MAX_LOG_ENTRIES", 0), "optional key-value snapshot trigger by unapplied Raft log entries")
	flag.StringVar(&cfg.WALFsyncPolicy, "wal-fsync", stringEnv("KV_WAL_FSYNC", "always"), "Raft WAL fsync policy: always or batch")
	flag.Parse()

	if cfg.ID == "" {
		return cfg, fmt.Errorf("-id is required")
	}
	peers, err := parsePeers(peerCSV)
	if err != nil {
		return cfg, err
	}
	cfg.Peers = peers
	return cfg, nil
}

func parsePeers(raw string) ([]Peer, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	parts := strings.Split(raw, ",")
	peers := make([]Peer, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		id, url, ok := strings.Cut(part, "=")
		if !ok || strings.TrimSpace(id) == "" || strings.TrimSpace(url) == "" {
			return nil, fmt.Errorf("invalid peer %q, want id=url", part)
		}
		peers = append(peers, Peer{ID: strings.TrimSpace(id), RaftURL: strings.TrimSpace(url)})
	}
	return peers, nil
}

func stringEnv(key string, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
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

func int64Env(key string, fallback int64) int64 {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return fallback
	}
	return parsed
}

func intEnv(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}
