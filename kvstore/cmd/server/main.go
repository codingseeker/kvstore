package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"kvstore/internal/api"
	"kvstore/internal/config"
	"kvstore/internal/observability"
	"kvstore/internal/raft"
	"kvstore/internal/store"
	"kvstore/internal/transport"
)

func main() {
	cfg, err := config.Parse()
	if err != nil {
		log.Fatal(err)
	}

	kv := store.New()
	logger := observability.NewLogger()
	metrics := observability.NewMetrics()
	nodeDataDir := filepath.Join(cfg.DataDir, cfg.ID)
	snapshotPath := filepath.Join(nodeDataDir, "kv-snapshot.json")
	snapshot, err := store.LoadSnapshot(snapshotPath)
	if err != nil {
		log.Fatal(err)
	}
	kv.Restore(snapshot)

	peers := make([]raft.Peer, 0, len(cfg.Peers))
	apiByPeer := map[string]string{cfg.ID: "http://" + cfg.APIAddr}
	for _, peer := range cfg.Peers {
		peers = append(peers, raft.Peer{ID: peer.ID, RaftURL: peer.RaftURL})
		apiByPeer[peer.ID] = inferAPIURL(peer.RaftURL)
	}

	httpClient, err := raftHTTPClient(cfg)
	if err != nil {
		log.Fatal(err)
	}
	faults := transport.NewFaultNetwork(transport.FaultConfig{
		MinDelay:  cfg.FaultMinDelay,
		MaxDelay:  cfg.FaultMaxDelay,
		DropRate:  cfg.FaultDropRate,
		Partition: expandFaultPartitions(cfg.FaultPartitions, peers),
	})
	if err := faults.Validate(); err != nil {
		log.Fatal(err)
	}
	httpClient.Transport = transport.FaultRoundTripper{Base: httpClient.Transport, Network: faults, From: cfg.ID}

	node, err := raft.New(raft.Config{
		ID:              cfg.ID,
		Peers:           peers,
		DataDir:         nodeDataDir,
		ElectionMin:     cfg.ElectionMin,
		ElectionMax:     cfg.ElectionMax,
		Heartbeat:       cfg.Heartbeat,
		RequestTimeout:  cfg.RequestTimeout,
		HTTPClient:      httpClient,
		Metrics:         metrics,
		Logger:          logger,
		LogSegmentBytes: cfg.LogSegmentBytes,
		WALFsyncPolicy:  cfg.WALFsyncPolicy,
	}, kv)
	if err != nil {
		log.Fatal(err)
	}
	node.Start()
	defer node.Stop()

	raftMux := http.NewServeMux()
	node.Register(raftMux)
	raftServer := &http.Server{Addr: cfg.RaftAddr, Handler: raftMux}
	go serve("raft", raftServer, cfg.RaftTLSCert, cfg.RaftTLSKey, logger)

	apiMux := http.NewServeMux()
	apiServer := api.New(node, kv, func(id string) string { return apiByPeer[id] }, cfg.RequestTimeout)
	apiServer.Register(apiMux)
	apiMux.Handle("/metrics", metrics.Handler())
	handler := api.WithTokenAuth(apiMux, cfg.AuthToken)
	handler = api.WithHMAC(handler, cfg.SigningSecret)
	handler = api.WithRateLimit(handler, cfg.RateLimitPerSec, metrics)
	handler = api.WithAudit(handler, logger, metrics)
	handler = api.WithMetrics(handler, metrics)
	handler = api.WithLogging(handler, logger)
	handler = api.WithCorrelation(handler)
	clientServer := &http.Server{Addr: cfg.APIAddr, Handler: handler}
	go serve("api", clientServer, "", "", logger)

	stopSnapshots := startPeriodicSnapshots(cfg.SnapshotEvery, cfg.SnapshotMaxLog, snapshotPath, kv, node, logger)
	defer stopSnapshots()

	logger.Info("node_started", map[string]any{"node": cfg.ID, "api": "http://" + cfg.APIAddr, "raft": raftScheme(cfg) + "://" + cfg.RaftAddr, "data": nodeDataDir})
	waitForSignal()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := store.SaveSnapshot(snapshotPath, kv.Snapshot()); err != nil {
		log.Printf("save snapshot: %v", err)
	}
	_ = clientServer.Shutdown(ctx)
	_ = raftServer.Shutdown(ctx)
}

func serve(name string, srv *http.Server, cert string, key string, logger *observability.Logger) {
	var err error
	if cert != "" || key != "" {
		err = srv.ListenAndServeTLS(cert, key)
	} else {
		err = srv.ListenAndServe()
	}
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		if logger != nil {
			logger.Error("server_failed", err, map[string]any{"server": name})
		}
		log.Fatalf("%s server: %v", name, err)
	}
}

func waitForSignal() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
}

func inferAPIURL(raftURL string) string {
	u, err := url.Parse(raftURL)
	if err != nil {
		return ""
	}
	host, portRaw, err := net.SplitHostPort(u.Host)
	if err != nil {
		return ""
	}
	port, err := strconv.Atoi(portRaw)
	if err != nil || port < 1000 {
		return ""
	}
	u.Scheme = "http"
	u.Host = net.JoinHostPort(host, strconv.Itoa(port-1000))
	return u.String()
}

func raftHTTPClient(cfg config.Config) (*http.Client, error) {
	tr := &http.Transport{}
	if cfg.RaftTLSCA != "" || cfg.RaftTLSCert != "" || cfg.RaftTLSKey != "" {
		tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
		if cfg.RaftTLSCA != "" {
			pem, err := os.ReadFile(cfg.RaftTLSCA)
			if err != nil {
				return nil, err
			}
			roots := x509.NewCertPool()
			if !roots.AppendCertsFromPEM(pem) {
				return nil, errors.New("invalid raft TLS CA")
			}
			tlsConfig.RootCAs = roots
		}
		if cfg.RaftTLSCert != "" || cfg.RaftTLSKey != "" {
			cert, err := tls.LoadX509KeyPair(cfg.RaftTLSCert, cfg.RaftTLSKey)
			if err != nil {
				return nil, err
			}
			tlsConfig.Certificates = []tls.Certificate{cert}
		}
		tr.TLSClientConfig = tlsConfig
	}
	return &http.Client{Transport: tr}, nil
}

func startPeriodicSnapshots(every time.Duration, maxLogEntries int64, path string, kv *store.KV, node *raft.Node, logger *observability.Logger) func() {
	if every <= 0 && maxLogEntries <= 0 {
		return func() {}
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		interval := every
		if interval <= 0 {
			interval = time.Second
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if every > 0 || node.ShouldSnapshot(maxLogEntries) {
					if err := store.SaveSnapshot(path, kv.Snapshot()); err != nil && logger != nil {
						logger.Error("snapshot_failed", err, map[string]any{"path": path})
					} else if node != nil {
						if err := node.CompactAppliedLog(); err != nil && logger != nil {
							logger.Error("log_compaction_failed", err, map[string]any{"path": path})
						}
					}
				}
			case <-stop:
				return
			}
		}
	}()
	return func() {
		close(stop)
		<-done
	}
}

func raftScheme(cfg config.Config) string {
	if cfg.RaftTLSCert != "" || cfg.RaftTLSKey != "" {
		return "https"
	}
	return "http"
}

func expandFaultPartitions(raw string, peers []raft.Peer) map[string]bool {
	parsed := transport.ParsePartitions(raw)
	hosts := map[string]string{}
	for _, peer := range peers {
		u, err := url.Parse(peer.RaftURL)
		if err == nil {
			hosts[peer.ID] = u.Host
		}
	}
	for key := range parsed {
		from, to, ok := strings.Cut(key, "->")
		if !ok {
			continue
		}
		if host, exists := hosts[to]; exists {
			parsed[from+"->"+host] = true
		}
	}
	return parsed
}
