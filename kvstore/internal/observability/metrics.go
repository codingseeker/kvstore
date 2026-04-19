package observability

import (
	"encoding/json"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"
)

type Metrics struct {
	mu             sync.Mutex
	started        time.Time
	requests       map[string]int64
	latencies      map[string][]time.Duration
	leaderChanges  int64
	replicationLag map[string]int64
	invariants     map[string]int64
	rateLimited    int64
	auditEvents    int64
	electionTerms  []time.Time
}

func NewMetrics() *Metrics {
	return &Metrics{
		started:        time.Now(),
		requests:       map[string]int64{},
		latencies:      map[string][]time.Duration{},
		replicationLag: map[string]int64{},
		invariants:     map[string]int64{},
	}
}

func (m *Metrics) RecordRequest(method string, path string, status int, elapsed time.Duration) {
	if m == nil {
		return
	}
	key := method + " " + path + " " + strconv.Itoa(status)
	m.mu.Lock()
	m.requests[key]++
	m.latencies[method+" "+path] = append(m.latencies[method+" "+path], elapsed)
	m.mu.Unlock()
}

func (m *Metrics) RecordLeaderChange() {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.leaderChanges++
	m.electionTerms = append(m.electionTerms, time.Now())
	m.mu.Unlock()
}

func (m *Metrics) SetReplicationLag(peer string, lag int64) {
	if m == nil {
		return
	}
	if lag < 0 {
		lag = 0
	}
	m.mu.Lock()
	m.replicationLag[peer] = lag
	m.mu.Unlock()
}

func (m *Metrics) RecordInvariantViolation(name string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.invariants[name]++
	m.mu.Unlock()
}

func (m *Metrics) RecordRateLimited() {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.rateLimited++
	m.mu.Unlock()
}

func (m *Metrics) RecordAuditEvent() {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.auditEvents++
	m.mu.Unlock()
}

func (m *Metrics) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(m.Snapshot())
	})
}

func (m *Metrics) Snapshot() map[string]any {
	if m == nil {
		return map[string]any{}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	requests := map[string]int64{}
	for key, value := range m.requests {
		requests[key] = value
	}
	latencies := map[string]map[string]int64{}
	for key, values := range m.latencies {
		cp := append([]time.Duration(nil), values...)
		sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
		latencies[key] = map[string]int64{
			"count":  int64(len(cp)),
			"p50_ms": percentileMillis(cp, 0.50),
			"p95_ms": percentileMillis(cp, 0.95),
			"p99_ms": percentileMillis(cp, 0.99),
		}
	}
	replicationLag := map[string]int64{}
	for key, value := range m.replicationLag {
		replicationLag[key] = value
	}
	invariants := map[string]int64{}
	for key, value := range m.invariants {
		invariants[key] = value
	}
	electionFrequency := int64(0)
	cutoff := time.Now().Add(-time.Minute)
	for _, at := range m.electionTerms {
		if at.After(cutoff) {
			electionFrequency++
		}
	}
	maxLag := int64(0)
	for _, value := range replicationLag {
		if value > maxLag {
			maxLag = value
		}
	}
	totalRequests := int64(0)
	for _, value := range requests {
		totalRequests += value
	}
	uptime := time.Since(m.started).Seconds()
	requestRate := float64(0)
	if uptime > 0 {
		requestRate = float64(totalRequests) / uptime
	}
	return map[string]any{
		"requests":                requests,
		"request_rate_per_second": requestRate,
		"latencies":               latencies,
		"leader_changes":          m.leaderChanges,
		"leader_elections":        m.leaderChanges,
		"replication_lag":         replicationLag,
		"invariant_violations":    invariants,
		"rate_limited_requests":   m.rateLimited,
		"audit_events":            m.auditEvents,
		"alerts": map[string]any{
			"elections_per_minute": electionFrequency,
			"max_replication_lag":  maxLag,
		},
	}
}

func percentileMillis(values []time.Duration, p float64) int64 {
	if len(values) == 0 {
		return 0
	}
	idx := int(float64(len(values)-1) * p)
	return values[idx].Milliseconds()
}
