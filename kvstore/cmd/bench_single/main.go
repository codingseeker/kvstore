package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	baseURL := flag.String("url", "http://127.0.0.1:8081", "leader API URL")
	clients := flag.Int("clients", 16, "concurrent clients")
	requests := flag.Int("requests", 1000, "total requests")
	token := flag.String("token", "", "API token")
	consistency := flag.String("consistency", "strong", "read consistency: strong or eventual")
	mode := flag.String("mode", "write", "benchmark mode: write, read, mixed")
	output := flag.String("output", "text", "output format: text or json")
	faultLabel := flag.String("fault-label", "", "fault injection label for structured output")
	flag.Parse()

	jobs := make(chan int)
	latencies := make([]time.Duration, 0, *requests)
	var latMu sync.Mutex
	var ok atomic.Int64
	var failed atomic.Int64
	start := time.Now()

	var wg sync.WaitGroup
	for i := 0; i < *clients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client := &http.Client{Timeout: 5 * time.Second}
			for id := range jobs {
				req := buildRequest(*baseURL, *mode, *consistency, id)
				if *token != "" {
					req.Header.Set("Authorization", "Bearer "+*token)
				}
				t0 := time.Now()
				resp, err := client.Do(req)
				elapsed := time.Since(t0)
				if err == nil && resp.StatusCode/100 == 2 {
					ok.Add(1)
				} else {
					failed.Add(1)
				}
				if resp != nil {
					_, _ = io.Copy(io.Discard, resp.Body)
					_ = resp.Body.Close()
				}
				latMu.Lock()
				latencies = append(latencies, elapsed)
				latMu.Unlock()
			}
		}()
	}

	for i := 0; i < *requests; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	total := time.Since(start)

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	result := map[string]any{
		"requests":                  *requests,
		"success":                   ok.Load(),
		"failed":                    failed.Load(),
		"clients":                   *clients,
		"duration_ms":               total.Milliseconds(),
		"throughput_per_second":     float64(ok.Load()) / total.Seconds(),
		"mode":                      *mode,
		"consistency":               *consistency,
		"fault_label":               *faultLabel,
		"latency_p50_ms":            percentile(latencies, 0.50).Milliseconds(),
		"latency_p95_ms":            percentile(latencies, 0.95).Milliseconds(),
		"latency_p99_ms":            percentile(latencies, 0.99).Milliseconds(),
		"degradation_success_ratio": successRatio(ok.Load(), *requests),
	}
	if *output == "json" {
		b, _ := json.MarshalIndent(result, "", "  ")
		fmt.Println(string(b))
		return
	}
	fmt.Printf("requests=%d success=%d failed=%d clients=%d duration=%s throughput=%.2f req/s mode=%s consistency=%s fault=%s\n", *requests, ok.Load(), failed.Load(), *clients, total, float64(ok.Load())/total.Seconds(), *mode, *consistency, *faultLabel)
	fmt.Printf("latency p50=%s p95=%s p99=%s success_ratio=%.4f\n", percentile(latencies, 0.50), percentile(latencies, 0.95), percentile(latencies, 0.99), successRatio(ok.Load(), *requests))
}

func buildRequest(baseURL string, mode string, consistency string, id int) *http.Request {
	if mode == "read" || (mode == "mixed" && id%2 == 1) {
		req, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/key-%d?consistency=%s", baseURL, id, consistency), nil)
		return req
	}
	body := []byte(fmt.Sprintf(`{"value":"value-%d"}`, id))
	req, _ := http.NewRequest(http.MethodPut, fmt.Sprintf("%s/key-%d", baseURL, id), bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	return req
}

func percentile(values []time.Duration, p float64) time.Duration {
	if len(values) == 0 {
		return 0
	}
	idx := int(float64(len(values)-1) * p)
	return values[idx]
}

func successRatio(success int64, total int) float64 {
	if total == 0 {
		return 0
	}
	return float64(success) / float64(total)
}
