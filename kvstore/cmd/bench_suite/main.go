package main

import (
	"bytes"
	"context"
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

var (
	baseURL  string
	clients  int
	requests int
	token    string
	output   string
)

func main() {
	flag.StringVar(&baseURL, "url", "http://127.0.0.1:8081", "leader API URL")
	flag.IntVar(&clients, "clients", 16, "concurrent clients")
	flag.IntVar(&requests, "requests", 1000, "total requests per benchmark")
	flag.StringVar(&token, "token", "", "API token")
	flag.StringVar(&output, "output", "text", "output format: text or json")
	flag.Parse()

	fmt.Println("=== KV Store Benchmark Suite ===")
	fmt.Println()

	results := runAllBenchmarks()

	if output == "json" {
		b, _ := json.MarshalIndent(results, "", "  ")
		fmt.Println(string(b))
	} else {
		printResults(results)
	}
}

type BenchmarkResult struct {
	Label        string  `json:"label"`
	Requests     int     `json:"requests"`
	Success      int64   `json:"success"`
	Failed       int64   `json:"failed"`
	Clients      int     `json:"clients"`
	DurationMs   int64   `json:"duration_ms"`
	Throughput   float64 `json:"throughput_per_second"`
	LatencyP50Ms int64   `json:"latency_p50_ms"`
	LatencyP95Ms int64   `json:"latency_p95_ms"`
	LatencyP99Ms int64   `json:"latency_p99_ms"`
	SuccessRatio float64 `json:"success_ratio"`
}

type Results struct {
	StrongWrites  BenchmarkResult `json:"strong_writes"`
	StrongReads   BenchmarkResult `json:"strong_reads"`
	EventualReads BenchmarkResult `json:"eventual_reads"`
	MixedOps      BenchmarkResult `json:"mixed_operations"`
	Summary       Summary         `json:"summary"`
}

type Summary struct {
	MaxThroughput         float64  `json:"max_throughput_rps"`
	StrongReadLatencyMs   int64    `json:"strong_read_latency_p50_ms"`
	EventualReadLatencyMs int64    `json:"eventual_read_latency_p50_ms"`
	LatencyImprovement    float64  `json:"eventual_vs_strong_improvement_ratio"`
	Conclusions           []string `json:"conclusions"`
}

func runAllBenchmarks() Results {
	ctx := context.Background()

	fmt.Println("Running: strong consistency writes...")
	strongWrites := runBenchmark(ctx, "strong_write", "write", "strong")

	fmt.Println("Running: strong consistency reads...")
	strongReads := runBenchmark(ctx, "strong_read", "read", "strong")

	fmt.Println("Running: eventual consistency reads...")
	eventualReads := runBenchmark(ctx, "eventual_read", "read", "eventual")

	fmt.Println("Running: mixed read/write (50/50)...")
	mixedOps := runBenchmark(ctx, "mixed", "mixed", "strong")

	results := Results{
		StrongWrites:  strongWrites,
		StrongReads:   strongReads,
		EventualReads: eventualReads,
		MixedOps:      mixedOps,
	}

	results.Summary = generateSummary(results)
	return results
}

func runBenchmark(_ context.Context, label, opMode, consistency string) BenchmarkResult {
	jobs := make(chan int, requests)
	latencies := make([]time.Duration, 0, requests)
	var latMu sync.Mutex
	var ok, failed atomic.Int64

	start := time.Now()

	var wg sync.WaitGroup
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client := &http.Client{Timeout: 10 * time.Second}
			for id := range jobs {
				req := buildRequest(baseURL, opMode, consistency, id)
				if token != "" {
					req.Header.Set("Authorization", "Bearer "+token)
				}
				t0 := time.Now()
				resp, err := client.Do(req)
				elapsed := time.Since(t0)
				if err == nil && resp != nil && resp.StatusCode/100 == 2 {
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

	for i := 0; i < requests; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	total := time.Since(start)

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	return BenchmarkResult{
		Label:        label,
		Requests:     requests,
		Success:      ok.Load(),
		Failed:       failed.Load(),
		Clients:      clients,
		DurationMs:   total.Milliseconds(),
		Throughput:   float64(ok.Load()) / total.Seconds(),
		LatencyP50Ms: percentile(latencies, 0.50).Milliseconds(),
		LatencyP95Ms: percentile(latencies, 0.95).Milliseconds(),
		LatencyP99Ms: percentile(latencies, 0.99).Milliseconds(),
		SuccessRatio: successRatio(ok.Load(), requests),
	}
}

func buildRequest(url, opMode, consistency string, id int) *http.Request {
	if opMode == "read" || opMode == "mixed" {
		req, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/key-%d?consistency=%s", url, id, consistency), nil)
		return req
	}
	body := []byte(fmt.Sprintf(`{"value":"value-%d"}`, id))
	req, _ := http.NewRequest(http.MethodPut, fmt.Sprintf("%s/key-%d", url, id), bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	return req
}

func percentile(values []time.Duration, p float64) time.Duration {
	if len(values) == 0 {
		return 0
	}
	idx := int(float64(len(values)-1) * p)
	if idx >= len(values) {
		idx = len(values) - 1
	}
	return values[idx]
}

func successRatio(success int64, total int) float64 {
	if total == 0 {
		return 0
	}
	return float64(success) / float64(total)
}

func generateSummary(r Results) Summary {
	var maxThroughput float64
	if r.StrongWrites.Throughput > maxThroughput {
		maxThroughput = r.StrongWrites.Throughput
	}

	var improvement float64 = 1.0
	if r.StrongReads.LatencyP50Ms > 0 {
		improvement = float64(r.StrongReads.LatencyP50Ms) / float64(r.EventualReads.LatencyP50Ms)
	}

	conclusions := []string{}
	if r.EventualReads.LatencyP50Ms < r.StrongReads.LatencyP50Ms {
		conclusions = append(conclusions, fmt.Sprintf("Eventual reads are %.1fx faster than strong reads", improvement))
	} else {
		conclusions = append(conclusions, "Eventual reads have similar latency to strong reads")
	}
	if r.StrongWrites.SuccessRatio > 0.99 {
		conclusions = append(conclusions, "Write operations have >99% success rate")
	}
	conclusions = append(conclusions, fmt.Sprintf("Max throughput: %.2f req/s", maxThroughput))

	return Summary{
		MaxThroughput:         maxThroughput,
		StrongReadLatencyMs:   r.StrongReads.LatencyP50Ms,
		EventualReadLatencyMs: r.EventualReads.LatencyP50Ms,
		LatencyImprovement:    improvement,
		Conclusions:           conclusions,
	}
}

func printResults(r Results) {
	fmt.Println()
	fmt.Println("=== Benchmark Results ===")
	fmt.Println()
	fmt.Printf("Strong Writes:    %7.2f req/s, p50=%4dms, p95=%4dms, p99=%5dms, success=%.2f%%\n",
		r.StrongWrites.Throughput, r.StrongWrites.LatencyP50Ms, r.StrongWrites.LatencyP95Ms, r.StrongWrites.LatencyP99Ms, r.StrongWrites.SuccessRatio*100)
	fmt.Printf("Strong Reads:     %7.2f req/s, p50=%4dms, p95=%4dms, p99=%5dms, success=%.2f%%\n",
		r.StrongReads.Throughput, r.StrongReads.LatencyP50Ms, r.StrongReads.LatencyP95Ms, r.StrongReads.LatencyP99Ms, r.StrongReads.SuccessRatio*100)
	fmt.Printf("Eventual Reads:   %7.2f req/s, p50=%4dms, p95=%4dms, p99=%5dms, success=%.2f%%\n",
		r.EventualReads.Throughput, r.EventualReads.LatencyP50Ms, r.EventualReads.LatencyP95Ms, r.EventualReads.LatencyP99Ms, r.EventualReads.SuccessRatio*100)
	fmt.Printf("Mixed Ops:        %7.2f req/s, p50=%4dms, p95=%4dms, p99=%5dms, success=%.2f%%\n",
		r.MixedOps.Throughput, r.MixedOps.LatencyP50Ms, r.MixedOps.LatencyP95Ms, r.MixedOps.LatencyP99Ms, r.MixedOps.SuccessRatio*100)
	fmt.Println()
	fmt.Println("=== Analysis ===")
	for _, c := range r.Summary.Conclusions {
		fmt.Printf(" - %s\n", c)
	}
}
