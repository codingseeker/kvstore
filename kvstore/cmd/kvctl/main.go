package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	baseURL := flag.String("url", envDefault("KVCTL_URL", "http://127.0.0.1:8080"), "server API URL")
	token := flag.String("token", envDefault("KV_AUTH_TOKEN", ""), "API token")
	timeout := flag.Duration("timeout", 5*time.Second, "request timeout")
	signSecret := flag.String("sign-secret", envDefault("KV_SIGNING_SECRET", ""), "HMAC signing secret")
	rateLimit := flag.Int("rate-limit", envDefaultInt("KV_RATE_LIMIT_PER_SEC", 0), "rate limit per second")
	verbose := flag.Bool("v", false, "verbose output")
	flag.Parse()

	if *verbose {
		fmt.Fprintf(os.Stderr, "kvctl: url=%s token=%t sign-secret=%t rate-limit=%d\n", *baseURL, *token != "", *signSecret != "", *rateLimit)
	}

	args := flag.Args()
	if len(args) < 1 {
		usage()
		os.Exit(2)
	}

	client := &http.Client{Timeout: *timeout}
	cfg := requestConfig{
		baseURL:    *baseURL,
		token:      *token,
		signSecret: *signSecret,
		rateLimit:  *rateLimit,
		verbose:    *verbose,
	}

	var err error
	switch args[0] {
	case "status":
		err = get(client, cfg, "/status")
	case "health":
		err = get(client, cfg, "/health")
	case "metrics":
		err = get(client, cfg, "/metrics")
	case "get":
		if len(args) < 2 {
			usage()
			os.Exit(2)
		}
		url := cfg.baseURL + "/" + args[1]
		if len(args) > 2 {
			url += "?consistency=" + args[2]
		}
		_ = strings.TrimRight(url, "/")
		err = get(client, cfg, "/"+args[1], withConsistency(argN(2, args)))
	case "put":
		if len(args) < 3 {
			usage()
			os.Exit(2)
		}
		err = put(client, cfg, args[1], args[2])
	case "delete":
		if len(args) < 2 {
			usage()
			os.Exit(2)
		}
		err = delete(client, cfg, args[1])
	case "list":
		err = listKeys(client, cfg)
	case "addnodes":
		err = addNodes(client, cfg, args[1:])
	case "removenode":
		if len(args) < 2 {
			usage()
			os.Exit(2)
		}
		err = removeNode(client, cfg, args[1])
	case "cluster":
		err = clusterInfo(client, cfg)
	case "replication":
		err = replicationStatus(client, cfg)
	case "compact":
		err = compactLogs(client, cfg)
	case "snapshot":
		err = triggerSnapshot(client, cfg)
	case "version":
		fmt.Println("kvctl version 2.0.0")
		return
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		os.Exit(1)
	}
}

type requestConfig struct {
	baseURL    string
	token      string
	signSecret string
	rateLimit  int
	verbose    bool
}

type requestOption func(*http.Request)

func withConsistency(c string) requestOption {
	return func(req *http.Request) {
		if c != "" {
			q := req.URL.Query()
			q.Set("consistency", c)
			req.URL.RawQuery = q.Encode()
		}
	}
}

func argN(n int, args []string) string {
	if n < len(args) {
		return args[n]
	}
	return ""
}

func get(client *http.Client, cfg requestConfig, path string, _ ...requestOption) error {
	url := strings.TrimRight(cfg.baseURL, "/") + path
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	applyOptions(req, cfg)
	return execute(client, req, cfg.verbose)
}

func put(client *http.Client, cfg requestConfig, key, value string) error {
	body, err := json.Marshal(map[string]string{"value": value})
	if err != nil {
		return err
	}
	url := strings.TrimRight(cfg.baseURL, "/") + "/" + key
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	return execute(client, req, cfg.verbose)
}

func delete(client *http.Client, cfg requestConfig, key string) error {
	url := strings.TrimRight(cfg.baseURL, "/") + "/" + key
	req, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		return err
	}
	return execute(client, req, cfg.verbose)
}

func listKeys(client *http.Client, cfg requestConfig) error {
	type listResponse struct {
		Keys []string `json:"keys"`
	}
	resp := &listResponse{}
	url := cfg.baseURL + "/keys"
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	applyOptions(req, cfg)
	if err := doRequest(client, req, resp); err != nil {
		return err
	}
	if len(resp.Keys) == 0 {
		fmt.Println("No keys found")
		return nil
	}
	for _, k := range resp.Keys {
		fmt.Println(k)
	}
	return nil
}

func addNodes(client *http.Client, cfg requestConfig, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("addnodes requires node definition(s) in format id=url")
	}
	type addNodeRequest struct {
		Nodes []struct {
			ID      string `json:"id"`
			RaftURL string `json:"raft_url"`
		} `json:"nodes"`
	}
	reqBody := addNodeRequest{}
	for _, arg := range args {
		parts := strings.Split(arg, "=")
		if len(parts) != 2 {
			continue
		}
		reqBody.Nodes = append(reqBody.Nodes, struct {
			ID      string `json:"id"`
			RaftURL string `json:"raft_url"`
		}{parts[0], parts[1]})
	}
	if len(reqBody.Nodes) == 0 {
		return fmt.Errorf("no valid nodes provided")
	}
	body, err := json.Marshal(reqBody)
	if err != nil {
		return err
	}
	url := cfg.baseURL + "/cluster/add-nodes"
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	return execute(client, req, cfg.verbose)
}

func removeNode(client *http.Client, cfg requestConfig, nodeID string) error {
	url := cfg.baseURL + "/cluster/remove-node/" + nodeID
	req, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		return err
	}
	return execute(client, req, cfg.verbose)
}

func clusterInfo(client *http.Client, cfg requestConfig) error {
	return get(client, cfg, "/cluster")
}

func replicationStatus(client *http.Client, cfg requestConfig) error {
	type repStatus struct {
		Leader string           `json:"leader_id"`
		Nodes  map[string]int64 `json:"replication_lag"`
		Quorum map[string]any   `json:"quorum"`
	}
	resp := &repStatus{}
	url := cfg.baseURL + "/replication"
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	applyOptions(req, cfg)
	if err := doRequest(client, req, resp); err != nil {
		return err
	}
	fmt.Printf("Leader: %s\n", resp.Leader)
	fmt.Printf("Quorum: %v\n", resp.Quorum)
	fmt.Println("Replication Lag:")
	for node, lag := range resp.Nodes {
		fmt.Printf("  %s: %d\n", node, lag)
	}
	return nil
}

func compactLogs(client *http.Client, cfg requestConfig) error {
	return postEmpty(client, cfg, "/admin/compact")
}

func triggerSnapshot(client *http.Client, cfg requestConfig) error {
	return postEmpty(client, cfg, "/admin/snapshot")
}

func postEmpty(client *http.Client, cfg requestConfig, path string) error {
	url := cfg.baseURL + path
	req, err := http.NewRequest(http.MethodPost, url, nil)
	if err != nil {
		return err
	}
	return execute(client, req, cfg.verbose)
}

func applyOptions(req *http.Request, cfg requestConfig) {
	if cfg.token != "" {
		req.Header.Set("Authorization", "Bearer "+cfg.token)
	}
}

func doRequest(client *http.Client, req *http.Request, resp interface{}) error {
	if req.Header.Get("Authorization") == "" && os.Getenv("KVCTL_TOKEN") != "" {
		req.Header.Set("Authorization", "Bearer "+os.Getenv("KVCTL_TOKEN"))
	}
	httpResp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = httpResp.Body.Close() }()
	if httpResp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(httpResp.Body)
		return fmt.Errorf("request failed: %s %s", httpResp.Status, string(body))
	}
	if resp == nil {
		return nil
	}
	return json.NewDecoder(httpResp.Body).Decode(resp)
}

func execute(client *http.Client, req *http.Request, verbose bool) error {
	if verbose {
		fmt.Fprintf(os.Stderr, "=> %s %s\n", req.Method, req.URL)
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if verbose {
		fmt.Fprintf(os.Stderr, "<= %d\n", resp.StatusCode)
	}
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("request failed: %s %s", resp.Status, string(body))
	}
	fmt.Print(string(body))
	return nil
}

func envDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envDefaultInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return fallback
}

func usage() {
	fmt.Fprintln(os.Stderr, `kvctl - KV Store CLI Client v2.0

Usage: kvctl [options] command [arguments]

Commands:
  status                 Show node status (role, term, leader)
  health                Show health info (role, leader, quorum, replication lag)
  metrics                Show metrics (requests, latencies, leader changes)
  get KEY [strong|eventual]  Get value with optional consistency
  put KEY VALUE          Set key to value
  delete KEY             Delete a key
  list                   List all keys
  addnodes ID=URL [...]  Add nodes to cluster
  removenode ID          Remove node from cluster
  cluster                Show cluster info
  replication            Show replication status
  compact                Trigger log compaction
  snapshot               Trigger KV snapshot
  version                Show version

Options:
  -url URL               Server URL (default: http://127.0.0.1:8080)
  -token TOKEN           API token
  -sign-secret SECRET    HMAC signing secret
  -rate-limit N          Rate limit per second
  -v                     Verbose output
  -timeout DURATION      Request timeout (default: 5s)

Environment Variables:
  KVCTL_URL, KVCTL_TOKEN, KV_AUTH_TOKEN, KV_SIGNING_SECRET, KV_RATE_LIMIT_PER_SEC`)
}
