package transport

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestSimulatedNetworkOrdersDelaysDropsAndPartitions(t *testing.T) {
	network := NewSimulatedNetwork(time.Unix(0, 0))
	network.Register("n2", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		_, _ = fmt.Fprintf(w, "ack:%s", string(b))
	}))
	client := &http.Client{Transport: SimulatedRoundTripper{Network: network, From: "n1"}}

	network.Delay("n1", "n2", time.Second)
	done := make(chan string, 1)
	go func() {
		resp, err := client.Post("http://n2/raft/append-entries", "text/plain", strings.NewReader("one"))
		if err != nil {
			done <- err.Error()
			return
		}
		b, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		done <- string(b)
	}()
	time.Sleep(20 * time.Millisecond)
	if network.DeliverNext() {
		t.Fatalf("delivered before delay elapsed")
	}
	network.Advance(time.Second)
	if !network.DeliverNext() {
		t.Fatalf("expected ready message")
	}
	if got := <-done; got != "ack:one" {
		t.Fatalf("response = %q", got)
	}

	network.DropNext("n1", "n2")
	_, err := client.Post("http://n2/raft/append-entries", "text/plain", strings.NewReader("two"))
	if err == nil {
		t.Fatalf("expected dropped message")
	}

	network.Partition("n1", "n2")
	_, err = client.Post("http://n2/raft/append-entries", "text/plain", strings.NewReader("three"))
	if err == nil {
		t.Fatalf("expected partitioned message")
	}
	network.Heal("n1", "n2")
}
