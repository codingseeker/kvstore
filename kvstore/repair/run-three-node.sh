#!/usr/bin/env sh
set -eu

ROOT="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
cd "$ROOT"

if [ ! -f go.mod ]; then
  ./repair/repair.sh
fi

mkdir -p repair/runtime/data repair/runtime/logs repair/runtime/pids

start_node() {
  id="$1"
  api="$2"
  raft="$3"
  peers="$4"
  log="repair/runtime/logs/$id.log"
  pidfile="repair/runtime/pids/$id.pid"

  go run ./cmd/server \
    -id "$id" \
    -api "$api" \
    -raft "$raft" \
    -data repair/runtime/data \
    -peers "$peers" \
    > "$log" 2>&1 &

  echo "$!" > "$pidfile"
  echo "$id pid=$(cat "$pidfile") api=http://$api raft=http://$raft log=$log"
}

echo "starting three-node cluster"
start_node n1 127.0.0.1:8001 127.0.0.1:9001 "n2=http://127.0.0.1:9002,n3=http://127.0.0.1:9003"
start_node n2 127.0.0.1:8002 127.0.0.1:9002 "n1=http://127.0.0.1:9001,n3=http://127.0.0.1:9003"
start_node n3 127.0.0.1:8003 127.0.0.1:9003 "n1=http://127.0.0.1:9001,n2=http://127.0.0.1:9002"

echo "wait 2-5 seconds for leader election, then run ./repair/smoke-test.sh"
echo "stop: ./repair/stop-repair-nodes.sh"

