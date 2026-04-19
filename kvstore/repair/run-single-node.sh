#!/usr/bin/env sh
set -eu

ROOT="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
cd "$ROOT"

if [ ! -f go.mod ]; then
  ./repair/repair.sh
fi

mkdir -p repair/runtime/data repair/runtime/logs repair/runtime/pids

echo "starting single node"
go run ./cmd/server \
  -id n1 \
  -api 127.0.0.1:8080 \
  -raft 127.0.0.1:9000 \
  -data repair/runtime/data \
  > repair/runtime/logs/single-node.log 2>&1 &

pid=$!
echo "$pid" > repair/runtime/pids/single-node.pid
echo "pid: $pid"
echo "api: http://127.0.0.1:8080"
echo "log: repair/runtime/logs/single-node.log"
echo "stop: ./repair/stop-repair-nodes.sh"

