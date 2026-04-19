#!/usr/bin/env sh
set -eu

ROOT="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
cd "$ROOT"

echo "== Project =="
echo "root: $ROOT"
echo

echo "== Go =="
if command -v go >/dev/null 2>&1; then
  go version
else
  echo "go: not found"
  echo "Install Go 1.22 or newer, then rerun this script."
  exit 1
fi
echo

echo "== Module =="
if [ -f go.mod ]; then
  cat go.mod
else
  echo "go.mod: missing"
  echo "Run ./repair/repair.sh to create it."
fi
echo

echo "== Required files =="
for path in cmd/server/main.go internal/api/api.go internal/raft/node.go internal/store/store.go; do
  if [ -f "$path" ]; then
    echo "ok: $path"
  else
    echo "missing: $path"
  fi
done
echo

echo "== Port checks =="
for port in 8080 8001 8002 8003 9000 9001 9002 9003; do
  if command -v lsof >/dev/null 2>&1; then
    if lsof -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
      echo "busy: $port"
    else
      echo "free: $port"
    fi
  elif command -v ss >/dev/null 2>&1; then
    if ss -ltn | grep -q "[.:]$port "; then
      echo "busy: $port"
    else
      echo "free: $port"
    fi
  else
    echo "unknown: $port (install lsof or ss for port checks)"
  fi
done
echo

echo "== Build =="
if go build ./cmd/server; then
  echo "build: ok"
else
  echo "build: failed"
fi
echo

echo "== Tests =="
if go test ./...; then
  echo "tests: ok"
else
  echo "tests: failed"
  echo "If the failure mentions sockets, rerun outside restricted sandboxing."
fi

