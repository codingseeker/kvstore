#!/usr/bin/env sh
set -eu

ROOT="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
cd "$ROOT"

PID_DIR="repair/runtime/pids"
if [ ! -d "$PID_DIR" ]; then
  echo "no repair pids found"
  exit 0
fi

for pidfile in "$PID_DIR"/*.pid; do
  [ -e "$pidfile" ] || continue
  pid="$(cat "$pidfile")"
  if kill -0 "$pid" >/dev/null 2>&1; then
    echo "stopping $pid from $pidfile"
    kill "$pid" >/dev/null 2>&1 || true
  else
    echo "not running: $pid from $pidfile"
  fi
  rm -f "$pidfile"
done

