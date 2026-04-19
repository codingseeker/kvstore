#!/usr/bin/env sh
set -eu

ROOT="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
cd "$ROOT"

usage() {
  cat <<'EOF'
Usage: ./repair/repair.sh [command]

Commands:
  help           Show this help.
  diagnose       Print environment, module, port, build, and test status.
  fix            Create missing module file, format, build, and test.
  start-single   Start one local node on API 8080 and Raft 9000.
  start-cluster  Start three local nodes on API 8001-8003 and Raft 9001-9003.
  smoke          Write and read a repair_check key through a reachable API.
  stop           Stop nodes started by repair scripts.
  reset          Stop repair nodes and remove repair/runtime data and logs.

No command is the same as: fix
EOF
}

command="${1:-fix}"
case "$command" in
  help|-h|--help)
    usage
    exit 0
    ;;
  diagnose)
    exec ./repair/diagnose.sh
    ;;
  start-single)
    exec ./repair/run-single-node.sh
    ;;
  start-cluster)
    exec ./repair/run-three-node.sh
    ;;
  smoke)
    exec ./repair/smoke-test.sh
    ;;
  stop)
    exec ./repair/stop-repair-nodes.sh
    ;;
  reset)
    exec ./repair/reset-data.sh
    ;;
  fix)
    ;;
  *)
    echo "unknown repair command: $command"
    echo
    usage
    exit 2
    ;;
esac

if ! command -v go >/dev/null 2>&1; then
  echo "go: not found"
  echo "Install Go 1.22 or newer, then rerun this script."
  exit 1
fi

if [ ! -f go.mod ]; then
  cat > go.mod <<'EOF'
module your-project-name

go 1.22
EOF
  echo "created go.mod"
fi

echo "formatting source"
gofmt -w cmd internal scripts

echo "building server"
go build ./cmd/server

echo "running tests"
if go test ./...; then
  echo "repair complete"
else
  status=$?
  echo
  echo "tests failed"
  echo "If the error says 'socket: operation not permitted', local TCP is blocked."
  echo "Run ./repair/diagnose.sh in an unrestricted terminal for a full check."
  exit "$status"
fi
