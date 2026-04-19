#!/usr/bin/env sh
set -eu

try_api() {
  base="$1"
  if curl -fsS "$base/status" >/tmp/kv-repair-status.json 2>/dev/null; then
    echo "$base"
    return 0
  fi
  return 1
}

BASE=""
for candidate in \
  http://127.0.0.1:8080 \
  http://127.0.0.1:8001 \
  http://127.0.0.1:8002 \
  http://127.0.0.1:8003
do
  if BASE="$(try_api "$candidate")"; then
    break
  fi
done

if [ -z "$BASE" ]; then
  echo "no reachable API found on 8080, 8001, 8002, or 8003"
  echo "start a node with ./repair/run-single-node.sh or ./repair/run-three-node.sh"
  exit 1
fi

echo "using $BASE"
echo "status:"
curl -fsS "$BASE/status"
echo

echo "writing repair_check=ok"
curl -fsS -L -X PUT "$BASE/repair_check" \
  -H 'Content-Type: application/json' \
  -d '{"value":"ok"}'
echo

echo "reading repair_check"
curl -fsS -L "$BASE/repair_check"
echo

