#!/usr/bin/env sh
set -eu

ROOT="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
cd "$ROOT"

./repair/stop-repair-nodes.sh

if [ -d repair/runtime ]; then
  rm -rf repair/runtime
  echo "removed repair/runtime"
else
  echo "repair/runtime does not exist"
fi

