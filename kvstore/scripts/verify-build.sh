#!/bin/bash
set -e

echo "=== Go Module Verification ==="
echo ""

cd "$(dirname "$0")"

echo "[1/5] Checking Go version..."
go version

echo ""
echo "[2/5] Running go mod tidy..."
go mod tidy

echo ""
echo "[3/5] Verifying module integrity..."
go mod verify

echo ""
echo "[4/5] Building all targets..."
go build -o /dev/null ./cmd/server
go build -o /dev/null ./cmd/kvctl
go build -o /dev/null ./cmd/bench_single
go build -o /dev/null ./cmd/bench_suite

echo ""
echo "[5/5] Running tests..."
go test -count=1 -timeout 60s ./...

echo ""
echo "=== All Checks Passed ==="
echo "Module: $(go list -m)"
echo "Dependencies: $(go list -m all | wc -l) packages"