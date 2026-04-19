Distributed Key-Value Store

A small Go implementation of a replicated key-value store built around the Raft consensus algorithm. The project is meant to be easy to run locally inspect and extend.

The server exposes a simple HTTP API for client reads and writes. Internally each node runs Raft leader election log replication commit tracking and durable state storage.

What is included

- HTTP API for `GET` `PUT` and `DELETE` operations on keys
- Raft roles: follower candidate and leader
- Leader election with randomized election timeouts
- AppendEntries and RequestVote RPC handlers over HTTP
- Log replication across peers
- Persistent Raft term vote commit index and log entries
- Snapshot save and restore for key-value data
- Tests for voting log mismatch handling single-node recovery replication and re-election
- Repair scripts for setup diagnostics local runs smoke tests and cleanup

Project layout

text
cmd/server/              Server entrypoint
internal/api/            Client HTTP API
internal/config/         CLI flag parsing and node configuration
internal/raft/           Raft implementation HTTP RPCs persistence tests
internal/store/          In-memory key-value store and snapshots
internal/transport/      Test network helper for delay drop and partition behavior
scripts/bench.go         Simple write benchmark client
repair/                  Diagnostics repair run smoke test and cleanup scripts


Requirements

- Go 1.22 or newer
- A shell that can run POSIX `sh` scripts
- Local loopback networking enabled for multi-node tests and local clusters

Check your Go version:

sh
go version


First-time setup

This repository uses the module path already referenced by the source code:

text
your-project-name


If `go.mod` is missing run:

sh
./repair/repair.sh fix


That command creates the module file formats the source builds the server and runs tests.

Run one node

sh
./repair/repair.sh start-single


The single node listens on:

text
API:  http://127.0.0.1:8080
Raft: http://127.0.0.1:9000


Check status:

sh
curl -s http://127.0.0.1:8080/status


Write a key:

sh
curl -s -X PUT http://127.0.0.1:8080/hello \
  -H Content-Type: application/json \
  -d {value:world}


Read it back:

sh
curl -s http://127.0.0.1:8080/hello


Delete it:

sh
curl -s -X DELETE http://127.0.0.1:8080/hello


Run a three-node cluster

sh
./repair/repair.sh start-cluster


The cluster uses these local ports:

text
n1 API:  http://127.0.0.1:8001
n2 API:  http://127.0.0.1:8002
n3 API:  http://127.0.0.1:8003

n1 Raft: http://127.0.0.1:9001
n2 Raft: http://127.0.0.1:9002
n3 Raft: http://127.0.0.1:9003


Wait a few seconds for leader election then run:

sh
./repair/repair.sh smoke


The smoke test finds a reachable API endpoint writes `repair_check=ok` and reads it back.

Manual server command

You can also run a node directly:

sh
go run ./cmd/server \
  -id n1 \
  -api 127.0.0.1:8080 \
  -raft 127.0.0.1:9000 \
  -data data


For a cluster node pass peers as `id=url` pairs:

sh
go run ./cmd/server \
  -id n1 \
  -api 127.0.0.1:8001 \
  -raft 127.0.0.1:9001 \
  -data data \
  -peers n2=http://127.0.0.1:9002n3=http://127.0.0.1:9003


API

`GET /status`

Returns the node role current term and known leader ID.

Example response:

json
{
  leader_id: n1
  role: leader
  term: 1
}


`PUT /{key}`

Commits a key-value write through Raft.

Request:

json
{
  value: world
}


Response:

json
{
  status: committed
}


`GET /{key}`

Reads a key after first proposing a no-op command so the leader serves a read after confirming it is still active in the current term.

Response:

json
{
  key: hello
  value: world
}


`DELETE /{key}`

Commits a delete through Raft.

Response:

json
{
  status: committed
}


Tests

Run all tests:

sh
go test ./...


Some tests open local TCP listeners for cluster behavior. If you see this error:

text
socket: operation not permitted


the environment is blocking loopback sockets. Run the tests from a normal terminal with local networking enabled.

Benchmark

Start a node or cluster first then run:

sh
go run ./scripts/bench.go -url http://127.0.0.1:8080 -clients 16 -requests 1000


For the three-node repair cluster point the benchmark at the current leader API. If you do not know the leader yet check:

sh
curl -s http://127.0.0.1:8001/status
curl -s http://127.0.0.1:8002/status
curl -s http://127.0.0.1:8003/status


Repair commands

The `repair/` folder exists so a user can recover from common setup and runtime errors without editing files by hand.

sh
./repair/repair.sh help
./repair/repair.sh diagnose
./repair/repair.sh fix
./repair/repair.sh start-single
./repair/repair.sh start-cluster
./repair/repair.sh smoke
./repair/repair.sh stop
./repair/repair.sh reset


Command summary:

- `diagnose` prints environment module port build and test status
- `fix` creates missing module metadata formats builds and tests
- `start-single` starts a one-node server
- `start-cluster` starts three local nodes
- `smoke` writes and reads a test key through the API
- `stop` stops repair-started nodes
- `reset` removes generated repair runtime data and logs

Generated repair files live under:

text
repair/runtime/


Data on disk

Each node stores files under its node data directory:

text
raft-state.json
raft-log.jsonl
kv-snapshot.json


The Raft log is JSON lines. The snapshot stores the key-value map as JSON.

Notes

This is a compact implementation intended for learning and local experiments. It covers the core Raft flow and the next step toward packaging it as a production database would be snapshot compaction stronger read leases membership changes metrics and more failure-injection tests.
