# Repair Commands

All commands are run from the project root.

```sh
./repair/repair.sh help
```

Shows all repair commands.

```sh
./repair/repair.sh diagnose
```

Checks Go installation, module status, required files, common local ports,
build status, and tests.

```sh
./repair/repair.sh fix
```

Creates `go.mod` if missing, formats Go files, builds the server, and runs
tests.

```sh
./repair/repair.sh start-single
```

Starts a single-node server at `http://127.0.0.1:8080`.

```sh
./repair/repair.sh start-cluster
```

Starts a three-node local cluster with APIs at `http://127.0.0.1:8001`,
`http://127.0.0.1:8002`, and `http://127.0.0.1:8003`.

```sh
./repair/repair.sh smoke
```

Finds a running API, writes `repair_check=ok`, and reads it back.

```sh
./repair/repair.sh stop
```

Stops nodes started by the repair scripts.

```sh
./repair/repair.sh reset
```

Stops repair nodes and removes generated data, logs, and PID files under
`repair/runtime/`.
