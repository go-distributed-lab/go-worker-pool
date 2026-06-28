# Architecture

## Overview

The worker pool is built around four primitives: a buffered job channel, a buffered result channel, a pool of goroutines, and a `sync.WaitGroup` that tracks their lifetime. A `context.Context` threads through every layer so the entire system can be cancelled cleanly from outside.

---

## Component map

```
cmd/workerpool/main.go
        │
        │  loads config, wires components
        ▼
internal/config/config.go       ← env vars → Config struct
        │
        ▼
internal/pool/pool.go           ← owns channels + WaitGroup
        │
        ├── internal/dispatcher/dispatcher.go   ← spawns N workers
        │           │
        │           └── internal/worker/worker.go  ← goroutine, select loop
        │
        ├── chan job.Job     (buffered, size = JOB_BUF_SIZE)
        ├── chan result.Result (buffered, size = RES_BUF_SIZE)
        └── sync.WaitGroup
```

---

## Data flow

```
1. main calls p.Start(ctx)
        │
        ▼
2. Dispatcher spawns N worker goroutines
        │
        ▼
3. main submits Job structs via p.Submit(j)
   → written to chan Job
        │
        ▼
4. A free worker picks up the job from chan Job
   → processes it
   → writes Result to chan Result
        │
        ▼
5. main (or a collector goroutine) reads from p.Results()
        │
        ▼
6. main calls p.Stop()
   → closes chan Job  (workers see channel closed, return)
   → WaitGroup.Wait() (blocks until all goroutines exit)
   → closes chan Result (consumer range loop exits)
```

---

## Package responsibilities

### `internal/job`

Defines the `Job` struct. No logic — pure data. Kept in its own package so `worker`, `dispatcher`, and `pool` can all import it without creating import cycles.

```go
type Job struct {
    ID      int
    Payload any
}
```

### `internal/result`

Defines the `Result` struct. Same rationale as `job` — isolated to prevent cycles.

```go
type Result struct {
    JobID int
    Value any
    Err   error
}
```

### `internal/config`

Reads environment variables and returns a `Config` value. Called once at startup in `main.go` and passed down by value — no global state, easy to test.

```go
type Config struct {
    WorkerCount int  // WORKER_COUNT, default 4
    JobBufSize  int  // JOB_BUF_SIZE,  default 100
    ResBufSize  int  // RES_BUF_SIZE,  default 100
    JobDelayMs  int  // JOB_DELAY_MS,  default 0
}
```

### `internal/worker`

A single `Worker` struct with a `Start(ctx)` method. The method runs a `select` loop:

```
select {
case <-ctx.Done()  → return (cancelled)
case j, ok := <-jobs:
    if !ok → return (channel closed)
    process j
    send result
}
```

No knowledge of how many workers exist or how they were spawned. Pure unit of work.

### `internal/dispatcher`

Owns the logic for spawning workers. Loops from `0` to `cfg.WorkerCount`, creates a `Worker`, calls `wg.Add(1)`, and launches a goroutine. The goroutine defers `wg.Done()` and calls `worker.Start(ctx)`.

Keeping this in its own package means the pool can be tested with a mock dispatcher, and the spawning strategy (fixed pool, elastic pool, etc.) can be swapped without touching `pool.go`.

### `internal/pool`

The public-facing API surface. Owns the channels and the `WaitGroup`. Wires `dispatcher` and exposes three methods to callers:

| Method | What it does |
|--------|-------------|
| `Start(ctx)` | Creates a cancel-wrapped context, calls dispatcher |
| `Submit(j)` | Sends a job onto the buffered channel |
| `Stop()` | Closes job channel → waits → closes result channel |
| `Results()` | Returns the read-only result channel for the caller to range over |

### `cmd/workerpool/main.go`

Entry point only — no business logic. Loads config, builds the pool, starts a producer goroutine, calls `p.Stop()`, and ranges over results. If the binary grows (flags, signal handling, HTTP health endpoint), it all lives here.

---

## Concurrency safety

- The job channel is the only shared state between the producer and workers. Go channels are safe for concurrent send/receive.
- The result channel is the only shared state between workers and the consumer. Same guarantee.
- `sync.WaitGroup` is used correctly: `Add` before the goroutine starts, `Done` deferred inside it.
- `close(jobChan)` is called exactly once, from `Stop()`, after all producers have finished submitting. Workers detect close via the `ok` bool in the receive expression.
- `close(resultChan)` is called after `wg.Wait()` returns, guaranteeing no worker will write to it after it is closed.

---

## Graceful shutdown sequence

```
p.Stop() called
    │
    ├─ close(jobChan)
    │       workers: next select iteration sees channel closed → return
    │       wg.Done() fires for each worker
    │
    ├─ wg.Wait()
    │       blocks here until all workers have returned
    │
    └─ close(resultChan)
            consumer's range loop exits
            main returns
```

Context cancellation (`ctx.Done()`) is an alternative path — useful for timeouts or external signals. The `select` in each worker handles both paths.

---

## Future extensions

| Feature | Where it lives |
|---------|---------------|
| Retry + dead-letter queue | `internal/worker` (retry counter on Job) + new `internal/deadletter` package |
| Dynamic worker scaling | `internal/dispatcher` (add/remove goroutines at runtime) |
| Metrics (Prometheus) | `pkg/metrics` — exported, importable by other services |
| HTTP health endpoint | `cmd/workerpool/main.go` (spin up an `http.Server` alongside the pool) |
| Priority queue | Replace `chan Job` with a heap-backed priority channel in `internal/pool` |