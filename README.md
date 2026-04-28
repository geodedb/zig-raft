# raft

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Zig](https://img.shields.io/badge/Zig-0.15.1+-orange.svg)](https://ziglang.org/)

A Zig library for raft

## Features

- **Minimal Dependencies**: Depends only on [spice](https://gitlab.com/devnw/zig/spice) for fork/join parallelism
- **Memory Safe**: Proper allocation patterns with no leaks
- **Cross-Platform**: Linux, macOS, Windows support
- **Well Tested**: Comprehensive unit and integration tests

## Installation

### Using Zig Package Manager

Add to your `build.zig.zon`:

```zig
.dependencies = .{
    .raft = .{
        .url = "https://gitlab.com/devnw/zig/raft/-/archive/v0.1.0/raft-v0.1.0.tar.gz",
        .hash = "...",
    },
},
```

Then in your `build.zig`:

```zig
const raft = b.dependency("raft", .{
    .target = target,
    .optimize = optimize,
});
exe.root_module.addImport("raft", raft.module("raft"));
```

## Quick Start

```zig
const std = @import("std");
const raft = @import("raft");

pub fn main() !void {
    const allocator = std.heap.page_allocator;

    // Create storage and transport backends.
    var store = raft.InMemoryStorage.init(allocator);
    defer store.deinit();
    // (Use raft.FileStorage for crash-safe persistence.)

    // Configure and initialize the node.
    const config = raft.Config{ .id = 1, .peers = &.{ 2, 3 } };
    var node = try raft.RaftNode.init(allocator, config, store.storage());
    defer node.deinit();

    // Consumer loop: tick → ready → persist/send/apply → advance.
    try node.tick(null);
    var rd = try node.ready();
    // ... persist rd.hard_state, send rd.messages, apply rd.committed_entries ...
    _ = rd;
    try node.advance();
}
```

## Thread-pool integration

`RaftNode.tick` and `RaftNode.sendHeartbeats` accept an optional
`*tp.ThreadPool`. When supplied, the heartbeat `AppendEntries` fan-out is
built in parallel across peers via `pool.submitBatch`, with a single
`WaitGroup` awaited before the heartbeats are sent. Passing `null` keeps
everything on the tick-driving thread (correct, just serial).

For the tick-driving thread, the canonical integration uses the
per-submitter fast path. Register the thread **once** before the loop,
release **once** after. This claims a dedicated submitter slot with its
own descriptor cache — heartbeat bursts from the same thread then bypass
global-freelist CAS traffic for task-descriptor acquisition.

```zig
const std = @import("std");
const raft = @import("raft");
const tp = raft.thread_pool;

pub fn run(allocator: std.mem.Allocator) !void {
    var storage = raft.InMemoryStorage.init(allocator);
    defer storage.deinit();

    var node = try raft.RaftNode.init(allocator, .{
        .id = 1,
        .peers = &.{ 2, 3 },
    }, storage.storage());
    defer node.deinit();

    // Reserve at least one slot in the submitter bitmap.
    var pool = try tp.ThreadPool.init(allocator, .{
        .max_threads = 4,
        .scheduler_variant = .work_stealing,
        .max_submitter_queues = 1,
    });
    defer {
        _ = pool.shutdown(.drain, 5 * std.time.ns_per_s) catch {};
        pool.deinit();
    }

    // Register ONCE before the loop on the thread that drives tick().
    const slot = try node.registerAsSubmitter(&pool);
    defer node.releaseAsSubmitter(&pool, slot);

    while (running()) {
        try node.tick(&pool);
        // ... step/ready/advance as usual ...
    }
}
```

### Lifecycle rules

- `registerAsSubmitter` must be called on the **same thread** that will
  call `tick(&pool)`. The per-slot descriptor cache is single-writer and
  assumes a stable producer identity.
- Call it **once** per tick loop, not per tick. Each call claims a fresh
  slot from the pool's 64-slot submitter bitmap.
- Pair it with exactly **one** `releaseAsSubmitter` on the same thread
  before the loop exits. The release drains the slot's descriptor cache
  back to the pool's global freelist.
- The pool's `max_submitter_queues` config option must be set to at
  least 1 (rounded up to a power of two internally). The default is 0,
  which leaves the bitmap empty and causes `registerAsSubmitter` to
  return `error.NoSubmitterSlots`.
- `registerAsSubmitter` returns `error.NoSubmitterSlots` when the bitmap
  is full. Callers that cannot afford to fail may ignore the error and
  fall back to the default submit path — correct, just slower under
  contention.

### Operator visibility

`RaftNode.perf_stats.heartbeat_partial_submits` counts heartbeat rounds
where `submitBatch` returned a partial result — some peers fell through
to the `emptyHeartbeat` fallback. A healthy pool with adequate queue
capacity should stay at `0`. A rising value signals pool saturation;
raise `Config.queue_capacity` or spread work across more submitters.

See [`examples/single_node_parallel.zig`](examples/single_node_parallel.zig)
for a complete runnable demo (`zig build example-parallel`).

## Observability

Raft ships with an OpenTelemetry instrumentation seam that is **off by
default and zero-cost when off**. Builds with the default flag value
contain no `otel`/`observability` linked symbols (verified by
[`otel/scripts/verify-consumer-noop.sh`](https://gitlab.com/devnw/zig/otel)).

### Enabling instrumentation

Compile with `-Dwith_otel=true`:

```bash
zig build -Dwith_otel=true
```

When the flag is on, the [otel](https://gitlab.com/devnw/zig/otel)
dependency is resolved and the seam forwards every span site to
`otel.api.trace`. When off, every helper in `raft.observability`
collapses to a comptime-known no-op the optimiser eliminates.

### What gets traced

| Site | Span name | Kind | Notes |
| --- | --- | --- | --- |
| `handleAppendEntries` | `raft.AppendEntries` | server | one span per inbound batch (per-entry would be too high-volume) |
| `handleRequestVote` | `raft.RequestVote` | server | one span per inbound vote request |
| `handleInstallSnapshot` | `raft.InstallSnapshot` | server | one span per inbound chunk |
| `sendHeartbeats` | `raft.AppendEntries` | client | one span per heartbeat broadcast (parallel + sequential paths) |
| `startElection` | `raft.RequestVote` | client | one span per RequestVote fan-out (covers pre-vote + real-vote) |

Span attributes follow the OpenTelemetry RPC semantic conventions:
`rpc.system="raft"`, `rpc.method="AppendEntries"|...`, plus
`raft.target_node_id` (client side) or `raft.source_node_id` (server
side). Telemetry is non-fatal — a failing exporter never propagates
into Raft consensus.

### Wiring otel into your application

The Raft library is consumer-driven: it does not call init itself. The
embedding application owns the otel lifecycle. From `main()`:

```zig
const std = @import("std");
const raft = @import("raft");

pub fn main() !void {
    var gpa: std.heap.GeneralPurposeAllocator(.{}) = .{};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // `Otel` must be parked at a stable address before publishing
    // the global tracer pointer — `installGlobals` writes
    // `&otel.tracer` into `raft.observability.global.tracer`.
    var otel = try raft.observability_init.Otel.init(allocator, "my-raft-service");
    defer otel.deinit();
    otel.installGlobals();

    // ... drive your RaftNode here ...
}
```

Configuration follows the standard OpenTelemetry environment
variables: `OTEL_SERVICE_NAME`, `OTEL_EXPORTER_OTLP_ENDPOINT`,
`OTEL_TRACES_SAMPLER`, etc. The default exporter is OTLP/HTTP to
`http://localhost:4318` with a 5% parent-based sampler. See the
[otel rollout recipe](https://gitlab.com/devnw/zig/otel/-/blob/main/docs/integration/RECIPE.md)
for the full configuration surface.

## Requirements

- Zig 0.15.1 or later

## Building

```bash
# Build the library
make build

# Run tests
make test

# Quick development iteration
make dev

# Full CI pipeline
make ci
```

## Documentation

See the [docs](docs/) directory for detailed documentation.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## Security

See [SECURITY.md](SECURITY.md) for security policy and vulnerability reporting.

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.
