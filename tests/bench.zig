// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-2025 CodePros

//! Core hot-path benchmarks for the Raft library (PERF-016).
//!
//! Run with:   zig build bench --summary all
//!
//! Each benchmark reports wall-clock ns/op for its critical path.
//! Results are printed to stderr so they appear in `--summary all` output.

const std = @import("std");
const raft = @import("raft");

const WARMUP_ITERS = 100;
const BENCH_ITERS = 10_000;

fn nsPerOp(comptime func: anytype, args: anytype) u64 {
    // Warmup.
    for (0..WARMUP_ITERS) |_| {
        const result = @call(.auto, func, args);
        // QA-007: Prevent compiler from eliding the benchmark body.
        std.mem.doNotOptimizeAway(&result);
    }

    var timer = std.time.Timer.start() catch @panic("timer");
    for (0..BENCH_ITERS) |_| {
        const result = @call(.auto, func, args);
        std.mem.doNotOptimizeAway(&result);
    }
    const elapsed = timer.read();
    return elapsed / BENCH_ITERS;
}

// ---------------------------------------------------------------------------
// Benchmark: propose + ready + advance cycle (in-memory storage)
// ---------------------------------------------------------------------------

fn benchProposeReadyAdvance(node: *raft.RaftNode) void {
    node.propose("bench-payload-data") catch return;
    _ = node.ready() catch return;
    node.advance() catch return;
}

test "bench: propose-ready-advance" {
    const allocator = std.testing.allocator;

    var mem_store = raft.InMemoryStorage.init(allocator);
    defer mem_store.deinit();

    var node = try raft.RaftNode.init(allocator, .{
        .id = 1,
        .peers = &.{2},
        .election_tick = 10,
        .heartbeat_tick = 1,
    }, mem_store.storage());
    defer node.deinit();

    // Force leader state so propose() works.
    // Tick enough times to trigger election, then force win.
    for (0..20) |_| {
        node.tick(null) catch {};
    }
    // Step a vote response to win election.
    try node.step(2, raft.Message{ .request_vote_response = .{
        .term = node.current_term,
        .vote_granted = true,
        .pre_vote = false,
    } }, null);

    // Drain the initial ready/advance from election.
    _ = try node.ready();
    try node.advance();

    const ns = nsPerOp(benchProposeReadyAdvance, .{&node});
    std.debug.print("\n  propose-ready-advance: {d} ns/op\n", .{ns});
}

// ---------------------------------------------------------------------------
// Benchmark: log.append (batch of 64 entries)
// ---------------------------------------------------------------------------

test "bench: log-append-64" {
    const allocator = std.testing.allocator;

    var mem_store = raft.InMemoryStorage.init(allocator);
    defer mem_store.deinit();

    var log = try raft.log.RaftLog.init(allocator, mem_store.storage(), 0, 0);
    defer log.deinit();

    var entries: [64]raft.Entry = undefined;
    for (&entries, 0..) |*e, i| {
        e.* = .{
            .index = @as(u64, @intCast(i)) + 1,
            .term = 1,
            .kind = .normal,
            .data = "benchmark-entry-payload",
        };
    }

    var timer = std.time.Timer.start() catch @panic("timer");
    for (0..BENCH_ITERS) |iter| {
        // Re-index entries so they don't conflict with existing.
        const base = @as(u64, @intCast(iter)) * 64 + 1;
        for (&entries, 0..) |*e, i| {
            e.index = base + @as(u64, @intCast(i));
        }
        try log.append(&entries);
        // QA-007: Prevent compiler from eliding the append.
        std.mem.doNotOptimizeAway(&log);

        // Stabilize so unstable doesn't grow unbounded.
        if (iter % 100 == 99) {
            const last = log.lastIndex();
            try log.stableTo(last, 1);
        }
    }
    const elapsed = timer.read();
    const ns = elapsed / BENCH_ITERS;
    std.debug.print("\n  log-append-64: {d} ns/op (64 entries per op)\n", .{ns});
}

// ---------------------------------------------------------------------------
// Benchmark: log.slice (unstable-only range)
// ---------------------------------------------------------------------------

test "bench: log-slice-unstable" {
    const allocator = std.testing.allocator;

    var mem_store = raft.InMemoryStorage.init(allocator);
    defer mem_store.deinit();

    var log = try raft.log.RaftLog.init(allocator, mem_store.storage(), 0, 0);
    defer log.deinit();

    // Populate 1000 unstable entries.
    var entries: [1000]raft.Entry = undefined;
    for (&entries, 0..) |*e, i| {
        e.* = .{
            .index = @as(u64, @intCast(i)) + 1,
            .term = 1,
            .kind = .normal,
            .data = "bench",
        };
    }
    try log.append(&entries);

    var timer = std.time.Timer.start() catch @panic("timer");
    for (0..BENCH_ITERS) |_| {
        // Borrow (zero-copy) path.
        if (log.borrowUnstable(500, 600)) |borrowed| {
            // QA-007: Prevent compiler from eliding the borrow.
            std.mem.doNotOptimizeAway(&borrowed);
        } else {
            // Fallback to allocating slice.
            const s = try log.slice(500, 600);
            std.mem.doNotOptimizeAway(&s);
            allocator.free(s);
        }
    }
    const elapsed = timer.read();
    const ns = elapsed / BENCH_ITERS;
    std.debug.print("\n  log-slice-unstable (borrow): {d} ns/op (100 entries)\n", .{ns});
}

// ---------------------------------------------------------------------------
// Benchmark: tick (leader heartbeat path)
// ---------------------------------------------------------------------------

test "bench: leader-tick" {
    const allocator = std.testing.allocator;

    var mem_store = raft.InMemoryStorage.init(allocator);
    defer mem_store.deinit();

    // 5-node cluster.
    var node = try raft.RaftNode.init(allocator, .{
        .id = 1,
        .peers = &.{ 2, 3, 4, 5 },
        .election_tick = 10,
        .heartbeat_tick = 1,
    }, mem_store.storage());
    defer node.deinit();

    // Force leader.
    for (0..20) |_| {
        node.tick(null) catch {};
    }
    for ([_]u64{ 2, 3, 4, 5 }) |peer| {
        try node.step(peer, raft.Message{ .request_vote_response = .{
            .term = node.current_term,
            .vote_granted = true,
            .pre_vote = false,
        } }, null);
    }
    _ = try node.ready();
    try node.advance();

    var timer = std.time.Timer.start() catch @panic("timer");
    for (0..BENCH_ITERS) |_| {
        node.tick(null) catch {};
        // Drain ready to prevent accumulation.
        _ = node.ready() catch {};
        node.advance() catch {};
        // QA-007: Prevent compiler from eliding the tick.
        std.mem.doNotOptimizeAway(&node);
    }
    const elapsed = timer.read();
    const ns = elapsed / BENCH_ITERS;
    std.debug.print("\n  leader-tick (5-node): {d} ns/op\n", .{ns});
}

// ---------------------------------------------------------------------------
// Benchmark: stableTo with start-offset tracking (PERF-009)
// ---------------------------------------------------------------------------

test "bench: stableTo" {
    const allocator = std.testing.allocator;

    var mem_store = raft.InMemoryStorage.init(allocator);
    defer mem_store.deinit();

    var log = try raft.log.RaftLog.init(allocator, mem_store.storage(), 0, 0);
    defer log.deinit();

    var timer = std.time.Timer.start() catch @panic("timer");
    for (0..BENCH_ITERS) |iter| {
        // Append 10 entries.
        var entries: [10]raft.Entry = undefined;
        const base = @as(u64, @intCast(iter)) * 10 + 1;
        for (&entries, 0..) |*e, i| {
            e.* = .{
                .index = base + @as(u64, @intCast(i)),
                .term = 1,
                .kind = .normal,
                .data = "stable-bench",
            };
        }
        try log.append(&entries);

        // Stabilize all.
        const last = log.lastIndex();
        try log.stableTo(last, 1);
        // QA-007: Prevent compiler from eliding stableTo.
        std.mem.doNotOptimizeAway(&log);
    }
    const elapsed = timer.read();
    const ns = elapsed / BENCH_ITERS;
    std.debug.print("\n  stableTo (10 entries): {d} ns/op\n", .{ns});
}

// ---------------------------------------------------------------------------
// Benchmark: FileStorage append (PERF-025)
// ---------------------------------------------------------------------------

const FS_BENCH_ITERS = 1_000;

test "bench: file_storage_append" {
    const allocator = std.heap.page_allocator;
    const base = "/tmp/raft-bench-fs-append";

    // Clean up from prior runs.
    std.fs.cwd().deleteTree(base) catch {};
    defer std.fs.cwd().deleteTree(base) catch {};

    var store = try raft.FileStorage.init(allocator, base);
    defer store.deinit();

    // Pre-build 64 entries (reused each iteration with re-indexed values).
    var entries: [64]raft.Entry = undefined;
    for (&entries, 0..) |*e, i| {
        e.* = .{
            .index = @as(u64, @intCast(i)) + 1,
            .term = 1,
            .kind = .normal,
            .data = "file-storage-bench-payload-data",
        };
    }

    var timer = std.time.Timer.start() catch @panic("timer");
    for (0..FS_BENCH_ITERS) |iter| {
        const base_idx = @as(u64, @intCast(iter)) * 64 + 1;
        for (&entries, 0..) |*e, i| {
            e.index = base_idx + @as(u64, @intCast(i));
        }
        try store.appendEntriesImpl(&entries);
        std.mem.doNotOptimizeAway(&store);
    }
    const elapsed = timer.read();
    const ns = elapsed / FS_BENCH_ITERS;
    std.debug.print("\n  file_storage_append: {d} ns/op (64 entries per op)\n", .{ns});
}

// ---------------------------------------------------------------------------
// Benchmark: FileStorage read (PERF-025)
// ---------------------------------------------------------------------------

test "bench: file_storage_read" {
    const allocator = std.heap.page_allocator;
    const base = "/tmp/raft-bench-fs-read";

    // Clean up from prior runs.
    std.fs.cwd().deleteTree(base) catch {};
    defer std.fs.cwd().deleteTree(base) catch {};

    var store = try raft.FileStorage.init(allocator, base);
    defer store.deinit();

    // Populate 1000 entries on disk.
    var entries: [1000]raft.Entry = undefined;
    for (&entries, 0..) |*e, i| {
        e.* = .{
            .index = @as(u64, @intCast(i)) + 1,
            .term = 1,
            .kind = .normal,
            .data = "file-storage-read-bench-data",
        };
    }
    try store.appendEntriesImpl(&entries);

    var timer = std.time.Timer.start() catch @panic("timer");
    for (0..FS_BENCH_ITERS) |_| {
        // Read a 100-entry range from the middle of the WAL.
        const result = try store.entriesImpl(400, 500);
        std.mem.doNotOptimizeAway(&result);
        // Free returned entries (entriesImpl allocates data + slice).
        for (result) |e| {
            if (e.data.len > 0) allocator.free(e.data);
        }
        allocator.free(result);
    }
    const elapsed = timer.read();
    const ns = elapsed / FS_BENCH_ITERS;
    std.debug.print("\n  file_storage_read: {d} ns/op (100 entries per op)\n", .{ns});
}

// ---------------------------------------------------------------------------
// Benchmark: FileStorage compact / WAL compaction (PERF-025)
// ---------------------------------------------------------------------------

test "bench: file_storage_compact" {
    const allocator = std.heap.page_allocator;

    const COMPACT_ITERS: u64 = 200;

    var timer = std.time.Timer.start() catch @panic("timer");
    for (0..COMPACT_ITERS) |round| {
        // Each round: fresh store, populate, compact.
        var path_buf: [64]u8 = undefined;
        const base = std.fmt.bufPrint(&path_buf, "/tmp/raft-bench-fs-compact-{d}", .{round}) catch unreachable;

        std.fs.cwd().deleteTree(base) catch {};

        var store = try raft.FileStorage.init(allocator, base);

        // Populate 200 entries.
        var entries: [200]raft.Entry = undefined;
        for (&entries, 0..) |*e, i| {
            e.* = .{
                .index = @as(u64, @intCast(i)) + 1,
                .term = 1,
                .kind = .normal,
                .data = "compact-bench-entry-payload",
            };
        }
        try store.appendEntriesImpl(&entries);

        // Compact the first 150 entries (leaves 50 remaining, triggers WAL rewrite).
        try store.compactImpl(150);
        std.mem.doNotOptimizeAway(&store);

        store.deinit();

        // Clean up this round's directory.
        std.fs.cwd().deleteTree(base) catch {};
    }
    const elapsed = timer.read();
    const ns = elapsed / COMPACT_ITERS;
    std.debug.print("\n  file_storage_compact: {d} ns/op (compact 150/200 entries)\n", .{ns});
}
