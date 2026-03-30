// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-2025 CodePros

//! Incremental scale benchmarks for the Raft library.
//!
//! Measures core Raft operations at progressively larger cluster sizes.
//! JSON output to stdout, human-readable summary to stderr.
//!
//! Run:   zig build scale-bench
//! Save:  zig build scale-bench 2>/dev/null > benchmarks/results/$(git rev-parse --short HEAD).json

const std = @import("std");
const builtin = @import("builtin");
const raft = @import("raft");
const spice = raft.spice;

const print = std.debug.print;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const WARMUP_ITERS = 50;

/// Adaptive iteration count based on cluster size.
fn benchIters(node_count: u64) usize {
    return switch (node_count) {
        0...1 => 50_000,
        2...3 => 20_000,
        4...5 => 10_000,
        6...7 => 5_000,
        else => 2_000,
    };
}

// ---------------------------------------------------------------------------
// Tier definitions
// ---------------------------------------------------------------------------

const Tier = struct {
    name: []const u8,
    node_count: u64,
};

const tiers = [_]Tier{
    .{ .name = "1-node", .node_count = 1 },
    .{ .name = "3-node", .node_count = 3 },
    .{ .name = "5-node", .node_count = 5 },
    .{ .name = "7-node", .node_count = 7 },
    .{ .name = "11-node", .node_count = 11 },
};

// ---------------------------------------------------------------------------
// Result types
// ---------------------------------------------------------------------------

const TierResult = struct {
    name: []const u8,
    node_count: u64,
    peer_count: u64,
    skipped: bool = false,
    skip_reason: ?[]const u8 = null,

    // Latency metrics (ns per operation, lower is better)
    propose_ns: f64 = 0,
    tick_ns: f64 = 0,
    tick_parallel_ns: f64 = 0,
    step_ae_ns: f64 = 0,
    log_append_ns: f64 = 0,
    ready_advance_ns: f64 = 0,

    // Throughput metrics (ops/sec, higher is better)
    heartbeat_ops_sec: f64 = 0,
    replication_ops_sec: f64 = 0,
};

// ---------------------------------------------------------------------------
// Stdout writer (JSON to stdout, separate from stderr debug output)
// ---------------------------------------------------------------------------

const StdoutWriter = struct {
    file: std.fs.File,
    buf: [8192]u8 = undefined,
    writer: ?std.fs.File.Writer = null,

    fn open(self: *StdoutWriter) void {
        self.writer = self.file.writer(&self.buf);
    }

    fn writeAll(self: *StdoutWriter, data: []const u8) !void {
        try self.writer.?.interface.writeAll(data);
    }

    fn writeFmt(self: *StdoutWriter, comptime fmt: []const u8, args: anytype) !void {
        try self.writer.?.interface.print(fmt, args);
    }

    fn flush(self: *StdoutWriter) !void {
        try self.writer.?.interface.flush();
    }
};

// ---------------------------------------------------------------------------
// Benchmark helpers
// ---------------------------------------------------------------------------

/// Create a single-node Raft leader for isolated benchmarks.
fn createLeaderNode(allocator: std.mem.Allocator, peers: []const u64) !*raft.RaftNode {
    const node = try allocator.create(raft.RaftNode);
    const peers_dup = try allocator.dupe(u64, peers);
    node.* = try raft.RaftNode.init(allocator, .{
        .id = 1,
        .peers = peers_dup,
        .election_tick = 10,
        .heartbeat_tick = 1,
        .pre_vote = if (peers.len == 0) false else true,
    }, (try createStorage(allocator)).storage());

    // Tick until leader (single-node wins immediately).
    if (peers.len == 0) {
        for (0..20) |_| {
            node.tick(null) catch {};
        }
    }
    return node;
}

fn createStorage(allocator: std.mem.Allocator) !*raft.InMemoryStorage {
    const store = try allocator.create(raft.InMemoryStorage);
    store.* = raft.InMemoryStorage.init(allocator);
    return store;
}

// ---------------------------------------------------------------------------
// Individual benchmarks
// ---------------------------------------------------------------------------

/// Benchmark: propose() latency on a single-node leader.
fn benchPropose(allocator: std.mem.Allocator, iters: usize) !f64 {
    const node = try createLeaderNode(allocator, &.{});
    _ = try node.ready();
    try node.advance();

    // Warmup
    for (0..WARMUP_ITERS) |_| {
        try node.propose("bench-payload");
        _ = try node.ready();
        try node.advance();
    }

    var timer = std.time.Timer.start() catch @panic("timer");
    for (0..iters) |_| {
        try node.propose("bench-payload-data-64bytes-padded-to-be-realistic!!");
        _ = try node.ready();
        try node.advance();
    }
    const elapsed = timer.read();
    return @as(f64, @floatFromInt(elapsed)) / @as(f64, @floatFromInt(iters));
}

/// Benchmark: tick() latency for a leader with N peers (sequential).
fn benchTick(allocator: std.mem.Allocator, peer_count: u64, iters: usize) !f64 {
    var peers = std.ArrayList(u64){};
    defer peers.deinit(allocator);
    for (0..peer_count) |i| {
        try peers.append(allocator, @as(u64, @intCast(i)) + 2);
    }

    const node = try createLeaderNode(allocator, peers.items);
    // For multi-peer, force leader state manually.
    if (peer_count > 0) {
        node.role = .leader;
        node.current_term = 1;
        // Initialize replication state.
        node.replication_state = try raft.replication.ReplicationState.init(allocator, peers.items, 0);
    }
    _ = try node.ready();
    try node.advance();

    // Warmup
    for (0..WARMUP_ITERS) |_| {
        node.tick(null) catch {};
        _ = node.ready() catch unreachable;
        node.advance() catch {};
    }

    var timer = std.time.Timer.start() catch @panic("timer");
    for (0..iters) |_| {
        node.tick(null) catch {};
        _ = node.ready() catch unreachable;
        node.advance() catch {};
    }
    const elapsed = timer.read();
    return @as(f64, @floatFromInt(elapsed)) / @as(f64, @floatFromInt(iters));
}

/// Benchmark: tick() latency for a leader with N peers (parallel via spice).
fn benchTickParallel(allocator: std.mem.Allocator, pool: *spice.ThreadPool, peer_count: u64, iters: usize) !f64 {
    if (peer_count == 0) return 0;

    var peers = std.ArrayList(u64){};
    defer peers.deinit(allocator);
    for (0..peer_count) |i| {
        try peers.append(allocator, @as(u64, @intCast(i)) + 2);
    }

    const node = try createLeaderNode(allocator, peers.items);
    if (peer_count > 0) {
        node.role = .leader;
        node.current_term = 1;
        node.replication_state = try raft.replication.ReplicationState.init(allocator, peers.items, 0);
    }
    _ = try node.ready();
    try node.advance();

    // Warmup
    for (0..WARMUP_ITERS) |_| {
        _ = pool.call(bool, struct {
            fn run(task: *spice.Task, n: *raft.RaftNode) bool {
                n.tick(task) catch return true;
                return false;
            }
        }.run, node);
        _ = node.ready() catch unreachable;
        node.advance() catch {};
    }

    var timer = std.time.Timer.start() catch @panic("timer");
    for (0..iters) |_| {
        _ = pool.call(bool, struct {
            fn run(task: *spice.Task, n: *raft.RaftNode) bool {
                n.tick(task) catch return true;
                return false;
            }
        }.run, node);
        _ = node.ready() catch unreachable;
        node.advance() catch {};
    }
    const elapsed = timer.read();
    return @as(f64, @floatFromInt(elapsed)) / @as(f64, @floatFromInt(iters));
}

/// Benchmark: follower step(AppendEntries) latency with 10 entries.
fn benchStepAE(allocator: std.mem.Allocator, iters: usize) !f64 {
    const node = try createLeaderNode(allocator, &.{2});
    // Make it a follower.
    node.role = .follower;
    node.current_term = 1;
    node.leader_id = 2;

    const payload = "A" ** 64;
    var ae_entries: [10]raft.Entry = undefined;

    // Warmup
    for (0..WARMUP_ITERS) |iter| {
        const base_idx = @as(u64, @intCast(iter)) * 10 + 1;
        for (&ae_entries, 0..) |*e, i| {
            e.* = .{
                .index = base_idx + @as(u64, @intCast(i)),
                .term = 1,
                .kind = .normal,
                .data = payload,
            };
        }
        const prev_idx = if (base_idx > 1) base_idx - 1 else 0;
        const prev_term: u64 = if (prev_idx > 0) 1 else 0;
        try node.step(2, raft.Message{ .append_entries = .{
            .term = 1,
            .leader_id = 2,
            .prev_log_index = prev_idx,
            .prev_log_term = prev_term,
            .entries = &ae_entries,
            .leader_commit = base_idx,
        } });
        _ = node.ready() catch {};
        node.advance() catch {};
    }

    var timer = std.time.Timer.start() catch @panic("timer");
    for (0..iters) |iter| {
        const base_idx = @as(u64, @intCast(WARMUP_ITERS + iter)) * 10 + 1;
        for (&ae_entries, 0..) |*e, i| {
            e.* = .{
                .index = base_idx + @as(u64, @intCast(i)),
                .term = 1,
                .kind = .normal,
                .data = payload,
            };
        }
        const prev_idx = base_idx - 1;
        try node.step(2, raft.Message{ .append_entries = .{
            .term = 1,
            .leader_id = 2,
            .prev_log_index = prev_idx,
            .prev_log_term = 1,
            .entries = &ae_entries,
            .leader_commit = base_idx,
        } });
        _ = node.ready() catch {};
        node.advance() catch {};
    }
    const elapsed = timer.read();
    return @as(f64, @floatFromInt(elapsed)) / @as(f64, @floatFromInt(iters));
}

/// Benchmark: log.append() with 64-entry batches.
fn benchLogAppend(allocator: std.mem.Allocator, iters: usize) !f64 {
    const store = try createStorage(allocator);
    var log = try raft.log.RaftLog.init(allocator, store.storage(), 0, 0);

    var entries: [64]raft.Entry = undefined;

    // Warmup
    for (0..WARMUP_ITERS) |iter| {
        const base = @as(u64, @intCast(iter)) * 64 + 1;
        for (&entries, 0..) |*e, i| {
            e.* = .{
                .index = base + @as(u64, @intCast(i)),
                .term = 1,
                .kind = .normal,
                .data = "benchmark-entry-payload",
            };
        }
        try log.append(&entries);
        if (iter % 50 == 49) {
            try log.stableTo(log.lastIndex(), 1);
        }
    }

    var timer = std.time.Timer.start() catch @panic("timer");
    for (0..iters) |iter| {
        const base = @as(u64, @intCast(WARMUP_ITERS + iter)) * 64 + 1;
        for (&entries, 0..) |*e, i| {
            e.* = .{
                .index = base + @as(u64, @intCast(i)),
                .term = 1,
                .kind = .normal,
                .data = "benchmark-entry-payload",
            };
        }
        try log.append(&entries);
        if (iter % 100 == 99) {
            try log.stableTo(log.lastIndex(), 1);
        }
    }
    const elapsed = timer.read();
    _ = &log;
    return @as(f64, @floatFromInt(elapsed)) / @as(f64, @floatFromInt(iters));
}

/// Benchmark: ready() + advance() cycle on a leader.
fn benchReadyAdvance(allocator: std.mem.Allocator, iters: usize) !f64 {
    const node = try createLeaderNode(allocator, &.{});
    _ = try node.ready();
    try node.advance();

    // Warmup
    for (0..WARMUP_ITERS) |_| {
        try node.propose("ready-advance-bench");
        _ = try node.ready();
        try node.advance();
    }

    var timer = std.time.Timer.start() catch @panic("timer");
    for (0..iters) |_| {
        try node.propose("ready-advance-bench-payload");
        _ = try node.ready();
        try node.advance();
    }
    const elapsed = timer.read();
    return @as(f64, @floatFromInt(elapsed)) / @as(f64, @floatFromInt(iters));
}

/// Benchmark: heartbeat throughput (ticks that trigger heartbeats).
fn benchHeartbeatThroughput(allocator: std.mem.Allocator, peer_count: u64, iters: usize) !f64 {
    var peers = std.ArrayList(u64){};
    defer peers.deinit(allocator);
    for (0..peer_count) |i| {
        try peers.append(allocator, @as(u64, @intCast(i)) + 2);
    }

    const node = try createLeaderNode(allocator, peers.items);
    if (peer_count > 0) {
        node.role = .leader;
        node.current_term = 1;
        node.replication_state = try raft.replication.ReplicationState.init(allocator, peers.items, 0);
    }

    // Warmup
    for (0..WARMUP_ITERS) |_| {
        node.tick(null) catch {};
        _ = node.ready() catch unreachable;
        node.advance() catch {};
    }

    var timer = std.time.Timer.start() catch @panic("timer");
    for (0..iters) |_| {
        node.tick(null) catch {};
    }
    const elapsed = timer.read();

    if (elapsed == 0) return 0;
    const count_f: f64 = @floatFromInt(iters);
    const ns_f: f64 = @floatFromInt(elapsed);
    return count_f / (ns_f / 1_000_000_000.0);
}

/// Benchmark: full propose-replicate-commit throughput (single-node).
fn benchReplicationThroughput(allocator: std.mem.Allocator, iters: usize) !f64 {
    const node = try createLeaderNode(allocator, &.{});
    _ = try node.ready();
    try node.advance();

    // Warmup
    for (0..WARMUP_ITERS) |_| {
        try node.propose("repl-bench");
        _ = try node.ready();
        try node.advance();
    }

    var timer = std.time.Timer.start() catch @panic("timer");
    for (0..iters) |_| {
        try node.propose("replication-throughput-payload");
        _ = try node.ready();
        try node.advance();
    }
    const elapsed = timer.read();

    if (elapsed == 0) return 0;
    const count_f: f64 = @floatFromInt(iters);
    const ns_f: f64 = @floatFromInt(elapsed);
    return count_f / (ns_f / 1_000_000_000.0);
}

// ---------------------------------------------------------------------------
// Tier runner
// ---------------------------------------------------------------------------

fn runTier(allocator: std.mem.Allocator, pool: *spice.ThreadPool, tier: Tier) TierResult {
    const peer_count = if (tier.node_count > 1) tier.node_count - 1 else 0;
    const iters = benchIters(tier.node_count);

    var result = TierResult{
        .name = tier.name,
        .node_count = tier.node_count,
        .peer_count = peer_count,
    };

    // propose (single-node only — multi-node propose requires full cluster)
    result.propose_ns = benchPropose(allocator, iters) catch |err| blk: {
        print("    propose failed: {s}\n", .{@errorName(err)});
        break :blk 0;
    };

    // tick (sequential)
    result.tick_ns = benchTick(allocator, peer_count, iters) catch |err| blk: {
        print("    tick failed: {s}\n", .{@errorName(err)});
        break :blk 0;
    };

    // tick (parallel)
    result.tick_parallel_ns = benchTickParallel(allocator, pool, peer_count, iters) catch |err| blk: {
        print("    tick_parallel failed: {s}\n", .{@errorName(err)});
        break :blk 0;
    };

    // step AppendEntries
    result.step_ae_ns = benchStepAE(allocator, iters) catch |err| blk: {
        print("    step_ae failed: {s}\n", .{@errorName(err)});
        break :blk 0;
    };

    // log append
    result.log_append_ns = benchLogAppend(allocator, iters) catch |err| blk: {
        print("    log_append failed: {s}\n", .{@errorName(err)});
        break :blk 0;
    };

    // ready+advance
    result.ready_advance_ns = benchReadyAdvance(allocator, iters) catch |err| blk: {
        print("    ready_advance failed: {s}\n", .{@errorName(err)});
        break :blk 0;
    };

    // heartbeat throughput
    result.heartbeat_ops_sec = benchHeartbeatThroughput(allocator, peer_count, iters) catch |err| blk: {
        print("    heartbeat_throughput failed: {s}\n", .{@errorName(err)});
        break :blk 0;
    };

    // replication throughput (single-node)
    result.replication_ops_sec = benchReplicationThroughput(allocator, iters) catch |err| blk: {
        print("    replication_throughput failed: {s}\n", .{@errorName(err)});
        break :blk 0;
    };

    return result;
}

// ---------------------------------------------------------------------------
// JSON output to stdout
// ---------------------------------------------------------------------------

fn writeJsonOutput(out: *StdoutWriter, results: []const TierResult) !void {
    try out.writeAll("{\n");
    try out.writeAll("  \"version\": 1,\n");

    // Timestamp
    const ts = std.time.timestamp();
    const epoch_secs: u64 = @intCast(ts);
    const es = std.time.epoch.EpochSeconds{ .secs = epoch_secs };
    const day_secs = es.getDaySeconds();
    const year_day = es.getEpochDay().calculateYearDay();
    const month_day = year_day.calculateMonthDay();

    try out.writeFmt("  \"timestamp\": \"{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}Z\",\n", .{
        year_day.year,
        month_day.month.numeric(),
        month_day.day_index + 1,
        day_secs.getHoursIntoDay(),
        day_secs.getMinutesIntoHour(),
        day_secs.getSecondsIntoMinute(),
    });

    try out.writeAll("  \"git_commit\": \"unknown\",\n");

    // System info
    try out.writeFmt("  \"system\": {{ \"os\": \"{s}\", \"arch\": \"{s}\" }},\n", .{
        @tagName(builtin.os.tag),
        @tagName(builtin.cpu.arch),
    });

    // Tiers
    try out.writeAll("  \"tiers\": [\n");
    for (results, 0..) |r, i| {
        try out.writeAll("    {\n");
        try out.writeFmt("      \"name\": \"{s}\",\n", .{r.name});
        try out.writeFmt("      \"node_count\": {d},\n", .{r.node_count});
        try out.writeFmt("      \"peer_count\": {d},\n", .{r.peer_count});
        if (r.skipped) {
            try out.writeAll("      \"skipped\": true,\n");
            if (r.skip_reason) |reason| {
                try out.writeFmt("      \"skip_reason\": \"{s}\",\n", .{reason});
            }
        }
        try out.writeFmt("      \"propose_ns\": {d:.0},\n", .{r.propose_ns});
        try out.writeFmt("      \"tick_ns\": {d:.0},\n", .{r.tick_ns});
        try out.writeFmt("      \"tick_parallel_ns\": {d:.0},\n", .{r.tick_parallel_ns});
        try out.writeFmt("      \"step_ae_ns\": {d:.0},\n", .{r.step_ae_ns});
        try out.writeFmt("      \"log_append_ns\": {d:.0},\n", .{r.log_append_ns});
        try out.writeFmt("      \"ready_advance_ns\": {d:.0},\n", .{r.ready_advance_ns});
        try out.writeFmt("      \"heartbeat_ops_sec\": {d:.0},\n", .{r.heartbeat_ops_sec});
        try out.writeFmt("      \"replication_ops_sec\": {d:.0}\n", .{r.replication_ops_sec});
        if (i < results.len - 1) {
            try out.writeAll("    },\n");
        } else {
            try out.writeAll("    }\n");
        }
    }
    try out.writeAll("  ]\n");
    try out.writeAll("}\n");
    try out.flush();
}

// ---------------------------------------------------------------------------
// Human-readable summary to stderr
// ---------------------------------------------------------------------------

fn writeSummary(results: []const TierResult) void {
    print("\n", .{});
    print("{s}\n", .{"=" ** 120});
    print("  raft incremental scale benchmark\n", .{});
    print("{s}\n\n", .{"=" ** 120});

    print("  {s:<8} {s:>6} {s:>12} {s:>12} {s:>12} {s:>12} {s:>12} {s:>12} {s:>14} {s:>14}\n", .{
        "Tier", "Peers", "Propose", "Tick(seq)", "Tick(par)", "StepAE", "LogAppend", "Ready/Adv", "HB ops/s", "Repl ops/s",
    });
    print("  {s:<8} {s:>6} {s:>12} {s:>12} {s:>12} {s:>12} {s:>12} {s:>12} {s:>14} {s:>14}\n", .{
        "-" ** 8, "-" ** 6, "-" ** 12, "-" ** 12, "-" ** 12, "-" ** 12, "-" ** 12, "-" ** 12, "-" ** 14, "-" ** 14,
    });

    for (results) |r| {
        if (r.skipped) {
            print("  {s:<8} {d:>6} SKIPPED ({s})\n", .{
                r.name, r.peer_count, r.skip_reason orelse "unknown",
            });
        } else {
            print("  {s:<8} {d:>6} {d:>10.0}ns {d:>10.0}ns {d:>10.0}ns {d:>10.0}ns {d:>10.0}ns {d:>10.0}ns {d:>14.0} {d:>14.0}\n", .{
                r.name,
                r.peer_count,
                r.propose_ns,
                r.tick_ns,
                r.tick_parallel_ns,
                r.step_ae_ns,
                r.log_append_ns,
                r.ready_advance_ns,
                r.heartbeat_ops_sec,
                r.replication_ops_sec,
            });
        }
    }

    print("\n{s}\n\n", .{"=" ** 120});
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

pub fn main() !void {
    const allocator = std.heap.page_allocator;

    // Parse optional max-tier argument.
    var args = std.process.args();
    _ = args.next(); // skip program name

    var max_tier_count: usize = tiers.len;
    if (args.next()) |arg| {
        for (tiers, 0..) |t, i| {
            if (std.mem.eql(u8, arg, t.name)) {
                max_tier_count = i + 1;
                break;
            }
        }
    }

    // Create thread pool for parallel benchmarks.
    var pool = spice.ThreadPool.init(allocator);
    pool.start(.{});
    defer pool.deinit();

    var results: [tiers.len]TierResult = undefined;
    var completed: usize = 0;

    for (tiers[0..max_tier_count], 0..) |tier, i| {
        print("  Running tier {s} ({d} nodes)...\n", .{ tier.name, tier.node_count });

        results[i] = runTier(allocator, &pool, tier);
        completed = i + 1;

        if (results[i].skipped) {
            print("  Tier {s} skipped: {s}\n", .{
                tier.name, results[i].skip_reason orelse "unknown",
            });
        } else {
            print("  Tier {s} complete.\n", .{tier.name});
        }
    }

    writeSummary(results[0..completed]);

    var out = StdoutWriter{ .file = .{ .handle = std.posix.STDOUT_FILENO } };
    out.open();
    try writeJsonOutput(&out, results[0..completed]);
}
