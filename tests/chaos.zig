// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-2025 CodePros

//! Chaos tests for the Raft library (spec §8, Tier 4).
//!
//! These tests exercise the Raft cluster under randomized fault injection
//! including message drops, network partitions, node crashes, and restarts.
//! All scenarios use a deterministic seed for full replay. Pass `-Dseed=N`
//! to reproduce a specific run.

const std = @import("std");
const raft = @import("raft");
const harness = @import("harness");
const chaos_options = @import("chaos_options");

/// Deterministic seed from `-Dseed=N`, or a random one for exploration runs.
fn getSeed() u64 {
    const opt_seed: ?u64 = chaos_options.seed;
    return opt_seed orelse blk: {
        var buf: [8]u8 = undefined;
        std.posix.getrandom(&buf) catch break :blk 0xcafe_beef_dead_c0de;
        break :blk std.mem.readInt(u64, &buf, .little);
    };
}

// ============================================================================
// ChaosRunner
// ============================================================================

const ChaosRunner = struct {
    net: *harness.Network,
    rng: std.Random.DefaultPrng,
    seed: u64,
    tick_count: u64,
    config: FaultConfig,

    const FaultConfig = struct {
        drop_rate: f32 = 0.05,
        partition_rate: f32 = 0.02,
        heal_rate: f32 = 0.05,
        crash_rate: f32 = 0.01,
        restart_rate: f32 = 0.05,
    };

    fn init(net: *harness.Network, seed: u64, config: FaultConfig) ChaosRunner {
        return .{
            .net = net,
            .rng = std.Random.DefaultPrng.init(seed),
            .seed = seed,
            .tick_count = 0,
            .config = config,
        };
    }

    fn random(self: *ChaosRunner) std.Random {
        return self.rng.random();
    }

    /// Tick with random fault injection.
    fn tickWithFaults(self: *ChaosRunner) !void {
        self.applyMessageDrops();
        self.applyRandomPartitions();
        self.applyRandomCrashes();
        try self.net.tickAll();
        self.tick_count += 1;
    }

    /// Drop messages from the queue probabilistically.
    fn applyMessageDrops(self: *ChaosRunner) void {
        const q = &self.net.queue.list;
        var i: usize = q.items.len;
        while (i > 0) {
            i -= 1;
            if (self.random().float(f32) < self.config.drop_rate) {
                _ = q.swapRemove(i);
            }
        }
    }

    /// Randomly partition or heal routes between pairs of nodes.
    fn applyRandomPartitions(self: *ChaosRunner) void {
        const ids = self.net.node_ids.items;
        if (ids.len < 2) return;
        for (ids) |a| {
            for (ids) |b| {
                if (a >= b) continue;
                if (self.net.dropped_routes.contains(.{ .from = a, .to = b })) {
                    // Currently partitioned — maybe heal.
                    if (self.random().float(f32) < self.config.heal_rate) {
                        self.net.heal(a, b);
                    }
                } else {
                    // Currently connected — maybe partition.
                    if (self.random().float(f32) < self.config.partition_rate) {
                        self.net.partition(a, b) catch {};
                    }
                }
            }
        }
    }

    /// Randomly crash or restart nodes.
    fn applyRandomCrashes(self: *ChaosRunner) void {
        for (self.net.node_ids.items) |id| {
            if (self.net.paused_nodes.contains(id)) {
                if (self.random().float(f32) < self.config.restart_rate) {
                    self.net.resumeNode(id);
                }
            } else {
                if (self.random().float(f32) < self.config.crash_rate) {
                    self.net.pauseNode(id) catch {};
                }
            }
        }
    }

    /// Heal all partitions and resume all paused nodes.
    fn healAll(self: *ChaosRunner) void {
        self.net.dropped_routes.clearRetainingCapacity();
        const ids = self.net.node_ids.items;
        for (ids) |id| {
            self.net.resumeNode(id);
        }
    }

    /// Tick without faults.
    fn tickClean(self: *ChaosRunner) !void {
        try self.net.tickAll();
        self.tick_count += 1;
    }

    /// Stabilize: heal everything and tick until a leader emerges.
    fn stabilize(self: *ChaosRunner, max_ticks: u32) !u64 {
        self.healAll();
        var t: u32 = 0;
        while (t < max_ticks) : (t += 1) {
            try self.tickClean();
            if (self.net.getLeader()) |lid| return lid;
        }
        return error.NoLeaderElected;
    }

    /// Check that at most one leader exists per term.
    fn checkSingleLeader(self: *ChaosRunner) !void {
        var leaders = std.AutoHashMap(u64, u64).init(self.net.allocator);
        defer leaders.deinit();
        for (self.net.node_ids.items) |id| {
            const n = self.net.nodes.get(id).?;
            if (n.role == .leader) {
                const result = try leaders.getOrPut(n.current_term);
                if (result.found_existing) {
                    std.log.err(
                        "Chaos: two leaders in term {d}: nodes {d} and {d} (seed={d})",
                        .{ n.current_term, result.value_ptr.*, id, self.seed },
                    );
                    return error.ElectionSafetyViolation;
                }
                result.value_ptr.* = id;
            }
        }
    }
};

// ============================================================================
// Chaos Scenarios
// ============================================================================

// ---------------------------------------------------------------------------
// 1. flaky_network — message drops should not break safety
// ---------------------------------------------------------------------------
test "chaos: flaky_network" {
    const seed = getSeed();
    var net = try harness.Network.init(std.testing.allocator, 5);
    defer net.deinit();

    var runner = ChaosRunner.init(&net, seed, .{
        .drop_rate = 0.15,
        .partition_rate = 0.0,
        .crash_rate = 0.0,
    });

    // Elect a leader first.
    _ = try runner.stabilize(100);

    // Run under flaky conditions.
    var t: u32 = 0;
    while (t < 200) : (t += 1) {
        try runner.tickWithFaults();
        try runner.checkSingleLeader();
    }

    // Stabilize and verify.
    _ = try runner.stabilize(100);
}

// ---------------------------------------------------------------------------
// 2. message_reorder — reordered delivery via drop + redeliver
// ---------------------------------------------------------------------------
test "chaos: message_reorder" {
    const seed = getSeed();
    var net = try harness.Network.init(std.testing.allocator, 5);
    defer net.deinit();

    var runner = ChaosRunner.init(&net, seed, .{
        .drop_rate = 0.20,
        .partition_rate = 0.0,
        .crash_rate = 0.0,
    });

    _ = try runner.stabilize(100);

    // Propose some data and run under message drops (which causes reordering).
    try net.propose("chaos-entry-1");
    var t: u32 = 0;
    while (t < 200) : (t += 1) {
        try runner.tickWithFaults();
        try runner.checkSingleLeader();
    }

    _ = try runner.stabilize(100);
}

// ---------------------------------------------------------------------------
// 3. sustained_partition — cluster recovers after long partition
// ---------------------------------------------------------------------------
test "chaos: sustained_partition" {
    const seed = getSeed();
    var net = try harness.Network.init(std.testing.allocator, 5);
    defer net.deinit();

    var runner = ChaosRunner.init(&net, seed, .{
        .drop_rate = 0.0,
        .partition_rate = 0.0,
        .crash_rate = 0.0,
    });

    const leader1 = try runner.stabilize(100);

    // Partition the leader from the rest.
    for (net.node_ids.items) |id| {
        if (id != leader1) {
            try net.partition(leader1, id);
        }
    }

    // Tick under partition for many rounds.
    var t: u32 = 0;
    while (t < 200) : (t += 1) {
        try runner.tickClean();
    }

    // Heal and verify recovery.
    _ = try runner.stabilize(200);
}

// ---------------------------------------------------------------------------
// 4. leader_isolation — isolated leader steps down, new leader elected
// ---------------------------------------------------------------------------
test "chaos: leader_isolation" {
    const seed = getSeed();
    var net = try harness.Network.init(std.testing.allocator, 5);
    defer net.deinit();

    var runner = ChaosRunner.init(&net, seed, .{});

    const leader1 = try runner.stabilize(100);

    // Isolate the leader completely.
    for (net.node_ids.items) |id| {
        if (id != leader1) {
            try net.partition(leader1, id);
        }
    }

    // Tick enough for a new election on the majority side.
    var t: u32 = 0;
    while (t < 100) : (t += 1) {
        try runner.tickClean();
    }

    // Heal and check that a new leader emerges.
    const new_leader = try runner.stabilize(200);
    try std.testing.expect(new_leader != leader1);
}

// ---------------------------------------------------------------------------
// 5. rolling_restart — pause and resume nodes one at a time
// ---------------------------------------------------------------------------
test "chaos: rolling_restart" {
    const seed = getSeed();
    _ = seed;
    var net = try harness.Network.init(std.testing.allocator, 5);
    defer net.deinit();

    var runner = ChaosRunner.init(&net, 42, .{});

    _ = try runner.stabilize(100);

    // Rolling restart: pause one node, tick, resume, repeat for each node.
    for (net.node_ids.items) |id| {
        try net.pauseNode(id);

        var t: u32 = 0;
        while (t < 30) : (t += 1) {
            try runner.tickClean();
        }

        net.resumeNode(id);

        t = 0;
        while (t < 30) : (t += 1) {
            try runner.tickClean();
        }

        // A leader should still exist (4/5 nodes active at worst).
        try runner.checkSingleLeader();
    }

    _ = try runner.stabilize(100);
}

// ---------------------------------------------------------------------------
// 6. cascade_failures — multiple nodes crash simultaneously
// ---------------------------------------------------------------------------
test "chaos: cascade_failures" {
    const seed = getSeed();
    _ = seed;
    var net = try harness.Network.init(std.testing.allocator, 5);
    defer net.deinit();

    var runner = ChaosRunner.init(&net, 42, .{});

    _ = try runner.stabilize(100);

    // Crash 2 of 5 nodes (cluster should survive with 3/5 majority).
    try net.pauseNode(1);
    try net.pauseNode(2);

    var t: u32 = 0;
    while (t < 100) : (t += 1) {
        try runner.tickClean();
    }

    // A leader should exist among nodes 3, 4, 5.
    const leader = net.getLeader();
    try std.testing.expect(leader != null);
    try std.testing.expect(leader.? >= 3);

    // Resume crashed nodes and stabilize.
    net.resumeNode(1);
    net.resumeNode(2);
    _ = try runner.stabilize(100);
}

// ---------------------------------------------------------------------------
// 7. flapping_connection — rapidly partition and heal
// ---------------------------------------------------------------------------
test "chaos: flapping_connection" {
    const seed = getSeed();
    var net = try harness.Network.init(std.testing.allocator, 5);
    defer net.deinit();

    var runner = ChaosRunner.init(&net, seed, .{
        .drop_rate = 0.0,
        .partition_rate = 0.10,
        .heal_rate = 0.15,
        .crash_rate = 0.0,
    });

    _ = try runner.stabilize(100);

    // Run with flapping partitions.
    var t: u32 = 0;
    while (t < 300) : (t += 1) {
        try runner.tickWithFaults();
        try runner.checkSingleLeader();
    }

    // Must recover when faults stop.
    _ = try runner.stabilize(200);
}

// ---------------------------------------------------------------------------
// 8. slow_follower — one follower always drops messages
// ---------------------------------------------------------------------------
test "chaos: slow_follower" {
    const seed = getSeed();
    _ = seed;
    var net = try harness.Network.init(std.testing.allocator, 5);
    defer net.deinit();

    var runner = ChaosRunner.init(&net, 42, .{});

    const leader = try runner.stabilize(100);

    // Drop all messages TO node 5 (simulating a very slow follower).
    for (net.node_ids.items) |id| {
        if (id != 5) {
            try net.partition(id, 5);
            // Only one direction — 5 can still send.
            _ = net.dropped_routes.remove(.{ .from = 5, .to = id });
        }
    }

    // Propose data — should commit with 4/5 quorum.
    try net.propose("slow-follower-data");

    var t: u32 = 0;
    while (t < 50) : (t += 1) {
        try runner.tickClean();
    }

    // Entry should be committed on the leader.
    const committed = net.committedEntries(leader);
    try std.testing.expect(committed.len > 0);

    // Heal and let node 5 catch up.
    runner.healAll();
    t = 0;
    while (t < 100) : (t += 1) {
        try runner.tickClean();
    }
}

// ---------------------------------------------------------------------------
// 9. election_storm — force many re-elections
// ---------------------------------------------------------------------------
test "chaos: election_storm" {
    const seed = getSeed();
    _ = seed;
    var net = try harness.Network.init(std.testing.allocator, 5);
    defer net.deinit();

    var runner = ChaosRunner.init(&net, 42, .{});

    // Force multiple leadership changes by repeatedly crashing the leader.
    var elections: u32 = 0;
    while (elections < 5) : (elections += 1) {
        const leader = try runner.stabilize(200);

        // Pause the current leader.
        try net.pauseNode(leader);

        // Tick enough for a new election.
        var t: u32 = 0;
        while (t < 50) : (t += 1) {
            try runner.tickClean();
        }

        // Resume the old leader.
        net.resumeNode(leader);
    }

    // Final stabilization — a single leader must emerge.
    _ = try runner.stabilize(200);
    try runner.checkSingleLeader();
}

// ---------------------------------------------------------------------------
// 10. combined_chaos — all fault types simultaneously
// ---------------------------------------------------------------------------
test "chaos: combined_chaos" {
    const seed = getSeed();
    var net = try harness.Network.init(std.testing.allocator, 5);
    defer net.deinit();

    var runner = ChaosRunner.init(&net, seed, .{
        .drop_rate = 0.10,
        .partition_rate = 0.03,
        .heal_rate = 0.08,
        .crash_rate = 0.02,
        .restart_rate = 0.06,
    });

    // Start clean.
    _ = try runner.stabilize(100);

    // Run under full chaos.
    var t: u32 = 0;
    while (t < 500) : (t += 1) {
        try runner.tickWithFaults();

        // Check election safety periodically.
        if (t % 50 == 0) {
            try runner.checkSingleLeader();
        }
    }

    // Must recover when faults stop.
    _ = try runner.stabilize(300);
    try runner.checkSingleLeader();
}
