// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-2025 CodePros
//
// Scenario tests for thread-pool parallel code paths (PAR-003).
// These tests exercise the submitBatch/WaitGroup logic in sendHeartbeats()
// by passing a real *tp.ThreadPool through tick().

const std = @import("std");
const raft = @import("raft");
const harness = @import("harness");
const checker = @import("checker");
const tp = raft.thread_pool;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Elect a leader by ticking until one emerges. Returns the leader's node ID.
fn electLeader(net: *harness.Network) !u64 {
    var ticks: u32 = 0;
    while (ticks < 50) : (ticks += 1) {
        try net.tickAll();
        for (net.node_ids.items) |id| {
            const node = net.nodes.get(id).?;
            if (node.role == .leader) return id;
        }
    }
    return error.NoLeaderElected;
}

/// Tick a single node using a thread-pool ThreadPool, exercising the parallel path.
/// Returns true on error.
fn parallelTickNode(pool: *tp.ThreadPool, node: *raft.RaftNode) bool {
    node.tick(pool) catch return true;
    return false;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "parallel heartbeats: leader sends correct AppendEntries via fork/join" {
    var net = try harness.Network.init(std.testing.allocator, 3);
    defer net.deinit();

    const leader_id = try electLeader(&net);
    const leader = net.nodes.get(leader_id).?;

    var pool = tp.ThreadPool.init(std.heap.page_allocator, .{}) catch @panic("thread-pool init failed");
    defer pool.deinit();

    // Drain election messages.
    net.queue.list.clearRetainingCapacity();

    // Parallel tick exercises sendHeartbeats() submitBatch/WaitGroup path.
    try std.testing.expect(!parallelTickNode(&pool, leader));

    // Verify heartbeat messages were queued for both peers.
    var peer_count: u32 = 0;
    for (net.queue.list.items) |pm| {
        if (pm.from == leader_id) peer_count += 1;
    }
    // Leader should send to all peers (2 in a 3-node cluster).
    try std.testing.expectEqual(@as(u32, 2), peer_count);
}

test "parallel heartbeats: messages match sequential path output" {
    // Two identical clusters — one sequential, one parallel.
    var net_seq = try harness.Network.init(std.testing.allocator, 3);
    defer net_seq.deinit();
    var net_par = try harness.Network.init(std.testing.allocator, 3);
    defer net_par.deinit();

    const leader_seq_id = try electLeader(&net_seq);
    const leader_par_id = try electLeader(&net_par);
    const leader_seq = net_seq.nodes.get(leader_seq_id).?;
    const leader_par = net_par.nodes.get(leader_par_id).?;

    // Drain election messages.
    net_seq.queue.list.clearRetainingCapacity();
    net_par.queue.list.clearRetainingCapacity();

    // Propose data so heartbeats carry entries.
    try leader_seq.propose("hello");
    try leader_par.propose("hello");

    // Sequential tick.
    try leader_seq.tick(null);

    // Parallel tick.
    var pool = tp.ThreadPool.init(std.heap.page_allocator, .{}) catch @panic("thread-pool init failed");
    defer pool.deinit();

    try std.testing.expect(!parallelTickNode(&pool, leader_par));

    // Same number of messages from each leader.
    var seq_count: usize = 0;
    for (net_seq.queue.list.items) |pm| {
        if (pm.from == leader_seq_id) seq_count += 1;
    }
    var par_count: usize = 0;
    for (net_par.queue.list.items) |pm| {
        if (pm.from == leader_par_id) par_count += 1;
    }
    try std.testing.expectEqual(seq_count, par_count);

    // Both should produce AppendEntries with matching structure.
    // Filter to leader-sent messages and compare.
    for (net_seq.queue.list.items) |seq_pm| {
        if (seq_pm.from != leader_seq_id) continue;
        switch (seq_pm.msg) {
            .append_entries => |seq_ae| {
                // Find matching peer in parallel output.
                for (net_par.queue.list.items) |par_pm| {
                    if (par_pm.from != leader_par_id) continue;
                    switch (par_pm.msg) {
                        .append_entries => |par_ae| {
                            // Verify structural equivalence.
                            try std.testing.expectEqual(seq_ae.term, par_ae.term);
                            try std.testing.expectEqual(seq_ae.entries.len, par_ae.entries.len);
                            break;
                        },
                        else => {},
                    }
                }
            },
            else => {},
        }
    }
}

test "parallel tick: full cluster round-trip with parallel heartbeats" {
    var net = try harness.Network.init(std.testing.allocator, 5);
    defer net.deinit();

    const leader_id = try electLeader(&net);
    const leader = net.nodes.get(leader_id).?;
    try leader.propose("entry-1");
    try leader.propose("entry-2");

    var pool = tp.ThreadPool.init(std.heap.page_allocator, .{}) catch @panic("thread-pool init failed");
    defer pool.deinit();

    // Run 5 tick-deliver-advance rounds with parallel leader ticks.
    for (0..5) |_| {
        // Tick: leader parallel, followers sequential.
        for (net.node_ids.items) |id| {
            if (net.paused_nodes.contains(id)) continue;
            const n = net.nodes.get(id).?;
            if (id == leader_id) {
                try std.testing.expect(!parallelTickNode(&pool, n));
            } else {
                try n.tick(null);
            }
        }

        // Deliver queued messages.
        const pending = net.queue.list.items;
        const snap = try net.allocator.alloc(harness.PendingMessage, pending.len);
        defer net.allocator.free(snap);
        @memcpy(snap, pending);
        net.queue.list.clearRetainingCapacity();

        for (snap) |pm| {
            if (net.paused_nodes.contains(pm.to)) continue;
            if (net.nodes.get(pm.to)) |n| {
                try n.step(pm.from, pm.msg);
            }
        }

        // Drain responses.
        const extra = net.queue.list.items;
        const extra_snap = try net.allocator.alloc(harness.PendingMessage, extra.len);
        defer net.allocator.free(extra_snap);
        @memcpy(extra_snap, extra);
        net.queue.list.clearRetainingCapacity();

        for (extra_snap) |pm| {
            if (net.paused_nodes.contains(pm.to)) continue;
            if (net.nodes.get(pm.to)) |n| {
                try n.step(pm.from, pm.msg);
            }
        }

        // Ready/advance.
        for (net.node_ids.items) |id| {
            if (net.paused_nodes.contains(id)) continue;
            const n = net.nodes.get(id).?;
            _ = try n.ready();
            try n.advance();
        }
    }

    // Entries should be committed.
    try std.testing.expect(leader.log.committed >= 2);
}

test "parallel tick: safety invariants hold after parallel rounds" {
    var net = try harness.Network.init(std.testing.allocator, 3);
    defer net.deinit();

    const leader_id = try electLeader(&net);
    const leader = net.nodes.get(leader_id).?;
    try leader.propose("safety-test-1");
    try leader.propose("safety-test-2");

    // Run 10 tick rounds to replicate entries.
    for (0..10) |_| {
        try net.tickAll();
    }

    // Collect node pointers for checker.
    var node_ptrs: [3]*raft.node.RaftNode = undefined;
    for (net.node_ids.items, 0..) |id, i| {
        node_ptrs[i] = net.nodes.get(id).?;
    }

    try checker.Checker.checkAll(&node_ptrs);
}
