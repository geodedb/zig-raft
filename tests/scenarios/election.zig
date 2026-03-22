// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-2025 CodePros

//! Election scenario tests (spec section 8, Tier 2).
//!
//! These tests exercise leader election, pre-vote protocol, split-vote
//! resolution, step-down on stale term, double-vote prevention, and
//! no-op entry semantics using the deterministic test harness.

const std = @import("std");
const raft = @import("raft");
const harness = @import("harness");
const checker = @import("checker");

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Collect node pointers from the Network into a heap-allocated slice
/// suitable for passing to `Checker.checkAll`.
fn collectNodePtrs(
    net: *harness.Network,
    allocator: std.mem.Allocator,
) ![]*raft.node.RaftNode {
    const ptrs = try allocator.alloc(*raft.node.RaftNode, net.node_ids.items.len);
    for (net.node_ids.items, 0..) |id, i| {
        ptrs[i] = net.nodes.get(id).?;
    }
    return ptrs;
}

/// Count nodes that are currently in the leader role.
fn countLeaders(net: *harness.Network) usize {
    var count: usize = 0;
    for (net.node_ids.items) |id| {
        const n = net.nodes.get(id).?;
        if (n.role == .leader) count += 1;
    }
    return count;
}

/// Return the ID of the first follower node found, or null.
fn findFollower(net: *harness.Network) ?u64 {
    for (net.node_ids.items) |id| {
        const n = net.nodes.get(id).?;
        if (n.role == .follower) return id;
    }
    return null;
}

// ---------------------------------------------------------------------------
// Scenario 1: basic_leader_election
// ---------------------------------------------------------------------------

test "scenario: basic_leader_election" {
    var net = try harness.Network.init(std.testing.allocator, 5);
    defer net.deinit();

    // Wait for a leader within 100 ticks.
    const leader_id = try net.waitForLeader(100);
    try std.testing.expect(leader_id >= 1 and leader_id <= 5);

    // Exactly one leader must exist.
    try std.testing.expectEqual(@as(usize, 1), countLeaders(&net));

    // Run all four Raft invariant checks.
    const ptrs = try collectNodePtrs(&net, std.testing.allocator);
    defer std.testing.allocator.free(ptrs);
    try checker.Checker.checkAll(ptrs);
}

// ---------------------------------------------------------------------------
// Scenario 2: split_vote_resolves
// ---------------------------------------------------------------------------

test "scenario: split_vote_resolves" {
    // Build a 5-node network with deterministic seeds. Each node gets
    // rng_seed = id (1..5) which produces staggered but deterministic
    // timeouts. Split votes can still occur under the right conditions.
    // We tick aggressively to verify that a leader eventually emerges
    // even when split votes happen.
    var net = try harness.Network.init(std.testing.allocator, 5);
    defer net.deinit();

    // Tick generously — even with split votes a leader must eventually emerge.
    var leader_found = false;
    var t: u32 = 0;
    while (t < 300) : (t += 1) {
        try net.tickAll();
        if (net.getLeader() != null) {
            leader_found = true;
            break;
        }
    }

    try std.testing.expect(leader_found);
    try std.testing.expectEqual(@as(usize, 1), countLeaders(&net));
}

// ---------------------------------------------------------------------------
// Scenario 3: pre_vote_prevents_disruption
// ---------------------------------------------------------------------------

test "scenario: pre_vote_prevents_disruption" {
    var net = try harness.Network.init(std.testing.allocator, 5);
    defer net.deinit();

    // Elect a leader.
    const original_leader = try net.waitForLeader(100);

    // Pick a non-leader node to partition.
    var isolated_id: u64 = 0;
    for (net.node_ids.items) |id| {
        if (id != original_leader) {
            isolated_id = id;
            break;
        }
    }
    try std.testing.expect(isolated_id != 0);

    // Partition the isolated node from every other node.
    for (net.node_ids.items) |id| {
        if (id != isolated_id) {
            try net.partition(isolated_id, id);
        }
    }

    // Tick so the isolated node's election timeout fires repeatedly.
    // Without pre-vote it would increment its term; with pre-vote it should
    // not disrupt the cluster upon rejoining.
    var i: u32 = 0;
    while (i < 60) : (i += 1) {
        try net.tickAll();
    }

    // Heal the partition.
    for (net.node_ids.items) |id| {
        if (id != isolated_id) {
            net.heal(isolated_id, id);
        }
    }

    // Let the cluster stabilize.
    i = 0;
    while (i < 30) : (i += 1) {
        try net.tickAll();
    }

    // The original leader should still be leading — pre-vote prevented
    // the isolated node from bumping the term.
    const current_leader = net.getLeader();
    try std.testing.expect(current_leader != null);
    try std.testing.expectEqual(original_leader, current_leader.?);

    // Invariant checks.
    const ptrs = try collectNodePtrs(&net, std.testing.allocator);
    defer std.testing.allocator.free(ptrs);
    try checker.Checker.checkAll(ptrs);
}

// ---------------------------------------------------------------------------
// Scenario 4: leader_steps_down_on_stale_term
// ---------------------------------------------------------------------------

test "scenario: leader_steps_down_on_stale_term" {
    var net = try harness.Network.init(std.testing.allocator, 3);
    defer net.deinit();

    const leader_id = try net.waitForLeader(100);
    const leader = net.nodes.get(leader_id).?;
    const leader_term = leader.current_term;

    // Send a RequestVote with a much higher term directly to the leader.
    try leader.step(99, raft.Message{ .request_vote = .{
        .term = leader_term + 10,
        .candidate_id = 99,
        .last_log_index = 0,
        .last_log_term = 0,
        .pre_vote = false,
    } }, null);

    // The leader must have stepped down to follower.
    try std.testing.expectEqual(raft.Role.follower, leader.role);
    // Its term should have advanced to the higher term.
    try std.testing.expectEqual(leader_term + 10, leader.current_term);
}

// ---------------------------------------------------------------------------
// Scenario 5: no_double_vote_same_term
// ---------------------------------------------------------------------------

test "scenario: no_double_vote_same_term" {
    var net = try harness.Network.init(std.testing.allocator, 3);
    defer net.deinit();

    _ = try net.waitForLeader(100);

    // Find a follower.
    const follower_id = findFollower(&net) orelse return error.NoFollower;
    const follower = net.nodes.get(follower_id).?;

    // The follower has already voted this term (for the candidate that won).
    const current_term = follower.current_term;
    try std.testing.expect(follower.voted_for != null);

    // Send a RequestVote from a fake node 99 for the same term.
    try follower.step(99, raft.Message{ .request_vote = .{
        .term = current_term,
        .candidate_id = 99,
        .last_log_index = follower.log.lastIndex(),
        .last_log_term = follower.log.lastTerm(),
        .pre_vote = false,
    } }, null);

    // The follower must NOT have changed its vote.
    try std.testing.expect(follower.voted_for.? != 99);

    // Verify a rejection was enqueued in the message queue.
    // The harness collects outbound messages in net.queue.list.
    var found_rejection = false;
    for (net.queue.list.items) |pm| {
        if (pm.from == follower_id and pm.to == 99) {
            switch (pm.msg) {
                .request_vote_response => |rvr| {
                    if (!rvr.vote_granted and !rvr.pre_vote) {
                        found_rejection = true;
                    }
                },
                else => {},
            }
        }
    }
    try std.testing.expect(found_rejection);
}

// ---------------------------------------------------------------------------
// Scenario 6: noop_entry_enables_prior_term_commit
// ---------------------------------------------------------------------------

test "scenario: noop_entry_enables_prior_term_commit" {
    var net = try harness.Network.init(std.testing.allocator, 3);
    defer net.deinit();

    const leader_id = try net.waitForLeader(100);
    const leader = net.nodes.get(leader_id).?;

    // The leader's log must contain a no-op entry.
    const last_idx = leader.log.lastIndex();
    try std.testing.expect(last_idx >= 1);

    // Walk the log backwards to find a noop entry at the leader's current term.
    var found_noop = false;
    var idx: u64 = last_idx;
    while (idx >= 1) : (idx -= 1) {
        const entries = try leader.log.slice(idx, idx + 1);
        defer net.allocator.free(entries);
        if (entries.len > 0 and entries[0].kind == .noop and entries[0].term == leader.current_term) {
            found_noop = true;
            break;
        }
        if (idx == 1) break;
    }
    try std.testing.expect(found_noop);

    // Run invariant checks.
    const ptrs = try collectNodePtrs(&net, std.testing.allocator);
    defer std.testing.allocator.free(ptrs);
    try checker.Checker.checkAll(ptrs);
}
