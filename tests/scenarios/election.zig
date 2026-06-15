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
    } });

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
    } });

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

// ---------------------------------------------------------------------------
// Scenario 7: candidate_demotes_on_quorum_prevote_rejection (RC-B)
// ---------------------------------------------------------------------------
//
// Regression for the k8s pre_vote frozen-term split-vote livelock
// (docs/bugs/2026-06-04-cluster-raft-no-stable-quorum-writes-time-out.md).
//
// etcd's stepCandidate demotes to follower on quorum.VoteLost. Without that,
// a candidate denied by a quorum stays a candidate (only re-electing on its
// timer), so a node that lost an election keeps cycling instead of stepping
// down to grant a competitor's vote — the consensus-layer half of the
// no-leader all-candidate livelock.

/// Drive a single node into a pre-vote candidacy by ticking it in isolation
/// until its (randomized) election timeout fires `startElection(pre_vote)`.
fn driveToPreVoteCandidate(n: *raft.node.RaftNode) !void {
    var t: u32 = 0;
    while (t < 200 and n.role != .candidate) : (t += 1) {
        try n.tick(null);
    }
}

test "scenario: candidate_demotes_on_quorum_prevote_rejection" {
    var net = try harness.Network.init(std.testing.allocator, 3);
    defer net.deinit();

    const n1 = net.nodes.get(1).?;
    try driveToPreVoteCandidate(n1);

    try std.testing.expectEqual(raft.Role.candidate, n1.role);
    try std.testing.expect(n1.is_pre_vote_election);
    const term_before = n1.current_term;

    // A quorum (nodes 2 and 3) rejects the pre-vote at the same term — no
    // higher-term signal, so the only thing that can step the candidate down
    // is the VoteLost path.
    try n1.step(2, raft.Message{ .request_vote_response = .{
        .term = term_before,
        .vote_granted = false,
        .pre_vote = true,
    } });
    try n1.step(3, raft.Message{ .request_vote_response = .{
        .term = term_before,
        .vote_granted = false,
        .pre_vote = true,
    } });

    // etcd parity: a quorum-denied candidate becomes a follower at the SAME
    // term (pre-vote never incremented it), resetting its randomized election
    // timer so it can grant a competitor's vote.
    try std.testing.expectEqual(raft.Role.follower, n1.role);
    try std.testing.expectEqual(term_before, n1.current_term);
    try std.testing.expect(!n1.is_pre_vote_election);
}

// ---------------------------------------------------------------------------
// Scenario 7b: stuck candidate demotes on a TIMER (no quorum needed) (RC-B it.2)
// ---------------------------------------------------------------------------
//
// v0.1.8's VoteLost demotion only fires when a quorum of peers REJECTS the
// candidate. A transport-degraded / isolated candidate (or a pre-vote
// mutual-grant lockstep where peers grant each other's pre-votes so no rejection
// quorum ever forms) collects neither a winning nor a rejecting quorum, and
// without a timer it stays a candidate indefinitely. After K consecutive failed
// elections the candidate must step down to follower + back off, so a lockstep
// peer can get ahead and win.

test "scenario: stuck candidate demotes to follower after consecutive failed elections" {
    var net = try harness.Network.init(std.testing.allocator, 3);
    defer net.deinit();

    const n1 = net.nodes.get(1).?;
    // Isolate node 1 entirely: it never hears a grant, a rejection, or a leader.
    // (pause 2/3 so only node 1 advances; partition so nothing is delivered.)
    try net.pauseNode(2);
    try net.pauseNode(3);
    try net.partition(1, 2);
    try net.partition(1, 3);

    var saw_candidate = false;
    var saw_demote_to_follower = false;
    var t: u32 = 0;
    while (t < 600) : (t += 1) {
        try net.tickAll();
        if (n1.role == .candidate) saw_candidate = true;
        // After becoming a candidate, an isolated node can only return to
        // follower via the timer-based demotion (no higher-term message exists).
        if (saw_candidate and n1.role == .follower) {
            saw_demote_to_follower = true;
            break;
        }
    }

    try std.testing.expect(saw_candidate);
    // WITHOUT the fix: an isolated pre-vote candidate stays a candidate forever
    // (term frozen, no demotion path) -> saw_demote_to_follower stays false.
    try std.testing.expect(saw_demote_to_follower);
}

test "scenario: candidate_demotes_on_quorum_realvote_rejection" {
    var net = try harness.Network.init(std.testing.allocator, 3);
    defer net.deinit();

    const n1 = net.nodes.get(1).?;
    try driveToPreVoteCandidate(n1);
    try std.testing.expectEqual(raft.Role.candidate, n1.role);
    const prevote_term = n1.current_term;

    // Win the pre-vote (a single grant gives self+1 = 2/3 quorum), promoting
    // node 1 to a REAL candidate at prevote_term + 1.
    try n1.step(2, raft.Message{ .request_vote_response = .{
        .term = prevote_term,
        .vote_granted = true,
        .pre_vote = true,
    } });
    try std.testing.expectEqual(raft.Role.candidate, n1.role);
    try std.testing.expect(!n1.is_pre_vote_election);
    const real_term = n1.current_term;
    try std.testing.expectEqual(prevote_term + 1, real_term);

    // A quorum now rejects the real election at the same term.
    try n1.step(2, raft.Message{ .request_vote_response = .{
        .term = real_term,
        .vote_granted = false,
        .pre_vote = false,
    } });
    try n1.step(3, raft.Message{ .request_vote_response = .{
        .term = real_term,
        .vote_granted = false,
        .pre_vote = false,
    } });

    // VoteLost demotion: follower at the same (real) term.
    try std.testing.expectEqual(raft.Role.follower, n1.role);
    try std.testing.expectEqual(real_term, n1.current_term);
}
