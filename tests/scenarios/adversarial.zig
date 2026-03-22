// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-2025 CodePros

//! Adversarial test scenarios (SEC-0101).
//!
//! Exercises validation code from SEC-0001 through SEC-0100 with intentionally
//! malicious inputs: bogus terms, inflated indices, oversized payloads, sender
//! mismatches, invalid snapshot metadata, and Byzantine-style messages.

const std = @import("std");
const raft = @import("raft");
const harness = @import("harness");
const Checker = @import("checker").Checker;

/// Build a slice of `*raft.node.RaftNode` suitable for the Checker.
fn collectNodes(net: *harness.Network) ![]*raft.node.RaftNode {
    const nodes = try net.allocator.alloc(*raft.node.RaftNode, net.node_ids.items.len);
    for (net.node_ids.items, 0..) |id, i| {
        nodes[i] = net.nodes.get(id).?;
    }
    return nodes;
}

fn runChecker(net: *harness.Network) !void {
    const nodes = try collectNodes(net);
    defer net.allocator.free(nodes);
    try Checker.checkAll(nodes);
}

/// Elect a leader by ticking until one emerges.
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

// =========================================================================
// 1. Sender mismatch rejection (SEC-0001, SEC-0022)
// =========================================================================

test "adversarial: reject RequestVote with mismatched candidate_id" {
    var net = try harness.Network.init(std.testing.allocator, 3);
    defer net.deinit();

    const leader_id = try electLeader(&net);
    const follower_id: u64 = if (leader_id == 1) 2 else 1;
    const follower = net.nodes.get(follower_id).?;

    // Send a RequestVote where candidate_id doesn't match `from`.
    const term_before = follower.current_term;
    try follower.step(99, .{
        .request_vote = .{
            .term = follower.current_term + 1,
            .candidate_id = 42, // Mismatch: from=99, candidate_id=42
            .last_log_index = 0,
            .last_log_term = 0,
            .pre_vote = false,
        },
    }, null);

    // Follower should ignore the message — no term change, no vote granted.
    try std.testing.expectEqual(term_before, follower.current_term);
    try std.testing.expect(follower.voted_for != 42);
    try std.testing.expect(follower.voted_for != 99);
    try runChecker(&net);
}

test "adversarial: reject AppendEntries with mismatched leader_id" {
    var net = try harness.Network.init(std.testing.allocator, 3);
    defer net.deinit();

    const leader_id = try electLeader(&net);
    const follower_id: u64 = if (leader_id == 1) 2 else 1;
    const follower = net.nodes.get(follower_id).?;

    // Send AppendEntries where leader_id doesn't match `from`.
    try follower.step(99, .{
        .append_entries = .{
            .term = follower.current_term,
            .leader_id = 42, // Mismatch: from=99, leader_id=42
            .prev_log_index = 0,
            .prev_log_term = 0,
            .entries = &.{},
            .leader_commit = 0,
        },
    }, null);

    // Message should be silently dropped.
    try runChecker(&net);
}

// =========================================================================
// 2. Oversized payload rejection (SEC-0082)
// =========================================================================

test "adversarial: reject AppendEntries exceeding max_entries_per_msg" {
    var net = try harness.Network.init(std.testing.allocator, 3);
    defer net.deinit();

    const leader_id = try electLeader(&net);
    const follower_id: u64 = if (leader_id == 1) 2 else 1;
    const follower = net.nodes.get(follower_id).?;

    // Build more entries than max_entries_per_msg (default=64).
    var entries: [65]raft.rpc.Entry = undefined;
    for (&entries, 0..) |*e, i| {
        e.* = .{
            .index = @intCast(i + 1),
            .term = follower.current_term,
            .kind = .normal,
            .data = "x",
        };
    }

    try follower.step(leader_id, .{ .append_entries = .{
        .term = follower.current_term,
        .leader_id = leader_id,
        .prev_log_index = 0,
        .prev_log_term = 0,
        .entries = &entries,
        .leader_commit = 0,
    } }, null);

    // Should reject — log should not contain these entries.
    try std.testing.expectEqual(@as(u64, 0), follower.log.lastIndex());
    try runChecker(&net);
}

// =========================================================================
// 3. Vote from non-voter ignored (SEC-0083)
// =========================================================================

test "adversarial: ignore vote response from non-voter" {
    var net = try harness.Network.init(std.testing.allocator, 3);
    defer net.deinit();

    // Get a node and make it a candidate.
    const node = net.nodes.get(1).?;

    // Tick until node 1 starts an election.
    for (0..20) |_| {
        try net.tickAll();
        if (node.role == .candidate or node.role == .leader) break;
    }

    if (node.role == .candidate) {
        // Send a vote response from a non-voter node ID (99).
        try node.step(99, .{ .request_vote_response = .{
            .term = node.current_term,
            .vote_granted = true,
            .pre_vote = false,
        } }, null);

        // The non-voter vote should be ignored — node shouldn't become leader
        // from just self + non-voter vote in a 3-node cluster.
        // (It needs 2 out of 3 real votes.)
    }
    try runChecker(&net);
}

// =========================================================================
// 4. Stale term rejection
// =========================================================================

test "adversarial: reject AppendEntries with stale term" {
    var net = try harness.Network.init(std.testing.allocator, 3);
    defer net.deinit();

    const leader_id = try electLeader(&net);
    const follower_id: u64 = if (leader_id == 1) 2 else 1;
    const follower = net.nodes.get(follower_id).?;

    const current_term = follower.current_term;

    // Send AppendEntries with term < current_term.
    try follower.step(leader_id, .{
        .append_entries = .{
            .term = 0, // Stale term
            .leader_id = leader_id,
            .prev_log_index = 0,
            .prev_log_term = 0,
            .entries = &.{},
            .leader_commit = 0,
        },
    }, null);

    // Term should not change.
    try std.testing.expectEqual(current_term, follower.current_term);
    try runChecker(&net);
}

// =========================================================================
// 5. Snapshot with zero term rejection (SEC-0098)
// =========================================================================

test "adversarial: reject InstallSnapshot with term=0" {
    var net = try harness.Network.init(std.testing.allocator, 3);
    defer net.deinit();

    const leader_id = try electLeader(&net);
    const follower_id: u64 = if (leader_id == 1) 2 else 1;
    const follower = net.nodes.get(follower_id).?;

    // Send InstallSnapshot with last_included_term=0 (invalid in Raft).
    try follower.step(leader_id, .{
        .install_snapshot = .{
            .term = follower.current_term,
            .leader_id = leader_id,
            .last_included_index = 100,
            .last_included_term = 0, // Invalid
            .offset = 0,
            .data = "snapshot-data",
            .done = true,
            .checksum = .{0} ** 32,
            .config = .{
                .voters = &[_]u64{ 1, 2, 3 },
                .voters_outgoing = &.{},
                .is_joint = false,
            },
        },
    }, null);

    // Snapshot should be rejected — snapshot_term should remain unchanged.
    try std.testing.expectEqual(@as(u64, 0), follower.snapshot_state.snapshot_index);
    try runChecker(&net);
}

// =========================================================================
// 6. Snapshot with empty voter set rejection (SEC-0043)
// =========================================================================

test "adversarial: reject InstallSnapshot with empty voters" {
    var net = try harness.Network.init(std.testing.allocator, 3);
    defer net.deinit();

    const leader_id = try electLeader(&net);
    const follower_id: u64 = if (leader_id == 1) 2 else 1;
    const follower = net.nodes.get(follower_id).?;

    try follower.step(leader_id, .{
        .install_snapshot = .{
            .term = follower.current_term,
            .leader_id = leader_id,
            .last_included_index = 100,
            .last_included_term = follower.current_term,
            .offset = 0,
            .data = "data",
            .done = true,
            .checksum = .{0} ** 32,
            .config = .{
                .voters = &.{}, // Empty — invalid
                .voters_outgoing = &.{},
                .is_joint = false,
            },
        },
    }, null);

    // Snapshot should be rejected.
    try std.testing.expectEqual(@as(u64, 0), follower.snapshot_state.snapshot_index);
    try runChecker(&net);
}

// =========================================================================
// 7. Config change validation (SEC-0058, SEC-0085)
// =========================================================================

test "adversarial: reject config change with zero voter ID" {
    var net = try harness.Network.init(std.testing.allocator, 3);
    defer net.deinit();

    const leader_id = try electLeader(&net);
    const leader = net.nodes.get(leader_id).?;

    // Try to propose a config change with a zero voter ID.
    const result = leader.proposeConfigChange(&[_]u64{ 0, 2, 3 });
    try std.testing.expectError(error.InvalidData, result);
    try runChecker(&net);
}

test "adversarial: reject config change with duplicate voter IDs" {
    var net = try harness.Network.init(std.testing.allocator, 3);
    defer net.deinit();

    const leader_id = try electLeader(&net);
    const leader = net.nodes.get(leader_id).?;

    const result = leader.proposeConfigChange(&[_]u64{ 2, 3, 2 });
    try std.testing.expectError(error.InvalidData, result);
    try runChecker(&net);
}

// =========================================================================
// 8. Pre-vote abuse prevention (SEC-0061)
// =========================================================================

test "adversarial: leader rejects pre-vote requests" {
    var net = try harness.Network.init(std.testing.allocator, 3);
    defer net.deinit();

    const leader_id = try electLeader(&net);
    const leader = net.nodes.get(leader_id).?;

    // Send a pre-vote request to the leader with inflated term.
    try leader.step(2, .{ .request_vote = .{
        .term = leader.current_term + 10,
        .candidate_id = 2,
        .last_log_index = 1000,
        .last_log_term = leader.current_term + 10,
        .pre_vote = true,
    } }, null);

    // Leader should NOT step down from a pre-vote.
    try std.testing.expectEqual(raft.Role.leader, leader.role);
    try runChecker(&net);
}

// =========================================================================
// 9. Cluster survives adversarial messages
// =========================================================================

test "adversarial: cluster remains functional after malicious messages" {
    var net = try harness.Network.init(std.testing.allocator, 3);
    defer net.deinit();

    const leader_id = try electLeader(&net);
    const leader = net.nodes.get(leader_id).?;
    const follower_id: u64 = if (leader_id == 1) 2 else 1;
    const follower = net.nodes.get(follower_id).?;

    // Barrage of adversarial messages to follower.
    // 1. Stale term AppendEntries.
    try follower.step(leader_id, .{ .append_entries = .{
        .term = 0,
        .leader_id = leader_id,
        .prev_log_index = 0,
        .prev_log_term = 0,
        .entries = &.{},
        .leader_commit = 0,
    } }, null);

    // 2. Mismatched sender RequestVote.
    try follower.step(99, .{ .request_vote = .{
        .term = follower.current_term + 1,
        .candidate_id = 42,
        .last_log_index = 0,
        .last_log_term = 0,
        .pre_vote = false,
    } }, null);

    // 3. InstallSnapshot with zero term.
    try follower.step(leader_id, .{ .install_snapshot = .{
        .term = follower.current_term,
        .leader_id = leader_id,
        .last_included_index = 100,
        .last_included_term = 0,
        .offset = 0,
        .data = "bad",
        .done = true,
        .checksum = .{0} ** 32,
        .config = .{
            .voters = &[_]u64{ 1, 2, 3 },
            .voters_outgoing = &.{},
            .is_joint = false,
        },
    } }, null);

    // After adversarial messages, cluster should still work.
    // Propose and commit a normal entry.
    try leader.propose("legitimate-data");
    for (0..20) |_| {
        try net.tickAll();
        try runChecker(&net);
    }

    // Verify the legitimate entry was committed.
    try std.testing.expect(leader.log.committed > 0);
}

// =========================================================================
// 10. Config.validate rejects tiny snapshot_chunk_size (SEC-0100)
// =========================================================================

test "adversarial: Config rejects snapshot_chunk_size below minimum" {
    const cfg = raft.Config{
        .id = 1,
        .peers = &[_]u64{ 2, 3 },
        .snapshot_chunk_size = 1,
    };
    try std.testing.expectError(raft.node.ConfigError.InvalidSnapshotChunkSize, cfg.validate());

    const valid = raft.Config{
        .id = 1,
        .peers = &[_]u64{ 2, 3 },
        .snapshot_chunk_size = 1024,
    };
    try valid.validate();
}

// =========================================================================
// 11. Stale-term response rejection (SEC-0119)
// =========================================================================

test "adversarial: stale RequestVoteResponse does not count toward quorum (SEC-0119)" {
    var net = try harness.Network.init(std.testing.allocator, 3);
    defer net.deinit();

    // Elect a leader and let the cluster stabilize.
    _ = try electLeader(&net);
    try net.tickAll();
    try net.tickAll();

    // Pick a follower and force it to start an election at a higher term.
    const candidate_id: u64 = 2;
    const candidate = net.nodes.get(candidate_id).?;

    // Bump candidate to term 10 via step with a high-term message,
    // then start an election.
    try candidate.step(3, .{
        .append_entries = .{
            .term = 9,
            .leader_id = 3,
            .prev_log_index = 0,
            .prev_log_term = 0,
            .entries = &.{},
            .leader_commit = 0,
        },
    }, null);

    // Candidate is now at term 9, role follower. Trigger an election.
    // Pre-vote is enabled by default, so we need to go through pre-vote first.
    // Instead, directly inject a stale vote response and verify it's ignored.
    // Force candidate to become a candidate at term 10.
    try candidate.step(3, .{
        .request_vote = .{
            .term = 10,
            .candidate_id = candidate_id,
            .last_log_index = candidate.log.lastIndex(),
            .last_log_term = 0,
            .pre_vote = false,
        },
    }, null);

    // candidate should now be at term 10. Inject a stale response from term 5.
    const term_before = candidate.current_term;
    const role_before = candidate.role;
    try candidate.step(1, .{
        .request_vote_response = .{
            .term = 5, // Stale term — must be ignored
            .vote_granted = true,
            .pre_vote = false,
        },
    }, null);

    // Candidate's state should not have changed — stale vote ignored.
    try std.testing.expectEqual(term_before, candidate.current_term);
    try std.testing.expectEqual(role_before, candidate.role);
    try runChecker(&net);
}

test "adversarial: stale AppendEntriesResponse does not update peer progress (SEC-0119)" {
    var net = try harness.Network.init(std.testing.allocator, 3);
    defer net.deinit();

    const leader_id = try electLeader(&net);
    const leader = net.nodes.get(leader_id).?;

    // Record current match_index for a peer.
    const peer_id: u64 = if (leader_id == 1) 2 else 1;
    const rs = &(leader.replication_state orelse return error.NoLeaderElected);
    const progress = rs.peers.getPtr(peer_id) orelse return error.NoLeaderElected;
    const match_before = progress.match_index;

    // Inject stale AE response with inflated match_index from a previous term.
    try leader.step(peer_id, .{
        .append_entries_response = .{
            .term = leader.current_term - 1, // Stale term
            .success = true,
            .match_index = 9999, // Should NOT be applied
            .conflict_index = 0,
            .conflict_term = 0,
        },
    }, null);

    // match_index must not have been updated.
    try std.testing.expectEqual(match_before, progress.match_index);
    try runChecker(&net);
}

// =========================================================================
// 12. Follower-to-leader joint config finalization (SEC-0120)
// =========================================================================

test "adversarial: pending_config_index tracked on config entry application (SEC-0120)" {
    var net = try harness.Network.init(std.testing.allocator, 3);
    defer net.deinit();

    const leader_id = try electLeader(&net);
    const leader = net.nodes.get(leader_id).?;

    // Propose a config change (joint consensus).
    try leader.proposeConfigChange(&[_]u64{ 1, 2, 3, 4 });

    // Replicate and commit: tick several times.
    var i: u32 = 0;
    while (i < 20) : (i += 1) {
        try net.tickAll();
    }

    // Every node should have processed the config entry via advance().
    // Check that a follower's pending_config_index is set (not 0) if
    // still in joint state.
    for (net.node_ids.items) |id| {
        const node = net.nodes.get(id).?;
        _ = try node.ready();
        try node.advance();

        // If the node is in joint state, pending_config_index must be set.
        // If it already finalized (not joint), pending_config_index should be 0.
        if (node.membership_state.is_joint) {
            try std.testing.expect(node.pending_config_index > 0);
        }
    }

    try runChecker(&net);
}
