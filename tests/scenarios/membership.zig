// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-2025 CodePros

//! Membership change scenario tests.
//!
//! Validates joint consensus mechanics: adding/removing servers,
//! leader step-down, concurrent config change rejection, and
//! automatic phase-2 finalization.

const std = @import("std");
const raft = @import("raft");
const harness = @import("harness");
const Checker = @import("checker").Checker;

const Network = harness.Network;

/// Helper: check whether `target` is present in `items`.
fn containsId(items: []const u64, target: u64) bool {
    for (items) |v| {
        if (v == target) return true;
    }
    return false;
}

/// Run all Raft invariant checks against the current network state.
fn runChecker(net: *Network) !void {
    const nodes = try net.allocator.alloc(*raft.node.RaftNode, net.node_ids.items.len);
    defer net.allocator.free(nodes);
    for (net.node_ids.items, 0..) |id, i| {
        nodes[i] = net.nodes.get(id).?;
    }
    try Checker.checkAll(nodes);
}

// ---------------------------------------------------------------------------
// 1. add_server_joint_consensus
// ---------------------------------------------------------------------------
test "membership: add server via joint consensus" {
    var net = try Network.init(std.testing.allocator, 3);
    defer net.deinit();

    const leader_id = try net.waitForLeader(50);
    const leader = net.nodes.get(leader_id).?;

    // Initial config: 3 voters.
    try std.testing.expectEqual(@as(usize, 3), leader.membership_state.voters.items.len);
    try std.testing.expect(!leader.membership_state.is_joint);

    // Propose adding node 4 (node 4 doesn't exist in the network -- that's OK,
    // we're testing the consensus protocol mechanics).
    try leader.proposeConfigChange(&.{ 1, 2, 3, 4 });

    // After proposal, leader enters joint consensus immediately.
    try std.testing.expect(leader.membership_state.is_joint);

    // Tick until the joint config commits and phase-2 auto-finalizes.
    var ticks: u32 = 0;
    while (ticks < 100) : (ticks += 1) {
        try net.tickAll();
        if (!leader.membership_state.is_joint) break;
    }

    // Joint consensus should have resolved.
    try std.testing.expect(!leader.membership_state.is_joint);

    // Final voter set should contain node 4.
    try std.testing.expectEqual(@as(usize, 4), leader.membership_state.voters.items.len);
    try std.testing.expect(containsId(leader.membership_state.voters.items, 4));

    try runChecker(&net);
}

// ---------------------------------------------------------------------------
// 2. remove_server_joint_consensus
// ---------------------------------------------------------------------------
test "membership: remove server via joint consensus" {
    var net = try Network.init(std.testing.allocator, 3);
    defer net.deinit();

    const leader_id = try net.waitForLeader(50);
    const leader = net.nodes.get(leader_id).?;

    // Remove one node (keep only two voters).
    // Build the new voter set excluding one non-leader node.
    var new_voters: [2]u64 = undefined;
    var idx: usize = 0;
    for (net.node_ids.items) |nid| {
        if (idx >= 2) break;
        // Keep the leader and one other node.
        if (nid == leader_id or idx == 0) {
            new_voters[idx] = nid;
            idx += 1;
        }
    }
    // If the leader was the first entry, fill the second slot with the next id.
    if (idx < 2) {
        for (net.node_ids.items) |nid| {
            if (nid != new_voters[0]) {
                new_voters[idx] = nid;
                idx += 1;
                break;
            }
        }
    }

    try leader.proposeConfigChange(&new_voters);

    // Tick until stable.
    var ticks: u32 = 0;
    while (ticks < 100) : (ticks += 1) {
        try net.tickAll();
        if (!leader.membership_state.is_joint) break;
    }

    try std.testing.expect(!leader.membership_state.is_joint);
    try std.testing.expectEqual(@as(usize, 2), leader.membership_state.voters.items.len);

    try runChecker(&net);
}

// ---------------------------------------------------------------------------
// 3. leader_removed_steps_down
// ---------------------------------------------------------------------------
test "membership: leader removed from config steps down" {
    var net = try Network.init(std.testing.allocator, 3);
    defer net.deinit();

    const leader_id = try net.waitForLeader(50);
    const leader = net.nodes.get(leader_id).?;

    // Build a new voter set that excludes the current leader.
    var new_voters: [2]u64 = undefined;
    var idx: usize = 0;
    for (net.node_ids.items) |nid| {
        if (nid != leader_id) {
            new_voters[idx] = nid;
            idx += 1;
            if (idx == 2) break;
        }
    }

    try leader.proposeConfigChange(&new_voters);

    // Tick until the old leader is no longer leader.
    var ticks: u32 = 0;
    while (ticks < 200) : (ticks += 1) {
        try net.tickAll();
        if (leader.role != .leader) break;
    }

    // The old leader should have stepped down to follower.
    try std.testing.expectEqual(raft.Role.follower, leader.role);

    // A new leader should eventually be elected among the remaining nodes.
    const new_leader_id = try net.waitForLeader(100);
    try std.testing.expect(new_leader_id != leader_id);
    try std.testing.expect(containsId(&new_voters, new_leader_id));

    try runChecker(&net);
}

// ---------------------------------------------------------------------------
// 4. config_change_pending_error
// ---------------------------------------------------------------------------
test "membership: concurrent config change returns error" {
    var net = try Network.init(std.testing.allocator, 3);
    defer net.deinit();

    const leader_id = try net.waitForLeader(50);
    const leader = net.nodes.get(leader_id).?;

    // Start a config change.
    try leader.proposeConfigChange(&.{ 1, 2, 3, 4 });

    // Immediately try another config change -- should fail.
    const result = leader.proposeConfigChange(&.{ 1, 2, 3, 5 });
    try std.testing.expectError(error.ConfigChangePending, result);

    try runChecker(&net);
}

// ---------------------------------------------------------------------------
// 5. joint_phase2_automatic
// ---------------------------------------------------------------------------
test "membership: joint phase-2 finalizes automatically" {
    var net = try Network.init(std.testing.allocator, 3);
    defer net.deinit();

    const leader_id = try net.waitForLeader(50);
    const leader = net.nodes.get(leader_id).?;

    try std.testing.expect(!leader.membership_state.is_joint);

    // Propose a config change.
    try leader.proposeConfigChange(&.{ 1, 2, 3, 4 });

    // Verify joint consensus is active.
    try std.testing.expect(leader.membership_state.is_joint);

    // Tick until phase-2 auto-finalizes (no consumer action needed).
    var saw_joint = false;
    var ticks: u32 = 0;
    while (ticks < 100) : (ticks += 1) {
        if (leader.membership_state.is_joint) saw_joint = true;
        try net.tickAll();
        // Once is_joint becomes false, phase-2 has completed.
        if (saw_joint and !leader.membership_state.is_joint) break;
    }

    // Confirm we observed the joint state and it resolved.
    try std.testing.expect(saw_joint);
    try std.testing.expect(!leader.membership_state.is_joint);

    // No pending change remains.
    try std.testing.expect(!leader.membership_state.pending_config_change);

    // Voters should reflect the new config.
    try std.testing.expectEqual(@as(usize, 4), leader.membership_state.voters.items.len);
    try std.testing.expect(containsId(leader.membership_state.voters.items, 4));

    try runChecker(&net);
}

// ---------------------------------------------------------------------------
// 6. no_double_commit_during_reconfig
// ---------------------------------------------------------------------------
test "membership: no_double_commit_during_reconfig" {
    var net = try Network.init(std.testing.allocator, 3);
    defer net.deinit();

    const leader_id = try net.waitForLeader(50);
    const leader = net.nodes.get(leader_id).?;

    // Propose a normal entry before the config change.
    try net.propose("before-reconfig");
    try net.waitForCommit(leader.log.lastIndex(), 50);

    // Start a config change (add node 4).
    try leader.proposeConfigChange(&.{ 1, 2, 3, 4 });
    try std.testing.expect(leader.membership_state.is_joint);

    // While in joint consensus, propose normal entries.
    try leader.propose("during-reconfig-1");
    try leader.propose("during-reconfig-2");

    // Tick until joint consensus resolves.
    var ticks: u32 = 0;
    while (ticks < 100) : (ticks += 1) {
        try net.tickAll();
        if (!leader.membership_state.is_joint) break;
    }
    try std.testing.expect(!leader.membership_state.is_joint);

    // Tick a few more times to ensure all entries commit.
    ticks = 0;
    while (ticks < 30) : (ticks += 1) {
        try net.tickAll();
    }

    // Verify no double commits: each log index appears exactly once
    // in the committed entries on every node.
    for (net.node_ids.items) |id| {
        const committed = net.committedEntries(id);
        var seen_indices = std.AutoHashMap(u64, void).init(std.testing.allocator);
        defer seen_indices.deinit();

        for (committed) |entry| {
            const result = try seen_indices.getOrPut(entry.index);
            // If this index was already seen, we have a double commit.
            try std.testing.expect(!result.found_existing);
        }
    }

    // Verify the expected number of normal entries committed.
    // We proposed 3 normal entries (before-reconfig, during-reconfig-1, during-reconfig-2).
    // There are also noop entries and config change entries.
    const committed = net.committedEntries(leader_id);
    var normal_count: usize = 0;
    var config_count: usize = 0;
    for (committed) |entry| {
        if (entry.kind == .normal) normal_count += 1;
        if (entry.kind == .config) config_count += 1;
    }
    // At least our 3 proposed normal entries must have committed.
    try std.testing.expect(normal_count >= 3);
    // At least 1 config change entry (the joint config entry) must have committed.
    try std.testing.expect(config_count >= 1);

    // Run all four Raft invariant checks.
    try runChecker(&net);
}
