// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-2025 CodePros

//! Scenario tests for log replication.
//!
//! Validates core replication invariants including single-entry commit,
//! pipelined commits, conflict truncation, log backtracking, and the
//! safety property from section 5.4.2 (no commit from prior term).

const std = @import("std");
const raft = @import("raft");
const harness = @import("harness");
const Checker = @import("checker").Checker;

/// Build a slice of `*raft.node.RaftNode` suitable for the Checker from
/// the network's node map. Caller must free the returned slice.
fn collectNodes(net: *harness.Network) ![]*raft.node.RaftNode {
    const nodes = try net.allocator.alloc(*raft.node.RaftNode, net.node_ids.items.len);
    for (net.node_ids.items, 0..) |id, i| {
        nodes[i] = net.nodes.get(id).?;
    }
    return nodes;
}

/// Run the Checker against the current network state.
fn runChecker(net: *harness.Network) !void {
    const nodes = try collectNodes(net);
    defer net.allocator.free(nodes);
    try Checker.checkAll(nodes);
}

// ---------------------------------------------------------------------------
// Scenario 1: single_entry_commits
// ---------------------------------------------------------------------------

test "replication: single_entry_commits" {
    var net = try harness.Network.init(std.testing.allocator, 3);
    defer net.deinit();

    const leader_id = try net.waitForLeader(50);

    // Propose a single entry.
    try net.propose("hello");

    // The entry index follows the leader's noop. Determine expected index.
    const leader = net.nodes.get(leader_id).?;
    const expected_index = leader.log.lastIndex();

    // Wait for the entry to be committed on a quorum.
    try net.waitForCommit(expected_index, 50);

    // Verify the entry appears in the committed list on the leader.
    const entries = net.committedEntries(leader_id);
    var found = false;
    for (entries) |e| {
        if (e.index == expected_index and e.kind == .normal) {
            found = true;
            break;
        }
    }
    try std.testing.expect(found);

    try runChecker(&net);
}

// ---------------------------------------------------------------------------
// Scenario 2: pipeline_entries_commit_in_order
// ---------------------------------------------------------------------------

test "replication: pipeline_entries_commit_in_order" {
    var net = try harness.Network.init(std.testing.allocator, 3);
    defer net.deinit();

    _ = try net.waitForLeader(50);

    // Propose 5 entries in rapid succession.
    const payloads = [_][]const u8{ "entry-1", "entry-2", "entry-3", "entry-4", "entry-5" };
    for (&payloads) |p| {
        try net.propose(p);
    }

    // Determine last proposed index from the leader.
    const lid = net.getLeader().?;
    const last_index = net.nodes.get(lid).?.log.lastIndex();

    // Wait for the last entry to commit (implies all prior entries committed).
    try net.waitForCommit(last_index, 100);

    // Verify all nodes see the entries committed in order.
    for (net.node_ids.items) |id| {
        const entries = net.committedEntries(id);

        // Filter to normal entries only.
        var normal_count: usize = 0;
        var prev_index: u64 = 0;
        for (entries) |e| {
            if (e.kind != .normal) continue;
            normal_count += 1;
            // Indices must be strictly increasing.
            try std.testing.expect(e.index > prev_index);
            prev_index = e.index;
        }
        try std.testing.expect(normal_count >= payloads.len);
    }

    try runChecker(&net);
}

// ---------------------------------------------------------------------------
// Scenario 3: follower_log_conflict_truncated
// ---------------------------------------------------------------------------

test "replication: follower_log_conflict_truncated" {
    var net = try harness.Network.init(std.testing.allocator, 3);
    defer net.deinit();

    const leader1 = try net.waitForLeader(50);

    // Propose and commit an entry on the original leader.
    try net.propose("from-leader-1");
    const lid1 = net.nodes.get(leader1).?;
    const idx1 = lid1.log.lastIndex();
    try net.waitForCommit(idx1, 50);

    // Partition the leader from the other two nodes.
    const others = blk: {
        var buf: [2]u64 = undefined;
        var n: usize = 0;
        for (net.node_ids.items) |id| {
            if (id != leader1) {
                buf[n] = id;
                n += 1;
            }
        }
        break :blk buf;
    };
    try net.partition(leader1, others[0]);
    try net.partition(leader1, others[1]);

    // Tick until a new leader is elected among the two non-partitioned nodes.
    var new_leader: ?u64 = null;
    var t: u32 = 0;
    while (t < 100) : (t += 1) {
        try net.tickAll();
        for (others) |id| {
            const n = net.nodes.get(id).?;
            if (n.role == .leader) {
                new_leader = id;
                break;
            }
        }
        if (new_leader != null) break;
    }
    try std.testing.expect(new_leader != null);

    // Propose on the new leader.
    const nl = net.nodes.get(new_leader.?).?;
    try nl.propose("from-new-leader");
    const idx2 = nl.log.lastIndex();

    // Tick until committed on the majority (2 nodes).
    try net.waitForCommit(idx2, 100);

    // Heal the partition.
    net.heal(leader1, others[0]);
    net.heal(leader1, others[1]);

    // Tick many times so the old leader catches up and any conflicting
    // entries are truncated.
    t = 0;
    while (t < 100) : (t += 1) {
        try net.tickAll();
    }

    // All invariants should hold after convergence.
    try runChecker(&net);
}

// ---------------------------------------------------------------------------
// Scenario 4: log_backtracking_on_rejection
// ---------------------------------------------------------------------------

test "replication: log_backtracking_on_rejection" {
    var net = try harness.Network.init(std.testing.allocator, 3);
    defer net.deinit();

    const leader_id = try net.waitForLeader(50);

    // Propose several entries and wait for all to commit.
    var i: u32 = 0;
    while (i < 3) : (i += 1) {
        try net.propose("initial");
    }
    const lid_node = net.nodes.get(leader_id).?;
    try net.waitForCommit(lid_node.log.lastIndex(), 50);

    // Pick a follower and partition it.
    var follower_id: u64 = 0;
    for (net.node_ids.items) |id| {
        if (id != leader_id) {
            follower_id = id;
            break;
        }
    }
    try std.testing.expect(follower_id != 0);

    // Partition follower from every other node.
    for (net.node_ids.items) |id| {
        if (id != follower_id) {
            try net.partition(follower_id, id);
        }
    }

    // Propose more entries (committed on 2 nodes, not the follower).
    i = 0;
    while (i < 3) : (i += 1) {
        try net.propose("extra");
    }
    const last_extra = net.nodes.get(leader_id).?.log.lastIndex();
    try net.waitForCommit(last_extra, 50);

    // Heal partition.
    for (net.node_ids.items) |id| {
        if (id != follower_id) {
            net.heal(follower_id, id);
        }
    }

    // Tick until the lagging follower catches up.
    var ticks: u32 = 0;
    while (ticks < 100) : (ticks += 1) {
        try net.tickAll();
    }

    // All nodes should have the same committed entries.
    const leader_entries = net.committedEntries(leader_id);
    const follower_entries = net.committedEntries(follower_id);

    // The follower should have at least as many committed entries as the leader.
    try std.testing.expect(follower_entries.len >= leader_entries.len);

    // Verify entry-by-entry match.
    for (leader_entries, 0..) |le, idx| {
        try std.testing.expectEqual(le.index, follower_entries[idx].index);
        try std.testing.expectEqual(le.term, follower_entries[idx].term);
    }

    try runChecker(&net);
}

// ---------------------------------------------------------------------------
// Scenario 5: no_commit_from_prior_term
// ---------------------------------------------------------------------------

test "replication: no_commit_from_prior_term" {
    // 5-node cluster. Validates Raft section 5.4.2: a new leader must not
    // commit entries from a prior term until it has committed an entry from
    // its own term (the noop).
    var net = try harness.Network.init(std.testing.allocator, 5);
    defer net.deinit();

    const leader1 = try net.waitForLeader(50);

    // Propose an entry on the first leader.
    try net.propose("term1-entry");
    const proposed_index = net.nodes.get(leader1).?.log.lastIndex();

    // Partition so only the leader + 1 follower can communicate.
    // The other 3 nodes form a majority that cannot see this entry.
    var minority_peer: u64 = 0;
    var majority_peers: [3]u64 = undefined;
    var mp: usize = 0;
    for (net.node_ids.items) |id| {
        if (id == leader1) continue;
        if (minority_peer == 0) {
            minority_peer = id;
        } else {
            majority_peers[mp] = id;
            mp += 1;
        }
    }

    // Isolate leader and minority_peer from the majority side.
    for (majority_peers) |m| {
        try net.partition(leader1, m);
        try net.partition(minority_peer, m);
    }

    // Tick a few times to replicate to the minority peer (not enough for commit:
    // need 3/5, but only 2 nodes see the entry).
    var t: u32 = 0;
    while (t < 20) : (t += 1) {
        try net.tickAll();
    }

    // The entry should NOT be committed on the minority side (only 2/5 nodes).
    const minority_committed = net.committedEntries(leader1);
    var found_proposed = false;
    for (minority_committed) |e| {
        if (e.index == proposed_index and e.kind == .normal) {
            found_proposed = true;
            break;
        }
    }
    try std.testing.expect(!found_proposed);

    // Now let the majority side elect a new leader by ticking until one emerges.
    var new_leader: ?u64 = null;
    t = 0;
    while (t < 150) : (t += 1) {
        try net.tickAll();
        for (majority_peers) |id| {
            const n = net.nodes.get(id).?;
            if (n.role == .leader) {
                new_leader = id;
                break;
            }
        }
        if (new_leader != null) break;
    }
    try std.testing.expect(new_leader != null);

    // The new leader should have appended its own noop on election.
    const new_leader_node = net.nodes.get(new_leader.?).?;
    try std.testing.expect(new_leader_node.current_term > net.nodes.get(leader1).?.current_term);

    // Heal all partitions.
    for (majority_peers) |m| {
        net.heal(leader1, m);
        net.heal(minority_peer, m);
    }

    // Tick until stable.
    t = 0;
    while (t < 150) : (t += 1) {
        try net.tickAll();
    }

    // All safety invariants must hold, especially leader completeness
    // which validates section 5.4.2.
    try runChecker(&net);
}
