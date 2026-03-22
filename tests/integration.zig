// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-2025 CodePros

//! Integration tests for raft
//!
//! These tests verify end-to-end functionality and cross-module interactions
//! using the test harness Network.

const std = @import("std");
const raft = @import("raft");
const harness = @import("harness");

// ============================================================================
// Integration Tests
// ============================================================================

test "integration: three-node cluster elects a leader" {
    var net = try harness.Network.init(std.testing.allocator, 3);
    defer net.deinit();

    const leader = try net.waitForLeader(50);
    try std.testing.expect(leader >= 1 and leader <= 3);
}

test "integration: leader failover triggers new election (QA-011)" {
    var net = try harness.Network.init(std.testing.allocator, 3);
    defer net.deinit();

    // Elect initial leader.
    const leader1 = try net.waitForLeader(50);
    try std.testing.expect(leader1 >= 1 and leader1 <= 3);

    // Record the term of the first leader.
    const term1 = net.nodes.get(leader1).?.current_term;

    // Kill the leader by pausing it.
    try net.pauseNode(leader1);

    // Also partition the dead leader from all others so its stale messages
    // don't interfere.
    var id_iter: u64 = 1;
    while (id_iter <= 3) : (id_iter += 1) {
        if (id_iter != leader1) {
            try net.partition(leader1, id_iter);
        }
    }

    // Tick until a new leader emerges among the remaining nodes.
    var new_leader: ?u64 = null;
    var t: u32 = 0;
    while (t < 100) : (t += 1) {
        try net.tickAll();
        // Check surviving nodes for a leader.
        for (net.node_ids.items) |id| {
            if (id == leader1) continue;
            const n = net.nodes.get(id).?;
            if (n.role == .leader) {
                new_leader = id;
                break;
            }
        }
        if (new_leader != null) break;
    }

    // A new leader must have been elected.
    try std.testing.expect(new_leader != null);
    // The new leader must be a different node.
    try std.testing.expect(new_leader.? != leader1);
    // The new leader's term must be higher than the original.
    const term2 = net.nodes.get(new_leader.?).?.current_term;
    try std.testing.expect(term2 > term1);
}

test "integration: propose entries and verify commit (QA-011)" {
    var net = try harness.Network.init(std.testing.allocator, 3);
    defer net.deinit();

    // Elect a leader.
    _ = try net.waitForLeader(50);

    // Propose three entries.
    try net.propose("entry-A");
    try net.propose("entry-B");
    try net.propose("entry-C");

    // Wait for entries to be committed across the cluster. The noop entry
    // from leader election occupies index 1, so our entries start at index 2.
    // Wait for index 4 (the last proposed entry) to commit on a quorum.
    try net.waitForCommit(4, 50);

    // Verify that committed entries appear on at least a quorum of nodes.
    var nodes_with_all: u32 = 0;
    for (net.node_ids.items) |id| {
        const entries = net.committedEntries(id);
        // Count how many of our proposed entries appear in committed list.
        var found_count: u32 = 0;
        for (entries) |entry| {
            if (entry.kind == .normal and entry.data.len > 0) {
                found_count += 1;
            }
        }
        if (found_count >= 3) {
            nodes_with_all += 1;
        }
    }
    // At least a quorum (2 of 3) should have all 3 entries.
    try std.testing.expect(nodes_with_all >= 2);
}
