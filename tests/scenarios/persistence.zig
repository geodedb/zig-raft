// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-2025 CodePros

//! Persistence scenario tests.
//!
//! Verify that InMemoryStorage retains hard state, log entries, and snapshots
//! after a node operates (advance persists state). Since we cannot restart a
//! real process, we read back from the storage backing each node to confirm
//! the data survived the advance() cycle.

const std = @import("std");
const raft = @import("raft");
const harness = @import("harness");
const Checker = @import("checker").Checker;

/// Run all Raft invariant checks against the current network state.
fn runChecker(net: *harness.Network) !void {
    const nodes = try net.allocator.alloc(*raft.node.RaftNode, net.node_ids.items.len);
    defer net.allocator.free(nodes);
    for (net.node_ids.items, 0..) |id, i| {
        nodes[i] = net.nodes.get(id).?;
    }
    try Checker.checkAll(nodes);
}

// ============================================================================
// Scenario 1: hard_state_survives_restart
// ============================================================================

test "persistence: hard state survives restart" {
    var net = try harness.Network.init(std.testing.allocator, 3);
    defer net.deinit();

    const leader_id = try net.waitForLeader(50);

    // The leader's node should have a non-zero term after election.
    const leader_node = net.nodes.get(leader_id).?;
    try std.testing.expect(leader_node.current_term > 0);

    // advance() was already called by tickAll/waitForLeader, which persists
    // hard state to storage. Read it back from the storage map.
    const store = net.storages.get(leader_id).?;
    const hs = store.loadHardState();

    try std.testing.expect(hs.current_term > 0);
    try std.testing.expectEqual(leader_node.current_term, hs.current_term);

    // At least one node must have voted_for set (the leader voted for itself).
    // Check the leader's storage directly.
    try std.testing.expectEqual(leader_node.voted_for, hs.voted_for);

    try runChecker(&net);
}

// ============================================================================
// Scenario 2: log_survives_restart
// ============================================================================

test "persistence: log survives restart" {
    var net = try harness.Network.init(std.testing.allocator, 3);
    defer net.deinit();

    const leader_id = try net.waitForLeader(50);

    // Propose data on the leader.
    try net.propose("persistent-data");

    // Tick until the entry is committed (index 2: noop at 1, our entry at 2).
    try net.waitForCommit(2, 50);

    // Read entries from the leader's backing storage.
    const store = net.storages.get(leader_id).?;
    const last_idx = try store.lastIndexImpl();
    try std.testing.expect(last_idx >= 2);

    // Retrieve all persisted entries.
    const entries = try store.entriesImpl(1, last_idx + 1);
    defer net.allocator.free(entries);

    // Find the entry containing our proposed data.
    var found = false;
    for (entries) |entry| {
        if (std.mem.eql(u8, entry.data, "persistent-data")) {
            found = true;
            try std.testing.expectEqual(raft.EntryKind.normal, entry.kind);
            break;
        }
    }
    try std.testing.expect(found);

    try runChecker(&net);
}

// ============================================================================
// Scenario 3: snapshot_survives_restart
// ============================================================================

test "persistence: snapshot survives restart" {
    var net = try harness.Network.init(std.testing.allocator, 3);
    defer net.deinit();

    const leader_id = try net.waitForLeader(50);

    // Propose an entry and wait for it to be committed and applied.
    try net.propose("snap-entry");
    try net.waitForCommit(2, 50);

    // Create a snapshot on the leader via reportSnapshot.
    const leader_node = net.nodes.get(leader_id).?;
    try leader_node.reportSnapshot("my-snapshot");

    // Advance to persist the snapshot to storage.
    try leader_node.advance();

    // Read the snapshot back from the leader's storage.
    const store = net.storages.get(leader_id).?;
    const loaded = try store.loadSnapshotImpl();
    try std.testing.expect(loaded != null);

    const snapshot = loaded.?;
    try std.testing.expectEqualStrings("my-snapshot", snapshot.data);
    try std.testing.expect(snapshot.metadata.index > 0);
    try std.testing.expect(snapshot.metadata.term > 0);

    try runChecker(&net);
}

// ============================================================================
// Scenario 4: storage_entries_after_compaction
// ============================================================================

test "persistence: storage entries after compaction" {
    var store = raft.InMemoryStorage.init(std.testing.allocator);
    defer store.deinit();

    // Append several entries (indices 1..5).
    const entries = [_]raft.Entry{
        .{ .index = 1, .term = 1, .kind = .normal, .data = "a" },
        .{ .index = 2, .term = 1, .kind = .normal, .data = "b" },
        .{ .index = 3, .term = 1, .kind = .normal, .data = "c" },
        .{ .index = 4, .term = 2, .kind = .normal, .data = "d" },
        .{ .index = 5, .term = 2, .kind = .normal, .data = "e" },
    };
    try store.appendEntriesImpl(&entries);

    // Compact at index 3 — entries 1..3 are removed.
    try store.compactImpl(3);

    // Requesting entries at or before the compacted index should return Compacted.
    try std.testing.expectError(
        error.Compacted,
        store.entriesImpl(1, 3),
    );
    try std.testing.expectError(
        error.Compacted,
        store.entriesImpl(2, 4),
    );
    try std.testing.expectError(
        error.Compacted,
        store.entriesImpl(3, 5),
    );

    // Entries after the compaction point should be retrievable.
    const remaining = try store.entriesImpl(4, 6);
    defer std.testing.allocator.free(remaining);

    try std.testing.expectEqual(@as(usize, 2), remaining.len);
    try std.testing.expectEqual(@as(u64, 4), remaining[0].index);
    try std.testing.expectEqual(@as(u64, 5), remaining[1].index);
    try std.testing.expectEqualStrings("d", remaining[0].data);
    try std.testing.expectEqualStrings("e", remaining[1].data);

    // Verify snapshot_index was updated.
    try std.testing.expectEqual(@as(u64, 3), store.snapshot_index);

    // Term at the compaction boundary should return the snapshot_term.
    const term_at_boundary = try store.termImpl(3);
    try std.testing.expectEqual(@as(u64, 1), term_at_boundary);
}
