// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-2025 CodePros

//! Snapshot scenario tests.
//!
//! Validates snapshot-related behaviour in a multi-node cluster:
//!   1. should_snapshot flag triggers at threshold
//!   2. reportSnapshot updates snapshot_state
//!   3. Log compaction after snapshot
//!   4. Chunked snapshot message construction (integration-level)
//!
//! Note: An arena allocator wraps std.testing.allocator because the harness's
//! tickAll path has known allocations (log.slice in ready/replication) whose
//! lifetimes outlast a single tick round. The arena ensures clean teardown.

const std = @import("std");
const raft = @import("raft");
const harness = @import("harness");
const Checker = @import("checker").Checker;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Collect node pointers from the harness into a slice suitable for the Checker.
fn collectNodes(allocator: std.mem.Allocator, net: *harness.Network) ![]*raft.node.RaftNode {
    const nodes = try allocator.alloc(*raft.node.RaftNode, net.node_ids.items.len);
    for (net.node_ids.items, 0..) |id, i| {
        nodes[i] = net.nodes.get(id).?;
    }
    return nodes;
}

/// Run the invariant checker over all nodes in the network.
fn checkInvariants(allocator: std.mem.Allocator, net: *harness.Network) !void {
    const nodes = try collectNodes(allocator, net);
    defer allocator.free(nodes);
    try Checker.checkAll(nodes);
}

// ---------------------------------------------------------------------------
// 1. should_snapshot flag triggers at threshold
// ---------------------------------------------------------------------------

test "scenario: should_snapshot flag triggers at threshold" {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var net = try harness.Network.init(allocator, 3);
    defer net.deinit();

    const leader_id = try net.waitForLeader(50);

    // Lower the snapshot threshold on the leader so we don't need 1000 entries.
    const leader = net.nodes.get(leader_id).?;
    leader.snapshot_state.snapshot_threshold = 5;

    // Propose 6 entries (above threshold of 5).
    var i: u32 = 0;
    while (i < 6) : (i += 1) {
        try net.propose("snap-data");
    }

    // Tick until the entries are committed on the leader.
    try net.waitForCommit(leader.log.lastIndex(), 50);

    // The harness tickAll already called ready()/advance() for this tick round,
    // so we need one more ready() call. Advance first to reset the pending flag.
    try leader.advance();
    const r = leader.ready();

    // lastIndex - snapshot_index (0) >= threshold (5) => should_snapshot == true.
    try std.testing.expect(r.should_snapshot);

    try checkInvariants(allocator, &net);
}

// ---------------------------------------------------------------------------
// 2. reportSnapshot updates snapshot state
// ---------------------------------------------------------------------------

test "scenario: reportSnapshot updates snapshot_state and storage" {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var net = try harness.Network.init(allocator, 3);
    defer net.deinit();

    const leader_id = try net.waitForLeader(50);
    const leader = net.nodes.get(leader_id).?;

    // Propose a few entries and wait for commitment.
    var i: u32 = 0;
    while (i < 3) : (i += 1) {
        try net.propose("entry");
    }
    try net.waitForCommit(leader.log.lastIndex(), 50);

    // Verify pre-conditions.
    try std.testing.expect(leader.log.applied > 0);
    const applied_before = leader.log.applied;

    // Take a snapshot at the applied index.
    try leader.reportSnapshot("test-snapshot-payload");

    // snapshot_state should be updated.
    try std.testing.expectEqual(applied_before, leader.snapshot_state.snapshot_index);
    try std.testing.expect(leader.snapshot_state.snapshot_term > 0);

    // Storage should have the snapshot.
    const stored = try leader.storage.loadSnapshot();
    try std.testing.expect(stored != null);
    try std.testing.expectEqual(applied_before, stored.?.metadata.index);
    try std.testing.expectEqualStrings("test-snapshot-payload", stored.?.data);

    try checkInvariants(allocator, &net);
}

// ---------------------------------------------------------------------------
// 3. Log compaction after snapshot
// ---------------------------------------------------------------------------

test "scenario: log compaction after snapshot removes old entries" {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var net = try harness.Network.init(allocator, 3);
    defer net.deinit();

    const leader_id = try net.waitForLeader(50);
    const leader = net.nodes.get(leader_id).?;

    // Propose several entries.
    var i: u32 = 0;
    while (i < 5) : (i += 1) {
        try net.propose("compact-me");
    }
    try net.waitForCommit(leader.log.lastIndex(), 50);

    const applied = leader.log.applied;
    try std.testing.expect(applied >= 3);

    // Record snapshot_index before compaction.
    const snapshot_idx_before = leader.log.snapshot_index;

    // Take a snapshot -- this calls log.compact() which updates snapshot_index.
    try leader.reportSnapshot("compaction-snapshot");

    // The log's snapshot_index should advance to the applied index.
    try std.testing.expectEqual(applied, leader.log.snapshot_index);
    try std.testing.expect(leader.log.snapshot_index > snapshot_idx_before);

    // Requesting a term at an index below the snapshot should return Compacted.
    try std.testing.expectError(error.Compacted, leader.log.term(1));

    // The snapshot_state should also reflect the compaction point.
    try std.testing.expectEqual(applied, leader.snapshot_state.snapshot_index);

    // The snapshot term should match the term of the entry at applied.
    try std.testing.expect(leader.snapshot_state.snapshot_term > 0);

    try checkInvariants(allocator, &net);
}

// ---------------------------------------------------------------------------
// 4. Chunked snapshot transfer (integration-level)
// ---------------------------------------------------------------------------

test "scenario: buildInstallSnapshotMessages produces correct chunks" {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var net = try harness.Network.init(allocator, 3);
    defer net.deinit();

    const leader_id = try net.waitForLeader(50);
    const leader = net.nodes.get(leader_id).?;

    // Propose entries so we have something to snapshot.
    var i: u32 = 0;
    while (i < 3) : (i += 1) {
        try net.propose("chunk-test");
    }
    try net.waitForCommit(leader.log.lastIndex(), 50);

    // Take a snapshot.
    try leader.reportSnapshot("ABCDEFGHIJKLMNOP"); // 16 bytes

    // Use a small chunk size to force multiple chunks.
    leader.snapshot_state.chunk_size = 4;

    const snap = raft.Snapshot{
        .metadata = .{
            .index = leader.snapshot_state.snapshot_index,
            .term = leader.snapshot_state.snapshot_term,
            .config = leader.membership_state.config(),
        },
        .data = "ABCDEFGHIJKLMNOP",
    };

    const msgs = try leader.snapshot_state.buildInstallSnapshotMessages(
        snap,
        leader.current_term,
        leader.id,
    );

    // 16 bytes / 4-byte chunks = 4 messages.
    try std.testing.expectEqual(@as(usize, 4), msgs.len);

    // Verify offsets and done flags.
    try std.testing.expectEqual(@as(u64, 0), msgs[0].install_snapshot.offset);
    try std.testing.expect(!msgs[0].install_snapshot.done);

    try std.testing.expectEqual(@as(u64, 4), msgs[1].install_snapshot.offset);
    try std.testing.expect(!msgs[1].install_snapshot.done);

    try std.testing.expectEqual(@as(u64, 8), msgs[2].install_snapshot.offset);
    try std.testing.expect(!msgs[2].install_snapshot.done);

    try std.testing.expectEqual(@as(u64, 12), msgs[3].install_snapshot.offset);
    try std.testing.expect(msgs[3].install_snapshot.done);

    // Last chunk carries the full-data CRC.
    const expected_crc = std.hash.Crc32.hash("ABCDEFGHIJKLMNOP");
    try std.testing.expectEqual(expected_crc, msgs[3].install_snapshot.checksum);

    // Reassemble on a follower's snapshot_state and verify round-trip.
    const follower_id: u64 = if (leader_id == 1) 2 else 1;
    const follower = net.nodes.get(follower_id).?;

    for (msgs) |m| {
        const result = try follower.snapshot_state.receiveChunk(m.install_snapshot);
        if (m.install_snapshot.done) {
            try std.testing.expect(result != null);
            const assembled = result.?;
            try std.testing.expectEqualStrings("ABCDEFGHIJKLMNOP", assembled.data);
            try std.testing.expectEqual(snap.metadata.index, assembled.metadata.index);
        } else {
            try std.testing.expect(result == null);
        }
    }

    try checkInvariants(allocator, &net);
}
