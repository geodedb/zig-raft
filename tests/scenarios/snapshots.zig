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

/// Compute BLAKE3 hash of data (test helper).
fn blake3Hash(data: []const u8) [32]u8 {
    var h = std.crypto.hash.Blake3.init(.{});
    h.update(data);
    var digest: [32]u8 = undefined;
    h.final(&digest);
    return digest;
}

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
    const r = try leader.ready();

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
    defer raft.freeSnapshot(allocator, stored.?);
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
        null,
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

    // Last chunk carries the full-data BLAKE3 digest.
    const expected_digest = blake3Hash("ABCDEFGHIJKLMNOP");
    try std.testing.expectEqualSlices(u8, &expected_digest, &msgs[3].install_snapshot.checksum);

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

// ---------------------------------------------------------------------------
// 5. snapshot_catches_up_laggard
// ---------------------------------------------------------------------------

test "scenario: snapshot_catches_up_laggard" {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var net = try harness.Network.init(allocator, 3);
    defer net.deinit();

    const leader_id = try net.waitForLeader(50);
    const leader = net.nodes.get(leader_id).?;

    // Propose entries and wait for full replication.
    var i: u32 = 0;
    while (i < 5) : (i += 1) {
        try net.propose("pre-snapshot");
    }
    try net.waitForCommit(leader.log.lastIndex(), 50);

    // Pick a follower to partition (the "laggard").
    var laggard_id: u64 = 0;
    for (net.node_ids.items) |id| {
        if (id != leader_id) {
            laggard_id = id;
            break;
        }
    }

    // Partition the laggard from all other nodes.
    for (net.node_ids.items) |id| {
        if (id != laggard_id) {
            try net.partition(laggard_id, id);
        }
    }

    // Propose more entries (committed on leader + remaining follower).
    i = 0;
    while (i < 5) : (i += 1) {
        try net.propose("post-partition");
    }
    try net.waitForCommit(leader.log.lastIndex(), 50);

    // Take a snapshot on the leader and compact the log.
    try leader.reportSnapshot("laggard-snapshot-data");
    try leader.advance();

    // The leader's log is now compacted — the laggard cannot catch up via
    // AppendEntries. Build InstallSnapshot messages and deliver manually.
    const snap = raft.Snapshot{
        .metadata = .{
            .index = leader.snapshot_state.snapshot_index,
            .term = leader.snapshot_state.snapshot_term,
            .config = leader.membership_state.config(),
        },
        .data = "laggard-snapshot-data",
    };

    leader.snapshot_state.chunk_size = 1024;
    const msgs = try leader.snapshot_state.buildInstallSnapshotMessages(
        snap,
        leader.current_term,
        leader.id,
        null,
    );

    // Deliver each chunk to the laggard.
    const laggard = net.nodes.get(laggard_id).?;
    for (msgs) |m| {
        try laggard.step(leader_id, m);
    }

    // The laggard's snapshot_state should reflect the received snapshot.
    try std.testing.expectEqual(snap.metadata.index, laggard.snapshot_state.snapshot_index);
    try std.testing.expectEqual(snap.metadata.term, laggard.snapshot_state.snapshot_term);

    // Heal the partition and let the cluster converge.
    for (net.node_ids.items) |id| {
        if (id != laggard_id) {
            net.heal(laggard_id, id);
        }
    }

    i = 0;
    while (i < 50) : (i += 1) {
        try net.tickAll();
    }

    try checkInvariants(allocator, &net);
}

// ---------------------------------------------------------------------------
// 6. chunked_snapshot_transfer
// ---------------------------------------------------------------------------

test "scenario: chunked_snapshot_transfer" {
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
        try net.propose("chunk-data");
    }
    try net.waitForCommit(leader.log.lastIndex(), 50);

    // Create a large snapshot payload to force multiple chunks.
    const snapshot_data = "A" ** 100; // 100 bytes
    try leader.reportSnapshot(snapshot_data);
    try leader.advance();

    // Use a small chunk size to force many chunks.
    leader.snapshot_state.chunk_size = 7; // 100 / 7 = ~15 chunks

    const snap = raft.Snapshot{
        .metadata = .{
            .index = leader.snapshot_state.snapshot_index,
            .term = leader.snapshot_state.snapshot_term,
            .config = leader.membership_state.config(),
        },
        .data = snapshot_data,
    };

    const msgs = try leader.snapshot_state.buildInstallSnapshotMessages(
        snap,
        leader.current_term,
        leader.id,
        null,
    );

    // Should produce ceil(100/7) = 15 chunks.
    try std.testing.expectEqual(@as(usize, 15), msgs.len);

    // Pick a follower and deliver all chunks via step().
    var follower_id: u64 = 0;
    for (net.node_ids.items) |id| {
        if (id != leader_id) {
            follower_id = id;
            break;
        }
    }
    const follower = net.nodes.get(follower_id).?;

    // Deliver each chunk in order.
    for (msgs, 0..) |m, idx| {
        const is_chunk = m.install_snapshot;
        // Verify offset progression.
        try std.testing.expectEqual(@as(u64, idx * 7), is_chunk.offset);

        try follower.step(leader_id, m);
    }

    // Last chunk should be done=true.
    try std.testing.expect(msgs[msgs.len - 1].install_snapshot.done);

    // Verify BLAKE3 digest matches on the last chunk.
    const expected_digest = blake3Hash(snapshot_data);
    try std.testing.expectEqualSlices(u8, &expected_digest, &msgs[msgs.len - 1].install_snapshot.checksum);

    // Follower's snapshot state should be updated.
    try std.testing.expectEqual(snap.metadata.index, follower.snapshot_state.snapshot_index);
    try std.testing.expectEqual(snap.metadata.term, follower.snapshot_state.snapshot_term);

    try checkInvariants(allocator, &net);
}

// ---------------------------------------------------------------------------
// 7. Leader AUTO-drives InstallSnapshot to a compacted laggard (REGRESSION)
// ---------------------------------------------------------------------------
//
// Regression for the "replication driver never auto-drives InstallSnapshot"
// bug: `buildAppendEntries` returns null on StorageError.Compacted
// (src/replication.zig:94-97 and :122-125), and all three driver call sites
// in src/node.zig mishandle that null — the two sendHeartbeats paths fall
// through to `emptyHeartbeat` and the handleAppendEntriesResponse rejection
// branch silently does nothing. `buildInstallSnapshotMessages`
// (src/snapshot.zig) is never called by library code, so a peer whose needed
// entries were compacted NEVER catches up.
//
// Unlike `snapshot_catches_up_laggard` (test 5 above), this test performs NO
// manual buildInstallSnapshotMessages / step delivery. It heals the partition
// and then only `net.tickAll()`s in a bounded loop, asserting the library
// itself drives the catch-up. This MUST fail at HEAD (laggard stays behind,
// snapshots_installed == 0) and pass once the driver escalates to
// InstallSnapshot automatically.
test "scenario: leader auto-sends InstallSnapshot to compacted laggard" {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var net = try harness.Network.init(allocator, 3);
    defer net.deinit();

    const leader_id = try net.waitForLeader(50);
    const leader = net.nodes.get(leader_id).?;

    // Commit an initial batch fully replicated to all three nodes.
    var i: u32 = 0;
    while (i < 3) : (i += 1) {
        try net.propose("pre-snapshot");
    }
    try net.waitForCommit(leader.log.lastIndex(), 50);

    // Pick a follower to partition (the "laggard").
    var laggard_id: u64 = 0;
    for (net.node_ids.items) |id| {
        if (id != leader_id) {
            laggard_id = id;
            break;
        }
    }
    const laggard = net.nodes.get(laggard_id).?;

    // Record where the laggard is before it falls behind.
    const laggard_applied_before = laggard.log.applied;

    // Partition the laggard from every other node.
    for (net.node_ids.items) |id| {
        if (id != laggard_id) {
            try net.partition(laggard_id, id);
        }
    }

    // Commit ~8 more entries on the surviving quorum (leader + 1 follower).
    i = 0;
    while (i < 8) : (i += 1) {
        try net.propose("post-partition");
    }
    try net.waitForCommit(leader.log.lastIndex(), 50);

    // Snapshot + compact the leader's log past the laggard's next_index, so
    // the laggard can no longer be caught up via AppendEntries.
    try leader.reportSnapshot("auto-laggard-snapshot-data");
    try leader.advance();
    try std.testing.expect(leader.log.snapshot_index > laggard_applied_before);

    const leader_applied = leader.log.applied;
    try std.testing.expect(leader_applied > laggard_applied_before);

    // Heal the partition and let the cluster converge with ONLY tickAll —
    // no manual snapshot construction or delivery. The library must drive
    // InstallSnapshot on its own.
    for (net.node_ids.items) |id| {
        if (id != laggard_id) {
            net.heal(laggard_id, id);
        }
    }

    var rounds: u32 = 0;
    const max_rounds: u32 = 200;
    while (rounds < max_rounds) : (rounds += 1) {
        try net.tickAll();
        // Early exit once the laggard has caught up to the leader.
        if (laggard.log.applied >= leader.log.applied and
            laggard.stats().snapshots_installed > 0)
        {
            break;
        }
    }

    // The laggard must have installed a snapshot (proving the leader
    // auto-drove InstallSnapshot) and caught up to the leader's applied index.
    try std.testing.expect(laggard.stats().snapshots_installed > 0);
    try std.testing.expectEqual(leader.log.applied, laggard.log.applied);

    try checkInvariants(allocator, &net);
}

// ---------------------------------------------------------------------------
// 8. Driver escalation signal: outbox carries InstallSnapshot, not an empty
//    AppendEntries heartbeat, once the peer needs a compacted snapshot.
// ---------------------------------------------------------------------------
//
// Unit-level observable assertion of the same bug at the message-traffic
// layer. After the leader compacts past the laggard's next_index, a single
// leader heartbeat tick must enqueue an `install_snapshot` message addressed
// to the laggard. At HEAD the driver enqueues an empty `append_entries`
// heartbeat instead (the `emptyHeartbeat` fallback), so no install_snapshot
// message is ever produced. This test inspects the harness message queue
// directly (it compiles at HEAD; only the assertion fails).
test "scenario: compacted laggard heartbeat enqueues install_snapshot not empty append_entries" {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var net = try harness.Network.init(allocator, 3);
    defer net.deinit();

    const leader_id = try net.waitForLeader(50);
    const leader = net.nodes.get(leader_id).?;

    // Commit an initial batch fully replicated to all three nodes.
    var i: u32 = 0;
    while (i < 3) : (i += 1) {
        try net.propose("pre-snapshot");
    }
    try net.waitForCommit(leader.log.lastIndex(), 50);

    // Pick a follower to partition (the "laggard").
    var laggard_id: u64 = 0;
    for (net.node_ids.items) |id| {
        if (id != leader_id) {
            laggard_id = id;
            break;
        }
    }

    // Partition the laggard.
    for (net.node_ids.items) |id| {
        if (id != laggard_id) {
            try net.partition(laggard_id, id);
        }
    }

    // Commit more entries on the surviving quorum, then compact past the
    // laggard's next_index.
    i = 0;
    while (i < 8) : (i += 1) {
        try net.propose("post-partition");
    }
    try net.waitForCommit(leader.log.lastIndex(), 50);
    try leader.reportSnapshot("escalation-snapshot");
    try leader.advance();

    // Drain any queued traffic so the next tick's outbox is clean.
    net.queue.list.clearRetainingCapacity();

    // A single leader heartbeat tick (heartbeat_tick == 1 fires every tick).
    // This populates the outbox with whatever the driver decides to send the
    // laggard — at HEAD an empty AppendEntries heartbeat, after the fix an
    // InstallSnapshot.
    try leader.tick(null);

    var saw_install_snapshot_to_laggard = false;
    var saw_empty_append_entries_to_laggard = false;
    for (net.queue.list.items) |pm| {
        if (pm.from != leader_id or pm.to != laggard_id) continue;
        switch (pm.msg) {
            .install_snapshot => saw_install_snapshot_to_laggard = true,
            .append_entries => |ae| {
                if (ae.entries.len == 0) saw_empty_append_entries_to_laggard = true;
            },
            else => {},
        }
    }

    // The leader must escalate to InstallSnapshot for the compacted laggard,
    // NOT fall back to an empty heartbeat that can never make it progress.
    try std.testing.expect(saw_install_snapshot_to_laggard);
    try std.testing.expect(!saw_empty_append_entries_to_laggard);

    try checkInvariants(allocator, &net);
}

// ---------------------------------------------------------------------------
// 9. Re-send throttle / duplicate-ack safety: repeated auto snapshot drive
//    must not corrupt or regress the laggard, and forward progress continues.
// ---------------------------------------------------------------------------
//
// After auto catch-up, the leader keeps ticking (potentially re-sending
// snapshots before the in-flight ack lands). `receiveChunk` already rejects
// StaleSnapshot, so repeated sends must not regress the laggard. We then
// commit one more entry and assert it replicates to the laggard via normal
// AppendEntries — proving a late/duplicate InstallSnapshotResponse did not
// regress next_index and the leader still makes forward progress. This test
// reuses the auto-drive path, so it also fails at HEAD (the laggard never
// catches up in the first place).
test "scenario: auto snapshot re-send does not regress laggard and forward progress continues" {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var net = try harness.Network.init(allocator, 3);
    defer net.deinit();

    const leader_id = try net.waitForLeader(50);
    const leader = net.nodes.get(leader_id).?;

    var i: u32 = 0;
    while (i < 3) : (i += 1) {
        try net.propose("pre-snapshot");
    }
    try net.waitForCommit(leader.log.lastIndex(), 50);

    var laggard_id: u64 = 0;
    for (net.node_ids.items) |id| {
        if (id != leader_id) {
            laggard_id = id;
            break;
        }
    }
    const laggard = net.nodes.get(laggard_id).?;

    for (net.node_ids.items) |id| {
        if (id != laggard_id) {
            try net.partition(laggard_id, id);
        }
    }

    i = 0;
    while (i < 8) : (i += 1) {
        try net.propose("post-partition");
    }
    try net.waitForCommit(leader.log.lastIndex(), 50);
    try leader.reportSnapshot("resend-snapshot");
    try leader.advance();

    // Heal and let the cluster converge purely via tickAll. Run a generous
    // number of rounds so repeated snapshot drive / duplicate acks are
    // exercised even after the first install lands.
    for (net.node_ids.items) |id| {
        if (id != laggard_id) {
            net.heal(laggard_id, id);
        }
    }

    var rounds: u32 = 0;
    while (rounds < 200) : (rounds += 1) {
        try net.tickAll();
        if (laggard.log.applied >= leader.log.applied and
            laggard.stats().snapshots_installed > 0)
        {
            break;
        }
        // Invariants must hold on EVERY round — repeated snapshot sends must
        // never corrupt the laggard.
        try checkInvariants(allocator, &net);
    }

    // Laggard caught up cleanly via the auto-driven snapshot.
    try std.testing.expect(laggard.stats().snapshots_installed > 0);
    try std.testing.expectEqual(leader.log.applied, laggard.log.applied);
    const caught_up_index = laggard.log.applied;

    // Forward progress: commit one MORE entry. It must reach the laggard via
    // normal AppendEntries (a late/duplicate InstallSnapshotResponse must not
    // have regressed the leader's next_index for this peer).
    try net.propose("after-catchup");
    const target = leader.log.lastIndex();
    try std.testing.expect(target > caught_up_index);

    rounds = 0;
    while (rounds < 100) : (rounds += 1) {
        try net.tickAll();
        if (laggard.log.applied >= leader.log.applied) break;
        try checkInvariants(allocator, &net);
    }

    try std.testing.expectEqual(leader.log.applied, laggard.log.applied);
    try std.testing.expect(laggard.log.applied >= target);

    try checkInvariants(allocator, &net);
}
