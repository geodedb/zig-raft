// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-2025 CodePros

const std = @import("std");
const tp = @import("thread_pool");
const rpc = @import("rpc.zig");
const membership = @import("membership.zig");

const zero_checksum: [32]u8 = .{0} ** 32;

pub const SnapshotError = error{
    ChecksumMismatch,
    UnexpectedOffset,
    StaleSnapshot,
    SnapshotTooLarge,
    InvalidSnapshotConfig,
};

const PendingSnapshot = struct {
    data: std.ArrayList(u8),
    last_included_index: u64,
    last_included_term: u64,
    expected_offset: u64,
    /// Owned copy of voter IDs from the snapshot config (SEC-0030).
    config_voters: std.ArrayList(u64),
    config_voters_outgoing: std.ArrayList(u64),
    config_is_joint: bool,

    fn deinitConfig(self: *PendingSnapshot, allocator: std.mem.Allocator) void {
        self.config_voters.deinit(allocator);
        self.config_voters_outgoing.deinit(allocator);
    }
};

pub const SnapshotState = struct {
    snapshot_index: u64,
    snapshot_term: u64,
    snapshot_threshold: u32,
    chunk_size: usize,
    max_snapshot_size: u64,
    pending_snapshot: ?PendingSnapshot,
    allocator: std.mem.Allocator,

    pub fn init(
        allocator: std.mem.Allocator,
        threshold: u32,
        chunk_size: usize,
        max_snapshot_size: u64,
    ) SnapshotState {
        return .{
            .snapshot_index = 0,
            .snapshot_term = 0,
            .snapshot_threshold = threshold,
            .chunk_size = chunk_size,
            .max_snapshot_size = max_snapshot_size,
            .pending_snapshot = null,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *SnapshotState) void {
        if (self.pending_snapshot) |*pending| {
            pending.data.deinit(self.allocator);
            pending.deinitConfig(self.allocator);
            self.pending_snapshot = null;
        }
    }

    /// Check if log has grown enough above snapshot to trigger.
    /// Returns true when last_index - snapshot_index >= snapshot_threshold.
    pub fn shouldSnapshot(self: *const SnapshotState, last_index: u64) bool {
        if (last_index <= self.snapshot_index) return false;
        return (last_index - self.snapshot_index) >= self.snapshot_threshold;
    }

    /// Process reportSnapshot() from consumer — create Snapshot struct.
    /// Updates snapshot_index/snapshot_term. Does NOT persist (node.zig handles that).
    pub fn createLocalSnapshot(
        self: *SnapshotState,
        data: []const u8,
        last_included_index: u64,
        last_included_term: u64,
        config: rpc.ClusterConfig,
    ) rpc.Snapshot {
        self.snapshot_index = last_included_index;
        self.snapshot_term = last_included_term;
        return .{
            .metadata = .{
                .index = last_included_index,
                .term = last_included_term,
                .config = config,
            },
            .data = data,
        };
    }

    /// Build InstallSnapshot messages (chunks) for sending to a lagging follower.
    /// Returns a list of Message values, each containing one chunk.
    ///
    /// When `pool` is non-null, thread-pool parallelism may be used for hash
    /// computation and chunk construction on large snapshots. Currently
    /// sequential: Zig's Blake3 API does not expose parallel tree hashing,
    /// and per-chunk work is trivially cheap.
    const Blake3 = std.crypto.hash.Blake3;

    pub fn buildInstallSnapshotMessages(
        self: *SnapshotState,
        snapshot: rpc.Snapshot,
        term: u64,
        leader_id: u64,
        pool: ?*tp.ThreadPool,
    ) ![]rpc.Message {
        _ = pool; // Reserved for future parallel hashing when Blake3 tree API available.
        const data = snapshot.data;

        // Compute BLAKE3 digest of the full snapshot data.
        var hasher = Blake3.init(.{});
        hasher.update(data);
        var digest: [32]u8 = undefined;
        hasher.final(&digest);

        const num_chunks = if (data.len == 0) 1 else (data.len + self.chunk_size - 1) / self.chunk_size;
        const messages = try self.allocator.alloc(rpc.Message, num_chunks);

        var offset: usize = 0;
        for (0..num_chunks) |i| {
            const end = @min(offset + self.chunk_size, data.len);
            const chunk_data = data[offset..end];
            const is_last = (i == num_chunks - 1);

            messages[i] = .{ .install_snapshot = .{
                .term = term,
                .leader_id = leader_id,
                .last_included_index = snapshot.metadata.index,
                .last_included_term = snapshot.metadata.term,
                .offset = @intCast(offset),
                .data = chunk_data,
                .done = is_last,
                .checksum = if (is_last) digest else zero_checksum,
                .config = snapshot.metadata.config,
            } };

            offset = end;
        }

        return messages;
    }

    /// Receive an InstallSnapshot chunk. Returns completed Snapshot on final chunk.
    /// Returns null if more chunks expected.
    /// Returns error on CRC mismatch or out-of-order chunk.
    pub fn receiveChunk(self: *SnapshotState, msg: rpc.InstallSnapshot) !?rpc.Snapshot {
        if (msg.offset == 0) {
            // Reject snapshots that would move us backwards.
            if (msg.last_included_index <= self.snapshot_index) {
                return SnapshotError.StaleSnapshot;
            }

            // Term 0 is invalid in Raft — the first election produces term 1 (SEC-0098).
            if (msg.last_included_term == 0) {
                return SnapshotError.InvalidSnapshotConfig;
            }

            // A snapshot cannot originate from a future term relative to the
            // sender's current term — reject to prevent term corruption (SEC-0141).
            if (msg.last_included_term > msg.term) {
                return SnapshotError.InvalidSnapshotConfig;
            }

            // Start a new pending snapshot (reset existing if any).
            // Null out BEFORE the fallible validateSnapshotConfig() call so
            // that a validation failure does not leave freed ArrayLists
            // reachable via self.pending_snapshot (SEC-0107).
            if (self.pending_snapshot) |*pending| {
                pending.data.deinit(self.allocator);
                pending.deinitConfig(self.allocator);
                self.pending_snapshot = null;
            }

            // Validate incoming config (SEC-0043): reject empty voter sets and duplicates.
            try validateSnapshotConfig(msg.config);

            // Copy config voter IDs into owned storage (SEC-0030).
            var config_voters = std.ArrayList(u64).empty;
            errdefer config_voters.deinit(self.allocator);
            try config_voters.appendSlice(self.allocator, msg.config.voters);

            var config_voters_outgoing = std.ArrayList(u64).empty;
            errdefer config_voters_outgoing.deinit(self.allocator);
            try config_voters_outgoing.appendSlice(self.allocator, msg.config.voters_outgoing);

            self.pending_snapshot = .{
                .data = .empty,
                .last_included_index = msg.last_included_index,
                .last_included_term = msg.last_included_term,
                .expected_offset = 0,
                .config_voters = config_voters,
                .config_voters_outgoing = config_voters_outgoing,
                .config_is_joint = msg.config.is_joint,
            };
        }

        const pending = &(self.pending_snapshot orelse return SnapshotError.UnexpectedOffset);

        if (msg.offset != pending.expected_offset) {
            return SnapshotError.UnexpectedOffset;
        }

        // Enforce max_snapshot_size BEFORE allocation (SEC-0063). 0 = unlimited.
        if (self.max_snapshot_size > 0) {
            const new_size = std.math.add(u64, pending.data.items.len, msg.data.len) catch {
                self.clearPending();
                return SnapshotError.SnapshotTooLarge;
            };
            if (new_size > self.max_snapshot_size) {
                self.clearPending();
                return SnapshotError.SnapshotTooLarge;
            }
        }

        pending.data.appendSlice(self.allocator, msg.data) catch |err| {
            // On OOM, clean up pending state to prevent leak (SEC-0096).
            self.clearPending();
            return err;
        };

        if (msg.done) {
            const assembled = pending.data.items;
            var hasher = Blake3.init(.{});
            hasher.update(assembled);
            var digest: [32]u8 = undefined;
            hasher.final(&digest);
            if (!std.mem.eql(u8, &digest, &msg.checksum)) {
                self.clearPending();
                return SnapshotError.ChecksumMismatch;
            }

            // Transfer ownership of buffers to the caller via toOwnedSlice.
            // Add errdefer between sequential fallible calls to prevent leaks (SEC-0049).
            const owned_data = try pending.data.toOwnedSlice(self.allocator);
            errdefer self.allocator.free(owned_data);
            const owned_voters = try pending.config_voters.toOwnedSlice(self.allocator);
            errdefer if (owned_voters.len > 0) self.allocator.free(owned_voters);
            const owned_voters_outgoing = try pending.config_voters_outgoing.toOwnedSlice(self.allocator);

            // Use the owned config copied from the first chunk (SEC-0030).
            const snapshot = rpc.Snapshot{
                .metadata = .{
                    .index = pending.last_included_index,
                    .term = pending.last_included_term,
                    .config = .{
                        .voters = owned_voters,
                        .voters_outgoing = owned_voters_outgoing,
                        .is_joint = pending.config_is_joint,
                    },
                },
                .data = owned_data,
            };

            self.snapshot_index = pending.last_included_index;
            self.snapshot_term = pending.last_included_term;
            self.pending_snapshot = null;

            return snapshot;
        }

        // Checked overflow on expected_offset accumulation (SEC-0050).
        pending.expected_offset = std.math.add(u64, msg.offset, msg.data.len) catch {
            self.clearPending();
            return SnapshotError.SnapshotTooLarge;
        };
        return null;
    }

    /// Reset/clear any pending snapshot receive state.
    pub fn clearPending(self: *SnapshotState) void {
        if (self.pending_snapshot) |*pending| {
            pending.data.deinit(self.allocator);
            pending.deinitConfig(self.allocator);
            self.pending_snapshot = null;
        }
    }
};

/// Shared voter-set cap from membership.zig (SEC-0108, SEC-0143).
const MAX_VOTERS_PER_SET: usize = membership.MAX_VOTERS_PER_SET;

/// Validate a snapshot config received from a peer (SEC-0043).
/// Rejects empty voter sets and duplicate voter IDs, matching the
/// checks in Config.validate() (SEC-0033).
fn validateSnapshotConfig(cfg: rpc.ClusterConfig) SnapshotError!void {
    // Must have at least one voter.
    if (cfg.voters.len == 0) return SnapshotError.InvalidSnapshotConfig;

    // Joint configs must have a non-empty outgoing voter set; non-joint
    // configs must NOT have one (SEC-0110).
    if (cfg.is_joint and cfg.voters_outgoing.len == 0) return SnapshotError.InvalidSnapshotConfig;
    if (!cfg.is_joint and cfg.voters_outgoing.len > 0) return SnapshotError.InvalidSnapshotConfig;

    // Reject unreasonably large voter sets (SEC-0108).
    if (cfg.voters.len > MAX_VOTERS_PER_SET) return SnapshotError.InvalidSnapshotConfig;
    if (cfg.voters_outgoing.len > MAX_VOTERS_PER_SET) return SnapshotError.InvalidSnapshotConfig;

    // Check for duplicates in voters.
    for (cfg.voters, 0..) |v, i| {
        if (v == 0) return SnapshotError.InvalidSnapshotConfig;
        for (cfg.voters[0..i]) |prev| {
            if (v == prev) return SnapshotError.InvalidSnapshotConfig;
        }
    }

    // Check for duplicates in voters_outgoing.
    for (cfg.voters_outgoing, 0..) |v, i| {
        if (v == 0) return SnapshotError.InvalidSnapshotConfig;
        for (cfg.voters_outgoing[0..i]) |prev| {
            if (v == prev) return SnapshotError.InvalidSnapshotConfig;
        }
    }
}

// --- Tests ---

/// Test helper: default config for snapshot test messages.
const test_config = rpc.ClusterConfig{
    .voters = &[_]u64{ 1, 2, 3 },
    .voters_outgoing = &.{},
    .is_joint = false,
};

/// Test helper: compute BLAKE3 hash of data.
fn blake3Hash(data: []const u8) [32]u8 {
    var hasher = std.crypto.hash.Blake3.init(.{});
    hasher.update(data);
    var digest: [32]u8 = undefined;
    hasher.final(&digest);
    return digest;
}

test "shouldSnapshot returns true when threshold exceeded" {
    const ss = SnapshotState.init(std.testing.allocator, 100, 1024, 0);
    try std.testing.expect(!ss.shouldSnapshot(50));
    try std.testing.expect(!ss.shouldSnapshot(99));
    try std.testing.expect(ss.shouldSnapshot(100));
    try std.testing.expect(ss.shouldSnapshot(200));
}

test "createLocalSnapshot creates correct metadata and updates state" {
    var ss = SnapshotState.init(std.testing.allocator, 100, 1024, 0);
    const data = "snapshot-payload";
    const config = rpc.ClusterConfig{
        .voters = &[_]u64{ 1, 2, 3 },
        .voters_outgoing = &.{},
        .is_joint = false,
    };

    const snap = ss.createLocalSnapshot(data, 50, 3, config);

    try std.testing.expectEqual(@as(u64, 50), ss.snapshot_index);
    try std.testing.expectEqual(@as(u64, 3), ss.snapshot_term);
    try std.testing.expectEqual(@as(u64, 50), snap.metadata.index);
    try std.testing.expectEqual(@as(u64, 3), snap.metadata.term);
    try std.testing.expectEqualStrings("snapshot-payload", snap.data);
}

test "buildInstallSnapshotMessages single chunk for small data" {
    var ss = SnapshotState.init(std.testing.allocator, 100, 1024, 0);
    defer ss.deinit();

    const snap = rpc.Snapshot{
        .metadata = .{ .index = 10, .term = 2, .config = .{
            .voters = &[_]u64{1},
            .voters_outgoing = &.{},
            .is_joint = false,
        } },
        .data = "small",
    };

    const msgs = try ss.buildInstallSnapshotMessages(snap, 5, 1, null);
    defer ss.allocator.free(msgs);

    try std.testing.expectEqual(@as(usize, 1), msgs.len);
    const is = msgs[0].install_snapshot;
    try std.testing.expectEqual(@as(u64, 5), is.term);
    try std.testing.expectEqual(@as(u64, 1), is.leader_id);
    try std.testing.expectEqual(@as(u64, 0), is.offset);
    try std.testing.expect(is.done);
    try std.testing.expectEqualSlices(u8, &blake3Hash("small"), &is.checksum);
    try std.testing.expectEqualStrings("small", is.data);
}

test "buildInstallSnapshotMessages multiple chunks with correct offsets and CRC" {
    var ss = SnapshotState.init(std.testing.allocator, 100, 4, 0);
    defer ss.deinit();

    const snap = rpc.Snapshot{
        .metadata = .{ .index = 5, .term = 1, .config = .{
            .voters = &[_]u64{1},
            .voters_outgoing = &.{},
            .is_joint = false,
        } },
        .data = "HelloWorld!", // 11 bytes, chunk_size=4 => 3 chunks (4+4+3)
    };

    const msgs = try ss.buildInstallSnapshotMessages(snap, 2, 1, null);
    defer ss.allocator.free(msgs);

    try std.testing.expectEqual(@as(usize, 3), msgs.len);

    // Chunk 0
    try std.testing.expectEqual(@as(u64, 0), msgs[0].install_snapshot.offset);
    try std.testing.expectEqualStrings("Hell", msgs[0].install_snapshot.data);
    try std.testing.expect(!msgs[0].install_snapshot.done);
    try std.testing.expectEqualSlices(u8, &zero_checksum, &msgs[0].install_snapshot.checksum);

    // Chunk 1
    try std.testing.expectEqual(@as(u64, 4), msgs[1].install_snapshot.offset);
    try std.testing.expectEqualStrings("oWor", msgs[1].install_snapshot.data);
    try std.testing.expect(!msgs[1].install_snapshot.done);

    // Chunk 2 (last)
    try std.testing.expectEqual(@as(u64, 8), msgs[2].install_snapshot.offset);
    try std.testing.expectEqualStrings("ld!", msgs[2].install_snapshot.data);
    try std.testing.expect(msgs[2].install_snapshot.done);
    try std.testing.expectEqualSlices(u8, &blake3Hash("HelloWorld!"), &msgs[2].install_snapshot.checksum);
}

test "receiveChunk assembles multiple chunks and verifies BLAKE3" {
    var ss = SnapshotState.init(std.testing.allocator, 100, 1024, 0);
    defer ss.deinit();

    const full_data = "HelloWorld!";
    const digest = blake3Hash(full_data);

    // Chunk 1: offset 0, not done
    // last_included_term must be <= term (SEC-0141).
    const r1 = try ss.receiveChunk(.{
        .term = 2,
        .leader_id = 1,
        .last_included_index = 10,
        .last_included_term = 2,
        .offset = 0,
        .data = "Hello",
        .done = false,
        .checksum = zero_checksum,
        .config = test_config,
    });
    try std.testing.expect(r1 == null);

    // Chunk 2: offset 5, not done
    const r2 = try ss.receiveChunk(.{
        .term = 2,
        .leader_id = 1,
        .last_included_index = 10,
        .last_included_term = 2,
        .offset = 5,
        .data = "World",
        .done = false,
        .checksum = zero_checksum,
        .config = test_config,
    });
    try std.testing.expect(r2 == null);

    // Chunk 3: offset 10, done with BLAKE3 digest
    const r3 = try ss.receiveChunk(.{
        .term = 2,
        .leader_id = 1,
        .last_included_index = 10,
        .last_included_term = 2,
        .offset = 10,
        .data = "!",
        .done = true,
        .checksum = digest,
        .config = test_config,
    });
    try std.testing.expect(r3 != null);
    const snap = r3.?;
    try std.testing.expectEqual(@as(u64, 10), snap.metadata.index);
    try std.testing.expectEqual(@as(u64, 2), snap.metadata.term);
    try std.testing.expectEqualStrings("HelloWorld!", snap.data);
    try std.testing.expectEqual(@as(u64, 10), ss.snapshot_index);
    try std.testing.expectEqual(@as(u64, 2), ss.snapshot_term);

    // Verify config is preserved through chunked transfer (SEC-0030).
    try std.testing.expectEqual(@as(usize, 3), snap.metadata.config.voters.len);
    try std.testing.expectEqual(@as(u64, 1), snap.metadata.config.voters[0]);
    try std.testing.expectEqual(@as(u64, 2), snap.metadata.config.voters[1]);
    try std.testing.expectEqual(@as(u64, 3), snap.metadata.config.voters[2]);
    try std.testing.expect(!snap.metadata.config.is_joint);

    // The assembled data buffer is owned by the ArrayList that was moved
    // into the snapshot. We need to free it manually since pending was cleared.
    defer ss.allocator.free(snap.data);
    // Free owned config voter slices transferred from PendingSnapshot.
    defer if (snap.metadata.config.voters.len > 0) ss.allocator.free(snap.metadata.config.voters);
    defer if (snap.metadata.config.voters_outgoing.len > 0) ss.allocator.free(snap.metadata.config.voters_outgoing);
}

test "receiveChunk checksum mismatch returns error" {
    var ss = SnapshotState.init(std.testing.allocator, 100, 1024, 0);
    defer ss.deinit();

    _ = try ss.receiveChunk(.{
        .term = 1,
        .leader_id = 1,
        .last_included_index = 5,
        .last_included_term = 1,
        .offset = 0,
        .data = "data",
        .done = false,
        .checksum = zero_checksum,
        .config = test_config,
    });

    // Send a bad checksum (all 0xFF bytes).
    var bad_checksum: [32]u8 = .{0xFF} ** 32;
    _ = &bad_checksum;
    const result = ss.receiveChunk(.{
        .term = 1,
        .leader_id = 1,
        .last_included_index = 5,
        .last_included_term = 1,
        .offset = 4,
        .data = "more",
        .done = true,
        .checksum = bad_checksum,
        .config = test_config,
    });

    try std.testing.expectError(SnapshotError.ChecksumMismatch, result);

    // Clean up the pending data that remains after the error.
    ss.clearPending();
}

test "receiveChunk out-of-order offset returns error" {
    var ss = SnapshotState.init(std.testing.allocator, 100, 1024, 0);
    defer ss.deinit();

    _ = try ss.receiveChunk(.{
        .term = 1,
        .leader_id = 1,
        .last_included_index = 5,
        .last_included_term = 1,
        .offset = 0,
        .data = "hello",
        .done = false,
        .checksum = zero_checksum,
        .config = test_config,
    });

    // Offset should be 5, but we send 10
    const result = ss.receiveChunk(.{
        .term = 1,
        .leader_id = 1,
        .last_included_index = 5,
        .last_included_term = 1,
        .offset = 10,
        .data = "bad",
        .done = false,
        .checksum = zero_checksum,
        .config = test_config,
    });

    try std.testing.expectError(SnapshotError.UnexpectedOffset, result);
    ss.clearPending();
}

test "clearPending resets receive state" {
    var ss = SnapshotState.init(std.testing.allocator, 100, 1024, 0);
    defer ss.deinit();

    _ = try ss.receiveChunk(.{
        .term = 1,
        .leader_id = 1,
        .last_included_index = 5,
        .last_included_term = 1,
        .offset = 0,
        .data = "partial",
        .done = false,
        .checksum = zero_checksum,
        .config = test_config,
    });

    try std.testing.expect(ss.pending_snapshot != null);
    ss.clearPending();
    try std.testing.expect(ss.pending_snapshot == null);
}

test "receiveChunk rejects stale snapshot" {
    var ss = SnapshotState.init(std.testing.allocator, 100, 1024, 0);
    defer ss.deinit();

    // Simulate an existing snapshot at index 10.
    ss.snapshot_index = 10;
    ss.snapshot_term = 2;

    // Attempt to receive a snapshot with index <= current snapshot_index.
    const result = ss.receiveChunk(.{
        .term = 2,
        .leader_id = 1,
        .last_included_index = 10,
        .last_included_term = 2,
        .offset = 0,
        .data = "stale",
        .done = true,
        .checksum = zero_checksum,
        .config = test_config,
    });
    try std.testing.expectError(SnapshotError.StaleSnapshot, result);

    // A newer snapshot should be accepted.
    const digest = blake3Hash("fresh");
    const snap = try ss.receiveChunk(.{
        .term = 3,
        .leader_id = 1,
        .last_included_index = 15,
        .last_included_term = 3,
        .offset = 0,
        .data = "fresh",
        .done = true,
        .checksum = digest,
        .config = test_config,
    });
    try std.testing.expect(snap != null);
    try std.testing.expectEqual(@as(u64, 15), ss.snapshot_index);
    defer ss.allocator.free(snap.?.data);
    defer if (snap.?.metadata.config.voters.len > 0) ss.allocator.free(snap.?.metadata.config.voters);
    defer if (snap.?.metadata.config.voters_outgoing.len > 0) ss.allocator.free(snap.?.metadata.config.voters_outgoing);
}

test "receiveChunk rejects zero term (SEC-0098)" {
    var ss = SnapshotState.init(std.testing.allocator, 100, 1024, 0);
    defer ss.deinit();

    const result = ss.receiveChunk(.{
        .term = 1,
        .leader_id = 1,
        .last_included_index = 5,
        .last_included_term = 0, // Invalid: term 0 is impossible in Raft
        .offset = 0,
        .data = "data",
        .done = true,
        .checksum = zero_checksum,
        .config = test_config,
    });
    try std.testing.expectError(SnapshotError.InvalidSnapshotConfig, result);
}

test "receiveChunk rejects invalid snapshot config (SEC-0043)" {
    var ss = SnapshotState.init(std.testing.allocator, 100, 1024, 0);
    defer ss.deinit();

    // Empty voter set should be rejected.
    const empty_config = rpc.ClusterConfig{
        .voters = &.{},
        .voters_outgoing = &.{},
        .is_joint = false,
    };
    const empty_result = ss.receiveChunk(.{
        .term = 1,
        .leader_id = 1,
        .last_included_index = 5,
        .last_included_term = 1,
        .offset = 0,
        .data = "data",
        .done = true,
        .checksum = zero_checksum,
        .config = empty_config,
    });
    try std.testing.expectError(SnapshotError.InvalidSnapshotConfig, empty_result);

    // Duplicate voter IDs should be rejected.
    const dup_config = rpc.ClusterConfig{
        .voters = &[_]u64{ 1, 2, 1 },
        .voters_outgoing = &.{},
        .is_joint = false,
    };
    const dup_result = ss.receiveChunk(.{
        .term = 1,
        .leader_id = 1,
        .last_included_index = 5,
        .last_included_term = 1,
        .offset = 0,
        .data = "data",
        .done = true,
        .checksum = zero_checksum,
        .config = dup_config,
    });
    try std.testing.expectError(SnapshotError.InvalidSnapshotConfig, dup_result);

    // Zero voter ID should be rejected.
    const zero_id_config = rpc.ClusterConfig{
        .voters = &[_]u64{ 0, 2, 3 },
        .voters_outgoing = &.{},
        .is_joint = false,
    };
    const zero_result = ss.receiveChunk(.{
        .term = 1,
        .leader_id = 1,
        .last_included_index = 5,
        .last_included_term = 1,
        .offset = 0,
        .data = "data",
        .done = true,
        .checksum = zero_checksum,
        .config = zero_id_config,
    });
    try std.testing.expectError(SnapshotError.InvalidSnapshotConfig, zero_result);

    // Valid config should be accepted.
    const valid_config = rpc.ClusterConfig{
        .voters = &[_]u64{ 1, 2, 3 },
        .voters_outgoing = &.{},
        .is_joint = false,
    };
    const digest = blake3Hash("valid-data");
    const valid_result = try ss.receiveChunk(.{
        .term = 1,
        .leader_id = 1,
        .last_included_index = 5,
        .last_included_term = 1,
        .offset = 0,
        .data = "valid-data",
        .done = true,
        .checksum = digest,
        .config = valid_config,
    });
    try std.testing.expect(valid_result != null);
    defer ss.allocator.free(valid_result.?.data);
    defer if (valid_result.?.metadata.config.voters.len > 0) ss.allocator.free(valid_result.?.metadata.config.voters);
    defer if (valid_result.?.metadata.config.voters_outgoing.len > 0) ss.allocator.free(valid_result.?.metadata.config.voters_outgoing);
}

test "receiveChunk restart after config validation failure does not double-free (SEC-0107)" {
    var ss = SnapshotState.init(std.testing.allocator, 100, 1024, 0);
    defer ss.deinit();

    // Start a valid chunked transfer.
    _ = try ss.receiveChunk(.{
        .term = 1,
        .leader_id = 1,
        .last_included_index = 5,
        .last_included_term = 1,
        .offset = 0,
        .data = "partial",
        .done = false,
        .checksum = zero_checksum,
        .config = test_config,
    });
    try std.testing.expect(ss.pending_snapshot != null);

    // Send a new first-chunk (offset=0) with an INVALID config.
    // This frees the old pending and then fails validation.
    // Before SEC-0107 fix, pending_snapshot held freed ArrayLists.
    const bad_config = rpc.ClusterConfig{
        .voters = &.{}, // empty = invalid
        .voters_outgoing = &.{},
        .is_joint = false,
    };
    const result = ss.receiveChunk(.{
        .term = 2,
        .leader_id = 1,
        .last_included_index = 10,
        .last_included_term = 2,
        .offset = 0,
        .data = "new",
        .done = true,
        .checksum = zero_checksum,
        .config = bad_config,
    });
    try std.testing.expectError(SnapshotError.InvalidSnapshotConfig, result);

    // pending_snapshot must be null after the failed restart.
    try std.testing.expect(ss.pending_snapshot == null);

    // A subsequent valid transfer must succeed without double-free.
    const digest = blake3Hash("recovery");
    const snap = try ss.receiveChunk(.{
        .term = 3,
        .leader_id = 1,
        .last_included_index = 15,
        .last_included_term = 3,
        .offset = 0,
        .data = "recovery",
        .done = true,
        .checksum = digest,
        .config = test_config,
    });
    try std.testing.expect(snap != null);
    defer ss.allocator.free(snap.?.data);
    defer if (snap.?.metadata.config.voters.len > 0) ss.allocator.free(snap.?.metadata.config.voters);
    defer if (snap.?.metadata.config.voters_outgoing.len > 0) ss.allocator.free(snap.?.metadata.config.voters_outgoing);
}

test "validateSnapshotConfig rejects is_joint=true with empty outgoing (SEC-0110)" {
    var ss = SnapshotState.init(std.testing.allocator, 100, 1024, 0);
    defer ss.deinit();

    // is_joint=true but voters_outgoing is empty — must be rejected.
    const result = ss.receiveChunk(.{
        .term = 1,
        .leader_id = 1,
        .last_included_index = 5,
        .last_included_term = 1,
        .offset = 0,
        .data = "data",
        .done = true,
        .checksum = zero_checksum,
        .config = .{ .voters = &[_]u64{ 1, 2, 3 }, .voters_outgoing = &.{}, .is_joint = true },
    });
    try std.testing.expectError(SnapshotError.InvalidSnapshotConfig, result);
}

test "validateSnapshotConfig rejects non-joint with outgoing voters (SEC-0110)" {
    var ss = SnapshotState.init(std.testing.allocator, 100, 1024, 0);
    defer ss.deinit();

    // is_joint=false but voters_outgoing is non-empty — must be rejected.
    const result = ss.receiveChunk(.{
        .term = 1,
        .leader_id = 1,
        .last_included_index = 5,
        .last_included_term = 1,
        .offset = 0,
        .data = "data",
        .done = true,
        .checksum = zero_checksum,
        .config = .{ .voters = &[_]u64{ 1, 2, 3 }, .voters_outgoing = &[_]u64{ 4, 5 }, .is_joint = false },
    });
    try std.testing.expectError(SnapshotError.InvalidSnapshotConfig, result);
}

test "validateSnapshotConfig rejects oversized voter sets (SEC-0108)" {
    var ss = SnapshotState.init(std.testing.allocator, 100, 1024, 0);
    defer ss.deinit();

    // Build a voter set exceeding MAX_VOTERS_PER_SET.
    var oversized: [256]u64 = undefined;
    for (&oversized, 1..) |*v, i| v.* = i;
    const big_config = rpc.ClusterConfig{
        .voters = &oversized,
        .voters_outgoing = &.{},
        .is_joint = false,
    };
    const result = ss.receiveChunk(.{
        .term = 1,
        .leader_id = 1,
        .last_included_index = 5,
        .last_included_term = 1,
        .offset = 0,
        .data = "data",
        .done = true,
        .checksum = zero_checksum,
        .config = big_config,
    });
    try std.testing.expectError(SnapshotError.InvalidSnapshotConfig, result);
}

test "receiveChunk rejects last_included_term > term (SEC-0141)" {
    var ss = SnapshotState.init(std.testing.allocator, 100, 1024, 0);
    defer ss.deinit();

    // A snapshot whose last_included_term exceeds the sender's current term
    // is impossible in a correct Raft cluster — reject it.
    const result = ss.receiveChunk(.{
        .term = 1,
        .leader_id = 1,
        .last_included_index = 10,
        .last_included_term = 5, // > term (1)
        .offset = 0,
        .data = "bad",
        .done = true,
        .checksum = zero_checksum,
        .config = test_config,
    });
    try std.testing.expectError(SnapshotError.InvalidSnapshotConfig, result);

    // Equal terms should be accepted.
    const digest = blake3Hash("ok");
    const snap = try ss.receiveChunk(.{
        .term = 3,
        .leader_id = 1,
        .last_included_index = 10,
        .last_included_term = 3, // == term
        .offset = 0,
        .data = "ok",
        .done = true,
        .checksum = digest,
        .config = test_config,
    });
    try std.testing.expect(snap != null);
    defer ss.allocator.free(snap.?.data);
    defer if (snap.?.metadata.config.voters.len > 0) ss.allocator.free(snap.?.metadata.config.voters);
    defer if (snap.?.metadata.config.voters_outgoing.len > 0) ss.allocator.free(snap.?.metadata.config.voters_outgoing);
}
