// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-2025 CodePros

const std = @import("std");
const rpc = @import("rpc.zig");
const stg = @import("storage.zig");

pub const SnapshotError = error{
    ChecksumMismatch,
    UnexpectedOffset,
};

const PendingSnapshot = struct {
    data: std.ArrayList(u8),
    last_included_index: u64,
    last_included_term: u64,
    expected_offset: u64,
};

pub const SnapshotState = struct {
    snapshot_index: u64,
    snapshot_term: u64,
    snapshot_threshold: u32,
    chunk_size: usize,
    pending_snapshot: ?PendingSnapshot,
    allocator: std.mem.Allocator,

    pub fn init(
        allocator: std.mem.Allocator,
        threshold: u32,
        chunk_size: usize,
    ) SnapshotState {
        return .{
            .snapshot_index = 0,
            .snapshot_term = 0,
            .snapshot_threshold = threshold,
            .chunk_size = chunk_size,
            .pending_snapshot = null,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *SnapshotState) void {
        if (self.pending_snapshot) |*pending| {
            pending.data.deinit(self.allocator);
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
    pub fn buildInstallSnapshotMessages(
        self: *SnapshotState,
        snapshot: rpc.Snapshot,
        term: u64,
        leader_id: u64,
    ) ![]rpc.Message {
        const data = snapshot.data;
        const crc = std.hash.Crc32.hash(data);

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
                .checksum = if (is_last) crc else 0,
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
            // Start a new pending snapshot (reset existing if any).
            if (self.pending_snapshot) |*pending| {
                pending.data.deinit(self.allocator);
            }
            self.pending_snapshot = .{
                .data = .{},
                .last_included_index = msg.last_included_index,
                .last_included_term = msg.last_included_term,
                .expected_offset = 0,
            };
        }

        const pending = &(self.pending_snapshot orelse return SnapshotError.UnexpectedOffset);

        if (msg.offset != pending.expected_offset) {
            return SnapshotError.UnexpectedOffset;
        }

        try pending.data.appendSlice(self.allocator, msg.data);

        if (msg.done) {
            const assembled = pending.data.items;
            const crc = std.hash.Crc32.hash(assembled);
            if (crc != msg.checksum) {
                return SnapshotError.ChecksumMismatch;
            }

            // Transfer ownership of the buffer to the caller via toOwnedSlice.
            const owned_data = try pending.data.toOwnedSlice(self.allocator);

            const snapshot = rpc.Snapshot{
                .metadata = .{
                    .index = pending.last_included_index,
                    .term = pending.last_included_term,
                    .config = .{
                        .voters = &.{},
                        .voters_outgoing = &.{},
                        .is_joint = false,
                    },
                },
                .data = owned_data,
            };

            self.snapshot_index = pending.last_included_index;
            self.snapshot_term = pending.last_included_term;
            self.pending_snapshot = null;

            return snapshot;
        }

        pending.expected_offset = msg.offset + msg.data.len;
        return null;
    }

    /// Reset/clear any pending snapshot receive state.
    pub fn clearPending(self: *SnapshotState) void {
        if (self.pending_snapshot) |*pending| {
            pending.data.deinit(self.allocator);
            self.pending_snapshot = null;
        }
    }
};

// --- Tests ---

test "shouldSnapshot returns true when threshold exceeded" {
    const ss = SnapshotState.init(std.testing.allocator, 100, 1024);
    try std.testing.expect(!ss.shouldSnapshot(50));
    try std.testing.expect(!ss.shouldSnapshot(99));
    try std.testing.expect(ss.shouldSnapshot(100));
    try std.testing.expect(ss.shouldSnapshot(200));
}

test "createLocalSnapshot creates correct metadata and updates state" {
    var ss = SnapshotState.init(std.testing.allocator, 100, 1024);
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
    var ss = SnapshotState.init(std.testing.allocator, 100, 1024);
    defer ss.deinit();

    const snap = rpc.Snapshot{
        .metadata = .{ .index = 10, .term = 2, .config = .{
            .voters = &[_]u64{1},
            .voters_outgoing = &.{},
            .is_joint = false,
        } },
        .data = "small",
    };

    const msgs = try ss.buildInstallSnapshotMessages(snap, 5, 1);
    defer ss.allocator.free(msgs);

    try std.testing.expectEqual(@as(usize, 1), msgs.len);
    const is = msgs[0].install_snapshot;
    try std.testing.expectEqual(@as(u64, 5), is.term);
    try std.testing.expectEqual(@as(u64, 1), is.leader_id);
    try std.testing.expectEqual(@as(u64, 0), is.offset);
    try std.testing.expect(is.done);
    try std.testing.expectEqual(std.hash.Crc32.hash("small"), is.checksum);
    try std.testing.expectEqualStrings("small", is.data);
}

test "buildInstallSnapshotMessages multiple chunks with correct offsets and CRC" {
    var ss = SnapshotState.init(std.testing.allocator, 100, 4);
    defer ss.deinit();

    const snap = rpc.Snapshot{
        .metadata = .{ .index = 5, .term = 1, .config = .{
            .voters = &[_]u64{1},
            .voters_outgoing = &.{},
            .is_joint = false,
        } },
        .data = "HelloWorld!", // 11 bytes, chunk_size=4 => 3 chunks (4+4+3)
    };

    const msgs = try ss.buildInstallSnapshotMessages(snap, 2, 1);
    defer ss.allocator.free(msgs);

    try std.testing.expectEqual(@as(usize, 3), msgs.len);

    // Chunk 0
    try std.testing.expectEqual(@as(u64, 0), msgs[0].install_snapshot.offset);
    try std.testing.expectEqualStrings("Hell", msgs[0].install_snapshot.data);
    try std.testing.expect(!msgs[0].install_snapshot.done);
    try std.testing.expectEqual(@as(u32, 0), msgs[0].install_snapshot.checksum);

    // Chunk 1
    try std.testing.expectEqual(@as(u64, 4), msgs[1].install_snapshot.offset);
    try std.testing.expectEqualStrings("oWor", msgs[1].install_snapshot.data);
    try std.testing.expect(!msgs[1].install_snapshot.done);

    // Chunk 2 (last)
    try std.testing.expectEqual(@as(u64, 8), msgs[2].install_snapshot.offset);
    try std.testing.expectEqualStrings("ld!", msgs[2].install_snapshot.data);
    try std.testing.expect(msgs[2].install_snapshot.done);
    try std.testing.expectEqual(std.hash.Crc32.hash("HelloWorld!"), msgs[2].install_snapshot.checksum);
}

test "receiveChunk assembles multiple chunks and verifies CRC" {
    var ss = SnapshotState.init(std.testing.allocator, 100, 1024);
    defer ss.deinit();

    const full_data = "HelloWorld!";
    const crc = std.hash.Crc32.hash(full_data);

    // Chunk 1: offset 0, not done
    const r1 = try ss.receiveChunk(.{
        .term = 1,
        .leader_id = 1,
        .last_included_index = 10,
        .last_included_term = 2,
        .offset = 0,
        .data = "Hello",
        .done = false,
        .checksum = 0,
    });
    try std.testing.expect(r1 == null);

    // Chunk 2: offset 5, not done
    const r2 = try ss.receiveChunk(.{
        .term = 1,
        .leader_id = 1,
        .last_included_index = 10,
        .last_included_term = 2,
        .offset = 5,
        .data = "World",
        .done = false,
        .checksum = 0,
    });
    try std.testing.expect(r2 == null);

    // Chunk 3: offset 10, done with CRC
    const r3 = try ss.receiveChunk(.{
        .term = 1,
        .leader_id = 1,
        .last_included_index = 10,
        .last_included_term = 2,
        .offset = 10,
        .data = "!",
        .done = true,
        .checksum = crc,
    });
    try std.testing.expect(r3 != null);
    const snap = r3.?;
    try std.testing.expectEqual(@as(u64, 10), snap.metadata.index);
    try std.testing.expectEqual(@as(u64, 2), snap.metadata.term);
    try std.testing.expectEqualStrings("HelloWorld!", snap.data);
    try std.testing.expectEqual(@as(u64, 10), ss.snapshot_index);
    try std.testing.expectEqual(@as(u64, 2), ss.snapshot_term);

    // The assembled data buffer is owned by the ArrayList that was moved
    // into the snapshot. We need to free it manually since pending was cleared.
    defer ss.allocator.free(snap.data);
}

test "receiveChunk CRC mismatch returns error" {
    var ss = SnapshotState.init(std.testing.allocator, 100, 1024);
    defer ss.deinit();

    _ = try ss.receiveChunk(.{
        .term = 1,
        .leader_id = 1,
        .last_included_index = 5,
        .last_included_term = 1,
        .offset = 0,
        .data = "data",
        .done = false,
        .checksum = 0,
    });

    const result = ss.receiveChunk(.{
        .term = 1,
        .leader_id = 1,
        .last_included_index = 5,
        .last_included_term = 1,
        .offset = 4,
        .data = "more",
        .done = true,
        .checksum = 0xDEADBEEF,
    });

    try std.testing.expectError(SnapshotError.ChecksumMismatch, result);

    // Clean up the pending data that remains after the error.
    ss.clearPending();
}

test "receiveChunk out-of-order offset returns error" {
    var ss = SnapshotState.init(std.testing.allocator, 100, 1024);
    defer ss.deinit();

    _ = try ss.receiveChunk(.{
        .term = 1,
        .leader_id = 1,
        .last_included_index = 5,
        .last_included_term = 1,
        .offset = 0,
        .data = "hello",
        .done = false,
        .checksum = 0,
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
        .checksum = 0,
    });

    try std.testing.expectError(SnapshotError.UnexpectedOffset, result);
    ss.clearPending();
}

test "clearPending resets receive state" {
    var ss = SnapshotState.init(std.testing.allocator, 100, 1024);
    defer ss.deinit();

    _ = try ss.receiveChunk(.{
        .term = 1,
        .leader_id = 1,
        .last_included_index = 5,
        .last_included_term = 1,
        .offset = 0,
        .data = "partial",
        .done = false,
        .checksum = 0,
    });

    try std.testing.expect(ss.pending_snapshot != null);
    ss.clearPending();
    try std.testing.expect(ss.pending_snapshot == null);
}
