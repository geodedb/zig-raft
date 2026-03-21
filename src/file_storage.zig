// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-2025 CodePros

//! File-backed implementation of the `Storage` interface.
//!
//! Provides crash-safe, durable storage using flat files on disk. This
//! serves as both a production-ready storage backend and a reference
//! implementation for consumers who want to build custom backends
//! (S3, database, etc.) against the `Storage` vtable.
//!
//! ## Directory layout
//!
//! ```
//! <data_dir>/
//!   hard_state    -- 16 bytes: [term: u64 LE][voted_for: u64 LE]
//!   wal           -- append-only write-ahead log of entry records
//!   snapshot.meta -- 16 bytes: [index: u64 LE][term: u64 LE]
//!   snapshot.data -- raw snapshot bytes
//! ```
//!
//! ## WAL record format
//!
//! Each record is:
//! ```
//! [data_len: u32 LE][index: u64 LE][term: u64 LE][kind: u8][data: data_len bytes][crc32: u32 LE]
//! ```
//!
//! On open, the WAL is scanned and CRC-verified. Torn writes are detected
//! and truncated. An in-memory index is built for O(1) term lookups and
//! efficient range reads.
//!
//! ## Crash safety
//!
//! - Hard state and snapshots: write to temp file, sync, rename.
//! - WAL: append + sync. Torn write recovery on next open.
//! - Compaction: rewrite WAL atomically via temp + rename.
//!
//! ## Example usage
//!
//! ```zig
//! var file_store = try FileStorage.init(allocator, "/var/lib/raft/node1");
//! defer file_store.deinit();
//!
//! var raft_node = try RaftNode.init(allocator, config, file_store.storage());
//! ```

const std = @import("std");
const state = @import("state.zig");
const rpc = @import("rpc.zig");
const stg = @import("storage.zig");

/// Sentinel value for voted_for = null in the hard_state file.
const VOTED_FOR_NONE: u64 = std.math.maxInt(u64);

/// Fixed portion of a WAL record (before variable-length data).
/// data_len(4) + index(8) + term(8) + kind(1) = 21 bytes
const WAL_HEADER_SIZE: usize = 4 + 8 + 8 + 1;

/// CRC footer size.
const WAL_CRC_SIZE: usize = 4;

pub const FileStorage = struct {
    dir: std.fs.Dir,
    wal_file: ?std.fs.File,
    wal_end: u64,
    index: std.ArrayList(IndexEntry),
    snapshot_index: u64,
    snapshot_term: u64,
    hard_state: state.HardState,
    allocator: std.mem.Allocator,

    /// In-memory index entry for fast lookups.
    const IndexEntry = struct {
        file_offset: u64,
        raft_index: u64,
        term: u64,
        data_len: u32,
    };

    /// Open or create a FileStorage at the given directory path.
    /// Recovers existing state from files if present.
    pub fn init(allocator: std.mem.Allocator, dir_path: []const u8) !FileStorage {
        // Open or create the directory.
        var dir = try std.fs.cwd().makeOpenPath(dir_path, .{});
        errdefer dir.close();

        var fs = FileStorage{
            .dir = dir,
            .wal_file = null,
            .wal_end = 0,
            .index = .{},
            .snapshot_index = 0,
            .snapshot_term = 0,
            .hard_state = state.HardState.initial,
            .allocator = allocator,
        };

        // Recover hard state.
        fs.hard_state = fs.readHardStateFile() catch state.HardState.initial;

        // Recover snapshot metadata.
        const snap_meta = fs.readSnapshotMeta() catch null;
        if (snap_meta) |meta| {
            fs.snapshot_index = meta.index;
            fs.snapshot_term = meta.term;
        }

        // Open WAL and rebuild in-memory index with torn write recovery.
        try fs.recoverWal();

        return fs;
    }

    pub fn deinit(self: *FileStorage) void {
        if (self.wal_file) |f| f.close();
        self.index.deinit(self.allocator);
        self.dir.close();
    }

    /// Return a `Storage` vtable wrapper backed by this FileStorage.
    pub fn storage(self: *FileStorage) stg.Storage {
        return .{
            .ptr = @ptrCast(self),
            .vtable = &vtable_inst,
        };
    }

    const vtable_inst = stg.Storage.VTable{
        .saveHardState = typeErasedSaveHardState,
        .appendEntries = typeErasedAppendEntries,
        .entries = typeErasedEntries,
        .term = typeErasedTerm,
        .lastIndex = typeErasedLastIndex,
        .saveSnapshot = typeErasedSaveSnapshot,
        .loadSnapshot = typeErasedLoadSnapshot,
        .compact = typeErasedCompact,
    };

    // ========================================================================
    // Public implementation methods
    // ========================================================================

    pub fn saveHardStateImpl(self: *FileStorage, hs: state.HardState) !void {
        var buf: [16]u8 = undefined;
        std.mem.writeInt(u64, buf[0..8], hs.current_term, .little);
        std.mem.writeInt(u64, buf[8..16], hs.voted_for orelse VOTED_FOR_NONE, .little);

        // Write to temp, sync, rename.
        var tmp = try self.dir.createFile("hard_state.tmp", .{});
        errdefer {
            tmp.close();
            self.dir.deleteFile("hard_state.tmp") catch {};
        }
        try tmp.writeAll(&buf);
        try tmp.sync();
        tmp.close();

        try self.dir.rename("hard_state.tmp", "hard_state");
        self.hard_state = hs;
    }

    pub fn appendEntriesImpl(self: *FileStorage, ents: []const rpc.Entry) !void {
        if (ents.len == 0) return;

        const first_new_index = ents[0].index;

        // Handle truncation: if appending entries that overlap with existing,
        // truncate the WAL and index from the conflict point.
        try self.truncateFrom(first_new_index);

        // Open WAL for appending (create if not exists).
        if (self.wal_file == null) {
            // Ensure the file exists.
            {
                var f = try self.dir.createFile("wal", .{ .truncate = false });
                f.close();
            }
            // Open for read+write.
            self.wal_file = try self.dir.openFile("wal", .{ .mode = .read_write });
        }
        const file = self.wal_file.?;

        // Seek to the end of valid data.
        try file.seekTo(self.wal_end);

        for (ents) |entry| {
            const data_len: u32 = @intCast(entry.data.len);

            // Build the record payload for CRC.
            const payload_size = WAL_HEADER_SIZE + entry.data.len;
            const payload = try self.allocator.alloc(u8, payload_size);
            defer self.allocator.free(payload);

            std.mem.writeInt(u32, payload[0..4], data_len, .little);
            std.mem.writeInt(u64, payload[4..12], entry.index, .little);
            std.mem.writeInt(u64, payload[12..20], entry.term, .little);
            payload[20] = @intFromEnum(entry.kind);
            if (entry.data.len > 0) {
                @memcpy(payload[21..][0..entry.data.len], entry.data);
            }

            const crc = std.hash.Crc32.hash(payload);
            var crc_buf: [4]u8 = undefined;
            std.mem.writeInt(u32, &crc_buf, crc, .little);

            // Write record.
            const offset = self.wal_end;
            try file.writeAll(payload);
            try file.writeAll(&crc_buf);

            // Update in-memory index.
            try self.index.append(self.allocator, .{
                .file_offset = offset,
                .raft_index = entry.index,
                .term = entry.term,
                .data_len = data_len,
            });

            self.wal_end += payload_size + WAL_CRC_SIZE;
        }

        // Sync to disk.
        try file.sync();
    }

    pub fn entriesImpl(self: *FileStorage, lo: u64, hi: u64) ![]rpc.Entry {
        if (lo <= self.snapshot_index) return stg.StorageError.Compacted;
        if (lo >= hi) return &[_]rpc.Entry{};

        // Find index entries in range [lo, hi).
        const start_pos = self.findIndexPos(lo) orelse return stg.StorageError.Unavailable;
        const end_pos = self.findIndexPos(hi - 1) orelse return stg.StorageError.Unavailable;
        const count = end_pos - start_pos + 1;

        const result = try self.allocator.alloc(rpc.Entry, count);
        errdefer self.allocator.free(result);

        const file = self.wal_file orelse return stg.StorageError.Unavailable;

        for (0..count) |i| {
            const ie = self.index.items[start_pos + i];
            result[i] = try self.readEntryAt(file, ie);
        }

        return result;
    }

    pub fn termImpl(self: *FileStorage, index: u64) !u64 {
        if (index < self.snapshot_index) return stg.StorageError.Compacted;
        if (index == self.snapshot_index) return self.snapshot_term;

        const pos = self.findIndexPos(index) orelse return stg.StorageError.Unavailable;
        return self.index.items[pos].term;
    }

    pub fn lastIndexImpl(self: *FileStorage) !u64 {
        if (self.index.items.len == 0) return self.snapshot_index;
        return self.index.items[self.index.items.len - 1].raft_index;
    }

    pub fn saveSnapshotImpl(self: *FileStorage, snap: rpc.Snapshot) !void {
        // Write snapshot data atomically.
        {
            var tmp = try self.dir.createFile("snapshot.data.tmp", .{});
            errdefer {
                tmp.close();
                self.dir.deleteFile("snapshot.data.tmp") catch {};
            }
            try tmp.writeAll(snap.data);
            try tmp.sync();
            tmp.close();
            try self.dir.rename("snapshot.data.tmp", "snapshot.data");
        }

        // Write snapshot metadata atomically.
        try self.writeSnapshotMeta(snap.metadata.index, snap.metadata.term);

        self.snapshot_index = snap.metadata.index;
        self.snapshot_term = snap.metadata.term;
    }

    pub fn loadSnapshotImpl(self: *FileStorage) !?rpc.Snapshot {
        const meta = self.readSnapshotMeta() catch return null;

        const data_file = self.dir.openFile("snapshot.data", .{}) catch return null;
        defer data_file.close();

        const size = try data_file.getEndPos();
        const data = try self.allocator.alloc(u8, size);
        const bytes_read = try data_file.readAll(data);
        if (bytes_read != size) {
            self.allocator.free(data);
            return null;
        }

        return rpc.Snapshot{
            .metadata = .{
                .index = meta.index,
                .term = meta.term,
                .config = .{
                    .voters = &.{},
                    .voters_outgoing = &.{},
                    .is_joint = false,
                },
            },
            .data = data,
        };
    }

    pub fn compactImpl(self: *FileStorage, index: u64) !void {
        if (index <= self.snapshot_index) return;

        // Find the position of the entry being compacted to.
        const compact_pos = self.findIndexPos(index) orelse return;

        // Record the term at the compaction point.
        self.snapshot_term = self.index.items[compact_pos].term;
        self.snapshot_index = index;

        // Save updated snapshot metadata atomically.
        try self.writeSnapshotMeta(self.snapshot_index, self.snapshot_term);

        const remaining_count = self.index.items.len - (compact_pos + 1);

        if (remaining_count == 0) {
            // All entries compacted — truncate WAL.
            self.index.shrinkRetainingCapacity(0);
            if (self.wal_file) |f| {
                f.close();
                self.wal_file = null;
            }
            self.dir.deleteFile("wal") catch {};
            self.wal_end = 0;
            return;
        }

        // Rewrite WAL with only the remaining entries.
        const file = self.wal_file orelse return;

        // Read remaining entries from the old WAL.
        const entries = try self.allocator.alloc(rpc.Entry, remaining_count);
        defer {
            for (entries) |e| {
                if (e.data.len > 0) self.allocator.free(@constCast(e.data));
            }
            self.allocator.free(entries);
        }

        for (0..remaining_count) |i| {
            entries[i] = try self.readEntryAt(file, self.index.items[compact_pos + 1 + i]);
        }

        // Close old WAL before rewriting.
        file.close();
        self.wal_file = null;

        // Write new WAL to temp, sync, rename.
        var new_index = std.ArrayList(IndexEntry){};
        errdefer new_index.deinit(self.allocator);
        var new_end: u64 = 0;

        {
            var tmp = try self.dir.createFile("wal.tmp", .{});
            errdefer {
                tmp.close();
                self.dir.deleteFile("wal.tmp") catch {};
            }

            for (entries) |entry| {
                const data_len: u32 = @intCast(entry.data.len);
                const payload_size = WAL_HEADER_SIZE + entry.data.len;
                const payload = try self.allocator.alloc(u8, payload_size);
                defer self.allocator.free(payload);

                std.mem.writeInt(u32, payload[0..4], data_len, .little);
                std.mem.writeInt(u64, payload[4..12], entry.index, .little);
                std.mem.writeInt(u64, payload[12..20], entry.term, .little);
                payload[20] = @intFromEnum(entry.kind);
                if (entry.data.len > 0) {
                    @memcpy(payload[21..][0..entry.data.len], entry.data);
                }

                const crc = std.hash.Crc32.hash(payload);
                var crc_buf: [4]u8 = undefined;
                std.mem.writeInt(u32, &crc_buf, crc, .little);

                try new_index.append(self.allocator, .{
                    .file_offset = new_end,
                    .raft_index = entry.index,
                    .term = entry.term,
                    .data_len = data_len,
                });

                try tmp.writeAll(payload);
                try tmp.writeAll(&crc_buf);
                new_end += payload_size + WAL_CRC_SIZE;
            }

            try tmp.sync();
            tmp.close();
        }

        try self.dir.rename("wal.tmp", "wal");

        // Update in-memory state.
        self.index.deinit(self.allocator);
        self.index = new_index;
        self.wal_end = new_end;

        // Reopen WAL for future appends.
        self.wal_file = try self.dir.openFile("wal", .{ .mode = .read_write });
    }

    /// Write snapshot metadata file atomically.
    fn writeSnapshotMeta(self: *FileStorage, snap_index: u64, snap_term: u64) !void {
        var buf: [16]u8 = undefined;
        std.mem.writeInt(u64, buf[0..8], snap_index, .little);
        std.mem.writeInt(u64, buf[8..16], snap_term, .little);

        var tmp = try self.dir.createFile("snapshot.meta.tmp", .{});
        try tmp.writeAll(&buf);
        try tmp.sync();
        tmp.close();
        try self.dir.rename("snapshot.meta.tmp", "snapshot.meta");
    }

    pub fn loadHardState(self: *FileStorage) state.HardState {
        return self.hard_state;
    }

    // ========================================================================
    // Private helpers
    // ========================================================================

    /// Read the hard_state file. Returns error if not found or malformed.
    fn readHardStateFile(self: *FileStorage) !state.HardState {
        const file = try self.dir.openFile("hard_state", .{});
        defer file.close();

        var buf: [16]u8 = undefined;
        const n = try file.readAll(&buf);
        if (n != 16) return error.InvalidHardState;

        const current_term = std.mem.readInt(u64, buf[0..8], .little);
        const voted_for_raw = std.mem.readInt(u64, buf[8..16], .little);
        const voted_for: ?u64 = if (voted_for_raw == VOTED_FOR_NONE) null else voted_for_raw;

        return .{ .current_term = current_term, .voted_for = voted_for };
    }

    /// Read snapshot metadata file.
    fn readSnapshotMeta(self: *FileStorage) !struct { index: u64, term: u64 } {
        const file = try self.dir.openFile("snapshot.meta", .{});
        defer file.close();

        var buf: [16]u8 = undefined;
        const n = try file.readAll(&buf);
        if (n != 16) return error.InvalidSnapshotMeta;

        return .{
            .index = std.mem.readInt(u64, buf[0..8], .little),
            .term = std.mem.readInt(u64, buf[8..16], .little),
        };
    }

    /// Open the WAL file, scan all records, verify CRCs, and build in-memory index.
    /// Truncates at the first corrupt or incomplete record (torn write recovery).
    fn recoverWal(self: *FileStorage) !void {
        self.wal_file = self.dir.openFile("wal", .{ .mode = .read_write }) catch |err| {
            if (err == error.FileNotFound) {
                return; // No WAL yet, fresh start.
            }
            return err;
        };

        const file = self.wal_file.?;
        const file_size = try file.getEndPos();
        var offset: u64 = 0;

        while (offset < file_size) {
            // Try to read one record.
            const record_start = offset;

            // Read header.
            if (offset + WAL_HEADER_SIZE > file_size) break; // Incomplete header.

            var header_buf: [WAL_HEADER_SIZE]u8 = undefined;
            try file.seekTo(offset);
            const header_read = try file.readAll(&header_buf);
            if (header_read != WAL_HEADER_SIZE) break;

            const data_len = std.mem.readInt(u32, header_buf[0..4], .little);
            const raft_index = std.mem.readInt(u64, header_buf[4..12], .little);
            const term = std.mem.readInt(u64, header_buf[12..20], .little);

            const total_record_size = WAL_HEADER_SIZE + data_len + WAL_CRC_SIZE;
            if (offset + total_record_size > file_size) break; // Incomplete record.

            // Read full payload + CRC for verification.
            const payload_size = WAL_HEADER_SIZE + data_len;
            const full_buf = try self.allocator.alloc(u8, payload_size + WAL_CRC_SIZE);
            defer self.allocator.free(full_buf);

            try file.seekTo(offset);
            const full_read = try file.readAll(full_buf);
            if (full_read != payload_size + WAL_CRC_SIZE) break;

            // Verify CRC.
            const payload = full_buf[0..payload_size];
            const stored_crc = std.mem.readInt(u32, full_buf[payload_size..][0..4], .little);
            const computed_crc = std.hash.Crc32.hash(payload);

            if (stored_crc != computed_crc) break; // Corrupt record — truncate here.

            // Valid record — add to index.
            try self.index.append(self.allocator, .{
                .file_offset = record_start,
                .raft_index = raft_index,
                .term = term,
                .data_len = data_len,
            });

            offset += total_record_size;
        }

        self.wal_end = offset;

        // Truncate any garbage beyond valid data (torn write recovery).
        if (offset < file_size) {
            try file.setEndPos(offset);
        }
    }

    /// Find the position in self.index for a given raft log index.
    /// Returns null if not found.
    fn findIndexPos(self: *FileStorage, raft_index: u64) ?usize {
        for (self.index.items, 0..) |ie, i| {
            if (ie.raft_index == raft_index) return i;
        }
        return null;
    }

    /// Read an entry from the WAL at the given index entry's offset.
    fn readEntryAt(self: *FileStorage, file: std.fs.File, ie: IndexEntry) !rpc.Entry {
        const payload_size = WAL_HEADER_SIZE + ie.data_len;
        const buf = try self.allocator.alloc(u8, payload_size);
        defer self.allocator.free(buf);

        try file.seekTo(ie.file_offset);
        const n = try file.readAll(buf);
        if (n != payload_size) return stg.StorageError.Unavailable;

        const kind_byte = buf[20];
        const kind: rpc.EntryKind = @enumFromInt(kind_byte);

        const data = if (ie.data_len > 0)
            try self.allocator.dupe(u8, buf[21..][0..ie.data_len])
        else
            @as([]const u8, &.{});

        return .{
            .index = ie.raft_index,
            .term = ie.term,
            .kind = kind,
            .data = data,
        };
    }

    /// Truncate the WAL and index from the given raft index onward.
    fn truncateFrom(self: *FileStorage, from_index: u64) !void {
        const pos = self.findIndexPos(from_index) orelse return;

        // Truncate in-memory index.
        self.index.shrinkRetainingCapacity(pos);

        // Truncate the WAL file.
        if (pos == 0) {
            self.wal_end = 0;
        } else {
            const last = self.index.items[pos - 1];
            self.wal_end = last.file_offset + WAL_HEADER_SIZE + last.data_len + WAL_CRC_SIZE;
        }

        if (self.wal_file) |file| {
            try file.setEndPos(self.wal_end);
        }
    }

    // ========================================================================
    // Type-erased vtable wrappers
    // ========================================================================

    fn typeErasedSaveHardState(ptr: *anyopaque, hs: state.HardState) anyerror!void {
        const self: *FileStorage = @ptrCast(@alignCast(ptr));
        return self.saveHardStateImpl(hs);
    }

    fn typeErasedAppendEntries(ptr: *anyopaque, ents: []const rpc.Entry) anyerror!void {
        const self: *FileStorage = @ptrCast(@alignCast(ptr));
        return self.appendEntriesImpl(ents);
    }

    fn typeErasedEntries(ptr: *anyopaque, lo: u64, hi: u64) anyerror![]rpc.Entry {
        const self: *FileStorage = @ptrCast(@alignCast(ptr));
        return self.entriesImpl(lo, hi);
    }

    fn typeErasedTerm(ptr: *anyopaque, index: u64) anyerror!u64 {
        const self: *FileStorage = @ptrCast(@alignCast(ptr));
        return self.termImpl(index);
    }

    fn typeErasedLastIndex(ptr: *anyopaque) anyerror!u64 {
        const self: *FileStorage = @ptrCast(@alignCast(ptr));
        return self.lastIndexImpl();
    }

    fn typeErasedSaveSnapshot(ptr: *anyopaque, snap: rpc.Snapshot) anyerror!void {
        const self: *FileStorage = @ptrCast(@alignCast(ptr));
        return self.saveSnapshotImpl(snap);
    }

    fn typeErasedLoadSnapshot(ptr: *anyopaque) anyerror!?rpc.Snapshot {
        const self: *FileStorage = @ptrCast(@alignCast(ptr));
        return self.loadSnapshotImpl();
    }

    fn typeErasedCompact(ptr: *anyopaque, index: u64) anyerror!void {
        const self: *FileStorage = @ptrCast(@alignCast(ptr));
        return self.compactImpl(index);
    }
};

// ============================================================================
// Unit Tests
// ============================================================================

const testing = std.testing;

/// Create a temporary directory for testing and return its path.
fn createTestDir(comptime label: []const u8) !struct { path: []const u8, dir: std.fs.Dir } {
    const path = "/tmp/raft-file-storage-test-" ++ label;
    // Clean up any leftover from a previous run.
    std.fs.cwd().deleteTree(path) catch {};
    const dir = try std.fs.cwd().makeOpenPath(path, .{});
    return .{ .path = path, .dir = dir };
}

fn cleanupTestDir(comptime label: []const u8) void {
    const path = "/tmp/raft-file-storage-test-" ++ label;
    std.fs.cwd().deleteTree(path) catch {};
}

test "file_storage: hard state round-trip" {
    defer cleanupTestDir("hard-state");
    var fs = try FileStorage.init(testing.allocator, "/tmp/raft-file-storage-test-hard-state");
    defer fs.deinit();

    // Initially should be the initial hard state.
    try testing.expectEqual(@as(u64, 0), fs.hard_state.current_term);
    try testing.expect(fs.hard_state.voted_for == null);

    // Save and verify.
    try fs.saveHardStateImpl(.{ .current_term = 5, .voted_for = 3 });
    try testing.expectEqual(@as(u64, 5), fs.hard_state.current_term);
    try testing.expectEqual(@as(u64, 3), fs.hard_state.voted_for.?);

    // Close and re-open — state should persist.
    fs.deinit();
    fs = try FileStorage.init(testing.allocator, "/tmp/raft-file-storage-test-hard-state");
    try testing.expectEqual(@as(u64, 5), fs.hard_state.current_term);
    try testing.expectEqual(@as(u64, 3), fs.hard_state.voted_for.?);
}

test "file_storage: hard state with null voted_for" {
    defer cleanupTestDir("hard-state-null");
    var fs = try FileStorage.init(testing.allocator, "/tmp/raft-file-storage-test-hard-state-null");
    defer fs.deinit();

    try fs.saveHardStateImpl(.{ .current_term = 10, .voted_for = null });

    fs.deinit();
    fs = try FileStorage.init(testing.allocator, "/tmp/raft-file-storage-test-hard-state-null");
    try testing.expectEqual(@as(u64, 10), fs.hard_state.current_term);
    try testing.expect(fs.hard_state.voted_for == null);
}

test "file_storage: append and read entries" {
    defer cleanupTestDir("append-read");
    var fs = try FileStorage.init(testing.allocator, "/tmp/raft-file-storage-test-append-read");
    defer fs.deinit();

    const entries_in = [_]rpc.Entry{
        .{ .index = 1, .term = 1, .kind = .normal, .data = "hello" },
        .{ .index = 2, .term = 1, .kind = .normal, .data = "world" },
        .{ .index = 3, .term = 2, .kind = .noop, .data = &.{} },
    };
    try fs.appendEntriesImpl(&entries_in);

    const result = try fs.entriesImpl(1, 4);
    defer {
        for (result) |e| {
            if (e.data.len > 0) testing.allocator.free(e.data);
        }
        testing.allocator.free(result);
    }

    try testing.expectEqual(@as(usize, 3), result.len);
    try testing.expectEqual(@as(u64, 1), result[0].index);
    try testing.expectEqual(@as(u64, 2), result[1].index);
    try testing.expectEqual(@as(u64, 3), result[2].index);
    try testing.expectEqualStrings("hello", result[0].data);
    try testing.expectEqualStrings("world", result[1].data);
    try testing.expectEqual(@as(usize, 0), result[2].data.len);
    try testing.expectEqual(rpc.EntryKind.noop, result[2].kind);
}

test "file_storage: term lookup" {
    defer cleanupTestDir("term-lookup");
    var fs = try FileStorage.init(testing.allocator, "/tmp/raft-file-storage-test-term-lookup");
    defer fs.deinit();

    const entries_in = [_]rpc.Entry{
        .{ .index = 1, .term = 1, .kind = .normal, .data = "a" },
        .{ .index = 2, .term = 1, .kind = .normal, .data = "b" },
        .{ .index = 3, .term = 2, .kind = .normal, .data = "c" },
    };
    try fs.appendEntriesImpl(&entries_in);

    try testing.expectEqual(@as(u64, 1), try fs.termImpl(1));
    try testing.expectEqual(@as(u64, 1), try fs.termImpl(2));
    try testing.expectEqual(@as(u64, 2), try fs.termImpl(3));

    // Index 0 is the snapshot index (0), should return snapshot_term (0).
    try testing.expectEqual(@as(u64, 0), try fs.termImpl(0));
}

test "file_storage: lastIndex empty and non-empty" {
    defer cleanupTestDir("last-index");
    var fs = try FileStorage.init(testing.allocator, "/tmp/raft-file-storage-test-last-index");
    defer fs.deinit();

    try testing.expectEqual(@as(u64, 0), try fs.lastIndexImpl());

    const entries_in = [_]rpc.Entry{
        .{ .index = 1, .term = 1, .kind = .normal, .data = &.{} },
        .{ .index = 2, .term = 1, .kind = .normal, .data = &.{} },
    };
    try fs.appendEntriesImpl(&entries_in);
    try testing.expectEqual(@as(u64, 2), try fs.lastIndexImpl());
}

test "file_storage: compact removes old entries" {
    defer cleanupTestDir("compact");
    var fs = try FileStorage.init(testing.allocator, "/tmp/raft-file-storage-test-compact");
    defer fs.deinit();

    const entries_in = [_]rpc.Entry{
        .{ .index = 1, .term = 1, .kind = .normal, .data = "a" },
        .{ .index = 2, .term = 1, .kind = .normal, .data = "b" },
        .{ .index = 3, .term = 2, .kind = .normal, .data = "c" },
        .{ .index = 4, .term = 2, .kind = .normal, .data = "d" },
    };
    try fs.appendEntriesImpl(&entries_in);

    try fs.compactImpl(2);

    try testing.expectEqual(@as(u64, 2), fs.snapshot_index);
    try testing.expectEqual(@as(u64, 1), fs.snapshot_term);
    try testing.expectEqual(@as(usize, 2), fs.index.items.len);

    // Compacted entries should return Compacted error.
    try testing.expectError(stg.StorageError.Compacted, fs.entriesImpl(1, 3));

    // Remaining entries should be accessible.
    const result = try fs.entriesImpl(3, 5);
    defer {
        for (result) |e| {
            if (e.data.len > 0) testing.allocator.free(e.data);
        }
        testing.allocator.free(result);
    }
    try testing.expectEqual(@as(usize, 2), result.len);
    try testing.expectEqual(@as(u64, 3), result[0].index);
    try testing.expectEqual(@as(u64, 4), result[1].index);
    try testing.expectEqualStrings("c", result[0].data);
    try testing.expectEqualStrings("d", result[1].data);
}

test "file_storage: compact all entries" {
    defer cleanupTestDir("compact-all");
    var fs = try FileStorage.init(testing.allocator, "/tmp/raft-file-storage-test-compact-all");
    defer fs.deinit();

    const entries_in = [_]rpc.Entry{
        .{ .index = 1, .term = 1, .kind = .normal, .data = "a" },
        .{ .index = 2, .term = 1, .kind = .normal, .data = "b" },
    };
    try fs.appendEntriesImpl(&entries_in);
    try fs.compactImpl(2);

    try testing.expectEqual(@as(u64, 2), fs.snapshot_index);
    try testing.expectEqual(@as(usize, 0), fs.index.items.len);
    try testing.expectEqual(@as(u64, 2), try fs.lastIndexImpl());
}

test "file_storage: snapshot save and load" {
    defer cleanupTestDir("snapshot");
    var fs = try FileStorage.init(testing.allocator, "/tmp/raft-file-storage-test-snapshot");
    defer fs.deinit();

    const snap = rpc.Snapshot{
        .metadata = .{
            .index = 10,
            .term = 3,
            .config = .{ .voters = &[_]u64{ 1, 2, 3 }, .voters_outgoing = &.{}, .is_joint = false },
        },
        .data = "my-snapshot-data",
    };
    try fs.saveSnapshotImpl(snap);

    try testing.expectEqual(@as(u64, 10), fs.snapshot_index);
    try testing.expectEqual(@as(u64, 3), fs.snapshot_term);

    const loaded = try fs.loadSnapshotImpl();
    try testing.expect(loaded != null);
    defer testing.allocator.free(loaded.?.data);
    try testing.expectEqual(@as(u64, 10), loaded.?.metadata.index);
    try testing.expectEqual(@as(u64, 3), loaded.?.metadata.term);
    try testing.expectEqualStrings("my-snapshot-data", loaded.?.data);
}

test "file_storage: snapshot persists across restart" {
    defer cleanupTestDir("snap-persist");
    {
        var fs = try FileStorage.init(testing.allocator, "/tmp/raft-file-storage-test-snap-persist");
        defer fs.deinit();

        try fs.saveSnapshotImpl(.{
            .metadata = .{
                .index = 5,
                .term = 2,
                .config = .{ .voters = &.{}, .voters_outgoing = &.{}, .is_joint = false },
            },
            .data = "persistent-snap",
        });
    }

    // Re-open.
    var fs = try FileStorage.init(testing.allocator, "/tmp/raft-file-storage-test-snap-persist");
    defer fs.deinit();

    try testing.expectEqual(@as(u64, 5), fs.snapshot_index);
    try testing.expectEqual(@as(u64, 2), fs.snapshot_term);

    const loaded = try fs.loadSnapshotImpl();
    try testing.expect(loaded != null);
    defer testing.allocator.free(loaded.?.data);
    try testing.expectEqualStrings("persistent-snap", loaded.?.data);
}

test "file_storage: WAL recovery after clean close" {
    defer cleanupTestDir("wal-recovery");
    {
        var fs = try FileStorage.init(testing.allocator, "/tmp/raft-file-storage-test-wal-recovery");
        defer fs.deinit();

        const entries_in = [_]rpc.Entry{
            .{ .index = 1, .term = 1, .kind = .normal, .data = "first" },
            .{ .index = 2, .term = 1, .kind = .normal, .data = "second" },
            .{ .index = 3, .term = 2, .kind = .config, .data = "cfg" },
        };
        try fs.appendEntriesImpl(&entries_in);
    }

    // Re-open and verify recovery.
    var fs = try FileStorage.init(testing.allocator, "/tmp/raft-file-storage-test-wal-recovery");
    defer fs.deinit();

    try testing.expectEqual(@as(u64, 3), try fs.lastIndexImpl());
    try testing.expectEqual(@as(usize, 3), fs.index.items.len);

    const result = try fs.entriesImpl(1, 4);
    defer {
        for (result) |e| {
            if (e.data.len > 0) testing.allocator.free(e.data);
        }
        testing.allocator.free(result);
    }
    try testing.expectEqual(@as(usize, 3), result.len);
    try testing.expectEqualStrings("first", result[0].data);
    try testing.expectEqualStrings("second", result[1].data);
    try testing.expectEqualStrings("cfg", result[2].data);
    try testing.expectEqual(rpc.EntryKind.config, result[2].kind);
}

test "file_storage: torn write recovery truncates corrupt record" {
    defer cleanupTestDir("torn-write");
    const dir_path = "/tmp/raft-file-storage-test-torn-write";
    {
        var fs = try FileStorage.init(testing.allocator, dir_path);
        defer fs.deinit();

        const entries_in = [_]rpc.Entry{
            .{ .index = 1, .term = 1, .kind = .normal, .data = "valid" },
        };
        try fs.appendEntriesImpl(&entries_in);
    }

    // Append garbage to the WAL to simulate a torn write.
    {
        var dir = try std.fs.cwd().openDir(dir_path, .{});
        defer dir.close();
        var file = try dir.openFile("wal", .{ .mode = .read_write });
        defer file.close();
        const end = try file.getEndPos();
        try file.seekTo(end);
        try file.writeAll("GARBAGE_TORN_WRITE_DATA");
    }

    // Re-open — should recover by truncating the garbage.
    var fs = try FileStorage.init(testing.allocator, dir_path);
    defer fs.deinit();

    try testing.expectEqual(@as(u64, 1), try fs.lastIndexImpl());
    try testing.expectEqual(@as(usize, 1), fs.index.items.len);

    const result = try fs.entriesImpl(1, 2);
    defer {
        for (result) |e| {
            if (e.data.len > 0) testing.allocator.free(e.data);
        }
        testing.allocator.free(result);
    }
    try testing.expectEqualStrings("valid", result[0].data);
}

test "file_storage: appendEntries truncates conflicting entries" {
    defer cleanupTestDir("truncate-conflict");
    var fs = try FileStorage.init(testing.allocator, "/tmp/raft-file-storage-test-truncate-conflict");
    defer fs.deinit();

    // Append entries at indices 1-3, term 1.
    const batch1 = [_]rpc.Entry{
        .{ .index = 1, .term = 1, .kind = .normal, .data = "a" },
        .{ .index = 2, .term = 1, .kind = .normal, .data = "b" },
        .{ .index = 3, .term = 1, .kind = .normal, .data = "c" },
    };
    try fs.appendEntriesImpl(&batch1);

    // Overwrite from index 2 with a new term.
    const batch2 = [_]rpc.Entry{
        .{ .index = 2, .term = 2, .kind = .normal, .data = "B2" },
        .{ .index = 3, .term = 2, .kind = .normal, .data = "C2" },
    };
    try fs.appendEntriesImpl(&batch2);

    try testing.expectEqual(@as(u64, 3), try fs.lastIndexImpl());
    try testing.expectEqual(@as(u64, 2), try fs.termImpl(2));
    try testing.expectEqual(@as(u64, 2), try fs.termImpl(3));
    try testing.expectEqual(@as(u64, 1), try fs.termImpl(1)); // unchanged

    const result = try fs.entriesImpl(1, 4);
    defer {
        for (result) |e| {
            if (e.data.len > 0) testing.allocator.free(e.data);
        }
        testing.allocator.free(result);
    }
    try testing.expectEqualStrings("a", result[0].data);
    try testing.expectEqualStrings("B2", result[1].data);
    try testing.expectEqualStrings("C2", result[2].data);
}

test "file_storage: vtable dispatch works" {
    defer cleanupTestDir("vtable");
    var fs = try FileStorage.init(testing.allocator, "/tmp/raft-file-storage-test-vtable");
    defer fs.deinit();

    const s = fs.storage();

    const entries_in = [_]rpc.Entry{
        .{ .index = 1, .term = 1, .kind = .normal, .data = "x" },
    };
    try s.appendEntries(&entries_in);

    try testing.expectEqual(@as(u64, 1), try s.lastIndex());
    try testing.expectEqual(@as(u64, 1), try s.term(1));

    const result = try s.entries(1, 2);
    defer {
        for (result) |e| {
            if (e.data.len > 0) testing.allocator.free(e.data);
        }
        testing.allocator.free(result);
    }
    try testing.expectEqual(@as(usize, 1), result.len);
    try testing.expectEqualStrings("x", result[0].data);
}

test "file_storage: compaction persists across restart" {
    defer cleanupTestDir("compact-persist");
    const dir_path = "/tmp/raft-file-storage-test-compact-persist";
    {
        var fs = try FileStorage.init(testing.allocator, dir_path);
        defer fs.deinit();

        const entries_in = [_]rpc.Entry{
            .{ .index = 1, .term = 1, .kind = .normal, .data = "a" },
            .{ .index = 2, .term = 1, .kind = .normal, .data = "b" },
            .{ .index = 3, .term = 2, .kind = .normal, .data = "c" },
        };
        try fs.appendEntriesImpl(&entries_in);
        try fs.compactImpl(1);
    }

    var fs = try FileStorage.init(testing.allocator, dir_path);
    defer fs.deinit();

    try testing.expectEqual(@as(u64, 1), fs.snapshot_index);
    try testing.expectEqual(@as(u64, 1), fs.snapshot_term);
    try testing.expectEqual(@as(u64, 3), try fs.lastIndexImpl());

    const result = try fs.entriesImpl(2, 4);
    defer {
        for (result) |e| {
            if (e.data.len > 0) testing.allocator.free(e.data);
        }
        testing.allocator.free(result);
    }
    try testing.expectEqual(@as(usize, 2), result.len);
    try testing.expectEqualStrings("b", result[0].data);
    try testing.expectEqualStrings("c", result[1].data);
}

test "file_storage: entries returns Compacted for compacted range" {
    defer cleanupTestDir("compacted-err");
    var fs = try FileStorage.init(testing.allocator, "/tmp/raft-file-storage-test-compacted-err");
    defer fs.deinit();

    const entries_in = [_]rpc.Entry{
        .{ .index = 1, .term = 1, .kind = .normal, .data = "a" },
        .{ .index = 2, .term = 1, .kind = .normal, .data = "b" },
        .{ .index = 3, .term = 2, .kind = .normal, .data = "c" },
    };
    try fs.appendEntriesImpl(&entries_in);
    try fs.compactImpl(2);

    try testing.expectError(stg.StorageError.Compacted, fs.entriesImpl(1, 3));
    try testing.expectError(stg.StorageError.Compacted, fs.entriesImpl(2, 3));
}

test "file_storage: large data entries" {
    defer cleanupTestDir("large-data");
    var fs = try FileStorage.init(testing.allocator, "/tmp/raft-file-storage-test-large-data");
    defer fs.deinit();

    // Create a 4KB entry.
    const large_data = try testing.allocator.alloc(u8, 4096);
    defer testing.allocator.free(large_data);
    @memset(large_data, 0xAB);

    const entries_in = [_]rpc.Entry{
        .{ .index = 1, .term = 1, .kind = .normal, .data = large_data },
    };
    try fs.appendEntriesImpl(&entries_in);

    const result = try fs.entriesImpl(1, 2);
    defer {
        for (result) |e| {
            if (e.data.len > 0) testing.allocator.free(e.data);
        }
        testing.allocator.free(result);
    }

    try testing.expectEqual(@as(usize, 4096), result[0].data.len);
    try testing.expectEqual(@as(u8, 0xAB), result[0].data[0]);
    try testing.expectEqual(@as(u8, 0xAB), result[0].data[4095]);
}
