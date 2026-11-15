// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-2025 CodePros

//! File-backed implementation of the `Storage` interface.
//!
//! Provides crash-safe, durable storage using flat files on disk. Uses the
//! `wal` library for the write-ahead log (append, recovery, compaction) and
//! maintains an in-memory index for O(1) term lookups.
//!
//! ## Directory layout
//!
//! ```
//! <data_dir>/
//!   hard_state    -- 20 bytes: [term: u64 LE][voted_for: u64 LE][crc32: u32 LE]
//!   entries.wal   -- write-ahead log of entry records (managed by wal library)
//!   snapshot.meta -- variable: [index: u64 LE][term: u64 LE][data_blake3: 32 bytes][config_data...][crc32: u32 LE]
//!   snapshot.data -- raw snapshot bytes
//! ```
//!
//! ## WAL payload format
//!
//! Each WAL record payload (inside the wal library's framing) contains:
//! ```
//! [term: u64 LE][kind: u8][data: N bytes]
//! ```
//! The raft entry index is mapped to the WAL LSN.
//!
//! ## Crash safety
//!
//! - Hard state and snapshots: write to temp file, sync, rename.
//! - WAL: handled by the wal library (CRC32, fdatasync, torn write recovery).
//!   Only a torn tail is truncated on open; a damaged record with valid ones
//!   after it, or a WAL in the other key mode, fails `init` with
//!   `StorageError.LogCorrupted` / `LogAuthFailed` / `LogEncryptionMismatch`
//!   and leaves the file untouched (SONNY-6622).
//! - Compaction: handled by the wal library (atomic rewrite via temp + rename).
//!   A compaction the WAL refuses is logged, not fatal: the snapshot is
//!   durable, the WAL stays in use, and recovery skips records at or below
//!   the snapshot index.

const std = @import("std");
const wal_mod = @import("wal");
const state = @import("state.zig");
const rpc = @import("rpc.zig");
const stg = @import("storage.zig");
const mem = @import("membership.zig");

/// Sentinel value for voted_for = null in the hard_state file.
const VOTED_FOR_NONE: u64 = std.math.maxInt(u64);

/// Payload header size: [term: u64 LE][kind: u8] = 9 bytes.
const PAYLOAD_HEADER_SIZE: usize = 8 + 1;

pub const FileStorage = struct {
    dir: std.Io.Dir,
    lock_file: ?std.Io.File,
    wal_log: ?wal_mod.WriteAheadLog,
    index: std.ArrayList(IndexEntry),
    snapshot_index: u64,
    snapshot_term: u64,
    snapshot_data_digest: [32]u8,
    snapshot_config_voters: std.ArrayList(u64),
    snapshot_config_voters_outgoing: std.ArrayList(u64),
    snapshot_config_is_joint: bool,
    hard_state: state.HardState,
    allocator: std.mem.Allocator,
    max_snapshot_size: u64 = 256 * 1024 * 1024,

    pub const DirectoryAlreadyLocked = error.WouldBlock;

    /// In-memory index entry for fast term lookups.
    const IndexEntry = struct {
        raft_index: u64,
        term: u64,
        data_len: u32,
    };

    /// Flush directory metadata to disk after a rename (SEC-0065).
    fn syncDir(self: *FileStorage) !void {
        const sync_fd = try std.posix.openat(self.dir.handle, ".", .{ .DIRECTORY = true }, 0);
        defer _ = std.c.close(sync_fd);
        if (std.c.fdatasync(sync_fd) != 0) return error.InputOutput;
    }

    pub fn init(allocator: std.mem.Allocator, dir_path: []const u8) !FileStorage {
        var dir = try std.Io.Dir.cwd().createDirPathOpen(std.Io.Threaded.global_single_threaded.io(), dir_path, .{});
        errdefer dir.close(std.Io.Threaded.global_single_threaded.io());

        const lock_file = try dir.createFile(std.Io.Threaded.global_single_threaded.io(), ".lock", .{
            .lock = .exclusive,
            .lock_nonblocking = true,
            .truncate = false,
        });
        errdefer lock_file.close(std.Io.Threaded.global_single_threaded.io());

        var fs = FileStorage{
            .dir = dir,
            .lock_file = lock_file,
            .wal_log = null,
            .index = .empty,
            .snapshot_index = 0,
            .snapshot_term = 0,
            .snapshot_data_digest = .{0} ** 32,
            .snapshot_config_voters = .empty,
            .snapshot_config_voters_outgoing = .empty,
            .snapshot_config_is_joint = false,
            .hard_state = state.HardState.initial,
            .allocator = allocator,
        };
        errdefer {
            fs.index.deinit(allocator);
            fs.snapshot_config_voters.deinit(allocator);
            fs.snapshot_config_voters_outgoing.deinit(allocator);
            if (fs.wal_log) |*w| w.deinit();
        }

        dir.deleteFile(std.Io.Threaded.global_single_threaded.io(), "hard_state.tmp") catch {};

        // Recover hard state.
        fs.hard_state = fs.readHardStateFile() catch state.HardState.initial;

        // Recover snapshot metadata.
        const snap_meta = fs.readSnapshotMeta() catch null;
        if (snap_meta) |meta| {
            var m = meta;
            defer m.freeConfig(allocator);

            fs.snapshot_index = meta.index;
            fs.snapshot_term = meta.term;
            fs.snapshot_data_digest = meta.data_digest orelse (.{0} ** 32);
            if (meta.config) |cfg| {
                try fs.snapshot_config_voters.appendSlice(allocator, cfg.voters);
                errdefer fs.snapshot_config_voters.clearRetainingCapacity();
                try fs.snapshot_config_voters_outgoing.appendSlice(allocator, cfg.voters_outgoing);
                fs.snapshot_config_is_joint = cfg.is_joint;
            }
        }

        dir.deleteFile(std.Io.Threaded.global_single_threaded.io(), "snapshot.data.tmp") catch {};
        dir.deleteFile(std.Io.Threaded.global_single_threaded.io(), "snapshot.meta.tmp") catch {};

        // Open WAL and rebuild in-memory index.
        try fs.recoverWal();

        return fs;
    }

    pub fn deinit(self: *FileStorage) void {
        if (self.wal_log) |*w| w.deinit();
        self.index.deinit(self.allocator);
        self.snapshot_config_voters.deinit(self.allocator);
        self.snapshot_config_voters_outgoing.deinit(self.allocator);
        if (self.lock_file) |lf| lf.close(std.Io.Threaded.global_single_threaded.io());
        self.dir.close(std.Io.Threaded.global_single_threaded.io());
    }

    pub fn storage(self: *FileStorage) stg.Storage {
        return .{
            .ptr = @ptrCast(self),
            .vtable = &vtable_inst,
        };
    }

    const vtable_inst = stg.Storage.VTable{
        .saveHardState = typeErasedSaveHardState,
        .loadHardState = typeErasedLoadHardState,
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
        var buf: [20]u8 = undefined;
        std.mem.writeInt(u64, buf[0..8], hs.current_term, .little);
        std.mem.writeInt(u64, buf[8..16], hs.voted_for orelse VOTED_FOR_NONE, .little);
        // fips-exempt: non-security — CRC32 record-integrity check for the on-disk
        // hard-state file; detects corruption, not tampering.
        const crc = std.hash.Crc32.hash(buf[0..16]);
        std.mem.writeInt(u32, buf[16..20], crc, .little);

        var tmp = try self.dir.createFile(std.Io.Threaded.global_single_threaded.io(), "hard_state.tmp", .{});
        errdefer {
            tmp.close(std.Io.Threaded.global_single_threaded.io());
            self.dir.deleteFile(std.Io.Threaded.global_single_threaded.io(), "hard_state.tmp") catch {};
        }
        try tmp.writeStreamingAll(std.Io.Threaded.global_single_threaded.io(), &buf);
        try tmp.sync(std.Io.Threaded.global_single_threaded.io());
        tmp.close(std.Io.Threaded.global_single_threaded.io());

        try self.dir.rename("hard_state.tmp", self.dir, "hard_state", std.Io.Threaded.global_single_threaded.io());
        try self.syncDir();
        self.hard_state = hs;
    }

    pub fn appendEntriesImpl(self: *FileStorage, ents: []const rpc.Entry) !void {
        if (ents.len == 0) return;

        const first_new_index = ents[0].index;

        // Handle truncation: if appending entries that overlap with existing,
        // truncate the WAL and index from the conflict point.
        try self.truncateFrom(first_new_index);

        // Ensure WAL is open.
        if (self.wal_log == null) {
            self.wal_log = try self.openWal();
        }

        // Pre-reserve index capacity.
        self.index.ensureTotalCapacity(self.allocator, self.index.items.len + ents.len) catch |err| {
            return err;
        };

        // Serialize entries as WAL payloads: [term:u64 LE][kind:u8][data]
        const payloads = try self.allocator.alloc([]const u8, ents.len);
        defer {
            for (payloads) |p| {
                if (p.len > 0) self.allocator.free(p);
            }
            self.allocator.free(payloads);
        }

        const lsns = try self.allocator.alloc(u64, ents.len);
        defer self.allocator.free(lsns);

        for (ents, 0..) |entry, i| {
            const payload_size = PAYLOAD_HEADER_SIZE + entry.data.len;
            const buf = try self.allocator.alloc(u8, payload_size);
            std.mem.writeInt(u64, buf[0..8], entry.term, .little);
            buf[8] = @intFromEnum(entry.kind);
            if (entry.data.len > 0) {
                @memcpy(buf[9..][0..entry.data.len], entry.data);
            }
            payloads[i] = buf;
            lsns[i] = entry.index;
        }

        // Append to WAL using entry indices as LSNs.
        _ = self.wal_log.?.appendBatchWithLsns(lsns, payloads) catch |err| {
            return err;
        };

        // Update in-memory index (capacity pre-reserved, cannot fail).
        for (ents) |entry| {
            self.index.appendAssumeCapacity(.{
                .raft_index = entry.index,
                .term = entry.term,
                .data_len = @intCast(entry.data.len),
            });
        }
    }

    pub fn entriesImpl(self: *FileStorage, lo: u64, hi: u64) ![]rpc.Entry {
        if (lo <= self.snapshot_index) return stg.StorageError.Compacted;
        if (lo >= hi) return &[_]rpc.Entry{};

        // Validate range exists in index.
        const start_pos = self.findIndexPos(lo) orelse return stg.StorageError.Unavailable;
        const end_pos = self.findIndexPos(hi - 1) orelse return stg.StorageError.Unavailable;
        const count = end_pos - start_pos + 1;

        const result = try self.allocator.alloc(rpc.Entry, count);
        var read_count: usize = 0;
        errdefer {
            for (result[0..read_count]) |e| {
                if (e.data.len > 0) self.allocator.free(@constCast(e.data));
            }
            self.allocator.free(result);
        }

        var w = self.wal_log orelse return stg.StorageError.Unavailable;

        // Use WAL iterator starting from lo-1 (iterator returns records with LSN > from_lsn).
        var iter = try w.iterator(lo - 1);
        defer iter.deinit();

        var i: usize = 0;
        while (i < count) : (i += 1) {
            var entry_opt = try iter.next();
            if (entry_opt == null) return stg.StorageError.Unavailable;
            var wal_entry = &entry_opt.?;
            defer wal_entry.deinit();

            const payload = wal_entry.payload;
            if (payload.len < PAYLOAD_HEADER_SIZE) return stg.StorageError.Unavailable;

            const term = std.mem.readInt(u64, payload[0..8], .little);
            const kind: rpc.EntryKind = std.enums.fromInt(rpc.EntryKind, payload[8]) orelse
                return stg.StorageError.Unavailable;
            const data_slice = payload[PAYLOAD_HEADER_SIZE..];

            const data = if (data_slice.len > 0)
                try self.allocator.dupe(u8, data_slice)
            else
                @as([]const u8, &.{});

            result[i] = .{
                .index = wal_entry.lsn,
                .term = term,
                .kind = kind,
                .data = data,
            };
            read_count += 1;
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
        const prev_snapshot_index = self.snapshot_index;
        // fips-exempt: non-security — snapshot-data integrity digest on disk.
        const Blake3 = std.crypto.hash.Blake3;
        var hasher = Blake3.init(.{});
        hasher.update(snap.data);
        var data_digest: [32]u8 = undefined;
        hasher.final(&data_digest);

        {
            var tmp = try self.dir.createFile(std.Io.Threaded.global_single_threaded.io(), "snapshot.data.tmp", .{});
            errdefer {
                tmp.close(std.Io.Threaded.global_single_threaded.io());
                self.dir.deleteFile(std.Io.Threaded.global_single_threaded.io(), "snapshot.data.tmp") catch {};
            }
            try tmp.writeStreamingAll(std.Io.Threaded.global_single_threaded.io(), snap.data);
            try tmp.sync(std.Io.Threaded.global_single_threaded.io());
            tmp.close(std.Io.Threaded.global_single_threaded.io());
        }

        {
            const meta_buf = try serializeSnapshotMeta(self.allocator, snap.metadata.index, snap.metadata.term, data_digest, snap.metadata.config);
            defer self.allocator.free(meta_buf);

            var tmp = try self.dir.createFile(std.Io.Threaded.global_single_threaded.io(), "snapshot.meta.tmp", .{});
            errdefer {
                tmp.close(std.Io.Threaded.global_single_threaded.io());
                self.dir.deleteFile(std.Io.Threaded.global_single_threaded.io(), "snapshot.meta.tmp") catch {};
            }
            try tmp.writeStreamingAll(std.Io.Threaded.global_single_threaded.io(), meta_buf);
            try tmp.sync(std.Io.Threaded.global_single_threaded.io());
            tmp.close(std.Io.Threaded.global_single_threaded.io());
        }

        try self.dir.rename("snapshot.meta.tmp", self.dir, "snapshot.meta", std.Io.Threaded.global_single_threaded.io());
        try self.dir.rename("snapshot.data.tmp", self.dir, "snapshot.data", std.Io.Threaded.global_single_threaded.io());
        try self.syncDir();

        self.snapshot_index = snap.metadata.index;
        self.snapshot_term = snap.metadata.term;
        self.snapshot_data_digest = data_digest;

        var new_voters: std.ArrayList(u64) = .empty;
        try new_voters.appendSlice(self.allocator, snap.metadata.config.voters);
        errdefer new_voters.deinit(self.allocator);

        var new_outgoing: std.ArrayList(u64) = .empty;
        try new_outgoing.appendSlice(self.allocator, snap.metadata.config.voters_outgoing);

        self.snapshot_config_voters.deinit(self.allocator);
        self.snapshot_config_voters = new_voters;
        self.snapshot_config_voters_outgoing.deinit(self.allocator);
        self.snapshot_config_voters_outgoing = new_outgoing;
        self.snapshot_config_is_joint = snap.metadata.config.is_joint;

        if (snap.metadata.index > prev_snapshot_index) {
            try self.discardCoveredBy(snap.metadata.index, snap.metadata.term);
        }
    }

    /// Drop the log entries a snapshot at `snap_index` / `snap_term` (already
    /// durable) covers (Raft §7, SONNY-6611). If the log holds that entry
    /// with that term, the entries up to it go and the rest is kept (a
    /// leader's own snapshot); otherwise the log conflicts with or lags the
    /// snapshot and all of it goes (a follower's install). Before this the
    /// WAL and index kept every entry, and a follower kept a conflicting
    /// tail. A compaction the WAL refuses is logged: recovery skips records
    /// at or below the snapshot.
    fn discardCoveredBy(self: *FileStorage, snap_index: u64, snap_term: u64) !void {
        if (self.findIndexPos(snap_index)) |pos| {
            const items = self.index.items;
            if (items[pos].term == snap_term and pos + 1 < items.len) {
                const remaining = items.len - (pos + 1);
                std.mem.copyForwards(IndexEntry, items[0..remaining], items[pos + 1 ..]);
                self.index.shrinkRetainingCapacity(remaining);
                if (self.wal_log) |*w| {
                    w.compact(snap_index) catch |err| {
                        if (!isWalRefusal(err)) return err;
                        std.log.warn("raft: FileStorage: entries.wal compaction up to snapshot {d} refused: {s}; " ++
                            "the log is unchanged and stays in use", .{ snap_index, @errorName(err) });
                    };
                }
                return;
            }
        }

        self.index.shrinkRetainingCapacity(0);
        if (self.wal_log) |*w| {
            w.deinit();
            self.wal_log = null;
        }
        self.dir.deleteFile(std.Io.Threaded.global_single_threaded.io(), "entries.wal") catch |err| switch (err) {
            error.FileNotFound => {},
            else => return err,
        };
        try self.syncDir();
    }

    pub fn loadSnapshotImpl(self: *FileStorage) !?rpc.Snapshot {
        var meta = self.readSnapshotMeta() catch |err| {
            if (err == error.FileNotFound) return null;
            return err;
        };

        const data_file = self.dir.openFile(std.Io.Threaded.global_single_threaded.io(), "snapshot.data", .{}) catch |err| {
            if (err == error.FileNotFound) {
                self.dir.rename("snapshot.data.tmp", self.dir, "snapshot.data", std.Io.Threaded.global_single_threaded.io()) catch {
                    meta.freeConfig(self.allocator);
                    return null;
                };
                self.syncDir() catch {};
                const recovered = self.dir.openFile(std.Io.Threaded.global_single_threaded.io(), "snapshot.data", .{}) catch {
                    meta.freeConfig(self.allocator);
                    return null;
                };
                return self.finishLoadSnapshot(meta, recovered);
            }
            meta.freeConfig(self.allocator);
            return null;
        };

        return self.finishLoadSnapshot(meta, data_file);
    }

    fn finishLoadSnapshot(self: *FileStorage, meta: SnapshotMeta, data_file: std.Io.File) !?rpc.Snapshot {
        var m = meta;
        defer data_file.close(std.Io.Threaded.global_single_threaded.io());
        errdefer m.freeConfig(self.allocator);

        const size = try data_file.length(std.Io.Threaded.global_single_threaded.io());
        if (size > self.max_snapshot_size) {
            return stg.StorageError.Unavailable;
        }
        const data = try self.allocator.alloc(u8, size);
        errdefer self.allocator.free(data);
        const bytes_read = try data_file.readPositionalAll(std.Io.Threaded.global_single_threaded.io(), data, 0);
        if (bytes_read != size) {
            self.allocator.free(data);
            m.freeConfig(self.allocator);
            return null;
        }

        if (m.data_digest) |expected_digest| {
            // fips-exempt: non-security — snapshot-data integrity verification on load.
            const Blake3 = std.crypto.hash.Blake3;
            var hasher = Blake3.init(.{});
            hasher.update(data);
            var actual_digest: [32]u8 = undefined;
            hasher.final(&actual_digest);
            if (!std.mem.eql(u8, &actual_digest, &expected_digest)) {
                return stg.StorageError.Unavailable;
            }
        }

        const cfg = m.config orelse rpc.ClusterConfig{
            .voters = &.{},
            .voters_outgoing = &.{},
            .is_joint = false,
        };

        return rpc.Snapshot{
            .metadata = .{
                .index = m.index,
                .term = m.term,
                .config = cfg,
            },
            .data = data,
        };
    }

    pub fn compactImpl(self: *FileStorage, index: u64) !void {
        if (index <= self.snapshot_index) return;

        const compact_pos = self.findIndexPos(index) orelse return;

        const new_term = self.index.items[compact_pos].term;
        const cached_cfg = rpc.ClusterConfig{
            .voters = self.snapshot_config_voters.items,
            .voters_outgoing = self.snapshot_config_voters_outgoing.items,
            .is_joint = self.snapshot_config_is_joint,
        };
        // What to restore if the WAL compaction fails (SONNY-6611).
        const old_index = self.snapshot_index;
        const old_term = self.snapshot_term;
        const had_meta = if (self.dir.access(std.Io.Threaded.global_single_threaded.io(), "snapshot.meta", .{})) |_| true else |_| false;
        try self.writeSnapshotMeta(index, new_term, self.snapshot_data_digest, cached_cfg);

        self.snapshot_term = new_term;
        self.snapshot_index = index;

        const remaining_count = self.index.items.len - (compact_pos + 1);

        if (remaining_count == 0) {
            // All entries compacted — close WAL.
            self.index.shrinkRetainingCapacity(0);
            if (self.wal_log) |*w| {
                w.deinit();
                self.wal_log = null;
            }
            self.dir.deleteFile(std.Io.Threaded.global_single_threaded.io(), "entries.wal") catch {};
            self.syncDir() catch {};
            return;
        }

        // Compact WAL: remove all records with LSN <= index. The WAL
        // validates every record first and refuses to rewrite a damaged log,
        // leaving it untouched and usable (wal v0.1.24). The snapshot is
        // already durable, so that is not fatal: the records <= index stay in
        // the file and recoverWal skips them (SONNY-6622).
        //
        // Any other failure leaves the file unchanged too, so the snapshot
        // point is rolled back (on disk and in memory) and the error
        // returned: before, the snapshot stayed advanced over entries the
        // WAL still held while compact() reported failure (SONNY-6611).
        // Except WalPoisoned: the rewrite may already have replaced the
        // file, so the advanced snapshot point is the one that matches it.
        if (self.wal_log) |*w| {
            w.compact(index) catch |err| {
                if (isWalRefusal(err)) {
                    std.log.warn("raft: FileStorage: entries.wal compaction up to {d} refused: {s}; " ++
                        "the log is unchanged and stays in use", .{ index, @errorName(err) });
                } else {
                    if (err != error.WalPoisoned) self.rollbackSnapshotPoint(old_index, old_term, had_meta, cached_cfg);
                    return err;
                }
            };
        }

        // Trim in-memory index.
        const new_items = self.index.items[compact_pos + 1 ..];
        std.mem.copyForwards(IndexEntry, self.index.items[0..remaining_count], new_items);
        self.index.shrinkRetainingCapacity(remaining_count);
    }

    /// Undo compactImpl's snapshot point after a failed WAL compaction.
    /// Best effort: if the meta cannot be restored on disk, recovery still
    /// finds every entry in the WAL, it just treats those up to the newer
    /// point as compacted.
    fn rollbackSnapshotPoint(self: *FileStorage, old_index: u64, old_term: u64, had_meta: bool, cfg: rpc.ClusterConfig) void {
        self.snapshot_index = old_index;
        self.snapshot_term = old_term;
        const restored = if (had_meta)
            self.writeSnapshotMeta(old_index, old_term, self.snapshot_data_digest, cfg)
        else blk: {
            self.dir.deleteFile(std.Io.Threaded.global_single_threaded.io(), "snapshot.meta") catch |err| break :blk err;
            break :blk self.syncDir();
        };
        restored catch |err| std.log.warn("raft: FileStorage: could not roll snapshot.meta back to {d}: {s}", .{ old_index, @errorName(err) });
    }

    fn writeSnapshotMeta(self: *FileStorage, snap_index: u64, snap_term: u64, data_digest: [32]u8, cfg: rpc.ClusterConfig) !void {
        const meta_buf = try serializeSnapshotMeta(self.allocator, snap_index, snap_term, data_digest, cfg);
        defer self.allocator.free(meta_buf);

        var tmp = try self.dir.createFile(std.Io.Threaded.global_single_threaded.io(), "snapshot.meta.tmp", .{});
        errdefer {
            tmp.close(std.Io.Threaded.global_single_threaded.io());
            self.dir.deleteFile(std.Io.Threaded.global_single_threaded.io(), "snapshot.meta.tmp") catch {};
        }
        try tmp.writeStreamingAll(std.Io.Threaded.global_single_threaded.io(), meta_buf);
        try tmp.sync(std.Io.Threaded.global_single_threaded.io());
        tmp.close(std.Io.Threaded.global_single_threaded.io());
        try self.dir.rename("snapshot.meta.tmp", self.dir, "snapshot.meta", std.Io.Threaded.global_single_threaded.io());
        try self.syncDir();
    }

    pub fn loadHardState(self: *FileStorage) state.HardState {
        return self.hard_state;
    }

    // ========================================================================
    // Private helpers
    // ========================================================================

    fn serializeSnapshotMeta(allocator: std.mem.Allocator, snap_index: u64, snap_term: u64, data_digest: [32]u8, cfg: rpc.ClusterConfig) ![]u8 {
        const config_data = try mem.MembershipState.serializeConfig(allocator, cfg);
        defer allocator.free(config_data);

        const config_len: u32 = @intCast(config_data.len);
        const total_size = 48 + 4 + config_data.len + 4;
        const buf = try allocator.alloc(u8, total_size);
        errdefer allocator.free(buf);

        std.mem.writeInt(u64, buf[0..8], snap_index, .little);
        std.mem.writeInt(u64, buf[8..16], snap_term, .little);
        @memcpy(buf[16..48], &data_digest);
        std.mem.writeInt(u32, buf[48..52], config_len, .little);
        if (config_data.len > 0) {
            @memcpy(buf[52..][0..config_data.len], config_data);
        }
        const payload = buf[0 .. total_size - 4];
        // fips-exempt: non-security — CRC32 record-integrity check for snapshot meta.
        const crc = std.hash.Crc32.hash(payload);
        std.mem.writeInt(u32, buf[total_size - 4 ..][0..4], crc, .little);

        return buf;
    }

    fn readHardStateFile(self: *FileStorage) !state.HardState {
        const file = try self.dir.openFile(std.Io.Threaded.global_single_threaded.io(), "hard_state", .{});
        defer file.close(std.Io.Threaded.global_single_threaded.io());
        const size = try file.length(std.Io.Threaded.global_single_threaded.io());

        if (size == 16) {
            // Legacy format without CRC.
            var buf: [16]u8 = undefined;
            const n = try file.readPositionalAll(std.Io.Threaded.global_single_threaded.io(), &buf, 0);
            if (n != 16) return error.InvalidHardState;
            const term = std.mem.readInt(u64, buf[0..8], .little);
            const vf_raw = std.mem.readInt(u64, buf[8..16], .little);
            return .{
                .current_term = term,
                .voted_for = if (vf_raw == VOTED_FOR_NONE) null else vf_raw,
            };
        } else if (size == 20) {
            var buf: [20]u8 = undefined;
            const n = try file.readPositionalAll(std.Io.Threaded.global_single_threaded.io(), &buf, 0);
            if (n != 20) return error.InvalidHardState;
            const stored_crc = std.mem.readInt(u32, buf[16..20], .little);
            // fips-exempt: non-security — CRC32 corruption check on read.
            const computed_crc = std.hash.Crc32.hash(buf[0..16]);
            if (stored_crc != computed_crc) return error.InvalidHardState;
            const term = std.mem.readInt(u64, buf[0..8], .little);
            const vf_raw = std.mem.readInt(u64, buf[8..16], .little);
            return .{
                .current_term = term,
                .voted_for = if (vf_raw == VOTED_FOR_NONE) null else vf_raw,
            };
        } else {
            return error.InvalidHardState;
        }
    }

    const SnapshotMeta = struct {
        index: u64,
        term: u64,
        data_digest: ?[32]u8,
        config: ?rpc.ClusterConfig,
        /// Owned slices from deserializeConfig — caller must free.
        config_voters_buf: ?[]u64 = null,
        config_voters_outgoing_buf: ?[]u64 = null,

        fn freeConfig(self: *SnapshotMeta, allocator: std.mem.Allocator) void {
            if (self.config_voters_buf) |v| allocator.free(v);
            if (self.config_voters_outgoing_buf) |v| allocator.free(v);
            self.config = null;
        }
    };

    fn readSnapshotMeta(self: *FileStorage) !SnapshotMeta {
        const file = try self.dir.openFile(std.Io.Threaded.global_single_threaded.io(), "snapshot.meta", .{});
        defer file.close(std.Io.Threaded.global_single_threaded.io());
        const size = try file.length(std.Io.Threaded.global_single_threaded.io());

        if (size == 16) {
            var buf: [16]u8 = undefined;
            const n = try file.readPositionalAll(std.Io.Threaded.global_single_threaded.io(), &buf, 0);
            if (n != 16) return error.InvalidSnapshotMeta;
            return .{
                .index = std.mem.readInt(u64, buf[0..8], .little),
                .term = std.mem.readInt(u64, buf[8..16], .little),
                .data_digest = null,
                .config = null,
            };
        } else if (size == 52) {
            var buf: [52]u8 = undefined;
            const n = try file.readPositionalAll(std.Io.Threaded.global_single_threaded.io(), &buf, 0);
            if (n != 52) return error.InvalidSnapshotMeta;
            const stored_crc = std.mem.readInt(u32, buf[48..52], .little);
            // fips-exempt: non-security — CRC32 corruption check on read.
            const computed_crc = std.hash.Crc32.hash(buf[0..48]);
            if (stored_crc != computed_crc) return error.InvalidSnapshotMeta;
            var digest: [32]u8 = undefined;
            @memcpy(&digest, buf[16..48]);
            return .{
                .index = std.mem.readInt(u64, buf[0..8], .little),
                .term = std.mem.readInt(u64, buf[8..16], .little),
                .data_digest = digest,
                .config = null,
            };
        } else if (size > 52) {
            // V3 format with config.
            if (size > 1024 * 1024) return error.InvalidSnapshotMeta;
            const buf = try self.allocator.alloc(u8, size);
            defer self.allocator.free(buf);
            const n = try file.readPositionalAll(std.Io.Threaded.global_single_threaded.io(), buf, 0);
            if (n != size) return error.InvalidSnapshotMeta;

            const stored_crc = std.mem.readInt(u32, buf[size - 4 ..][0..4], .little);
            // fips-exempt: non-security — CRC32 corruption check on read.
            const computed_crc = std.hash.Crc32.hash(buf[0 .. size - 4]);
            if (stored_crc != computed_crc) return error.InvalidSnapshotMeta;

            var digest: [32]u8 = undefined;
            @memcpy(&digest, buf[16..48]);
            const config_len = std.mem.readInt(u32, buf[48..52], .little);
            if (52 + config_len + 4 != size) return error.InvalidSnapshotMeta;

            const config_data = buf[52..][0..config_len];
            const cfg = try mem.MembershipState.deserializeConfig(self.allocator, config_data);

            return .{
                .index = std.mem.readInt(u64, buf[0..8], .little),
                .term = std.mem.readInt(u64, buf[8..16], .little),
                .data_digest = digest,
                .config = cfg.config,
                .config_voters_buf = cfg.voters_buf,
                .config_voters_outgoing_buf = cfg.voters_outgoing_buf,
            };
        } else {
            return error.InvalidSnapshotMeta;
        }
    }

    /// Open WAL and rebuild in-memory index by iterating all records.
    fn recoverWal(self: *FileStorage) !void {
        // Check if WAL file exists.
        self.dir.access(std.Io.Threaded.global_single_threaded.io(), "entries.wal", .{}) catch {
            return; // No WAL yet.
        };

        self.wal_log = try self.openWal();

        // Rebuild in-memory index by iterating all records.
        var iter = try self.wal_log.?.iterator(0);
        defer iter.deinit();

        while (true) {
            var entry_opt = iter.next() catch |err| return mapWalError(err);
            if (entry_opt == null) break;
            var wal_entry = &entry_opt.?;
            defer wal_entry.deinit();

            if (wal_entry.payload.len < PAYLOAD_HEADER_SIZE) continue;
            // Covered by the snapshot: left behind by a refused compaction.
            if (wal_entry.lsn <= self.snapshot_index) continue;

            const term = std.mem.readInt(u64, wal_entry.payload[0..8], .little);
            const data_len: u32 = @intCast(wal_entry.payload.len - PAYLOAD_HEADER_SIZE);

            try self.index.append(self.allocator, .{
                .raft_index = wal_entry.lsn,
                .term = term,
                .data_len = data_len,
            });
        }
    }

    /// Open entries.wal (plaintext). The WAL fails closed on damage it cannot
    /// safely truncate, leaving the file untouched; that surfaces as a
    /// StorageError (SONNY-6622). Only a torn tail is truncated.
    fn openWal(self: *FileStorage) !wal_mod.WriteAheadLog {
        return wal_mod.WriteAheadLog.init(self.allocator, self.dir, "entries.wal", .{}) catch |err| {
            if (isWalRefusal(err)) {
                std.log.warn("raft: FileStorage: refused to open entries.wal: {s}; the file was not modified", .{@errorName(err)});
            }
            return mapWalError(err);
        };
    }

    fn findIndexPos(self: *FileStorage, raft_index: u64) ?usize {
        const items = self.index.items;
        if (items.len == 0) return null;

        var lo: usize = 0;
        var hi: usize = items.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (items[mid].raft_index < raft_index) {
                lo = mid + 1;
            } else if (items[mid].raft_index > raft_index) {
                hi = mid;
            } else {
                return mid;
            }
        }
        return null;
    }

    fn truncateFrom(self: *FileStorage, from_index: u64) !void {
        const pos = self.findIndexPos(from_index) orelse return;

        // Truncate WAL: remove all records with LSN >= from_index.
        if (self.wal_log) |*w| {
            const after_lsn = if (pos == 0) 0 else self.index.items[pos - 1].raft_index;
            // wal's truncateAfter finds its cut with the recovery scan, which
            // stops at the first damaged record: a damaged record below the
            // cut would make it cut there instead, silently dropping the
            // valid records after it. Refuse unless every record it keeps is
            // readable (SONNY-6611).
            if (after_lsn > 0) try verifyReadableThrough(w, after_lsn);
            w.truncateAfter(after_lsn) catch |err| return mapWalError(err);
        }

        // Truncate the in-memory index only once the WAL matches it.
        self.index.shrinkRetainingCapacity(pos);
    }

    /// LogCorrupted unless the WAL's records can be read in order up to and
    /// including `lsn` (the iterator stops at the first damaged record).
    fn verifyReadableThrough(w: *wal_mod.WriteAheadLog, lsn: u64) !void {
        var iter = w.iterator(0) catch |err| return mapWalError(err);
        defer iter.deinit();
        while (true) {
            var entry_opt = iter.next() catch |err| return mapWalError(err);
            if (entry_opt == null) break;
            var wal_entry = &entry_opt.?;
            defer wal_entry.deinit();
            if (wal_entry.lsn == lsn) return;
            if (wal_entry.lsn > lsn) break;
        }
        std.log.warn("raft: FileStorage: entries.wal is damaged below index {d}; refusing to truncate it", .{lsn});
        return stg.StorageError.LogCorrupted;
    }

    // ========================================================================
    // Type-erased vtable wrappers
    // ========================================================================

    pub fn loadHardStateImpl(self: *FileStorage) anyerror!state.HardState {
        return self.hard_state;
    }

    fn typeErasedSaveHardState(ptr: *anyopaque, hs: state.HardState) anyerror!void {
        const self: *FileStorage = @ptrCast(@alignCast(ptr));
        return self.saveHardStateImpl(hs);
    }

    fn typeErasedLoadHardState(ptr: *anyopaque) anyerror!state.HardState {
        const self: *FileStorage = @ptrCast(@alignCast(ptr));
        return self.loadHardStateImpl();
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

/// True for the errors wal returns when it refuses to recover or compact a
/// log it cannot read, leaving the file untouched.
fn isWalRefusal(err: anyerror) bool {
    return err == error.WalCorrupted or err == error.WalAuthFailed or err == error.WalEncryptionMismatch;
}

/// Map wal's recovery errors to StorageError; other errors pass through.
fn mapWalError(err: anyerror) anyerror {
    return switch (err) {
        error.WalCorrupted => stg.StorageError.LogCorrupted,
        error.WalAuthFailed => stg.StorageError.LogAuthFailed,
        error.WalEncryptionMismatch => stg.StorageError.LogEncryptionMismatch,
        else => err,
    };
}

// ============================================================================
// Unit Tests
// ============================================================================

fn testDir() std.testing.TmpDir {
    return std.testing.tmpDir(.{});
}

/// Construct a path for FileStorage.init from a TmpDir, relative to cwd.
/// The returned string is allocator-owned and must be freed by the caller.
fn testPath(allocator: std.mem.Allocator, tmp: *std.testing.TmpDir, suffix: []const u8) []const u8 {
    return std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/{s}", .{ tmp.sub_path, suffix }) catch unreachable;
}

test "FileStorage: init creates directory and lock file" {
    var tmp = testDir();
    defer tmp.cleanup();

    const full_path = testPath(std.testing.allocator, &tmp, "fs-test-init");
    defer std.testing.allocator.free(full_path);

    var fs = try FileStorage.init(std.heap.page_allocator, full_path);
    defer fs.deinit();

    try std.testing.expectEqual(state.HardState.initial, fs.hard_state);
    try std.testing.expectEqual(@as(u64, 0), try fs.lastIndexImpl());
}

test "FileStorage: append and read entries round-trip" {
    var tmp = testDir();
    defer tmp.cleanup();

    const full_path = testPath(std.testing.allocator, &tmp, "fs-test-rtrip");
    defer std.testing.allocator.free(full_path);

    var fs = try FileStorage.init(std.heap.page_allocator, full_path);
    defer fs.deinit();

    const entries = [_]rpc.Entry{
        .{ .index = 1, .term = 1, .kind = .normal, .data = "hello" },
        .{ .index = 2, .term = 1, .kind = .normal, .data = "world" },
        .{ .index = 3, .term = 2, .kind = .config, .data = "cfg" },
    };
    try fs.appendEntriesImpl(&entries);

    try std.testing.expectEqual(@as(u64, 3), try fs.lastIndexImpl());
    try std.testing.expectEqual(@as(u64, 1), try fs.termImpl(1));
    try std.testing.expectEqual(@as(u64, 2), try fs.termImpl(3));

    const read = try fs.entriesImpl(1, 4);
    defer {
        for (read) |e| {
            if (e.data.len > 0) std.heap.page_allocator.free(@constCast(e.data));
        }
        std.heap.page_allocator.free(read);
    }
    try std.testing.expectEqual(@as(usize, 3), read.len);
    try std.testing.expectEqualStrings("hello", read[0].data);
    try std.testing.expectEqualStrings("world", read[1].data);
    try std.testing.expectEqualStrings("cfg", read[2].data);
    try std.testing.expectEqual(rpc.EntryKind.config, read[2].kind);
}

test "FileStorage: hard state persistence" {
    var tmp = testDir();
    defer tmp.cleanup();

    const full_path = testPath(std.testing.allocator, &tmp, "fs-test-hs");
    defer std.testing.allocator.free(full_path);

    {
        var fs = try FileStorage.init(std.heap.page_allocator, full_path);
        defer fs.deinit();
        try fs.saveHardStateImpl(.{ .current_term = 5, .voted_for = 3 });
    }

    // Reopen and verify.
    {
        var fs = try FileStorage.init(std.heap.page_allocator, full_path);
        defer fs.deinit();
        try std.testing.expectEqual(@as(u64, 5), fs.hard_state.current_term);
        try std.testing.expectEqual(@as(?u64, 3), fs.hard_state.voted_for);
    }
}

test "FileStorage: WAL recovery after reopen" {
    var tmp = testDir();
    defer tmp.cleanup();

    const full_path = testPath(std.testing.allocator, &tmp, "fs-test-wal-recovery");
    defer std.testing.allocator.free(full_path);

    {
        var fs = try FileStorage.init(std.heap.page_allocator, full_path);
        defer fs.deinit();
        const entries = [_]rpc.Entry{
            .{ .index = 1, .term = 1, .kind = .normal, .data = "a" },
            .{ .index = 2, .term = 1, .kind = .normal, .data = "b" },
        };
        try fs.appendEntriesImpl(&entries);
    }

    // Reopen and verify entries survived.
    {
        var fs = try FileStorage.init(std.heap.page_allocator, full_path);
        defer fs.deinit();
        try std.testing.expectEqual(@as(u64, 2), try fs.lastIndexImpl());
        try std.testing.expectEqual(@as(u64, 1), try fs.termImpl(1));
        try std.testing.expectEqual(@as(u64, 1), try fs.termImpl(2));
    }
}

test "FileStorage: compaction removes entries and rewrites WAL" {
    var tmp = testDir();
    defer tmp.cleanup();

    const full_path = testPath(std.testing.allocator, &tmp, "fs-test-compact");
    defer std.testing.allocator.free(full_path);

    var fs = try FileStorage.init(std.heap.page_allocator, full_path);
    defer fs.deinit();

    const entries = [_]rpc.Entry{
        .{ .index = 1, .term = 1, .kind = .normal, .data = "a" },
        .{ .index = 2, .term = 1, .kind = .normal, .data = "b" },
        .{ .index = 3, .term = 2, .kind = .normal, .data = "c" },
    };
    try fs.appendEntriesImpl(&entries);

    // Compact up to index 2.
    try fs.compactImpl(2);

    try std.testing.expectEqual(@as(u64, 3), try fs.lastIndexImpl());
    try std.testing.expectEqual(@as(u64, 2), try fs.termImpl(3));

    // Index 1 and 2 should be compacted.
    const result = fs.termImpl(1);
    try std.testing.expectError(stg.StorageError.Compacted, result);
}

// ----------------------------------------------------------------------------
// SONNY-6622: wal v0.1.24 fails closed on damage it cannot safely truncate.
// ----------------------------------------------------------------------------

fn walFile(path: []const u8) !std.Io.File {
    const io = std.Io.Threaded.global_single_threaded.io();
    const wal_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/entries.wal", .{path});
    defer std.testing.allocator.free(wal_path);
    return std.Io.Dir.cwd().openFile(io, wal_path, .{ .mode = .read_write });
}

/// Flip the first byte of `needle` (a record payload) in the WAL on disk.
fn corruptWalPayload(path: []const u8, needle: []const u8) !void {
    const io = std.Io.Threaded.global_single_threaded.io();
    var file = try walFile(path);
    defer file.close(io);
    const buf = try std.testing.allocator.alloc(u8, @intCast(try file.length(io)));
    defer std.testing.allocator.free(buf);
    _ = try file.readPositionalAll(io, buf, 0);
    const at = std.mem.indexOf(u8, buf, needle) orelse return error.TestNeedleNotFound;
    try file.writePositionalAll(io, &.{buf[at] ^ 0xff}, at);
    try file.sync(io);
}

/// Append bytes that are not a valid record to the WAL on disk (a torn tail).
fn appendWalGarbage(path: []const u8) !void {
    const io = std.Io.Threaded.global_single_threaded.io();
    var file = try walFile(path);
    defer file.close(io);
    try file.writePositionalAll(io, "\x07\x00torn-tail", try file.length(io));
    try file.sync(io);
}

fn payloadEntries() [5]rpc.Entry {
    return .{
        .{ .index = 1, .term = 1, .kind = .normal, .data = "entry-1-payload" },
        .{ .index = 2, .term = 1, .kind = .normal, .data = "entry-2-payload" },
        .{ .index = 3, .term = 1, .kind = .normal, .data = "entry-3-payload" },
        .{ .index = 4, .term = 2, .kind = .normal, .data = "entry-4-payload" },
        .{ .index = 5, .term = 2, .kind = .normal, .data = "entry-5-payload" },
    };
}

fn expectOpenRefused(path: []const u8, expected: []const u8) !void {
    if (FileStorage.init(std.testing.allocator, path)) |opened| {
        var fs = opened;
        defer fs.deinit();
        std.debug.print("damaged log opened with lastIndex {d}\n", .{try fs.lastIndexImpl()});
        return error.TestUnexpectedResult;
    } else |err| {
        try std.testing.expectEqualStrings(expected, @errorName(err));
    }
}

test "FileStorage: mid-log corruption refuses to open instead of dropping entries (SONNY-6622)" {
    var tmp = testDir();
    defer tmp.cleanup();
    const full_path = testPath(std.testing.allocator, &tmp, "fs-test-midlog");
    defer std.testing.allocator.free(full_path);

    {
        var fs = try FileStorage.init(std.testing.allocator, full_path);
        defer fs.deinit();
        const ents = payloadEntries();
        try fs.appendEntriesImpl(&ents);
    }

    // Record 3 of 5 is damaged; 4 and 5 are intact. Opening must not
    // silently truncate the log back to index 2.
    try corruptWalPayload(full_path, "entry-3-payload");
    try expectOpenRefused(full_path, "LogCorrupted");
    // The refusal left the file as it was: it still refuses.
    try expectOpenRefused(full_path, "LogCorrupted");
}

test "FileStorage: an encrypted WAL is refused, not read as plaintext (SONNY-6622)" {
    var tmp = testDir();
    defer tmp.cleanup();
    const full_path = testPath(std.testing.allocator, &tmp, "fs-test-encrypted");
    defer std.testing.allocator.free(full_path);

    const io = std.Io.Threaded.global_single_threaded.io();
    {
        var dir = try std.Io.Dir.cwd().createDirPathOpen(io, full_path, .{});
        defer dir.close(io);
        var w = try wal_mod.WriteAheadLog.init(std.testing.allocator, dir, "entries.wal", .{
            .encryption_key = [_]u8{0x42} ** 32,
        });
        defer w.deinit();
        _ = try w.append("sealed-record");
    }

    try expectOpenRefused(full_path, "LogEncryptionMismatch");
}

test "FileStorage: WAL recovery errors map to storage errors (SONNY-6622)" {
    try std.testing.expectEqual(stg.StorageError.LogCorrupted, mapWalError(error.WalCorrupted));
    try std.testing.expectEqual(stg.StorageError.LogAuthFailed, mapWalError(error.WalAuthFailed));
    try std.testing.expectEqual(stg.StorageError.LogEncryptionMismatch, mapWalError(error.WalEncryptionMismatch));
    try std.testing.expectEqual(@as(anyerror, error.AccessDenied), mapWalError(error.AccessDenied));
}

test "FileStorage: a refused WAL compaction is logged and storage keeps working (SONNY-6622)" {
    var tmp = testDir();
    defer tmp.cleanup();
    const full_path = testPath(std.testing.allocator, &tmp, "fs-test-refused-compact");
    defer std.testing.allocator.free(full_path);

    {
        var fs = try FileStorage.init(std.testing.allocator, full_path);
        defer fs.deinit();
        const ents = payloadEntries();
        try fs.appendEntriesImpl(&ents);

        // The log is damaged under the open WAL; compaction now validates
        // every record and refuses to rewrite it.
        try appendWalGarbage(full_path);
        try fs.compactImpl(3);

        try std.testing.expectEqual(@as(u64, 3), fs.snapshot_index);
        try std.testing.expectError(stg.StorageError.Compacted, fs.termImpl(2));
        try std.testing.expectEqual(@as(u64, 5), try fs.lastIndexImpl());
        const rest = try fs.entriesImpl(4, 6);
        defer rpc.freeEntries(std.testing.allocator, rest);
        try std.testing.expectEqualStrings("entry-4-payload", rest[0].data);
        try std.testing.expectEqualStrings("entry-5-payload", rest[1].data);
    }

    // Reopen: the torn tail is truncated, and the records the refused
    // compaction left behind (<= 3) do not come back into the index.
    var fs = try FileStorage.init(std.testing.allocator, full_path);
    defer fs.deinit();
    try std.testing.expectEqual(@as(u64, 3), fs.snapshot_index);
    try std.testing.expectEqual(@as(u64, 4), fs.index.items[0].raft_index);
    try std.testing.expectEqual(@as(u64, 5), try fs.lastIndexImpl());
    try std.testing.expectError(stg.StorageError.Compacted, fs.termImpl(2));
    try std.testing.expectEqual(@as(u64, 1), try fs.termImpl(3));
    const rest = try fs.entriesImpl(4, 6);
    defer rpc.freeEntries(std.testing.allocator, rest);
    try std.testing.expectEqualStrings("entry-5-payload", rest[1].data);
}

// ----------------------------------------------------------------------------
// SONNY-6611: snapshot trimming, compaction rollback, truncation over damage.
// ----------------------------------------------------------------------------

fn tenEntries(buf: *[10]rpc.Entry, names: *[10][16]u8) []const rpc.Entry {
    for (buf, 0..) |*e, i| {
        const name = std.fmt.bufPrint(&names[i], "entry-{d:0>2}-data", .{i + 1}) catch unreachable;
        e.* = .{ .index = i + 1, .term = 1, .kind = .normal, .data = name };
    }
    return buf;
}

fn testSnapshotAt(index: u64, term: u64) rpc.Snapshot {
    return .{
        .metadata = .{ .index = index, .term = term, .config = .{ .voters = &.{ 1, 2, 3 }, .voters_outgoing = &.{}, .is_joint = false } },
        .data = "snapshot-bytes",
    };
}

fn walContains(path: []const u8, needle: []const u8) !bool {
    const io = std.Io.Threaded.global_single_threaded.io();
    var file = walFile(path) catch |err| switch (err) {
        error.FileNotFound => return false,
        else => return err,
    };
    defer file.close(io);
    const buf = try std.testing.allocator.alloc(u8, @intCast(try file.length(io)));
    defer std.testing.allocator.free(buf);
    _ = try file.readPositionalAll(io, buf, 0);
    return std.mem.indexOf(u8, buf, needle) != null;
}

test "FileStorage: a snapshot matching the log compacts the WAL and trims the index (SONNY-6611)" {
    var tmp = testDir();
    defer tmp.cleanup();
    const full_path = testPath(std.testing.allocator, &tmp, "fs-test-snap-trim");
    defer std.testing.allocator.free(full_path);

    {
        var fs = try FileStorage.init(std.testing.allocator, full_path);
        defer fs.deinit();
        var buf: [10]rpc.Entry = undefined;
        var names: [10][16]u8 = undefined;
        try fs.appendEntriesImpl(tenEntries(&buf, &names));

        try fs.saveSnapshotImpl(testSnapshotAt(6, 1));
        try std.testing.expectEqual(@as(u64, 7), fs.index.items[0].raft_index);
        try std.testing.expectEqual(@as(u64, 10), try fs.lastIndexImpl());
        try std.testing.expect(!try walContains(full_path, "entry-03-data"));
        const rest = try fs.entriesImpl(7, 11);
        defer rpc.freeEntries(std.testing.allocator, rest);
        try std.testing.expectEqualStrings("entry-07-data", rest[0].data);
    }

    var fs = try FileStorage.init(std.testing.allocator, full_path);
    defer fs.deinit();
    try std.testing.expectEqual(@as(u64, 6), fs.snapshot_index);
    try std.testing.expectEqual(@as(u64, 7), fs.index.items[0].raft_index);
    try std.testing.expectEqual(@as(u64, 10), try fs.lastIndexImpl());
}

test "FileStorage: a snapshot that conflicts with the log drops the log (SONNY-6611)" {
    var tmp = testDir();
    defer tmp.cleanup();
    const full_path = testPath(std.testing.allocator, &tmp, "fs-test-snap-drop");
    defer std.testing.allocator.free(full_path);

    {
        var fs = try FileStorage.init(std.testing.allocator, full_path);
        defer fs.deinit();
        var buf: [10]rpc.Entry = undefined;
        var names: [10][16]u8 = undefined;
        try fs.appendEntriesImpl(tenEntries(&buf, &names));

        // The leader's snapshot has term 2 at index 6: this log diverged.
        try fs.saveSnapshotImpl(testSnapshotAt(6, 2));
        try std.testing.expectEqual(@as(usize, 0), fs.index.items.len);
        try std.testing.expectEqual(@as(u64, 6), try fs.lastIndexImpl());
        try std.testing.expectEqual(@as(u64, 2), try fs.termImpl(6));
    }

    var fs = try FileStorage.init(std.testing.allocator, full_path);
    defer fs.deinit();
    try std.testing.expectEqual(@as(u64, 6), try fs.lastIndexImpl());
    try std.testing.expectError(stg.StorageError.Unavailable, fs.termImpl(7));
}

test "FileStorage: a failed WAL compaction rolls the snapshot back (SONNY-6611)" {
    var tmp = testDir();
    defer tmp.cleanup();
    const full_path = testPath(std.testing.allocator, &tmp, "fs-test-compact-rollback");
    defer std.testing.allocator.free(full_path);
    const io = std.Io.Threaded.global_single_threaded.io();

    {
        var fs = try FileStorage.init(std.testing.allocator, full_path);
        defer fs.deinit();
        const ents = payloadEntries();
        try fs.appendEntriesImpl(&ents);

        // wal writes its rewrite to entries.wal.tmp; a directory there makes
        // the compaction fail without being a refusal.
        const blocker = try std.fmt.allocPrint(std.testing.allocator, "{s}/entries.wal.tmp", .{full_path});
        defer std.testing.allocator.free(blocker);
        var d = try std.Io.Dir.cwd().createDirPathOpen(io, blocker, .{});
        d.close(io);

        if (fs.compactImpl(3)) |_| return error.TestExpectedCompactionFailure else |_| {}
        try std.testing.expectEqual(@as(u64, 0), fs.snapshot_index);
        try std.testing.expectEqual(@as(u64, 1), try fs.termImpl(2));
        try std.testing.expectEqual(@as(u64, 5), try fs.lastIndexImpl());
    }

    var fs = try FileStorage.init(std.testing.allocator, full_path);
    defer fs.deinit();
    try std.testing.expectEqual(@as(u64, 0), fs.snapshot_index);
    const all = try fs.entriesImpl(1, 6);
    defer rpc.freeEntries(std.testing.allocator, all);
    try std.testing.expectEqualStrings("entry-2-payload", all[1].data);
}

test "FileStorage: truncating over a damaged record refuses instead of dropping entries (SONNY-6611)" {
    var tmp = testDir();
    defer tmp.cleanup();
    const full_path = testPath(std.testing.allocator, &tmp, "fs-test-truncate-damaged");
    defer std.testing.allocator.free(full_path);

    {
        var fs = try FileStorage.init(std.testing.allocator, full_path);
        defer fs.deinit();
        const ents = payloadEntries();
        try fs.appendEntriesImpl(&ents);
        try corruptWalPayload(full_path, "entry-2-payload");

        // A conflicting entry at 4 truncates after 3: records 1..3 must be
        // kept, but record 2 cannot be read.
        const replacement = [_]rpc.Entry{.{ .index = 4, .term = 2, .kind = .normal, .data = "replacement" }};
        if (fs.appendEntriesImpl(&replacement)) |_| {
            return error.TestExpectedRefusal;
        } else |err| {
            try std.testing.expectEqualStrings("LogCorrupted", @errorName(err));
        }
    }
    // The file was not cut back to before the damaged record.
    try expectOpenRefused(full_path, "LogCorrupted");
}
