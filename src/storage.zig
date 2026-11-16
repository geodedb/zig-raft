// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-2025 CodePros

const std = @import("std");
const state = @import("state.zig");
const rpc = @import("rpc.zig");

pub const StorageError = error{
    Compacted,
    Unavailable,
    /// The entry log holds a damaged record with valid records after it;
    /// it was not opened, and the file was left untouched (SONNY-6622).
    LogCorrupted,
    /// No record of the encrypted entry log authenticates under the key.
    LogAuthFailed,
    /// The entry log was written in the other key mode (encrypted vs
    /// plaintext).
    LogEncryptionMismatch,
};

pub const Storage = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        saveHardState: *const fn (*anyopaque, state.HardState) anyerror!void,
        loadHardState: *const fn (*anyopaque) anyerror!state.HardState,
        appendEntries: *const fn (*anyopaque, []const rpc.Entry) anyerror!void,
        entries: *const fn (*anyopaque, lo: u64, hi: u64) anyerror![]rpc.Entry,
        term: *const fn (*anyopaque, index: u64) anyerror!u64,
        lastIndex: *const fn (*anyopaque) anyerror!u64,
        saveSnapshot: *const fn (*anyopaque, rpc.Snapshot) anyerror!void,
        loadSnapshot: *const fn (*anyopaque) anyerror!?rpc.Snapshot,
        compact: *const fn (*anyopaque, index: u64) anyerror!void,
        /// Optional staged snapshot save (SONNY-6653); see `stageSnapshot`.
        /// A storage that leaves these null gets the generic fallback: the
        /// data is copied when staged and saved with `saveSnapshot` when
        /// committed.
        stageSnapshot: ?*const fn (*anyopaque, rpc.Snapshot) anyerror!StagedSnapshot = null,
        commitStagedSnapshot: ?*const fn (*anyopaque, *StagedSnapshot) anyerror!void = null,
        /// Remove what an uncommitted staged snapshot left behind (not its
        /// memory: `releaseStagedSnapshot` frees that).
        discardStagedSnapshot: ?*const fn (*anyopaque, *StagedSnapshot) void = null,
    };

    pub fn saveHardState(self: Storage, hs: state.HardState) anyerror!void {
        return self.vtable.saveHardState(self.ptr, hs);
    }

    pub fn loadHardState(self: Storage) anyerror!state.HardState {
        return self.vtable.loadHardState(self.ptr);
    }

    pub fn appendEntries(self: Storage, ents: []const rpc.Entry) anyerror!void {
        return self.vtable.appendEntries(self.ptr, ents);
    }

    /// Entries [lo, hi). The caller owns the slice AND each entry's `data`;
    /// free with `rpc.freeEntries`. Implementations must return copies, never
    /// views into their own buffers.
    pub fn entries(self: Storage, lo: u64, hi: u64) anyerror![]rpc.Entry {
        return self.vtable.entries(self.ptr, lo, hi);
    }

    pub fn term(self: Storage, index: u64) anyerror!u64 {
        return self.vtable.term(self.ptr, index);
    }

    pub fn lastIndex(self: Storage) anyerror!u64 {
        return self.vtable.lastIndex(self.ptr);
    }

    pub fn saveSnapshot(self: Storage, snap: rpc.Snapshot) anyerror!void {
        return self.vtable.saveSnapshot(self.ptr, snap);
    }

    pub fn loadSnapshot(self: Storage) anyerror!?rpc.Snapshot {
        return self.vtable.loadSnapshot(self.ptr);
    }

    pub fn compact(self: Storage, index: u64) anyerror!void {
        return self.vtable.compact(self.ptr, index);
    }

    /// Stage a snapshot for a later `commitStagedSnapshot` (SONNY-6653):
    /// the expensive part of saving it (writing and syncing the data).
    /// Storages that implement it (`FileStorage`) allow this call WITHOUT
    /// the lock the consumer holds around the RaftNode, concurrently with
    /// every other call; the generic fallback (copy the data) does too. The
    /// staged snapshot owns copies of `snap`'s config and is released with
    /// `releaseStagedSnapshot` whether or not it was committed.
    pub fn stageSnapshot(self: Storage, allocator: std.mem.Allocator, snap: rpc.Snapshot) anyerror!StagedSnapshot {
        if (self.vtable.stageSnapshot) |f| return f(self.ptr, snap);
        var staged = try StagedSnapshot.initMetadata(allocator, null, snap.metadata);
        errdefer staged.deinit();
        staged.payload = try allocator.dupe(u8, snap.data);
        staged.size = snap.data.len;
        staged.digest = blake3(snap.data);
        return staged;
    }

    /// Make a staged snapshot the storage's snapshot, with the same effect
    /// on the log as `saveSnapshot` (SONNY-6653). Call it under the node
    /// lock, as `saveSnapshot`. `FileStorage` does only small writes here
    /// (the snapshot metadata, two renames, directory syncs) plus the log
    /// compaction. `RaftNode.commitSnapshot` calls this and compacts the
    /// in-memory log; call it directly only for a storage outside a node.
    pub fn commitStagedSnapshot(self: Storage, staged: *StagedSnapshot) anyerror!void {
        if (staged.committed) return error.SnapshotAlreadyCommitted;
        if (staged.owner != (if (self.vtable.stageSnapshot != null) self.ptr else null))
            return error.StagedSnapshotForeign;
        if (self.vtable.commitStagedSnapshot) |f| {
            try f(self.ptr, staged);
        } else {
            try self.saveSnapshot(.{ .metadata = staged.metadata, .data = staged.payload });
            staged.committed = true;
        }
    }

    /// Release a staged snapshot: remove what it left on the storage if it
    /// was not committed, and free it. Safe without the node lock.
    pub fn releaseStagedSnapshot(self: Storage, staged: *StagedSnapshot) void {
        if (!staged.committed and staged.owner != null) {
            if (self.vtable.discardStagedSnapshot) |f| f(self.ptr, staged);
        }
        staged.deinit();
    }
};

/// A snapshot written by `Storage.stageSnapshot` and not yet (or already)
/// committed (SONNY-6653).
pub const StagedSnapshot = struct {
    /// Index, term and config of the snapshot; the config slices are owned.
    metadata: rpc.SnapshotMetadata,
    /// BLAKE3 digest and length of the snapshot data.
    digest: [32]u8 = .{0} ** 32,
    size: u64 = 0,
    /// Owned. What it holds is the storage's: `FileStorage` keeps the staged
    /// file's name; the generic fallback keeps the data.
    payload: []u8 = &.{},
    /// The storage that staged it (null: the generic fallback). A staged
    /// snapshot commits only on the storage that staged it.
    owner: ?*anyopaque,
    /// Set once the commit passed its commit point.
    committed: bool = false,
    allocator: std.mem.Allocator,

    /// A staged snapshot with an owned copy of `meta`'s config and nothing
    /// staged yet (for storage implementations).
    pub fn initMetadata(allocator: std.mem.Allocator, owner: ?*anyopaque, meta: rpc.SnapshotMetadata) !StagedSnapshot {
        const voters = try allocator.dupe(u64, meta.config.voters);
        errdefer allocator.free(voters);
        const outgoing = try allocator.dupe(u64, meta.config.voters_outgoing);
        return .{
            .metadata = .{ .index = meta.index, .term = meta.term, .config = .{
                .voters = voters,
                .voters_outgoing = outgoing,
                .is_joint = meta.config.is_joint,
            } },
            .owner = owner,
            .allocator = allocator,
        };
    }

    /// Free the memory only; anything staged on the storage stays (as after
    /// a crash). Use `Storage.releaseStagedSnapshot` normally.
    pub fn deinit(self: *StagedSnapshot) void {
        self.allocator.free(self.metadata.config.voters);
        self.allocator.free(self.metadata.config.voters_outgoing);
        self.allocator.free(self.payload);
        self.* = undefined;
    }
};

fn blake3(data: []const u8) [32]u8 {
    // fips-exempt: non-security — snapshot-data integrity digest.
    var d: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(data, &d, .{});
    return d;
}

/// Free a Snapshot returned by Storage.loadSnapshot().
/// Both InMemoryStorage and FileStorage return heap-allocated copies
/// from loadSnapshot(); callers must free with this helper (SEC-0072).
pub fn freeSnapshot(allocator: std.mem.Allocator, snap: rpc.Snapshot) void {
    if (snap.data.len > 0) allocator.free(snap.data);
    if (snap.metadata.config.voters.len > 0) allocator.free(snap.metadata.config.voters);
    if (snap.metadata.config.voters_outgoing.len > 0) allocator.free(snap.metadata.config.voters_outgoing);
}

pub const InMemoryStorage = struct {
    log: std.ArrayList(rpc.Entry) = .empty,
    hard_state: state.HardState = state.HardState.initial,
    snapshot_data: ?rpc.Snapshot = null,
    snapshot_index: u64 = 0,
    snapshot_term: u64 = 0,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) InMemoryStorage {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *InMemoryStorage) void {
        // Free snapshot data if present (SEC-0095).
        if (self.snapshot_data) |sd| {
            if (sd.data.len > 0) self.allocator.free(sd.data);
            if (sd.metadata.config.voters.len > 0) self.allocator.free(sd.metadata.config.voters);
            if (sd.metadata.config.voters_outgoing.len > 0) self.allocator.free(sd.metadata.config.voters_outgoing);
            self.snapshot_data = null;
        }
        for (self.log.items) |entry| {
            if (entry.data.len > 0) {
                self.allocator.free(entry.data);
            }
        }
        self.log.deinit(self.allocator);
    }

    pub fn storage(self: *InMemoryStorage) Storage {
        return .{
            .ptr = @ptrCast(self),
            .vtable = &.{
                .saveHardState = typeErasedSaveHardState,
                .loadHardState = typeErasedLoadHardState,
                .appendEntries = typeErasedAppendEntries,
                .entries = typeErasedEntries,
                .term = typeErasedTerm,
                .lastIndex = typeErasedLastIndex,
                .saveSnapshot = typeErasedSaveSnapshot,
                .loadSnapshot = typeErasedLoadSnapshot,
                .compact = typeErasedCompact,
            },
        };
    }

    pub fn appendEntriesImpl(self: *InMemoryStorage, ents: []const rpc.Entry) anyerror!void {
        if (ents.len == 0) return;

        const first_new_index = ents[0].index;
        const first_stored_index = self.snapshot_index + 1;

        // Truncate any existing entries from first_new_index onward.
        if (self.log.items.len > 0 and first_new_index >= first_stored_index) {
            const keep = first_new_index - first_stored_index;
            if (keep < self.log.items.len) {
                for (self.log.items[keep..]) |entry| {
                    if (entry.data.len > 0) {
                        self.allocator.free(entry.data);
                    }
                }
                self.log.shrinkRetainingCapacity(keep);
            }
        }

        const initial_len = self.log.items.len;
        // Roll back appended entries on failure to prevent partial
        // appends and leaked data copies (SEC-0125).
        errdefer {
            for (self.log.items[initial_len..]) |e| {
                if (e.data.len > 0) self.allocator.free(e.data);
            }
            self.log.shrinkRetainingCapacity(initial_len);
        }

        for (ents) |entry| {
            const data_copy = if (entry.data.len > 0)
                try self.allocator.dupe(u8, entry.data)
            else
                entry.data;
            // Free data_copy if ArrayList.append fails (SEC-0125).
            errdefer if (data_copy.len > 0) self.allocator.free(data_copy);

            try self.log.append(self.allocator, .{
                .index = entry.index,
                .term = entry.term,
                .kind = entry.kind,
                .data = data_copy,
            });
        }
    }

    pub fn entriesImpl(self: *InMemoryStorage, lo: u64, hi: u64) anyerror![]rpc.Entry {
        if (lo <= self.snapshot_index) return StorageError.Compacted;
        // Guard: lo >= hi returns empty, matching FileStorage (SEC-0118).
        if (lo >= hi) return &[_]rpc.Entry{};

        const start = lo - self.snapshot_index - 1;
        const end = hi - self.snapshot_index - 1;

        // Upper-bound guard: return Unavailable for out-of-range indices (SEC-0051).
        if (end > self.log.items.len) return StorageError.Unavailable;

        // The caller owns each entry's data, as with FileStorage (see
        // Storage.entries); free with rpc.freeEntries.
        return rpc.dupeEntries(self.allocator, self.log.items[start..end]);
    }

    pub fn termImpl(self: *InMemoryStorage, index: u64) anyerror!u64 {
        if (index < self.snapshot_index) return StorageError.Compacted;
        if (index == self.snapshot_index) return self.snapshot_term;

        const pos = index - self.snapshot_index - 1;
        // Upper-bound guard: return Unavailable for out-of-range index (SEC-0051).
        if (pos >= self.log.items.len) return StorageError.Unavailable;
        return self.log.items[pos].term;
    }

    pub fn lastIndexImpl(self: *InMemoryStorage) anyerror!u64 {
        if (self.log.items.len == 0) return self.snapshot_index;
        return self.log.items[self.log.items.len - 1].index;
    }

    pub fn compactImpl(self: *InMemoryStorage, index: u64) anyerror!void {
        if (index <= self.snapshot_index) return;

        const pos = index - self.snapshot_index - 1;
        // Bounds check: reject compact index beyond log range (SEC-0087).
        if (pos >= self.log.items.len) return StorageError.Unavailable;
        const remove_count = pos + 1;

        // Update snapshot_term from the entry being compacted to.
        self.snapshot_term = self.log.items[pos].term;

        // Free data for removed entries.
        for (self.log.items[0..remove_count]) |entry| {
            if (entry.data.len > 0) {
                self.allocator.free(entry.data);
            }
        }

        // Shift remaining entries forward.
        const remaining = self.log.items.len - remove_count;
        std.mem.copyForwards(rpc.Entry, self.log.items[0..remaining], self.log.items[remove_count..]);
        self.log.shrinkRetainingCapacity(remaining);

        self.snapshot_index = index;
    }

    pub fn saveSnapshotImpl(self: *InMemoryStorage, snap: rpc.Snapshot) anyerror!void {
        // Free previous snapshot data if present (SEC-0095).
        if (self.snapshot_data) |old| {
            if (old.data.len > 0) self.allocator.free(old.data);
            if (old.metadata.config.voters.len > 0) self.allocator.free(old.metadata.config.voters);
            if (old.metadata.config.voters_outgoing.len > 0) self.allocator.free(old.metadata.config.voters_outgoing);
        }
        // Duplicate snapshot data and config slices so deinit can free them (SEC-0095).
        const data_copy = if (snap.data.len > 0)
            try self.allocator.dupe(u8, snap.data)
        else
            snap.data;
        errdefer if (data_copy.len > 0) self.allocator.free(data_copy);
        const voters_copy = if (snap.metadata.config.voters.len > 0)
            try self.allocator.dupe(u64, snap.metadata.config.voters)
        else
            snap.metadata.config.voters;
        errdefer if (voters_copy.len > 0) self.allocator.free(voters_copy);
        const outgoing_copy = if (snap.metadata.config.voters_outgoing.len > 0)
            try self.allocator.dupe(u64, snap.metadata.config.voters_outgoing)
        else
            snap.metadata.config.voters_outgoing;
        if (snap.metadata.index > self.snapshot_index) {
            self.discardCoveredBy(snap.metadata.index, snap.metadata.term);
        }
        self.snapshot_data = .{
            .metadata = .{
                .index = snap.metadata.index,
                .term = snap.metadata.term,
                .config = .{
                    .voters = voters_copy,
                    .voters_outgoing = outgoing_copy,
                    .is_joint = snap.metadata.config.is_joint,
                },
            },
            .data = data_copy,
        };
        self.snapshot_index = snap.metadata.index;
        self.snapshot_term = snap.metadata.term;
    }

    /// Drop the entries a snapshot at `snap_index` / `snap_term` covers
    /// (Raft §7, SONNY-6611): up to it when the log holds that entry with
    /// that term, otherwise all of them. `log` is positional from
    /// `snapshot_index + 1`, so without this a snapshot saved past the
    /// stored entries misaligned every later `entries()` / `term()`.
    fn discardCoveredBy(self: *InMemoryStorage, snap_index: u64, snap_term: u64) void {
        const pos = snap_index - self.snapshot_index - 1;
        const keep_tail = pos < self.log.items.len and self.log.items[pos].term == snap_term;
        const drop = if (keep_tail) pos + 1 else self.log.items.len;
        for (self.log.items[0..drop]) |entry| {
            if (entry.data.len > 0) self.allocator.free(entry.data);
        }
        const remaining = self.log.items.len - drop;
        std.mem.copyForwards(rpc.Entry, self.log.items[0..remaining], self.log.items[drop..]);
        self.log.shrinkRetainingCapacity(remaining);
    }

    /// Returns a heap-allocated copy of the snapshot (data + config voter
    /// slices). Caller must free via `freeSnapshot()` (SEC-0072).
    pub fn loadSnapshotImpl(self: *InMemoryStorage) anyerror!?rpc.Snapshot {
        const sd = self.snapshot_data orelse return null;
        const data_copy = if (sd.data.len > 0)
            try self.allocator.dupe(u8, sd.data)
        else
            sd.data;
        errdefer if (data_copy.len > 0) self.allocator.free(data_copy);
        const voters_copy = if (sd.metadata.config.voters.len > 0)
            try self.allocator.dupe(u64, sd.metadata.config.voters)
        else
            sd.metadata.config.voters;
        errdefer if (voters_copy.len > 0) self.allocator.free(voters_copy);
        const outgoing_copy = if (sd.metadata.config.voters_outgoing.len > 0)
            try self.allocator.dupe(u64, sd.metadata.config.voters_outgoing)
        else
            sd.metadata.config.voters_outgoing;
        return rpc.Snapshot{
            .metadata = .{
                .index = sd.metadata.index,
                .term = sd.metadata.term,
                .config = .{
                    .voters = voters_copy,
                    .voters_outgoing = outgoing_copy,
                    .is_joint = sd.metadata.config.is_joint,
                },
            },
            .data = data_copy,
        };
    }

    pub fn saveHardStateImpl(self: *InMemoryStorage, hs: state.HardState) anyerror!void {
        self.hard_state = hs;
    }

    pub fn loadHardState(self: *InMemoryStorage) state.HardState {
        return self.hard_state;
    }

    pub fn loadHardStateImpl(self: *InMemoryStorage) anyerror!state.HardState {
        return self.hard_state;
    }

    // --- Type-erased wrappers for vtable ---

    fn typeErasedSaveHardState(ptr: *anyopaque, hs: state.HardState) anyerror!void {
        const self: *InMemoryStorage = @ptrCast(@alignCast(ptr));
        return self.saveHardStateImpl(hs);
    }

    fn typeErasedLoadHardState(ptr: *anyopaque) anyerror!state.HardState {
        const self: *InMemoryStorage = @ptrCast(@alignCast(ptr));
        return self.loadHardStateImpl();
    }

    fn typeErasedAppendEntries(ptr: *anyopaque, ents: []const rpc.Entry) anyerror!void {
        const self: *InMemoryStorage = @ptrCast(@alignCast(ptr));
        return self.appendEntriesImpl(ents);
    }

    fn typeErasedEntries(ptr: *anyopaque, lo: u64, hi: u64) anyerror![]rpc.Entry {
        const self: *InMemoryStorage = @ptrCast(@alignCast(ptr));
        return self.entriesImpl(lo, hi);
    }

    fn typeErasedTerm(ptr: *anyopaque, index: u64) anyerror!u64 {
        const self: *InMemoryStorage = @ptrCast(@alignCast(ptr));
        return self.termImpl(index);
    }

    fn typeErasedLastIndex(ptr: *anyopaque) anyerror!u64 {
        const self: *InMemoryStorage = @ptrCast(@alignCast(ptr));
        return self.lastIndexImpl();
    }

    fn typeErasedSaveSnapshot(ptr: *anyopaque, snap: rpc.Snapshot) anyerror!void {
        const self: *InMemoryStorage = @ptrCast(@alignCast(ptr));
        return self.saveSnapshotImpl(snap);
    }

    fn typeErasedLoadSnapshot(ptr: *anyopaque) anyerror!?rpc.Snapshot {
        const self: *InMemoryStorage = @ptrCast(@alignCast(ptr));
        return self.loadSnapshotImpl();
    }

    fn typeErasedCompact(ptr: *anyopaque, index: u64) anyerror!void {
        const self: *InMemoryStorage = @ptrCast(@alignCast(ptr));
        return self.compactImpl(index);
    }
};

// --- Tests ---

test "append entries then retrieve" {
    var store = InMemoryStorage.init(std.testing.allocator);
    defer store.deinit();

    const entries_in = [_]rpc.Entry{
        .{ .index = 1, .term = 1, .kind = .normal, .data = "hello" },
        .{ .index = 2, .term = 1, .kind = .normal, .data = "world" },
        .{ .index = 3, .term = 2, .kind = .noop, .data = &.{} },
    };
    try store.appendEntriesImpl(&entries_in);

    const result = try store.entriesImpl(1, 4);
    defer rpc.freeEntries(std.testing.allocator, result);

    try std.testing.expectEqual(@as(usize, 3), result.len);
    try std.testing.expectEqual(@as(u64, 1), result[0].index);
    try std.testing.expectEqual(@as(u64, 2), result[1].index);
    try std.testing.expectEqual(@as(u64, 3), result[2].index);
    try std.testing.expectEqualStrings("hello", result[0].data);
    try std.testing.expectEqualStrings("world", result[1].data);
}

test "term at various indices" {
    var store = InMemoryStorage.init(std.testing.allocator);
    defer store.deinit();

    const entries_in = [_]rpc.Entry{
        .{ .index = 1, .term = 1, .kind = .normal, .data = "a" },
        .{ .index = 2, .term = 1, .kind = .normal, .data = "b" },
        .{ .index = 3, .term = 2, .kind = .normal, .data = "c" },
    };
    try store.appendEntriesImpl(&entries_in);

    try std.testing.expectEqual(@as(u64, 1), try store.termImpl(1));
    try std.testing.expectEqual(@as(u64, 1), try store.termImpl(2));
    try std.testing.expectEqual(@as(u64, 2), try store.termImpl(3));

    // Term at snapshot_index (0) should return snapshot_term (0).
    try std.testing.expectEqual(@as(u64, 0), try store.termImpl(0));
}

test "lastIndex empty and non-empty" {
    var store = InMemoryStorage.init(std.testing.allocator);
    defer store.deinit();

    // Empty log returns snapshot_index (0).
    try std.testing.expectEqual(@as(u64, 0), try store.lastIndexImpl());

    const entries_in = [_]rpc.Entry{
        .{ .index = 1, .term = 1, .kind = .normal, .data = &.{} },
        .{ .index = 2, .term = 1, .kind = .normal, .data = &.{} },
    };
    try store.appendEntriesImpl(&entries_in);

    try std.testing.expectEqual(@as(u64, 2), try store.lastIndexImpl());
}

test "compact removes old entries" {
    var store = InMemoryStorage.init(std.testing.allocator);
    defer store.deinit();

    const entries_in = [_]rpc.Entry{
        .{ .index = 1, .term = 1, .kind = .normal, .data = "a" },
        .{ .index = 2, .term = 1, .kind = .normal, .data = "b" },
        .{ .index = 3, .term = 2, .kind = .normal, .data = "c" },
        .{ .index = 4, .term = 2, .kind = .normal, .data = "d" },
    };
    try store.appendEntriesImpl(&entries_in);

    try store.compactImpl(2);

    try std.testing.expectEqual(@as(u64, 2), store.snapshot_index);
    try std.testing.expectEqual(@as(u64, 1), store.snapshot_term);
    try std.testing.expectEqual(@as(usize, 2), store.log.items.len);

    // Remaining entries are indices 3 and 4.
    const result = try store.entriesImpl(3, 5);
    defer rpc.freeEntries(std.testing.allocator, result);
    try std.testing.expectEqual(@as(u64, 3), result[0].index);
    try std.testing.expectEqual(@as(u64, 4), result[1].index);
}

test "saveHardState and loadHardState round-trip" {
    var store = InMemoryStorage.init(std.testing.allocator);
    defer store.deinit();

    const hs = state.HardState{ .current_term = 5, .voted_for = 3 };
    try store.saveHardStateImpl(hs);

    const loaded = store.loadHardState();
    try std.testing.expect(hs.eql(loaded));
}

test "saveSnapshot and loadSnapshot round-trip" {
    var store = InMemoryStorage.init(std.testing.allocator);
    defer store.deinit();

    const snap = rpc.Snapshot{
        .metadata = .{
            .index = 10,
            .term = 3,
            .config = .{
                .voters = &[_]u64{ 1, 2, 3 },
                .voters_outgoing = &.{},
                .is_joint = false,
            },
        },
        .data = "snapshot-data",
    };

    try store.saveSnapshotImpl(snap);

    try std.testing.expectEqual(@as(u64, 10), store.snapshot_index);
    try std.testing.expectEqual(@as(u64, 3), store.snapshot_term);

    const loaded = try store.loadSnapshotImpl();
    try std.testing.expect(loaded != null);
    defer freeSnapshot(std.testing.allocator, loaded.?);
    try std.testing.expectEqual(@as(u64, 10), loaded.?.metadata.index);
    try std.testing.expectEqual(@as(u64, 3), loaded.?.metadata.term);
    try std.testing.expectEqualStrings("snapshot-data", loaded.?.data);
}

test "entries returns Compacted when lo <= snapshot_index" {
    var store = InMemoryStorage.init(std.testing.allocator);
    defer store.deinit();

    const entries_in = [_]rpc.Entry{
        .{ .index = 1, .term = 1, .kind = .normal, .data = "a" },
        .{ .index = 2, .term = 1, .kind = .normal, .data = "b" },
        .{ .index = 3, .term = 2, .kind = .normal, .data = "c" },
    };
    try store.appendEntriesImpl(&entries_in);
    try store.compactImpl(2);

    // Requesting entries at or before snapshot_index should fail.
    try std.testing.expectError(StorageError.Compacted, store.entriesImpl(1, 3));
    try std.testing.expectError(StorageError.Compacted, store.entriesImpl(2, 3));

    // Requesting entries after snapshot_index should succeed.
    const result = try store.entriesImpl(3, 4);
    defer rpc.freeEntries(std.testing.allocator, result);
    try std.testing.expectEqual(@as(usize, 1), result.len);
    try std.testing.expectEqual(@as(u64, 3), result[0].index);
}

test "vtable dispatch works" {
    var store = InMemoryStorage.init(std.testing.allocator);
    defer store.deinit();

    const s = store.storage();

    const entries_in = [_]rpc.Entry{
        .{ .index = 1, .term = 1, .kind = .normal, .data = "x" },
    };
    try s.appendEntries(&entries_in);

    try std.testing.expectEqual(@as(u64, 1), try s.lastIndex());
    try std.testing.expectEqual(@as(u64, 1), try s.term(1));

    const result = try s.entries(1, 2);
    defer rpc.freeEntries(std.testing.allocator, result);
    try std.testing.expectEqual(@as(usize, 1), result.len);
    try std.testing.expectEqualStrings("x", result[0].data);
}

test "InMemoryStorage: a snapshot matching the log trims it; a conflicting one drops it (SONNY-6611)" {
    var store = InMemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var ents: [10]rpc.Entry = undefined;
    for (&ents, 0..) |*e, i| e.* = .{ .index = i + 1, .term = 1, .kind = .normal, .data = "d" };
    try store.appendEntriesImpl(&ents);

    const snap = rpc.Snapshot{
        .metadata = .{ .index = 6, .term = 1, .config = .{ .voters = &.{1}, .voters_outgoing = &.{}, .is_joint = false } },
        .data = "s",
    };
    try store.saveSnapshotImpl(snap);
    try std.testing.expectEqual(@as(u64, 10), try store.lastIndexImpl());
    const got = try store.entriesImpl(7, 9);
    defer rpc.freeEntries(std.testing.allocator, got);
    try std.testing.expectEqual(@as(u64, 7), got[0].index);
    try std.testing.expectEqual(@as(u64, 8), got[1].index);

    var conflicting = snap;
    conflicting.metadata.index = 8;
    conflicting.metadata.term = 2;
    try store.saveSnapshotImpl(conflicting);
    try std.testing.expectEqual(@as(u64, 8), try store.lastIndexImpl());
    try std.testing.expectError(StorageError.Unavailable, store.termImpl(9));
}
