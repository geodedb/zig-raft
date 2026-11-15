// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-2025 CodePros

const std = @import("std");
const rpc = @import("rpc.zig");
const stg = @import("storage.zig");

pub const RaftLog = struct {
    storage: stg.Storage,
    unstable_entries: std.ArrayList(rpc.Entry),
    /// Logical start offset into unstable_entries.items — entries before
    /// this index have been stabilized and freed. Avoids O(n) shift on
    /// every stableTo/compact call (PERF-009).
    unstable_start: usize = 0,
    committed: u64,
    applied: u64,
    snapshot_index: u64,
    snapshot_term: u64,
    /// Cached result of storage.lastIndex() to avoid vtable dispatch on
    /// every lastIndex() call when unstable_entries is empty (PERF-010).
    cached_storage_last_index: u64,
    allocator: std.mem.Allocator,

    /// Initialize a RaftLog with pre-loaded snapshot metadata.
    /// The caller is responsible for loading the snapshot once and passing
    /// the index/term here, avoiding a redundant loadSnapshot() call (PERF-021).
    pub fn init(allocator: std.mem.Allocator, storage: stg.Storage, snap_idx: u64, snap_trm: u64) !RaftLog {
        const storage_last = storage.lastIndex() catch snap_idx;

        return .{
            .storage = storage,
            .unstable_entries = .empty,
            .committed = snap_idx,
            .applied = snap_idx,
            .snapshot_index = snap_idx,
            .snapshot_term = snap_trm,
            .cached_storage_last_index = storage_last,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *RaftLog) void {
        for (self.activeUnstable()) |entry| {
            if (entry.data.len > 0) {
                self.allocator.free(entry.data);
            }
        }
        self.unstable_entries.deinit(self.allocator);
    }

    /// Active (non-compacted) unstable entries (PERF-009).
    fn activeUnstable(self: *const RaftLog) []rpc.Entry {
        return self.unstable_entries.items[self.unstable_start..];
    }

    /// Compact dead prefix if wasted space exceeds active length (PERF-009).
    fn maybeCompactUnstable(self: *RaftLog) void {
        if (self.unstable_start > 0 and self.unstable_start >= self.activeUnstable().len) {
            const items = self.unstable_entries.items;
            const remaining = items.len - self.unstable_start;
            std.mem.copyForwards(rpc.Entry, items[0..remaining], items[self.unstable_start..]);
            self.unstable_entries.shrinkRetainingCapacity(remaining);
            self.unstable_start = 0;
        }
    }

    pub fn append(self: *RaftLog, entries: []const rpc.Entry) !void {
        const initial_len = self.unstable_entries.items.len;
        // Roll back all appended entries on failure to prevent partial
        // batch appends and leaked data copies (SEC-0124).
        errdefer {
            for (self.unstable_entries.items[initial_len..]) |e| {
                if (e.data.len > 0) self.allocator.free(e.data);
            }
            self.unstable_entries.shrinkRetainingCapacity(initial_len);
        }

        for (entries) |entry| {
            const data_copy = if (entry.data.len > 0)
                try self.allocator.dupe(u8, entry.data)
            else
                entry.data;
            // Free data_copy if ArrayList.append fails (SEC-0124).
            errdefer if (data_copy.len > 0) self.allocator.free(data_copy);

            try self.unstable_entries.append(self.allocator, .{
                .index = entry.index,
                .term = entry.term,
                .kind = entry.kind,
                .data = data_copy,
            });
        }
    }

    pub fn lastIndex(self: *RaftLog) u64 {
        const active = self.activeUnstable();
        if (active.len > 0) {
            return active[active.len - 1].index;
        }
        // Use cached value to avoid vtable dispatch (PERF-010).
        return self.cached_storage_last_index;
    }

    pub fn lastTerm(self: *RaftLog) u64 {
        return self.term(self.lastIndex()) catch 0;
    }

    pub fn term(self: *RaftLog, index: u64) !u64 {
        if (index == self.snapshot_index) return self.snapshot_term;
        if (index < self.snapshot_index) return stg.StorageError.Compacted;

        // Check unstable entries first via offset arithmetic.
        const first_unstable = self.firstUnstableIndex();
        const active = self.activeUnstable();
        if (index >= first_unstable) {
            const offset = index - first_unstable;
            if (offset < active.len) {
                return active[offset].term;
            }
            return stg.StorageError.Unavailable;
        }

        return self.storage.term(index);
    }

    /// Return a non-owning borrowed view if the range is entirely within
    /// unstable entries. Returns null if the range spans stable storage —
    /// caller should fall back to slice() which allocates (PERF-003).
    pub fn borrowUnstable(self: *RaftLog, lo: u64, hi: u64) ?[]const rpc.Entry {
        if (lo >= hi) return &[_]rpc.Entry{};
        const first_unstable = self.firstUnstableIndex();
        if (lo < first_unstable) return null;
        const active = self.activeUnstable();
        const start = lo - first_unstable;
        const end = hi - first_unstable;
        if (end > active.len or start > active.len)
            return null;
        return active[start..end];
    }

    /// Entries [lo, hi). The caller owns the slice AND each entry's `data`
    /// (stable entries come from `Storage.entries`, which hands out owned
    /// data; unstable ones are copied): free with `rpc.freeEntries`.
    /// Freeing only the slice leaked every stable entry's data (SONNY-6592).
    pub fn slice(self: *RaftLog, lo: u64, hi: u64) ![]rpc.Entry {
        if (lo >= hi) return &[_]rpc.Entry{};

        const first_unstable = self.firstUnstableIndex();

        // Entirely in stable storage.
        if (hi <= first_unstable) {
            return self.storage.entries(lo, hi);
        }

        // Entirely in unstable entries.
        const active = self.activeUnstable();
        if (lo >= first_unstable) {
            const start = lo - first_unstable;
            const end = hi - first_unstable;
            if (end > active.len or start > active.len) {
                return error.OutOfRange;
            }
            return rpc.dupeEntries(self.allocator, active[start..end]);
        }

        // Spans both regions.
        const unstable_end = hi - first_unstable;
        if (unstable_end > active.len) {
            return error.OutOfRange;
        }
        const stable = try self.storage.entries(lo, first_unstable);
        errdefer rpc.freeEntries(self.allocator, stable);
        const unstable = try rpc.dupeEntries(self.allocator, active[0..unstable_end]);
        errdefer rpc.freeEntries(self.allocator, unstable);

        const result = try self.allocator.alloc(rpc.Entry, stable.len + unstable.len);
        @memcpy(result[0..stable.len], stable);
        @memcpy(result[stable.len..], unstable);
        // The entries' data moved into `result`; free only the two outer slices.
        if (stable.len > 0) self.allocator.free(stable);
        if (unstable.len > 0) self.allocator.free(unstable);
        return result;
    }

    pub fn maybeAppend(
        self: *RaftLog,
        prev_log_index: u64,
        prev_log_term: u64,
        committed: u64,
        entries: []const rpc.Entry,
    ) !?u64 {
        if (!self.matchTerm(prev_log_index, prev_log_term)) return null;

        const last_new_index = std.math.add(u64, prev_log_index, entries.len) catch return error.Overflow;

        // Find the first conflicting entry.
        const first_new_index = std.math.add(u64, prev_log_index, 1) catch return error.Overflow;
        var conflict_idx: u64 = 0;
        for (entries, 0..) |entry, i| {
            const idx = std.math.add(u64, first_new_index, i) catch return error.Overflow;
            const existing_term = self.term(idx) catch {
                // Entry doesn't exist, append from here.
                conflict_idx = idx;
                break;
            };
            if (existing_term != entry.term) {
                conflict_idx = idx;
                break;
            }
        } else {
            // No conflict found, all entries match.
            self.commitTo(@min(committed, last_new_index));
            return last_new_index;
        }

        // Truncate from conflict point forward.
        try self.truncateUnstable(conflict_idx);

        // Append remaining entries starting from the conflict point.
        // Safe: conflict_idx >= prev_log_index + 1 by construction.
        const offset = conflict_idx - prev_log_index - 1;
        try self.append(entries[offset..]);

        self.commitTo(@min(committed, last_new_index));
        return last_new_index;
    }

    pub fn findConflict(self: *RaftLog, index: u64) struct { index: u64, term_val: u64 } {
        if (index <= self.snapshot_index) {
            return .{ .index = self.snapshot_index, .term_val = self.snapshot_term };
        }
        const t = self.term(index) catch 0;
        return .{ .index = index, .term_val = t };
    }

    pub fn commitTo(self: *RaftLog, committed: u64) void {
        const target = @min(committed, self.lastIndex());
        if (target > self.committed) {
            self.committed = target;
        }
    }

    pub fn appliedTo(self: *RaftLog, index: u64) error{AppliedBeyondCommitted}!void {
        if (index > self.committed) return error.AppliedBeyondCommitted;
        self.applied = index;
    }

    pub fn stableTo(self: *RaftLog, index: u64, term_val: u64) !void {
        const active = self.activeUnstable();
        if (active.len == 0) return;

        const first_unstable = self.firstUnstableIndex();
        if (index < first_unstable) return;

        const offset = index - first_unstable;
        if (offset >= active.len) return;

        // Only remove if the term matches.
        if (active[offset].term != term_val) return;

        // Update cache: entries up to `index` are now in storage (PERF-010).
        if (index > self.cached_storage_last_index) {
            self.cached_storage_last_index = index;
        }

        const remove_count = offset + 1;

        // Free data for removed entries.
        for (active[0..remove_count]) |entry| {
            if (entry.data.len > 0) {
                self.allocator.free(entry.data);
            }
        }

        // Advance start offset instead of O(n) shift (PERF-009).
        self.unstable_start += remove_count;
        self.maybeCompactUnstable();
    }

    pub fn compact(self: *RaftLog, compact_index: u64) !void {
        if (compact_index <= self.snapshot_index) return;

        // Capture term before mutating state (SEC-0084).
        const new_snapshot_term = self.term(compact_index) catch self.snapshot_term;

        // Call storage.compact() BEFORE mutating local state so that
        // a storage failure leaves RaftLog consistent (SEC-0084).
        try self.storage.compact(compact_index);

        self.snapshot_term = new_snapshot_term;
        self.snapshot_index = compact_index;

        // Trim unstable entries below compact_index (PERF-009).
        const active = self.activeUnstable();
        if (active.len > 0) {
            const first_unstable_idx = active[0].index;
            if (compact_index >= first_unstable_idx) {
                const trim_count = @min(
                    compact_index - first_unstable_idx + 1,
                    active.len,
                );
                for (active[0..trim_count]) |entry| {
                    if (entry.data.len > 0) {
                        self.allocator.free(entry.data);
                    }
                }
                // Advance start offset instead of O(n) shift (PERF-009).
                self.unstable_start += trim_count;
                self.maybeCompactUnstable();
            }
        }
    }

    pub fn restore(self: *RaftLog, snapshot: rpc.Snapshot) !void {
        // Free existing unstable entries (PERF-009: only active range).
        for (self.activeUnstable()) |entry| {
            if (entry.data.len > 0) {
                self.allocator.free(entry.data);
            }
        }
        self.unstable_entries.shrinkRetainingCapacity(0);
        self.unstable_start = 0;

        self.snapshot_index = snapshot.metadata.index;
        self.snapshot_term = snapshot.metadata.term;
        self.committed = snapshot.metadata.index;
        self.applied = snapshot.metadata.index;
        // Reset cache to snapshot index since storage is reset (PERF-010).
        self.cached_storage_last_index = snapshot.metadata.index;
    }

    pub fn matchTerm(self: *RaftLog, index: u64, term_val: u64) bool {
        const t = self.term(index) catch return false;
        return t == term_val;
    }

    pub fn isUpToDate(self: *RaftLog, last_log_index: u64, last_log_term: u64) bool {
        const our_term = self.lastTerm();
        const our_index = self.lastIndex();
        if (last_log_term != our_term) return last_log_term > our_term;
        return last_log_index >= our_index;
    }

    pub fn unstableSlice(self: *RaftLog) []const rpc.Entry {
        return self.activeUnstable();
    }

    // --- Private helpers ---

    fn firstUnstableIndex(self: *RaftLog) u64 {
        const active = self.activeUnstable();
        if (active.len > 0) {
            return active[0].index;
        }
        // Checked overflow: lastIndex() + 1 (SEC-0070).
        return std.math.add(u64, self.lastIndex(), 1) catch self.lastIndex();
    }

    fn truncateUnstable(self: *RaftLog, from_index: u64) !void {
        const first_unstable = self.firstUnstableIndex();
        if (from_index < first_unstable) {
            // Truncation point is in stable storage; clear all unstable.
            for (self.activeUnstable()) |entry| {
                if (entry.data.len > 0) {
                    self.allocator.free(entry.data);
                }
            }
            self.unstable_entries.shrinkRetainingCapacity(0);
            self.unstable_start = 0;
            return;
        }

        const keep = from_index - first_unstable;
        const active = self.activeUnstable();
        // Free entries being truncated.
        for (active[keep..]) |entry| {
            if (entry.data.len > 0) {
                self.allocator.free(entry.data);
            }
        }
        // Shrink to unstable_start + keep (absolute position in backing array).
        self.unstable_entries.shrinkRetainingCapacity(self.unstable_start + keep);
    }
};

// --- Tests ---

test "append entries and verify lastIndex/lastTerm" {
    var mem_store = stg.InMemoryStorage.init(std.testing.allocator);
    defer mem_store.deinit();

    var log = try RaftLog.init(std.testing.allocator, mem_store.storage(), 0, 0);
    defer log.deinit();

    try std.testing.expectEqual(@as(u64, 0), log.lastIndex());
    try std.testing.expectEqual(@as(u64, 0), log.lastTerm());

    const entries = [_]rpc.Entry{
        .{ .index = 1, .term = 1, .kind = .normal, .data = "a" },
        .{ .index = 2, .term = 1, .kind = .normal, .data = "b" },
        .{ .index = 3, .term = 2, .kind = .normal, .data = "c" },
    };
    try log.append(&entries);

    try std.testing.expectEqual(@as(u64, 3), log.lastIndex());
    try std.testing.expectEqual(@as(u64, 2), log.lastTerm());
}

test "maybeAppend successful append" {
    var mem_store = stg.InMemoryStorage.init(std.testing.allocator);
    defer mem_store.deinit();

    // Put some entries in stable storage.
    const stable_entries = [_]rpc.Entry{
        .{ .index = 1, .term = 1, .kind = .normal, .data = &.{} },
        .{ .index = 2, .term = 1, .kind = .normal, .data = &.{} },
    };
    try mem_store.appendEntriesImpl(&stable_entries);

    var log = try RaftLog.init(std.testing.allocator, mem_store.storage(), 0, 0);
    defer log.deinit();

    const new_entries = [_]rpc.Entry{
        .{ .index = 3, .term = 2, .kind = .normal, .data = &.{} },
        .{ .index = 4, .term = 2, .kind = .normal, .data = &.{} },
    };

    const result = try log.maybeAppend(2, 1, 4, &new_entries);
    try std.testing.expect(result != null);
    try std.testing.expectEqual(@as(u64, 4), result.?);
    try std.testing.expectEqual(@as(u64, 4), log.lastIndex());
    try std.testing.expectEqual(@as(u64, 4), log.committed);
}

test "maybeAppend returns null on prev mismatch" {
    var mem_store = stg.InMemoryStorage.init(std.testing.allocator);
    defer mem_store.deinit();

    const stable_entries = [_]rpc.Entry{
        .{ .index = 1, .term = 1, .kind = .normal, .data = &.{} },
        .{ .index = 2, .term = 1, .kind = .normal, .data = &.{} },
    };
    try mem_store.appendEntriesImpl(&stable_entries);

    var log = try RaftLog.init(std.testing.allocator, mem_store.storage(), 0, 0);
    defer log.deinit();

    const new_entries = [_]rpc.Entry{
        .{ .index = 3, .term = 2, .kind = .normal, .data = &.{} },
    };

    // Wrong prev_log_term (99 instead of 1).
    const result = try log.maybeAppend(2, 99, 3, &new_entries);
    try std.testing.expect(result == null);
}

test "maybeAppend truncation at conflict" {
    var mem_store = stg.InMemoryStorage.init(std.testing.allocator);
    defer mem_store.deinit();

    const stable_entries = [_]rpc.Entry{
        .{ .index = 1, .term = 1, .kind = .normal, .data = &.{} },
        .{ .index = 2, .term = 1, .kind = .normal, .data = &.{} },
    };
    try mem_store.appendEntriesImpl(&stable_entries);

    var log = try RaftLog.init(std.testing.allocator, mem_store.storage(), 0, 0);
    defer log.deinit();

    // Add unstable entries at term 1.
    const unstable = [_]rpc.Entry{
        .{ .index = 3, .term = 1, .kind = .normal, .data = &.{} },
        .{ .index = 4, .term = 1, .kind = .normal, .data = &.{} },
    };
    try log.append(&unstable);

    // Leader sends entries starting at index 3 but with term 2 (conflict).
    const leader_entries = [_]rpc.Entry{
        .{ .index = 3, .term = 2, .kind = .normal, .data = &.{} },
        .{ .index = 4, .term = 2, .kind = .normal, .data = &.{} },
    };
    const result = try log.maybeAppend(2, 1, 4, &leader_entries);
    try std.testing.expect(result != null);
    try std.testing.expectEqual(@as(u64, 4), result.?);

    // Verify the entries now have term 2.
    try std.testing.expectEqual(@as(u64, 2), try log.term(3));
    try std.testing.expectEqual(@as(u64, 2), try log.term(4));
}

test "commitTo advances but never decreases" {
    var mem_store = stg.InMemoryStorage.init(std.testing.allocator);
    defer mem_store.deinit();

    var log = try RaftLog.init(std.testing.allocator, mem_store.storage(), 0, 0);
    defer log.deinit();

    const entries = [_]rpc.Entry{
        .{ .index = 1, .term = 1, .kind = .normal, .data = &.{} },
        .{ .index = 2, .term = 1, .kind = .normal, .data = &.{} },
        .{ .index = 3, .term = 1, .kind = .normal, .data = &.{} },
    };
    try log.append(&entries);

    log.commitTo(2);
    try std.testing.expectEqual(@as(u64, 2), log.committed);

    // Should not decrease.
    log.commitTo(1);
    try std.testing.expectEqual(@as(u64, 2), log.committed);

    // Should advance.
    log.commitTo(3);
    try std.testing.expectEqual(@as(u64, 3), log.committed);

    // Should clamp to lastIndex.
    log.commitTo(100);
    try std.testing.expectEqual(@as(u64, 3), log.committed);
}

test "isUpToDate various term/index comparisons" {
    var mem_store = stg.InMemoryStorage.init(std.testing.allocator);
    defer mem_store.deinit();

    var log = try RaftLog.init(std.testing.allocator, mem_store.storage(), 0, 0);
    defer log.deinit();

    const entries = [_]rpc.Entry{
        .{ .index = 1, .term = 1, .kind = .normal, .data = &.{} },
        .{ .index = 2, .term = 2, .kind = .normal, .data = &.{} },
    };
    try log.append(&entries);
    // Our log: lastIndex=2, lastTerm=2.

    // Higher term is always more up-to-date.
    try std.testing.expect(log.isUpToDate(1, 3));

    // Same term, higher or equal index is up-to-date.
    try std.testing.expect(log.isUpToDate(2, 2));
    try std.testing.expect(log.isUpToDate(3, 2));

    // Same term, lower index is not up-to-date.
    try std.testing.expect(!log.isUpToDate(1, 2));

    // Lower term is not up-to-date regardless of index.
    try std.testing.expect(!log.isUpToDate(100, 1));
}

test "findConflict returns correct conflict info" {
    var mem_store = stg.InMemoryStorage.init(std.testing.allocator);
    defer mem_store.deinit();

    const stable_entries = [_]rpc.Entry{
        .{ .index = 1, .term = 1, .kind = .normal, .data = &.{} },
        .{ .index = 2, .term = 2, .kind = .normal, .data = &.{} },
    };
    try mem_store.appendEntriesImpl(&stable_entries);

    var log = try RaftLog.init(std.testing.allocator, mem_store.storage(), 0, 0);
    defer log.deinit();

    // Index in log returns its term.
    const c1 = log.findConflict(1);
    try std.testing.expectEqual(@as(u64, 1), c1.index);
    try std.testing.expectEqual(@as(u64, 1), c1.term_val);

    const c2 = log.findConflict(2);
    try std.testing.expectEqual(@as(u64, 2), c2.index);
    try std.testing.expectEqual(@as(u64, 2), c2.term_val);

    // Index beyond log returns 0 term.
    const c3 = log.findConflict(5);
    try std.testing.expectEqual(@as(u64, 5), c3.index);
    try std.testing.expectEqual(@as(u64, 0), c3.term_val);

    // Index at or below snapshot returns snapshot info.
    const c0 = log.findConflict(0);
    try std.testing.expectEqual(@as(u64, 0), c0.index);
    try std.testing.expectEqual(@as(u64, 0), c0.term_val);
}

test "term from both stable and unstable regions" {
    var mem_store = stg.InMemoryStorage.init(std.testing.allocator);
    defer mem_store.deinit();

    const stable_entries = [_]rpc.Entry{
        .{ .index = 1, .term = 1, .kind = .normal, .data = &.{} },
        .{ .index = 2, .term = 1, .kind = .normal, .data = &.{} },
    };
    try mem_store.appendEntriesImpl(&stable_entries);

    var log = try RaftLog.init(std.testing.allocator, mem_store.storage(), 0, 0);
    defer log.deinit();

    // Add unstable entries.
    const unstable = [_]rpc.Entry{
        .{ .index = 3, .term = 2, .kind = .normal, .data = &.{} },
        .{ .index = 4, .term = 3, .kind = .normal, .data = &.{} },
    };
    try log.append(&unstable);

    // Stable region.
    try std.testing.expectEqual(@as(u64, 1), try log.term(1));
    try std.testing.expectEqual(@as(u64, 1), try log.term(2));

    // Unstable region.
    try std.testing.expectEqual(@as(u64, 2), try log.term(3));
    try std.testing.expectEqual(@as(u64, 3), try log.term(4));

    // Snapshot index.
    try std.testing.expectEqual(@as(u64, 0), try log.term(0));
}

test "stableTo moves entries from unstable to stable" {
    var mem_store = stg.InMemoryStorage.init(std.testing.allocator);
    defer mem_store.deinit();

    var log = try RaftLog.init(std.testing.allocator, mem_store.storage(), 0, 0);
    defer log.deinit();

    const entries = [_]rpc.Entry{
        .{ .index = 1, .term = 1, .kind = .normal, .data = &.{} },
        .{ .index = 2, .term = 1, .kind = .normal, .data = &.{} },
        .{ .index = 3, .term = 2, .kind = .normal, .data = &.{} },
    };
    try log.append(&entries);
    try std.testing.expectEqual(@as(usize, 3), log.unstableSlice().len);

    // Stabilize up to index 2 with term 1.
    try log.stableTo(2, 1);
    try std.testing.expectEqual(@as(usize, 1), log.unstableSlice().len);
    try std.testing.expectEqual(@as(u64, 3), log.unstableSlice()[0].index);

    // Wrong term should not remove anything.
    try log.stableTo(3, 99);
    try std.testing.expectEqual(@as(usize, 1), log.unstableSlice().len);

    // Correct term should remove it.
    try log.stableTo(3, 2);
    try std.testing.expectEqual(@as(usize, 0), log.unstableSlice().len);
}
