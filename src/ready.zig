// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-2025 CodePros

const std = @import("std");
const state = @import("state.zig");
const rpc = @import("rpc.zig");

pub const Ready = struct {
    hard_state: ?state.HardState,
    snapshot: ?rpc.Snapshot,
    committed_entries: []const rpc.Entry,
    messages: []const rpc.Message,
    should_snapshot: bool,
};

pub const ReadyState = struct {
    prev_hard_state: state.HardState,
    has_pending_ready: bool,
    pending_messages: std.ArrayList(rpc.Message),
    pending_committed_entries: std.ArrayList(rpc.Entry),
    /// `RaftLog.slice` results whose data `pending_committed_entries`
    /// borrows; freed (data and slice) at `advance` / `deinit` (SONNY-6592).
    owned_committed: std.ArrayList([]rpc.Entry) = .empty,
    pending_snapshot: ?rpc.Snapshot,
    pending_should_snapshot: bool,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, initial_hs: state.HardState) ReadyState {
        return .{
            .prev_hard_state = initial_hs,
            .has_pending_ready = false,
            .pending_messages = .empty,
            .pending_committed_entries = .empty,
            .pending_snapshot = null,
            .pending_should_snapshot = false,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *ReadyState) void {
        self.pending_messages.deinit(self.allocator);
        self.pending_committed_entries.deinit(self.allocator);
        self.freeOwnedCommitted();
        self.owned_committed.deinit(self.allocator);
    }

    fn freeOwnedCommitted(self: *ReadyState) void {
        for (self.owned_committed.items) |entries| rpc.freeEntries(self.allocator, entries);
        self.owned_committed.clearRetainingCapacity();
    }

    /// Accumulate a message to be included in the next Ready.
    pub fn addMessage(self: *ReadyState, msg: rpc.Message) !void {
        try self.pending_messages.append(self.allocator, msg);
    }

    /// Accumulate multiple messages.
    pub fn addMessages(self: *ReadyState, msgs: []const rpc.Message) !void {
        try self.pending_messages.appendSlice(self.allocator, msgs);
    }

    /// Accumulate a committed entry.
    pub fn addCommittedEntry(self: *ReadyState, entry: rpc.Entry) !void {
        try self.pending_committed_entries.append(self.allocator, entry);
    }

    /// Accumulate committed entries (slice).
    pub fn addCommittedEntries(self: *ReadyState, entries: []const rpc.Entry) !void {
        try self.pending_committed_entries.appendSlice(self.allocator, entries);
    }

    /// `addCommittedEntries` for a slice the caller owns (a `RaftLog.slice`
    /// result): the ReadyState takes it, data included, and frees it at
    /// `advance`. On error the caller still owns it.
    pub fn addOwnedCommittedEntries(self: *ReadyState, entries: []rpc.Entry) !void {
        if (entries.len == 0) return;
        try self.owned_committed.ensureUnusedCapacity(self.allocator, 1);
        try self.pending_committed_entries.appendSlice(self.allocator, entries);
        self.owned_committed.appendAssumeCapacity(entries);
    }

    /// Set snapshot to deliver.
    pub fn setSnapshot(self: *ReadyState, snapshot: rpc.Snapshot) void {
        self.pending_snapshot = snapshot;
    }

    /// Set should_snapshot flag.
    pub fn setShouldSnapshot(self: *ReadyState, should: bool) void {
        self.pending_should_snapshot = should;
    }

    /// Build Ready from accumulated state.
    /// Returns error if advance() was not called on the previous Ready.
    pub fn ready(self: *ReadyState, current_hs: state.HardState) error{ReadyNotAdvanced}!Ready {
        if (self.has_pending_ready) return error.ReadyNotAdvanced;

        self.has_pending_ready = true;

        const hs: ?state.HardState = if (!current_hs.eql(self.prev_hard_state)) current_hs else null;

        return .{
            .hard_state = hs,
            .snapshot = self.pending_snapshot,
            .committed_entries = self.pending_committed_entries.items,
            .messages = self.pending_messages.items,
            .should_snapshot = self.pending_should_snapshot,
        };
    }

    /// Release Ready memory, reset pending state.
    pub fn advance(self: *ReadyState, current_hs: state.HardState) void {
        self.prev_hard_state = current_hs;
        self.has_pending_ready = false;

        self.pending_messages.clearRetainingCapacity();
        self.pending_committed_entries.clearRetainingCapacity();
        self.freeOwnedCommitted();
        self.pending_snapshot = null;
        self.pending_should_snapshot = false;
    }

    /// Returns true if there's anything to deliver.
    pub fn hasReady(self: *const ReadyState, current_hs: state.HardState) bool {
        if (self.pending_messages.items.len > 0) return true;
        if (self.pending_committed_entries.items.len > 0) return true;
        if (self.pending_snapshot != null) return true;
        if (self.pending_should_snapshot) return true;
        if (!current_hs.eql(self.prev_hard_state)) return true;
        return false;
    }
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

fn testMsg(term: u64) rpc.Message {
    return .{ .request_vote = .{
        .term = term,
        .candidate_id = 1,
        .last_log_index = 0,
        .last_log_term = 0,
        .pre_vote = false,
    } };
}

fn testEntry(index: u64) rpc.Entry {
    return .{ .index = index, .term = 1, .kind = .normal, .data = "x" };
}

fn testSnapshot() rpc.Snapshot {
    return .{
        .metadata = .{ .index = 5, .term = 2, .config = .{
            .voters = &.{},
            .voters_outgoing = &.{},
            .is_joint = false,
        } },
        .data = "snap",
    };
}

test "ready/advance handshake" {
    const alloc = std.testing.allocator;
    var rs = ReadyState.init(alloc, state.HardState.initial);
    defer rs.deinit();

    try rs.addMessage(testMsg(1));
    try rs.addCommittedEntry(testEntry(1));

    const hs1 = state.HardState{ .current_term = 1, .voted_for = null };
    const r = try rs.ready(hs1);

    try std.testing.expectEqual(@as(usize, 1), r.messages.len);
    try std.testing.expectEqual(@as(usize, 1), r.committed_entries.len);
    try std.testing.expect(r.hard_state != null);

    rs.advance(hs1);

    // Second round with no changes.
    try rs.addMessage(testMsg(2));
    const r2 = try rs.ready(hs1);
    try std.testing.expectEqual(@as(usize, 1), r2.messages.len);
    try std.testing.expect(r2.hard_state == null); // unchanged
    rs.advance(hs1);
}

test "hard_state only included when changed" {
    const alloc = std.testing.allocator;
    const hs = state.HardState{ .current_term = 3, .voted_for = 1 };
    var rs = ReadyState.init(alloc, hs);
    defer rs.deinit();

    try rs.addMessage(testMsg(1));
    const r = try rs.ready(hs);
    try std.testing.expect(r.hard_state == null);
    rs.advance(hs);

    const hs2 = state.HardState{ .current_term = 4, .voted_for = 1 };
    try rs.addMessage(testMsg(1));
    const r2 = try rs.ready(hs2);
    try std.testing.expect(r2.hard_state != null);
    try std.testing.expectEqual(@as(u64, 4), r2.hard_state.?.current_term);
    rs.advance(hs2);
}

test "hasReady returns false when empty, true when messages added" {
    const alloc = std.testing.allocator;
    const hs = state.HardState.initial;
    var rs = ReadyState.init(alloc, hs);
    defer rs.deinit();

    try std.testing.expect(!rs.hasReady(hs));

    try rs.addMessage(testMsg(1));
    try std.testing.expect(rs.hasReady(hs));
}

test "addMessage and addMessages accumulate" {
    const alloc = std.testing.allocator;
    var rs = ReadyState.init(alloc, state.HardState.initial);
    defer rs.deinit();

    try rs.addMessage(testMsg(1));
    const batch = [_]rpc.Message{ testMsg(2), testMsg(3) };
    try rs.addMessages(&batch);

    try std.testing.expectEqual(@as(usize, 3), rs.pending_messages.items.len);
    try std.testing.expectEqual(@as(u64, 1), rs.pending_messages.items[0].request_vote.term);
    try std.testing.expectEqual(@as(u64, 3), rs.pending_messages.items[2].request_vote.term);
}

test "advance clears all pending state" {
    const alloc = std.testing.allocator;
    const hs = state.HardState.initial;
    var rs = ReadyState.init(alloc, hs);
    defer rs.deinit();

    try rs.addMessage(testMsg(1));
    try rs.addCommittedEntry(testEntry(1));
    rs.setSnapshot(testSnapshot());
    rs.setShouldSnapshot(true);

    _ = try rs.ready(hs);
    rs.advance(hs);

    try std.testing.expectEqual(@as(usize, 0), rs.pending_messages.items.len);
    try std.testing.expectEqual(@as(usize, 0), rs.pending_committed_entries.items.len);
    try std.testing.expect(rs.pending_snapshot == null);
    try std.testing.expect(!rs.pending_should_snapshot);
    try std.testing.expect(!rs.has_pending_ready);
}

test "setSnapshot and setShouldSnapshot" {
    const alloc = std.testing.allocator;
    const hs = state.HardState.initial;
    var rs = ReadyState.init(alloc, hs);
    defer rs.deinit();

    try std.testing.expect(!rs.hasReady(hs));

    rs.setShouldSnapshot(true);
    try std.testing.expect(rs.hasReady(hs));

    rs.setShouldSnapshot(false);
    rs.setSnapshot(testSnapshot());
    try std.testing.expect(rs.hasReady(hs));

    const r = try rs.ready(hs);
    try std.testing.expect(r.should_snapshot == false);
    try std.testing.expect(r.snapshot != null);
    try std.testing.expectEqual(@as(u64, 5), r.snapshot.?.metadata.index);
    rs.advance(hs);
}

test "owned committed entries are freed at advance and at deinit" {
    const alloc = std.testing.allocator;
    const hs = state.HardState.initial;
    const src = [_]rpc.Entry{ testEntry(1), testEntry(2) };

    var rs = ReadyState.init(alloc, hs);
    defer rs.deinit();

    try rs.addOwnedCommittedEntries(try rpc.dupeEntries(alloc, &src));
    const r = try rs.ready(hs);
    try std.testing.expectEqual(@as(usize, 2), r.committed_entries.len);
    try std.testing.expectEqualStrings("x", r.committed_entries[1].data);
    rs.advance(hs);

    // Still pending at deinit: freed there.
    try rs.addOwnedCommittedEntries(try rpc.dupeEntries(alloc, &src));
}
