// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-2025 CodePros

//! SONNY-6592 regressions, driven through a real RaftNode on a real
//! FileStorage under the leak-checking `std.testing.allocator`.
//!
//! 1. Entry data handed out by `Storage.entries` (owned by the caller) leaked:
//!    `RaftLog.slice` callers freed only the outer slice.
//! 3. Vote / AppendEntries / InstallSnapshot replies (and a candidate's
//!    RequestVote) reached the network from inside `step()` / `tick()`, before
//!    `advancePersist()` made the vote, term or entries they announce durable.

const std = @import("std");
const rpc = @import("rpc.zig");
const st = @import("state.zig");
const node_mod = @import("node.zig");
const trans = @import("transport.zig");
const fstg = @import("file_storage.zig");

const RaftNode = node_mod.RaftNode;
const FileStorage = fstg.FileStorage;

/// Counts what reaches the transport. Keeps no slices, so it is safe whatever
/// the sender frees after `send` returns.
const CountingTransport = struct {
    sent: u32 = 0,
    request_vote: u32 = 0,
    request_vote_response: u32 = 0,
    append_entries: u32 = 0,
    append_entries_response: u32 = 0,
    install_snapshot_response: u32 = 0,

    const vtable_inst = trans.Transport.VTable{ .send = &sendImpl };

    fn transport(self: *CountingTransport) trans.Transport {
        return .{ .ptr = @ptrCast(self), .vtable = &vtable_inst };
    }

    fn sendImpl(ptr: *anyopaque, _: u64, msg: rpc.Message) void {
        const self: *CountingTransport = @ptrCast(@alignCast(ptr));
        self.sent += 1;
        switch (msg) {
            .request_vote => self.request_vote += 1,
            .request_vote_response => self.request_vote_response += 1,
            .append_entries => self.append_entries += 1,
            .append_entries_response => self.append_entries_response += 1,
            .install_snapshot_response => self.install_snapshot_response += 1,
            .install_snapshot => {},
        }
    }
};

/// A FileStorage in a fresh tmp dir, allocating from `std.testing.allocator`.
const TestStore = struct {
    tmp: std.testing.TmpDir,
    path: []const u8,
    fs: FileStorage,

    fn init(self: *TestStore) !void {
        self.tmp = std.testing.tmpDir(.{});
        errdefer self.tmp.cleanup();
        self.path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/raft", .{self.tmp.sub_path});
        errdefer std.testing.allocator.free(self.path);
        self.fs = try FileStorage.init(std.testing.allocator, self.path);
    }

    fn deinit(self: *TestStore) void {
        self.fs.deinit();
        std.testing.allocator.free(self.path);
        self.tmp.cleanup();
    }
};

fn appendEntries(prev_index: u64, prev_term: u64, entries: []const rpc.Entry, commit: u64) rpc.Message {
    return .{ .append_entries = .{
        .term = 1,
        .leader_id = 1,
        .prev_log_index = prev_index,
        .prev_log_term = prev_term,
        .entries = entries,
        .leader_commit = commit,
    } };
}

/// Elect node 1 (peers 2, 3; no pre-vote) by stepping a granted vote from 2.
fn electNode1(node: *RaftNode) !void {
    var i: u32 = 0;
    while (i < 40 and node.role != .candidate) : (i += 1) try node.tick(null);
    try std.testing.expectEqual(st.Role.candidate, node.role);
    try node.step(2, .{ .request_vote_response = .{
        .term = node.current_term,
        .vote_granted = true,
        .pre_vote = false,
    } });
    try std.testing.expectEqual(st.Role.leader, node.role);
}

// ---------------------------------------------------------------------------
// Item 1: entry data from Storage.entries is owned by the caller.
// ---------------------------------------------------------------------------

test "SONNY-6592: committed entries read back from FileStorage do not leak" {
    var store: TestStore = undefined;
    try store.init();
    defer store.deinit();

    var node = try RaftNode.init(std.testing.allocator, .{ .id = 2, .peers = &.{ 1, 3 } }, store.fs.storage());
    defer node.deinit();

    // Replicate two entries but do not commit them yet: they become stable
    // (persisted + stableTo) while still uncommitted.
    const ents = [_]rpc.Entry{
        .{ .index = 1, .term = 1, .kind = .normal, .data = "alpha" },
        .{ .index = 2, .term = 1, .kind = .normal, .data = "bravo" },
    };
    try node.step(1, appendEntries(0, 0, &ents, 0));
    _ = try node.ready();
    try node.advance();

    // Commit them: ready() must read them back through FileStorage.entries.
    try node.step(1, appendEntries(2, 1, &.{}, 2));
    const rd = try node.ready();
    try std.testing.expectEqual(@as(usize, 2), rd.committed_entries.len);
    try std.testing.expectEqualStrings("alpha", rd.committed_entries[0].data);
    try std.testing.expectEqualStrings("bravo", rd.committed_entries[1].data);
    try node.advance();
}

test "SONNY-6592: AppendEntries built from FileStorage entries do not leak" {
    var store: TestStore = undefined;
    try store.init();
    defer store.deinit();

    var node = try RaftNode.init(std.testing.allocator, .{
        .id = 1,
        .peers = &.{ 2, 3 },
        .pre_vote = false,
        .rng_seed = 7,
    }, store.fs.storage());
    defer node.deinit();

    try electNode1(&node);
    try node.propose("alpha");
    try node.propose("bravo");
    _ = try node.ready();
    try node.advance(); // noop + both proposals are now stable in FileStorage

    var ct: CountingTransport = .{};
    node.setTransport(ct.transport());
    // Heartbeat: peers are at next_index 1, so the leader slices [1, 4) out
    // of FileStorage for each of them.
    try node.tick(null);
    try std.testing.expectEqual(@as(u32, 2), ct.append_entries);
}

test "SONNY-6592: AppendEntries trimmed to max_bytes_per_msg do not leak" {
    var store: TestStore = undefined;
    try store.init();
    defer store.deinit();

    var node = try RaftNode.init(std.testing.allocator, .{
        .id = 1,
        .peers = &.{ 2, 3 },
        .pre_vote = false,
        .rng_seed = 7,
        // noop(0) + "alpha"(5) fit; "bravo" pushes past 6 and is trimmed off.
        .max_bytes_per_msg = 6,
    }, store.fs.storage());
    defer node.deinit();

    try electNode1(&node);
    try node.propose("alpha");
    try node.propose("bravo");
    try node.propose("charlie");
    _ = try node.ready();
    try node.advance();

    var ct: CountingTransport = .{};
    node.setTransport(ct.transport());
    try node.tick(null);
    try std.testing.expectEqual(@as(u32, 2), ct.append_entries);
}

// ---------------------------------------------------------------------------
// Item 3: replies leave only after advancePersist().
// ---------------------------------------------------------------------------

test "SONNY-6592: a vote grant is not sent before the vote is persisted" {
    var store: TestStore = undefined;
    try store.init();
    defer store.deinit();

    var node = try RaftNode.init(std.testing.allocator, .{ .id = 2, .peers = &.{ 1, 3 } }, store.fs.storage());
    defer node.deinit();
    var ct: CountingTransport = .{};
    node.setTransport(ct.transport());

    try node.step(1, .{ .request_vote = .{
        .term = 1,
        .candidate_id = 1,
        .last_log_index = 0,
        .last_log_term = 0,
        .pre_vote = false,
    } });
    try std.testing.expectEqual(@as(?u64, 1), node.voted_for);

    // The grant must not be observable while the vote is only in memory.
    try std.testing.expectEqual(@as(u32, 0), ct.sent);
    try std.testing.expectEqual(@as(?u64, null), (try store.fs.storage().loadHardState()).voted_for);

    const rd = try node.ready();
    try std.testing.expectEqual(@as(?u64, 1), rd.hard_state.?.voted_for);
    try std.testing.expectEqual(@as(usize, 1), rd.messages.len);
    try std.testing.expectEqual(@as(u64, 1), rd.messages[0].to);
    try std.testing.expect(rd.messages[0].msg.request_vote_response.vote_granted);
    try std.testing.expectEqual(@as(u64, 1), rd.messages[0].msg.request_vote_response.term);

    try node.advancePersist();
    try std.testing.expectEqual(@as(?u64, 1), (try store.fs.storage().loadHardState()).voted_for);
    try node.advanceApply();
    try std.testing.expectEqual(@as(u32, 0), ct.sent);
}

test "SONNY-6592: an AppendEntries ack is not sent before the entries are persisted" {
    var store: TestStore = undefined;
    try store.init();
    defer store.deinit();

    var node = try RaftNode.init(std.testing.allocator, .{ .id = 2, .peers = &.{ 1, 3 } }, store.fs.storage());
    defer node.deinit();
    var ct: CountingTransport = .{};
    node.setTransport(ct.transport());

    const ents = [_]rpc.Entry{
        .{ .index = 1, .term = 1, .kind = .normal, .data = "alpha" },
        .{ .index = 2, .term = 1, .kind = .normal, .data = "bravo" },
    };
    try node.step(1, appendEntries(0, 0, &ents, 0));

    try std.testing.expectEqual(@as(u32, 0), ct.sent);
    try std.testing.expectEqual(@as(u64, 0), try store.fs.storage().lastIndex());

    const rd = try node.ready();
    try std.testing.expectEqual(@as(usize, 1), rd.messages.len);
    try std.testing.expectEqual(@as(u64, 1), rd.messages[0].to);
    const ack = rd.messages[0].msg.append_entries_response;
    try std.testing.expect(ack.success);
    try std.testing.expectEqual(@as(u64, 2), ack.match_index);

    try node.advancePersist();
    try std.testing.expectEqual(@as(u64, 2), try store.fs.storage().lastIndex());
    try node.advanceApply();
    try std.testing.expectEqual(@as(u32, 0), ct.sent);
}

test "SONNY-6592: a candidate's RequestVote is not sent before its term and self-vote are persisted" {
    var store: TestStore = undefined;
    try store.init();
    defer store.deinit();

    var node = try RaftNode.init(std.testing.allocator, .{
        .id = 1,
        .peers = &.{ 2, 3 },
        .pre_vote = false,
        .rng_seed = 7,
    }, store.fs.storage());
    defer node.deinit();
    var ct: CountingTransport = .{};
    node.setTransport(ct.transport());

    var i: u32 = 0;
    while (i < 40 and node.role != .candidate) : (i += 1) try node.tick(null);
    try std.testing.expectEqual(st.Role.candidate, node.role);

    try std.testing.expectEqual(@as(u32, 0), ct.sent);
    try std.testing.expectEqual(@as(u64, 0), (try store.fs.storage().loadHardState()).current_term);

    const rd = try node.ready();
    try std.testing.expectEqual(@as(usize, 2), rd.messages.len);
    var seen = [_]bool{ false, false };
    for (rd.messages) |out| {
        try std.testing.expect(out.to == 2 or out.to == 3);
        seen[out.to - 2] = true;
        try std.testing.expectEqual(@as(u64, 1), out.msg.request_vote.term);
        try std.testing.expect(!out.msg.request_vote.pre_vote);
    }
    try std.testing.expect(seen[0] and seen[1]);
    try node.advancePersist();
    const hs = try store.fs.storage().loadHardState();
    try std.testing.expectEqual(@as(u64, 1), hs.current_term);
    try std.testing.expectEqual(@as(?u64, 1), hs.voted_for);
    try node.advanceApply();
}

test "SONNY-6592: InstallSnapshot replies are not sent from inside step()" {
    var store: TestStore = undefined;
    try store.init();
    defer store.deinit();

    var node = try RaftNode.init(std.testing.allocator, .{ .id = 2, .peers = &.{ 1, 3 } }, store.fs.storage());
    defer node.deinit();
    var ct: CountingTransport = .{};
    node.setTransport(ct.transport());

    // Move to term 2 (persisted), then a stale term-1 InstallSnapshot is
    // rejected with the current term.
    try node.step(1, .{ .request_vote = .{
        .term = 2,
        .candidate_id = 1,
        .last_log_index = 0,
        .last_log_term = 0,
        .pre_vote = false,
    } });
    _ = try node.ready();
    try node.advance();

    try node.step(1, .{ .install_snapshot = .{
        .term = 1,
        .leader_id = 1,
        .last_included_index = 5,
        .last_included_term = 1,
        .offset = 0,
        .data = "snap",
        .done = true,
        .checksum = .{0} ** 32,
        .config = .{ .voters = &.{ 1, 2, 3 }, .voters_outgoing = &.{}, .is_joint = false },
    } });
    try std.testing.expectEqual(@as(u32, 0), ct.sent);
    const rd = try node.ready();
    try std.testing.expectEqual(@as(usize, 1), rd.messages.len);
    try std.testing.expectEqual(@as(u64, 1), rd.messages[0].to);
    try std.testing.expect(!rd.messages[0].msg.install_snapshot_response.success);
    try std.testing.expectEqual(@as(u64, 2), rd.messages[0].msg.install_snapshot_response.term);
    try node.advance();
}

test "SONNY-6592: leader replication still goes through the transport at once" {
    var store: TestStore = undefined;
    try store.init();
    defer store.deinit();

    var node = try RaftNode.init(std.testing.allocator, .{
        .id = 1,
        .peers = &.{ 2, 3 },
        .pre_vote = false,
        .rng_seed = 7,
    }, store.fs.storage());
    defer node.deinit();

    try electNode1(&node);
    _ = try node.ready();
    try node.advance();

    var ct: CountingTransport = .{};
    node.setTransport(ct.transport());
    try node.tick(null);
    try std.testing.expectEqual(@as(u32, 2), ct.append_entries);
    const rd = try node.ready();
    try std.testing.expectEqual(@as(usize, 0), rd.messages.len);
    try node.advance();
}

test "SONNY-6592: RaftLog.slice across stable and unstable entries owns all data" {
    var store: TestStore = undefined;
    try store.init();
    defer store.deinit();

    const stable = [_]rpc.Entry{
        .{ .index = 1, .term = 1, .kind = .normal, .data = "alpha" },
        .{ .index = 2, .term = 1, .kind = .normal, .data = "bravo" },
    };
    try store.fs.storage().appendEntries(&stable);

    var log = try @import("log.zig").RaftLog.init(std.testing.allocator, store.fs.storage(), 0, 0);
    defer log.deinit();
    const unstable = [_]rpc.Entry{
        .{ .index = 3, .term = 1, .kind = .normal, .data = "charlie" },
        .{ .index = 4, .term = 1, .kind = .noop, .data = &.{} },
    };
    try log.append(&unstable);

    const all = try log.slice(1, 5);
    defer rpc.freeEntries(std.testing.allocator, all);
    try std.testing.expectEqual(@as(usize, 4), all.len);
    try std.testing.expectEqualStrings("alpha", all[0].data);
    try std.testing.expectEqualStrings("charlie", all[2].data);
    // The unstable entry is a copy, not a view into the log's buffer.
    try std.testing.expect(all[2].data.ptr != log.borrowUnstable(3, 4).?[0].data.ptr);

    const tail = try log.slice(3, 5);
    defer rpc.freeEntries(std.testing.allocator, tail);
    try std.testing.expectEqualStrings("charlie", tail[0].data);

    const head = try log.slice(1, 3);
    defer rpc.freeEntries(std.testing.allocator, head);
    try std.testing.expectEqualStrings("bravo", head[1].data);
}

test "SONNY-6592: InProcessTransport keeps entry data valid after the sender frees it" {
    var ipt = trans.InProcessTransport.init(std.testing.allocator);
    defer ipt.deinit();
    try ipt.register(2);

    const src = [_]rpc.Entry{.{ .index = 1, .term = 1, .kind = .normal, .data = "alpha" }};
    const sent = try rpc.dupeEntries(std.testing.allocator, &src);
    ipt.transport().send(2, appendEntries(0, 0, sent, 0));
    rpc.freeEntries(std.testing.allocator, sent);

    // A second, undrained message is freed by deinit.
    const again = try rpc.dupeEntries(std.testing.allocator, &src);
    defer rpc.freeEntries(std.testing.allocator, again);

    const msgs = try ipt.drain(2, std.testing.allocator);
    defer std.testing.allocator.free(msgs);
    defer for (msgs) |m| trans.InProcessTransport.freeMessage(std.testing.allocator, m);
    try std.testing.expectEqualStrings("alpha", msgs[0].append_entries.entries[0].data);

    ipt.transport().send(2, appendEntries(0, 0, again, 0));
}
