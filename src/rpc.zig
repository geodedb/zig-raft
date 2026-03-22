// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-2025 CodePros

const std = @import("std");

pub const EntryKind = enum {
    noop,
    normal,
    config,
};

pub const Entry = struct {
    index: u64,
    term: u64,
    kind: EntryKind,
    data: []const u8,

    pub fn isNoop(entry: Entry) bool {
        return entry.kind == .noop;
    }
};

pub const ClusterConfig = struct {
    voters: []const u64,
    voters_outgoing: []const u64,
    is_joint: bool,
};

pub const SnapshotMetadata = struct {
    index: u64,
    term: u64,
    config: ClusterConfig,
};

pub const Snapshot = struct {
    metadata: SnapshotMetadata,
    data: []const u8,
};

pub const RequestVote = struct {
    term: u64,
    candidate_id: u64,
    last_log_index: u64,
    last_log_term: u64,
    pre_vote: bool,
};

pub const RequestVoteResponse = struct {
    term: u64,
    vote_granted: bool,
    pre_vote: bool,
};

pub const AppendEntries = struct {
    term: u64,
    leader_id: u64,
    prev_log_index: u64,
    prev_log_term: u64,
    entries: []const Entry,
    leader_commit: u64,
};

pub const AppendEntriesResponse = struct {
    term: u64,
    success: bool,
    match_index: u64,
    conflict_index: u64,
    conflict_term: u64,
};

pub const InstallSnapshot = struct {
    term: u64,
    leader_id: u64,
    last_included_index: u64,
    last_included_term: u64,
    offset: u64,
    data: []const u8,
    done: bool,
    /// BLAKE3 digest of the full snapshot data (set on final chunk; zero on intermediate chunks).
    checksum: [32]u8,
    /// Cluster configuration at the time of the snapshot (SEC-0030).
    /// Carried on every chunk so the receiver can restore membership state.
    config: ClusterConfig,
};

pub const InstallSnapshotResponse = struct {
    term: u64,
    success: bool,
};

pub const Message = union(enum) {
    request_vote: RequestVote,
    request_vote_response: RequestVoteResponse,
    append_entries: AppendEntries,
    append_entries_response: AppendEntriesResponse,
    install_snapshot: InstallSnapshot,
    install_snapshot_response: InstallSnapshotResponse,
};

test "isNoop returns true for noop entries" {
    const noop = Entry{ .index = 1, .term = 1, .kind = .noop, .data = &.{} };
    try std.testing.expect(noop.isNoop());

    const normal = Entry{ .index = 2, .term = 1, .kind = .normal, .data = "hello" };
    try std.testing.expect(!normal.isNoop());

    const config = Entry{ .index = 3, .term = 1, .kind = .config, .data = "cfg" };
    try std.testing.expect(!config.isNoop());
}

test "Message tagged union field access" {
    const rv = Message{ .request_vote = .{ .term = 5, .candidate_id = 1, .last_log_index = 10, .last_log_term = 4, .pre_vote = false } };
    try std.testing.expectEqual(@as(u64, 5), rv.request_vote.term);

    const rvr = Message{ .request_vote_response = .{ .term = 5, .vote_granted = true, .pre_vote = false } };
    try std.testing.expect(rvr.request_vote_response.vote_granted);

    const ae = Message{ .append_entries = .{ .term = 3, .leader_id = 1, .prev_log_index = 0, .prev_log_term = 0, .entries = &.{}, .leader_commit = 0 } };
    try std.testing.expectEqual(@as(u64, 1), ae.append_entries.leader_id);

    const aer = Message{ .append_entries_response = .{ .term = 3, .success = true, .match_index = 5, .conflict_index = 0, .conflict_term = 0 } };
    try std.testing.expect(aer.append_entries_response.success);

    const is = Message{ .install_snapshot = .{ .term = 2, .leader_id = 1, .last_included_index = 10, .last_included_term = 2, .offset = 0, .data = "snap", .done = true, .checksum = .{0} ** 32, .config = .{ .voters = &.{}, .voters_outgoing = &.{}, .is_joint = false } } };
    try std.testing.expect(is.install_snapshot.done);

    const isr = Message{ .install_snapshot_response = .{ .term = 2, .success = true } };
    try std.testing.expect(isr.install_snapshot_response.success);
}

test "ClusterConfig joint consensus" {
    const voters = [_]u64{ 1, 2, 3 };
    const single = ClusterConfig{ .voters = &voters, .voters_outgoing = &.{}, .is_joint = false };
    try std.testing.expect(!single.is_joint);

    const outgoing = [_]u64{ 4, 5 };
    const joint = ClusterConfig{ .voters = &voters, .voters_outgoing = &outgoing, .is_joint = true };
    try std.testing.expect(joint.is_joint);
    try std.testing.expectEqual(@as(usize, 2), joint.voters_outgoing.len);
}

test "Entry construction with empty data for noop" {
    const entry = Entry{ .index = 1, .term = 1, .kind = .noop, .data = &.{} };
    try std.testing.expectEqual(@as(usize, 0), entry.data.len);
    try std.testing.expect(entry.isNoop());
    try std.testing.expectEqual(@as(u64, 1), entry.index);
    try std.testing.expectEqual(@as(u64, 1), entry.term);
}
