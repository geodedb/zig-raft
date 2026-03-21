// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-2025 CodePros

const std = @import("std");

pub fn hasMajority(voters: []const u64, votes: []const u64) bool {
    const count = countVotes(voters, votes);
    return count > voters.len / 2;
}

pub fn hasJointMajority(
    voters: []const u64,
    voters_outgoing: []const u64,
    votes: []const u64,
) bool {
    if (voters_outgoing.len == 0) return hasMajority(voters, votes);
    return hasMajority(voters, votes) and hasMajority(voters_outgoing, votes);
}

pub fn committedIndex(
    voters: []const u64,
    voters_outgoing: []const u64,
    match_indices: []const u64,
    node_ids: []const u64,
    current_commit: u64,
) u64 {
    const idx = committedForSet(voters, match_indices, node_ids);
    if (voters_outgoing.len == 0) return @max(idx, current_commit);
    const idx_out = committedForSet(voters_outgoing, match_indices, node_ids);
    return @max(@min(idx, idx_out), current_commit);
}

fn committedForSet(voters: []const u64, match_indices: []const u64, node_ids: []const u64) u64 {
    if (voters.len == 0) return 0;
    var buf: [16]u64 = undefined;
    var n: usize = 0;
    for (voters) |v| {
        for (node_ids, 0..) |nid, i| {
            if (nid == v) {
                buf[n] = match_indices[i];
                n += 1;
                break;
            }
        }
    }
    if (n == 0) return 0;
    std.mem.sort(u64, buf[0..n], {}, std.sort.desc(u64));
    return buf[n / 2];
}

fn countVotes(voters: []const u64, votes: []const u64) usize {
    var count: usize = 0;
    for (votes) |v| {
        for (voters) |voter| {
            if (v == voter) {
                count += 1;
                break;
            }
        }
    }
    return count;
}

test "hasMajority single node" {
    try std.testing.expect(hasMajority(&.{1}, &.{1}));
    try std.testing.expect(!hasMajority(&.{1}, &.{}));
}

test "hasMajority three nodes" {
    try std.testing.expect(hasMajority(&.{ 1, 2, 3 }, &.{ 1, 2 }));
    try std.testing.expect(hasMajority(&.{ 1, 2, 3 }, &.{ 1, 2, 3 }));
    try std.testing.expect(!hasMajority(&.{ 1, 2, 3 }, &.{1}));
}

test "hasMajority five nodes" {
    try std.testing.expect(hasMajority(&.{ 1, 2, 3, 4, 5 }, &.{ 1, 2, 3 }));
    try std.testing.expect(!hasMajority(&.{ 1, 2, 3, 4, 5 }, &.{ 1, 2 }));
}

test "hasMajority non-voter votes ignored" {
    try std.testing.expect(!hasMajority(&.{ 1, 2, 3 }, &.{ 1, 99, 100 }));
}

test "hasJointMajority both sets required" {
    try std.testing.expect(hasJointMajority(&.{ 1, 2, 3 }, &.{ 4, 5, 6 }, &.{ 1, 2, 4, 5 }));
    try std.testing.expect(!hasJointMajority(&.{ 1, 2, 3 }, &.{ 4, 5, 6 }, &.{ 1, 2, 4 }));
}

test "hasJointMajority empty outgoing" {
    try std.testing.expect(hasJointMajority(&.{ 1, 2, 3 }, &.{}, &.{ 1, 2 }));
    try std.testing.expect(!hasJointMajority(&.{ 1, 2, 3 }, &.{}, &.{1}));
}

test "committedIndex basic advancement" {
    const idx = committedIndex(&.{ 1, 2, 3 }, &.{}, &.{ 5, 3, 4 }, &.{ 1, 2, 3 }, 0);
    try std.testing.expectEqual(@as(u64, 4), idx);
}

test "committedIndex never goes backward" {
    const idx = committedIndex(&.{ 1, 2, 3 }, &.{}, &.{ 2, 2, 2 }, &.{ 1, 2, 3 }, 5);
    try std.testing.expectEqual(@as(u64, 5), idx);
}

test "committedIndex joint quorum takes minimum" {
    const idx = committedIndex(
        &.{ 1, 2, 3 },
        &.{ 4, 5, 6 },
        &.{ 10, 10, 10, 5, 5, 5 },
        &.{ 1, 2, 3, 4, 5, 6 },
        0,
    );
    try std.testing.expectEqual(@as(u64, 5), idx);
}
