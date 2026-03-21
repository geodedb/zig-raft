// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-2025 CodePros

const std = @import("std");

pub const Role = enum {
    follower,
    candidate,
    leader,
};

pub const HardState = struct {
    current_term: u64,
    voted_for: ?u64,

    pub const initial = HardState{ .current_term = 0, .voted_for = null };

    pub fn eql(a: HardState, b: HardState) bool {
        return a.current_term == b.current_term and a.voted_for == b.voted_for;
    }
};

pub const SoftState = struct {
    role: Role,
    leader_id: ?u64,
};

test "HardState.eql equal" {
    const a = HardState{ .current_term = 1, .voted_for = 2 };
    const b = HardState{ .current_term = 1, .voted_for = 2 };
    try std.testing.expect(a.eql(b));
}

test "HardState.eql not equal term" {
    const a = HardState{ .current_term = 1, .voted_for = 2 };
    const b = HardState{ .current_term = 2, .voted_for = 2 };
    try std.testing.expect(!a.eql(b));
}

test "HardState.eql not equal voted_for" {
    const a = HardState{ .current_term = 1, .voted_for = 2 };
    const b = HardState{ .current_term = 1, .voted_for = null };
    try std.testing.expect(!a.eql(b));
}

test "HardState.initial defaults" {
    try std.testing.expectEqual(@as(u64, 0), HardState.initial.current_term);
    try std.testing.expectEqual(@as(?u64, null), HardState.initial.voted_for);
}

test "SoftState construction" {
    const s = SoftState{ .role = .follower, .leader_id = 3 };
    try std.testing.expectEqual(Role.follower, s.role);
    try std.testing.expectEqual(@as(?u64, 3), s.leader_id);

    const s2 = SoftState{ .role = .leader, .leader_id = null };
    try std.testing.expectEqual(Role.leader, s2.role);
    try std.testing.expectEqual(@as(?u64, null), s2.leader_id);
}
