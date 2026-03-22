// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-2025 CodePros

const std = @import("std");
const rpc = @import("rpc.zig");

pub const MembershipError = error{ConfigChangePending};

/// Check if a node ID is a voter in the current config (either set).
pub fn isVoter(config: rpc.ClusterConfig, id: u64) bool {
    for (config.voters) |v| {
        if (v == id) return true;
    }
    for (config.voters_outgoing) |v| {
        if (v == id) return true;
    }
    return false;
}

/// Manages cluster membership transitions using joint consensus (Raft §6).
pub const MembershipState = struct {
    /// Current voter set (owned, allocated copy).
    voters: std.ArrayList(u64),
    /// Outgoing voter set during joint consensus (owned).
    voters_outgoing: std.ArrayList(u64),
    is_joint: bool,
    pending_config_change: bool,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, initial_voters: []const u64) !MembershipState {
        var voters = std.ArrayList(u64){};
        try voters.appendSlice(allocator, initial_voters);
        return .{
            .voters = voters,
            .voters_outgoing = .{},
            .is_joint = false,
            .pending_config_change = false,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *MembershipState) void {
        self.voters.deinit(self.allocator);
        self.voters_outgoing.deinit(self.allocator);
    }

    /// Get current config as a ClusterConfig (borrows slices from internal state).
    pub fn config(self: *const MembershipState) rpc.ClusterConfig {
        return .{
            .voters = self.voters.items,
            .voters_outgoing = self.voters_outgoing.items,
            .is_joint = self.is_joint,
        };
    }

    /// Start a config change: creates a joint config Entry.
    /// Returns error.ConfigChangePending if one is already in progress.
    pub fn proposeChange(self: *MembershipState, new_voters: []const u64, current_term: u64, index: u64) !rpc.Entry {
        if (new_voters.len == 0) return error.InvalidData;
        if (self.pending_config_change) return MembershipError.ConfigChangePending;

        self.pending_config_change = true;

        // Move current voters to voters_outgoing.
        self.voters_outgoing.deinit(self.allocator);
        self.voters_outgoing = self.voters;

        // Set voters to new_voters (copy).
        self.voters = .{};
        try self.voters.appendSlice(self.allocator, new_voters);

        self.is_joint = true;

        const data = try serializeJoint(self.allocator, new_voters, self.voters_outgoing.items);
        return .{
            .index = index,
            .term = current_term,
            .kind = .config,
            .data = data,
        };
    }

    /// After joint config commits, produce the phase-2 (new-only) config entry.
    /// Returns null if not in joint consensus.
    /// Propagates serialization errors (including OOM) instead of swallowing (SEC-0034).
    pub fn finalizeChange(self: *MembershipState, current_term: u64, index: u64) !?rpc.Entry {
        if (!self.is_joint) return null;

        self.voters_outgoing.clearAndFree(self.allocator);
        self.is_joint = false;
        self.pending_config_change = false;

        const data = try serializeFinal(self.allocator, self.voters.items);
        return .{
            .index = index,
            .term = current_term,
            .kind = .config,
            .data = data,
        };
    }

    /// Apply a committed config entry (update voter sets from entry data).
    pub fn applyConfig(self: *MembershipState, entry: rpc.Entry) !void {
        var parsed = try deserializeVoters(self.allocator, entry.data);
        defer parsed.deinit();

        self.voters.clearAndFree(self.allocator);
        try self.voters.appendSlice(self.allocator, parsed.voters);

        self.voters_outgoing.clearAndFree(self.allocator);
        if (parsed.is_joint) {
            try self.voters_outgoing.appendSlice(self.allocator, parsed.voters_outgoing);
        }

        self.is_joint = parsed.is_joint;
        self.pending_config_change = parsed.is_joint;
    }

    /// Check if leader should step down (leader ID not in current voters).
    /// Leader only steps down after the final non-joint config commits.
    pub fn shouldLeaderStepDown(self: *const MembershipState, leader_id: u64) bool {
        if (self.is_joint) return false;
        for (self.voters.items) |v| {
            if (v == leader_id) return false;
        }
        return true;
    }

    /// Serialize a joint config: [0x01][new_count: u32 LE][new u64s...][old_count: u32 LE][old u64s...]
    fn serializeJoint(allocator: std.mem.Allocator, new_voters: []const u64, old_voters: []const u64) ![]u8 {
        if (new_voters.len > std.math.maxInt(u32) or old_voters.len > std.math.maxInt(u32))
            return error.InvalidData;
        const new_size = std.math.mul(usize, new_voters.len, 8) catch return error.InvalidData;
        const old_size = std.math.mul(usize, old_voters.len, 8) catch return error.InvalidData;
        const size = 1 + 4 + new_size + 4 + old_size;
        const buf = try allocator.alloc(u8, size);
        var offset: usize = 0;

        buf[offset] = 0x01;
        offset += 1;

        std.mem.writeInt(u32, buf[offset..][0..4], @intCast(new_voters.len), .little);
        offset += 4;
        for (new_voters) |v| {
            std.mem.writeInt(u64, buf[offset..][0..8], v, .little);
            offset += 8;
        }

        std.mem.writeInt(u32, buf[offset..][0..4], @intCast(old_voters.len), .little);
        offset += 4;
        for (old_voters) |v| {
            std.mem.writeInt(u64, buf[offset..][0..8], v, .little);
            offset += 8;
        }

        return buf;
    }

    /// Serialize a final config: [0x00][count: u32 LE][voter u64s...]
    fn serializeFinal(allocator: std.mem.Allocator, voters: []const u64) ![]u8 {
        if (voters.len > std.math.maxInt(u32))
            return error.InvalidData;
        const voters_size = std.math.mul(usize, voters.len, 8) catch return error.InvalidData;
        const size = 1 + 4 + voters_size;
        const buf = try allocator.alloc(u8, size);
        var offset: usize = 0;

        buf[offset] = 0x00;
        offset += 1;

        std.mem.writeInt(u32, buf[offset..][0..4], @intCast(voters.len), .little);
        offset += 4;
        for (voters) |v| {
            std.mem.writeInt(u64, buf[offset..][0..8], v, .little);
            offset += 8;
        }

        return buf;
    }

    const DeserializedConfig = struct {
        voters: []u64,
        voters_outgoing: []u64,
        is_joint: bool,
        allocator: std.mem.Allocator,

        pub fn deinit(self: *DeserializedConfig) void {
            if (self.voters.len > 0) self.allocator.free(self.voters);
            if (self.voters_outgoing.len > 0) self.allocator.free(self.voters_outgoing);
        }
    };

    /// Deserialize a config entry produced by serializeJoint or serializeFinal.
    fn deserializeVoters(allocator: std.mem.Allocator, data: []const u8) !DeserializedConfig {
        if (data.len < 1) return error.InvalidData;

        const is_joint = data[0] == 0x01;
        var offset: usize = 1;

        if (data.len < offset + 4) return error.InvalidData;
        const new_count: usize = @intCast(std.mem.readInt(u32, data[offset..][0..4], .little));
        offset += 4;

        const new_bytes = std.math.mul(usize, new_count, 8) catch return error.InvalidData;
        if (data.len < offset + new_bytes) return error.InvalidData;
        const new_voters = try readU64Slice(allocator, data[offset..], new_count);
        errdefer if (new_voters.len > 0) allocator.free(new_voters);
        offset += new_bytes;

        if (is_joint) {
            if (data.len < offset + 4) return error.InvalidData;
            const old_count: usize = @intCast(std.mem.readInt(u32, data[offset..][0..4], .little));
            offset += 4;

            const old_bytes = std.math.mul(usize, old_count, 8) catch return error.InvalidData;
            if (data.len < offset + old_bytes) return error.InvalidData;
            const old_voters = try readU64Slice(allocator, data[offset..], old_count);

            return .{ .voters = new_voters, .voters_outgoing = old_voters, .is_joint = true, .allocator = allocator };
        }

        return .{ .voters = new_voters, .voters_outgoing = &.{}, .is_joint = false, .allocator = allocator };
    }

    /// Read `count` u64 values from a byte slice using readInt for alignment safety.
    fn readU64Slice(allocator: std.mem.Allocator, data: []const u8, count: usize) ![]u64 {
        if (count == 0) return &.{};
        const result = try allocator.alloc(u64, count);
        for (0..count) |i| {
            result[i] = std.mem.readInt(u64, data[i * 8 ..][0..8], .little);
        }
        return result;
    }

    pub fn serializeVoters(allocator: std.mem.Allocator, voters: []const u64) ![]u8 {
        return serializeFinal(allocator, voters);
    }

    /// Serialize a ClusterConfig to bytes. Uses the same format as proposeChange/finalizeChange:
    /// joint: [0x01][new_count: u32 LE][new u64s...][old_count: u32 LE][old u64s...]
    /// final: [0x00][count: u32 LE][voter u64s...]
    /// Caller must free the returned slice.
    pub fn serializeConfig(allocator: std.mem.Allocator, cfg: rpc.ClusterConfig) ![]u8 {
        if (cfg.is_joint) {
            return serializeJoint(allocator, cfg.voters, cfg.voters_outgoing);
        } else {
            return serializeFinal(allocator, cfg.voters);
        }
    }

    /// Deserialize a ClusterConfig from bytes produced by serializeConfig.
    /// Returns owned slices for voters/voters_outgoing that must be freed by the caller.
    pub fn deserializeConfig(allocator: std.mem.Allocator, data: []const u8) !struct {
        config: rpc.ClusterConfig,
        voters_buf: []u64,
        voters_outgoing_buf: []u64,
    } {
        const parsed = try deserializeVoters(allocator, data);
        // Don't defer deinit — caller takes ownership of the buffers.
        return .{
            .config = .{
                .voters = parsed.voters,
                .voters_outgoing = parsed.voters_outgoing,
                .is_joint = parsed.is_joint,
            },
            .voters_buf = parsed.voters,
            .voters_outgoing_buf = parsed.voters_outgoing,
        };
    }
};

// =============================================================================
// Tests
// =============================================================================

test "init — voters set correctly, not joint" {
    const allocator = std.testing.allocator;
    const initial = [_]u64{ 1, 2, 3 };
    var ms = try MembershipState.init(allocator, &initial);
    defer ms.deinit();

    try std.testing.expectEqual(@as(usize, 3), ms.voters.items.len);
    try std.testing.expectEqual(@as(u64, 1), ms.voters.items[0]);
    try std.testing.expectEqual(@as(u64, 2), ms.voters.items[1]);
    try std.testing.expectEqual(@as(u64, 3), ms.voters.items[2]);
    try std.testing.expect(!ms.is_joint);
    try std.testing.expect(!ms.pending_config_change);
}

test "proposeChange — creates joint config entry" {
    const allocator = std.testing.allocator;
    const initial = [_]u64{ 1, 2, 3 };
    var ms = try MembershipState.init(allocator, &initial);
    defer ms.deinit();

    const new_voters = [_]u64{ 2, 3, 4 };
    const entry = try ms.proposeChange(&new_voters, 5, 10);
    defer allocator.free(entry.data);

    try std.testing.expectEqual(rpc.EntryKind.config, entry.kind);
    try std.testing.expectEqual(@as(u64, 5), entry.term);
    try std.testing.expectEqual(@as(u64, 10), entry.index);
    try std.testing.expect(ms.is_joint);
    try std.testing.expect(ms.pending_config_change);
    try std.testing.expectEqual(@as(usize, 3), ms.voters.items.len);
    try std.testing.expectEqual(@as(usize, 3), ms.voters_outgoing.items.len);
}

test "proposeChange — returns error when change already pending" {
    const allocator = std.testing.allocator;
    const initial = [_]u64{ 1, 2, 3 };
    var ms = try MembershipState.init(allocator, &initial);
    defer ms.deinit();

    const new_voters = [_]u64{ 2, 3, 4 };
    const entry = try ms.proposeChange(&new_voters, 5, 10);
    defer allocator.free(entry.data);

    const second = [_]u64{ 5, 6 };
    const result = ms.proposeChange(&second, 5, 11);
    try std.testing.expectError(MembershipError.ConfigChangePending, result);
}

test "finalizeChange — produces final config entry, clears outgoing" {
    const allocator = std.testing.allocator;
    const initial = [_]u64{ 1, 2, 3 };
    var ms = try MembershipState.init(allocator, &initial);
    defer ms.deinit();

    const new_voters = [_]u64{ 2, 3, 4 };
    const joint_entry = try ms.proposeChange(&new_voters, 5, 10);
    defer allocator.free(joint_entry.data);

    const final_entry = (try ms.finalizeChange(5, 11)).?;
    defer allocator.free(final_entry.data);

    try std.testing.expectEqual(rpc.EntryKind.config, final_entry.kind);
    try std.testing.expectEqual(@as(u64, 11), final_entry.index);
    try std.testing.expect(!ms.is_joint);
    try std.testing.expect(!ms.pending_config_change);
    try std.testing.expectEqual(@as(usize, 0), ms.voters_outgoing.items.len);
    try std.testing.expectEqual(@as(usize, 3), ms.voters.items.len);
}

test "finalizeChange — returns null when not in joint consensus" {
    const allocator = std.testing.allocator;
    const initial = [_]u64{ 1, 2, 3 };
    var ms = try MembershipState.init(allocator, &initial);
    defer ms.deinit();

    try std.testing.expect((try ms.finalizeChange(1, 1)) == null);
}

test "shouldLeaderStepDown — true when leader not in new voters after finalize" {
    const allocator = std.testing.allocator;
    const initial = [_]u64{ 1, 2, 3 };
    var ms = try MembershipState.init(allocator, &initial);
    defer ms.deinit();

    const new_voters = [_]u64{ 2, 3, 4 };
    const joint_entry = try ms.proposeChange(&new_voters, 5, 10);
    defer allocator.free(joint_entry.data);

    const final_entry = (try ms.finalizeChange(5, 11)).?;
    defer allocator.free(final_entry.data);

    // Leader 1 is not in new voters {2,3,4}, should step down.
    try std.testing.expect(ms.shouldLeaderStepDown(1));
    // Leader 2 is in new voters, should not step down.
    try std.testing.expect(!ms.shouldLeaderStepDown(2));
}

test "shouldLeaderStepDown — false during joint consensus" {
    const allocator = std.testing.allocator;
    const initial = [_]u64{ 1, 2, 3 };
    var ms = try MembershipState.init(allocator, &initial);
    defer ms.deinit();

    const new_voters = [_]u64{ 2, 3, 4 };
    const entry = try ms.proposeChange(&new_voters, 5, 10);
    defer allocator.free(entry.data);

    // During joint consensus, leader should NOT step down even if not in new set.
    try std.testing.expect(!ms.shouldLeaderStepDown(1));
}

test "config — returns correct ClusterConfig" {
    const allocator = std.testing.allocator;
    const initial = [_]u64{ 1, 2, 3 };
    var ms = try MembershipState.init(allocator, &initial);
    defer ms.deinit();

    const cfg = ms.config();
    try std.testing.expectEqual(@as(usize, 3), cfg.voters.len);
    try std.testing.expectEqual(@as(usize, 0), cfg.voters_outgoing.len);
    try std.testing.expect(!cfg.is_joint);

    // After proposing a change, config should reflect joint state.
    const new_voters = [_]u64{ 4, 5 };
    const entry = try ms.proposeChange(&new_voters, 1, 1);
    defer allocator.free(entry.data);

    const joint_cfg = ms.config();
    try std.testing.expectEqual(@as(usize, 2), joint_cfg.voters.len);
    try std.testing.expectEqual(@as(usize, 3), joint_cfg.voters_outgoing.len);
    try std.testing.expect(joint_cfg.is_joint);
}

test "serialization round-trip — serialize then deserialize voters" {
    const allocator = std.testing.allocator;

    // Test final config round-trip.
    const voters = [_]u64{ 10, 20, 30 };
    const final_data = try MembershipState.serializeFinal(allocator, &voters);
    defer allocator.free(final_data);

    var final_parsed = try MembershipState.deserializeVoters(allocator, final_data);
    defer final_parsed.deinit();
    try std.testing.expect(!final_parsed.is_joint);
    try std.testing.expectEqual(@as(usize, 3), final_parsed.voters.len);
    try std.testing.expectEqual(@as(u64, 10), final_parsed.voters[0]);
    try std.testing.expectEqual(@as(u64, 20), final_parsed.voters[1]);
    try std.testing.expectEqual(@as(u64, 30), final_parsed.voters[2]);

    // Test joint config round-trip.
    const old_voters = [_]u64{ 1, 2 };
    const joint_data = try MembershipState.serializeJoint(allocator, &voters, &old_voters);
    defer allocator.free(joint_data);

    var joint_parsed = try MembershipState.deserializeVoters(allocator, joint_data);
    defer joint_parsed.deinit();
    try std.testing.expect(joint_parsed.is_joint);
    try std.testing.expectEqual(@as(usize, 3), joint_parsed.voters.len);
    try std.testing.expectEqual(@as(usize, 2), joint_parsed.voters_outgoing.len);
    try std.testing.expectEqual(@as(u64, 1), joint_parsed.voters_outgoing[0]);
    try std.testing.expectEqual(@as(u64, 2), joint_parsed.voters_outgoing[1]);
}

test "isVoter — checks both voter sets" {
    const voters = [_]u64{ 1, 2, 3 };
    const outgoing = [_]u64{ 4, 5 };
    const cfg = rpc.ClusterConfig{ .voters = &voters, .voters_outgoing = &outgoing, .is_joint = true };

    try std.testing.expect(isVoter(cfg, 1));
    try std.testing.expect(isVoter(cfg, 4));
    try std.testing.expect(!isVoter(cfg, 6));
}
