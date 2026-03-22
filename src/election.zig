// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-2025 CodePros

const std = @import("std");
const state = @import("state.zig");

const Allocator = std.mem.Allocator;

pub const VoteTally = struct {
    granted: usize,
    rejected: usize,
};

pub const ElectionState = struct {
    election_tick: u32,
    heartbeat_tick: u32,
    election_timeout: u32,
    heartbeat_elapsed: u32,
    election_elapsed: u32,
    votes_received: std.AutoHashMap(u64, bool),
    prng: std.Random.DefaultPrng,
    use_crypto_random: bool,
    allocator: Allocator,

    pub fn init(allocator: Allocator, election_tick: u32, heartbeat_tick: u32, rng_seed: ?u64) ElectionState {
        const use_crypto = rng_seed == null;
        const prng = std.Random.DefaultPrng.init(rng_seed orelse 0);

        return .{
            .election_tick = election_tick,
            .heartbeat_tick = heartbeat_tick,
            .election_timeout = election_tick,
            .heartbeat_elapsed = 0,
            .election_elapsed = 0,
            .votes_received = std.AutoHashMap(u64, bool).init(allocator),
            .prng = prng,
            .use_crypto_random = use_crypto,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *ElectionState) void {
        self.votes_received.deinit();
    }

    pub fn resetElectionTimeout(self: *ElectionState) void {
        const lower = self.election_tick;
        const upper = std.math.mul(u32, 2, self.election_tick) catch std.math.maxInt(u32);

        if (self.use_crypto_random) {
            self.election_timeout = std.crypto.random.intRangeLessThan(u32, lower, upper);
        } else {
            self.election_timeout = self.prng.random().intRangeLessThan(u32, lower, upper);
        }
    }

    pub fn electionTimeoutElapsed(self: *const ElectionState) bool {
        return self.election_elapsed >= self.election_timeout;
    }

    pub fn heartbeatTimeoutElapsed(self: *const ElectionState) bool {
        return self.heartbeat_elapsed >= self.heartbeat_tick;
    }

    pub fn tickElection(self: *ElectionState) void {
        // Saturating add prevents u32 wraparound from resetting the
        // election timer after ~4.3 billion ticks (SEC-0106).
        self.election_elapsed = self.election_elapsed +| 1;
    }

    pub fn tickHeartbeat(self: *ElectionState) void {
        // Saturating add for consistency with tickElection (SEC-0106).
        self.heartbeat_elapsed = self.heartbeat_elapsed +| 1;
    }

    pub fn resetElectionElapsed(self: *ElectionState) void {
        self.election_elapsed = 0;
    }

    pub fn resetHeartbeatElapsed(self: *ElectionState) void {
        self.heartbeat_elapsed = 0;
    }

    pub fn recordVote(self: *ElectionState, from: u64, granted: bool) !void {
        try self.votes_received.put(from, granted);
    }

    pub fn tallyVotes(self: *const ElectionState, voters: []const u64) VoteTally {
        var granted: usize = 0;
        var rejected: usize = 0;

        for (voters) |voter_id| {
            if (self.votes_received.get(voter_id)) |vote_granted| {
                if (vote_granted) {
                    granted += 1;
                } else {
                    rejected += 1;
                }
            }
        }

        return .{ .granted = granted, .rejected = rejected };
    }

    pub fn hasQuorum(self: *const ElectionState, voters: []const u64) bool {
        const tally = self.tallyVotes(voters);
        return tally.granted > voters.len / 2;
    }

    pub fn hasJointQuorum(self: *const ElectionState, voters: []const u64, voters_outgoing: []const u64) bool {
        if (voters_outgoing.len == 0) {
            return self.hasQuorum(voters);
        }

        return self.hasQuorum(voters) and self.hasQuorum(voters_outgoing);
    }

    pub fn resetVotes(self: *ElectionState) void {
        self.votes_received.clearAndFree();
    }
};

pub fn shouldGrantVote(
    voted_for: ?u64,
    candidate_id: u64,
    candidate_last_index: u64,
    candidate_last_term: u64,
    local_last_index: u64,
    local_last_term: u64,
    pre_vote: bool,
) bool {
    // Check voted_for constraint (skipped for pre-vote).
    if (!pre_vote) {
        if (voted_for) |vf| {
            if (vf != candidate_id) {
                return false;
            }
        }
    }

    // Candidate's log must be at least as up-to-date as the local log.
    if (candidate_last_term > local_last_term) {
        return true;
    }
    if (candidate_last_term == local_last_term and candidate_last_index >= local_last_index) {
        return true;
    }

    return false;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "timeout randomization is within range" {
    const allocator = std.testing.allocator;
    const election_tick: u32 = 10;

    var es = ElectionState.init(allocator, election_tick, 3, @as(u64, 42));
    defer es.deinit();

    var i: usize = 0;
    while (i < 50) : (i += 1) {
        es.resetElectionTimeout();
        try std.testing.expect(es.election_timeout >= election_tick);
        try std.testing.expect(es.election_timeout < 2 * election_tick);
    }
}

test "election timeout elapsed detection" {
    const allocator = std.testing.allocator;

    var es = ElectionState.init(allocator, 5, 2, @as(u64, 7));
    defer es.deinit();

    // election_timeout starts at election_tick (5).
    try std.testing.expect(!es.electionTimeoutElapsed());

    // Tick up to the timeout.
    var i: u32 = 0;
    while (i < 5) : (i += 1) {
        es.tickElection();
    }
    try std.testing.expect(es.electionTimeoutElapsed());

    // Reset should clear elapsed.
    es.resetElectionElapsed();
    try std.testing.expect(!es.electionTimeoutElapsed());
}

test "heartbeat timeout elapsed detection" {
    const allocator = std.testing.allocator;

    var es = ElectionState.init(allocator, 10, 3, @as(u64, 0));
    defer es.deinit();

    try std.testing.expect(!es.heartbeatTimeoutElapsed());

    es.tickHeartbeat();
    es.tickHeartbeat();
    es.tickHeartbeat();
    try std.testing.expect(es.heartbeatTimeoutElapsed());

    es.resetHeartbeatElapsed();
    try std.testing.expect(!es.heartbeatTimeoutElapsed());
}

test "vote recording and tally" {
    const allocator = std.testing.allocator;

    var es = ElectionState.init(allocator, 10, 3, @as(u64, 0));
    defer es.deinit();

    try es.recordVote(1, true);
    try es.recordVote(2, false);
    try es.recordVote(3, true);

    const voters = [_]u64{ 1, 2, 3 };
    const tally = es.tallyVotes(&voters);
    try std.testing.expectEqual(@as(usize, 2), tally.granted);
    try std.testing.expectEqual(@as(usize, 1), tally.rejected);

    // Only count voters in the voter set.
    const partial_voters = [_]u64{ 1, 2 };
    const partial_tally = es.tallyVotes(&partial_voters);
    try std.testing.expectEqual(@as(usize, 1), partial_tally.granted);
    try std.testing.expectEqual(@as(usize, 1), partial_tally.rejected);
}

test "hasQuorum majority counting" {
    const allocator = std.testing.allocator;

    var es = ElectionState.init(allocator, 10, 3, @as(u64, 0));
    defer es.deinit();

    const voters = [_]u64{ 1, 2, 3, 4, 5 };

    // 2 of 5 is not a majority.
    try es.recordVote(1, true);
    try es.recordVote(2, true);
    try std.testing.expect(!es.hasQuorum(&voters));

    // 3 of 5 is a majority.
    try es.recordVote(3, true);
    try std.testing.expect(es.hasQuorum(&voters));
}

test "hasJointQuorum both sets independently" {
    const allocator = std.testing.allocator;

    var es = ElectionState.init(allocator, 10, 3, @as(u64, 0));
    defer es.deinit();

    const voters_new = [_]u64{ 1, 2, 3 };
    const voters_old = [_]u64{ 4, 5, 6 };

    // Majority in new set only.
    try es.recordVote(1, true);
    try es.recordVote(2, true);
    try std.testing.expect(!es.hasJointQuorum(&voters_new, &voters_old));

    // Majority in both sets.
    try es.recordVote(4, true);
    try es.recordVote(5, true);
    try std.testing.expect(es.hasJointQuorum(&voters_new, &voters_old));

    // Empty outgoing falls back to simple majority.
    es.resetVotes();
    try es.recordVote(1, true);
    try es.recordVote(2, true);
    const empty = [_]u64{};
    try std.testing.expect(es.hasJointQuorum(&voters_new, &empty));
}

test "shouldGrantVote grants when log is up-to-date and not voted" {
    // Candidate has higher term — should grant.
    try std.testing.expect(shouldGrantVote(null, 2, 5, 3, 5, 2, false));

    // Candidate has same term, equal index — should grant.
    try std.testing.expect(shouldGrantVote(null, 2, 5, 2, 5, 2, false));

    // Candidate has same term, higher index — should grant.
    try std.testing.expect(shouldGrantVote(null, 2, 6, 2, 5, 2, false));

    // Already voted for same candidate — should grant.
    try std.testing.expect(shouldGrantVote(2, 2, 5, 3, 5, 2, false));
}

test "shouldGrantVote denies when already voted for different candidate" {
    try std.testing.expect(!shouldGrantVote(3, 2, 10, 10, 5, 2, false));
}

test "shouldGrantVote denies when candidate log is stale" {
    // Candidate has lower term.
    try std.testing.expect(!shouldGrantVote(null, 2, 10, 1, 5, 2, false));

    // Candidate has same term but lower index.
    try std.testing.expect(!shouldGrantVote(null, 2, 4, 2, 5, 2, false));
}

test "shouldGrantVote pre_vote skips voted_for check" {
    // Already voted for a different candidate, but pre_vote ignores that.
    try std.testing.expect(shouldGrantVote(3, 2, 5, 3, 5, 2, true));

    // Pre-vote still checks log freshness — stale log is denied.
    try std.testing.expect(!shouldGrantVote(3, 2, 4, 1, 5, 2, true));
}

test "tick counters use saturating addition (SEC-0106)" {
    var es = ElectionState.init(std.testing.allocator, 10, 1, 42);
    defer es.deinit();

    // Set election_elapsed near u32 max to verify saturation.
    es.election_elapsed = std.math.maxInt(u32) - 1;
    es.tickElection();
    try std.testing.expectEqual(std.math.maxInt(u32), es.election_elapsed);
    // One more tick should saturate at max, not wrap to 0.
    es.tickElection();
    try std.testing.expectEqual(std.math.maxInt(u32), es.election_elapsed);

    // Same for heartbeat.
    es.heartbeat_elapsed = std.math.maxInt(u32) - 1;
    es.tickHeartbeat();
    try std.testing.expectEqual(std.math.maxInt(u32), es.heartbeat_elapsed);
    es.tickHeartbeat();
    try std.testing.expectEqual(std.math.maxInt(u32), es.heartbeat_elapsed);
}
