// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-2025 CodePros

//! # raft
//!
//! A generalized, standalone Raft consensus library. Written in Zig 0.15.1+.
//! Implements the full Raft paper (Ongaro & Ousterhout, 2014) plus pre-vote,
//! joint consensus membership changes, snapshotting, and chunked snapshot transfer.

const std = @import("std");
pub const spice = @import("spice");

// All modules
pub const state = @import("state.zig");
pub const rpc = @import("rpc.zig");
pub const quorum = @import("quorum.zig");
pub const storage = @import("storage.zig");
pub const file_storage = @import("file_storage.zig");
pub const transport = @import("transport.zig");
pub const log = @import("log.zig");
pub const election = @import("election.zig");
pub const replication = @import("replication.zig");
pub const membership = @import("membership.zig");
pub const snapshot_mod = @import("snapshot.zig");
pub const ready = @import("ready.zig");
pub const node = @import("node.zig");

// Top-level convenience re-exports
pub const RaftNode = node.RaftNode;
pub const Config = node.Config;
pub const Ready = ready.Ready;
pub const Role = state.Role;
pub const HardState = state.HardState;
pub const SoftState = state.SoftState;
pub const Entry = rpc.Entry;
pub const EntryKind = rpc.EntryKind;
pub const Message = rpc.Message;
pub const Snapshot = rpc.Snapshot;
pub const SnapshotMetadata = rpc.SnapshotMetadata;
pub const ClusterConfig = rpc.ClusterConfig;
pub const Storage = storage.Storage;
pub const InMemoryStorage = storage.InMemoryStorage;
pub const FileStorage = file_storage.FileStorage;
pub const Transport = transport.Transport;
pub const InProcessTransport = transport.InProcessTransport;
pub const isNoop = rpc.Entry.isNoop;

test {
    std.testing.refAllDecls(@This());
}
