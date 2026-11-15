// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-2025 CodePros

//! # raft
//!
//! A generalized, standalone Raft consensus library. Written in Zig 0.15.1+.
//! Implements the full Raft paper (Ongaro & Ousterhout, 2014) plus pre-vote,
//! joint consensus membership changes, snapshotting, and chunked snapshot transfer.

const std = @import("std");
pub const thread_pool = @import("thread_pool");

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

// Observability seam (otel). Surfaced via the public root so consumers
// (tests, downstream apps) can import as `raft.observability`. The seam
// itself is the named module declared in build.zig — re-exporting it
// here keeps a single source of truth (no duplicate
// `@import("observability.zig")` from multiple modules, which would
// trip Zig's "file exists in modules X and Y" error).
pub const observability = @import("observability");

/// Bootstrap helper: builds OTEL TracerProvider/MeterProvider/LoggerProvider
/// from OTEL_* env vars and publishes Tracer/Meter handles to
/// `observability.global`. Application entry-point code calls
/// `raft.observability_init.Otel.init` then `installGlobals()` to publish
/// providers; when `-Dwith_otel=false` this collapses to an empty
/// struct so the import never resolves the otel namespace.
pub const observability_init = if (observability.enabled)
    @import("observability_init")
else
    struct {};

// Top-level convenience re-exports
pub const RaftNode = node.RaftNode;
pub const Config = node.Config;
pub const Stats = node.Stats;
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
pub const freeSnapshot = storage.freeSnapshot;
pub const freeEntries = rpc.freeEntries;
pub const dupeEntries = rpc.dupeEntries;
pub const FileStorage = file_storage.FileStorage;
pub const Transport = transport.Transport;
pub const InProcessTransport = transport.InProcessTransport;
pub const isNoop = rpc.Entry.isNoop;

test {
    std.testing.refAllDecls(@This());
    _ = @import("sonny6592_test.zig");
}
