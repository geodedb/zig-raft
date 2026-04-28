//! Verifies the no-op invariant when -Dwith_otel=false (the default).
//!
//! raft publishes its public surface as the "raft" module rooted at
//! src/root.zig. The seam is exposed as `raft.observability` so this test
//! can drive the disabled-mode invariants through the public root.

const std = @import("std");
const raft = @import("raft");

test "build_options.with_otel exists and defaults to false" {
    const build_options = @import("build_options");
    try std.testing.expect(@hasDecl(build_options, "with_otel"));
    // The "default" assertion only holds for the disabled build. When
    // the operator forces -Dwith_otel=true, this test runs against the
    // enabled module and must skip rather than misreport the default.
    if (build_options.with_otel) return error.SkipZigTest;
    try std.testing.expectEqual(false, build_options.with_otel);
}

test "observability.enabled tracks build_options.with_otel" {
    const build_options = @import("build_options");
    try std.testing.expectEqual(build_options.with_otel, raft.observability.enabled);
}

test "generic client/server seams are no-ops when otel disabled" {
    if (raft.observability.enabled) return error.SkipZigTest;
    var span = raft.observability.startClientSpan(.{
        .method = "GET",
        .route = "/",
        .url = "https://example.test/",
    });
    raft.observability.endClientSpan(span, 200);
    // Belt-and-suspenders: the seam's `endClientSpan` already covers
    // the lifecycle; this line just verifies the no-op fallback's
    // signature matches the real API's `Span.end(?u64) void`.
    span.end(null);

    var sspan = raft.observability.startServerSpan(.{
        .method = "GET",
        .route = "/",
        .url = "/",
    });
    raft.observability.endServerSpan(sspan, 200);
    sspan.end(null);
}

test "raft RPC client/server span helpers are no-ops when otel disabled" {
    if (raft.observability.enabled) return error.SkipZigTest;

    // Client-side AppendEntries span.
    var client_span = raft.observability.startRpcClientSpan("AppendEntries", 42);
    raft.observability.endRpcClientSpan(client_span, true, 0);
    client_span.end(null);

    // Server-side RequestVote span.
    var server_span = raft.observability.startRpcServerSpan("RequestVote", 7);
    raft.observability.endRpcServerSpan(server_span, false, -1);
    server_span.end(null);

    // InstallSnapshot client span (longer-running operation).
    var snap_span = raft.observability.startRpcClientSpan("InstallSnapshot", 99);
    raft.observability.endRpcClientSpan(snap_span, true, 0);
    snap_span.end(null);
}
