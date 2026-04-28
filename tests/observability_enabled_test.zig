//! Verifies one Raft RPC client span lands in an in-memory recorder
//! when `-Dwith_otel=true`. Skips under the disabled build.
//!
//! NOTE: deviation from template — we exercise the seam directly via
//! the in-memory recorder rather than driving a full multi-node Raft
//! cluster. Multi-node integration is already covered by
//! `tests/integration.zig` and the chaos suite; this smoke test's job
//! is to prove the seam emits a span with the documented Raft span
//! name (`"raft.AppendEntries"`) and `rpc.system="raft"` semantic
//! conventions when the flag is on.
//!
//! The `otel` module is only added to this test root when
//! `-Dwith_otel=true`. To compile cleanly under both flag values we
//! gate the otel-namespace identifiers behind `comptime if (enabled)`
//! and stub the alias to an empty struct when the flag is off — that
//! branch is dead code in the disabled build but the import resolves.

const std = @import("std");
const build_options = @import("build_options");
const raft = @import("raft");

const otel = if (build_options.with_otel) @import("otel") else struct {};

test "one Raft RPC client span recorded when otel enabled" {
    if (!raft.observability.enabled) return error.SkipZigTest;

    const allocator = std.testing.allocator;

    var resource = try otel.sdk.resource.defaultResource(allocator);
    defer resource.deinit();
    try resource.attributes.putString(otel.semconv.service_name, "raft-test");

    var recorder = otel.exporters.in_memory.Recorder.init(allocator);
    defer recorder.deinit();
    var tp = try otel.sdk.trace.TracerProvider.init(allocator, .{
        .resource = &resource,
        .default_exporter = recorder.spanExporter(),
    });
    defer tp.deinit();

    var tracer = tp.tracer(.{ .name = "raft-test" });
    raft.observability.global.tracer = &tracer;
    defer raft.observability.global.tracer = null;

    // Drive one client-side AppendEntries span at target node 42.
    // This is the canonical raft RPC client span shape: name
    // "raft.AppendEntries", kind .client, attrs include
    // rpc.system="raft", rpc.method="AppendEntries",
    // raft.target_node_id=42.
    const span = raft.observability.startRpcClientSpan("AppendEntries", 42);
    raft.observability.endRpcClientSpan(span, true, 0);

    try tp.forceFlush(1_000);
    try std.testing.expectEqual(@as(usize, 1), recorder.spans.items.len);

    // Verify span name shape: "raft.{method}".
    const recorded = recorder.spans.items[0];
    try std.testing.expectEqualStrings("raft.AppendEntries", recorded.name);

    // Verify the rpc.method attribute landed on the span. The in-memory
    // recorder stores attributes as `[]KV` (key/value string pairs).
    var found_method: ?[]const u8 = null;
    var found_system: ?[]const u8 = null;
    for (recorded.attribute_kv) |kv| {
        if (std.mem.eql(u8, kv.key, "rpc.method")) {
            found_method = kv.value;
        } else if (std.mem.eql(u8, kv.key, "rpc.system")) {
            found_system = kv.value;
        }
    }
    const method_value = found_method orelse return error.MissingRpcMethodAttribute;
    try std.testing.expectEqualStrings("AppendEntries", method_value);
    const system_value = found_system orelse return error.MissingRpcSystemAttribute;
    try std.testing.expectEqualStrings("raft", system_value);
}

test "one Raft RPC server span recorded when otel enabled" {
    if (!raft.observability.enabled) return error.SkipZigTest;

    const allocator = std.testing.allocator;

    var resource = try otel.sdk.resource.defaultResource(allocator);
    defer resource.deinit();
    try resource.attributes.putString(otel.semconv.service_name, "raft-test");

    var recorder = otel.exporters.in_memory.Recorder.init(allocator);
    defer recorder.deinit();
    var tp = try otel.sdk.trace.TracerProvider.init(allocator, .{
        .resource = &resource,
        .default_exporter = recorder.spanExporter(),
    });
    defer tp.deinit();

    var tracer = tp.tracer(.{ .name = "raft-test" });
    raft.observability.global.tracer = &tracer;
    defer raft.observability.global.tracer = null;

    // Drive one server-side RequestVote span from source node 7.
    const span = raft.observability.startRpcServerSpan("RequestVote", 7);
    raft.observability.endRpcServerSpan(span, true, 0);

    try tp.forceFlush(1_000);
    try std.testing.expectEqual(@as(usize, 1), recorder.spans.items.len);

    const recorded = recorder.spans.items[0];
    try std.testing.expectEqualStrings("raft.RequestVote", recorded.name);
}
