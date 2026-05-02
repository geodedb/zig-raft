//! observability.zig — single-file otel seam for raft.
//!
//! When `build_options.with_otel == false`, every public function in this
//! file collapses to a comptime-known no-op so the optimizer eliminates
//! the call site entirely. When true, calls forward to the appropriate
//! `otel.api.trace` helper.
//!
//! INVARIANT: every emit is `catch {}`-discarded. A failing exporter
//! never reaches the caller — telemetry must never break Raft consensus.
//!
//! NOTE: deviation from template — Raft RPCs (AppendEntries, RequestVote,
//! InstallSnapshot) don't fit the HTTP `instrumentation.http.*` shape
//! cleanly. We expose the generic `startClientSpan` / `startServerSpan`
//! helpers for symmetry with the template (and to keep the consumer
//! recipe applicable), AND raft-specific `startRpcClientSpan` /
//! `startRpcServerSpan` helpers that construct spans via
//! `otel.api.trace.Tracer.startSpan` directly with a Raft-shaped span
//! name (`"raft.{method}"`) and `rpc.system="raft"` semantic conventions.
//!
//! The Raft library is consumer-driven (Ready-state pattern) — the
//! consumer drives `tick`/`step`/`propose`/`ready`/`advance`. The seam
//! sits inside `step()` (server) and the heartbeat / election broadcast
//! paths (client) so individual RPC batches are spanned at the level the
//! Raft paper defines. Per-log-entry spans would be too high-volume; we
//! span the entire AppendEntries batch as one operation.

const std = @import("std");
// 2026-05-02: hardcoded enabled=false and removed the build_options
// import + the conditional otel import. The previous pattern surfaced
// a Zig 0.16 module-dedup quirk when the lib was reached through
// multiple paths in a downstream build graph (geode), producing
// "build_options" vs "build_options0" diamonds at this file. The
// seam is preserved as a permanent no-op for forward-compat;
// restore the conditional once the upstream Zig dedup issue is
// resolved.
pub const enabled: bool = false;

const otel = struct {};

/// Opaque handle for an in-flight operation. When disabled, this is a
/// zero-sized struct the optimizer drops entirely. The no-op fallback's
/// `end` signature MUST match `otel.api.trace.Span.end(?u64) void` so
/// callers can write `span.end(null)` regardless of `enabled` state.
pub const Span = if (enabled) otel.api.trace.Span else struct {
    pub fn end(self: @This(), _: ?u64) void {
        _ = self;
    }
};

/// Helper: reads either a `[]const u8` field OR a function-typed field
/// returning `[]const u8`, so the seam accepts both shapes — the closure
/// shape the recipe template ships AND the plain-string-field shape used
/// at hot-path call sites.
inline fn accessOr(req: anytype, comptime field: []const u8) []const u8 {
    const T = @TypeOf(@field(req, field));
    return switch (@typeInfo(T)) {
        .pointer => @field(req, field),
        .@"fn" => @field(req, field)(),
        else => @compileError("observability: unsupported field type for " ++ field),
    };
}

/// Start a span for a generic outbound operation. Kept for template
/// symmetry; raft hot paths use `startRpcClientSpan` instead.
pub fn startClientSpan(req: anytype) Span {
    if (!enabled) return Span{};
    const tracer = global.tracer orelse return Span{};
    const span = otel.instrumentation.http.startClientSpan(
        tracer.*,
        .empty(),
        .{
            .method = accessOr(req, "method"),
            .url = accessOr(req, "url"),
        },
    ) catch return Span{};
    return span;
}

pub fn endClientSpan(span: Span, status: u16) void {
    if (!enabled) return;
    otel.instrumentation.http.endClientSpan(span, .{ .status_code = status });
}

/// Start a span for a generic inbound operation. Kept for template
/// symmetry; raft hot paths use `startRpcServerSpan` instead.
pub fn startServerSpan(req: anytype) Span {
    if (!enabled) return Span{};
    const tracer = global.tracer orelse return Span{};
    const span = otel.instrumentation.http.startServerSpan(
        tracer.*,
        .empty(),
        .{
            .method = accessOr(req, "method"),
            .route = accessOr(req, "route"),
            .url = accessOr(req, "url"),
        },
    ) catch return Span{};
    return span;
}

pub fn endServerSpan(span: Span, status: u16) void {
    if (!enabled) return;
    otel.instrumentation.http.endServerSpan(span, status);
}

/// Start a span for an outbound Raft RPC (this node sends to `target`).
///
/// Span shape:
///   - name: `"raft.{method}"` (e.g. `"raft.AppendEntries"`)
///   - kind: `.client`
///   - attributes:
///       * `rpc.system = "raft"`
///       * `rpc.method = method`
///       * `raft.target_node_id = target`
///
/// `method` is one of `"AppendEntries"`, `"RequestVote"`,
/// `"InstallSnapshot"`. The string is borrowed — callers must pass a
/// stable string literal.
pub fn startRpcClientSpan(method: []const u8, target: u64) Span {
    if (!enabled) return Span{};
    const tracer = global.tracer orelse return Span{};
    var name_buf: [64]u8 = undefined;
    const span_name = std.fmt.bufPrint(
        name_buf[0..],
        "raft.{s}",
        .{method},
    ) catch "raft.rpc";
    const span = tracer.startSpan(.empty(), span_name, .{ .kind = .client }) catch return Span{};
    // Best-effort attribute attachment. Failures (OOM in the attribute
    // set) cannot block consensus — telemetry is non-fatal.
    span.setStringAttribute("rpc.system", "raft") catch {};
    span.setStringAttribute("rpc.method", method) catch {};
    span.setIntAttribute("raft.target_node_id", @intCast(target)) catch {};
    return span;
}

/// Close a Raft RPC client span, recording success/failure.
///
/// On `ok = true` we set status `.ok`; on `ok = false` we set status
/// `.error_` with a string-formatted error code. `end(null)` is then
/// called inside this helper. Callers typically pair this with
/// `defer span.end(null)` at the call site so the span is always ended
/// regardless of which control-flow path is taken — `setStatus` + `end`
/// are idempotent (only the first status write wins; the deferred
/// `end(null)` is a redundant no-op).
pub fn endRpcClientSpan(span: Span, ok: bool, error_code: i32) void {
    if (!enabled) return;
    if (ok) {
        span.setStatus(.ok, "");
    } else {
        var code_buf: [32]u8 = undefined;
        const msg = std.fmt.bufPrint(code_buf[0..], "raft.error_code={d}", .{error_code}) catch "raft.error";
        // `StatusCode` uses `@"error"` (escaped keyword) for the
        // failure variant — see otel/src/api/trace.zig.
        span.setStatus(.@"error", msg);
    }
    // Best-effort attach of the numeric error code as a span attribute.
    span.setIntAttribute("raft.error_code", error_code) catch {};
    span.end(null);
}

/// Start a span for an inbound Raft RPC (this node received from `source`).
///
/// Span shape:
///   - name: `"raft.{method}"`
///   - kind: `.server`
///   - attributes:
///       * `rpc.system = "raft"`
///       * `rpc.method = method`
///       * `raft.source_node_id = source`
pub fn startRpcServerSpan(method: []const u8, source: u64) Span {
    if (!enabled) return Span{};
    const tracer = global.tracer orelse return Span{};
    var name_buf: [64]u8 = undefined;
    const span_name = std.fmt.bufPrint(
        name_buf[0..],
        "raft.{s}",
        .{method},
    ) catch "raft.rpc";
    const span = tracer.startSpan(.empty(), span_name, .{ .kind = .server }) catch return Span{};
    span.setStringAttribute("rpc.system", "raft") catch {};
    span.setStringAttribute("rpc.method", method) catch {};
    span.setIntAttribute("raft.source_node_id", @intCast(source)) catch {};
    return span;
}

pub fn endRpcServerSpan(span: Span, ok: bool, error_code: i32) void {
    if (!enabled) return;
    if (ok) {
        span.setStatus(.ok, "");
    } else {
        var code_buf: [32]u8 = undefined;
        const msg = std.fmt.bufPrint(code_buf[0..], "raft.error_code={d}", .{error_code}) catch "raft.error";
        span.setStatus(.@"error", msg);
    }
    span.setIntAttribute("raft.error_code", error_code) catch {};
    span.end(null);
}

/// Set once at process start by `observability_init.Otel.installGlobals`.
/// Only referenced when `enabled == true`; otherwise the optimizer drops
/// the whole struct.
pub const global = if (enabled) struct {
    pub var tracer: ?*otel.api.trace.Tracer = null;
    pub var meter: ?*otel.api.metrics.Meter = null;
} else struct {};
