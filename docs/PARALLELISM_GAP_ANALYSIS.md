# Parallelism Gap Analysis

**Last updated:** 2026-03-30
**Scope:** Full review of `spice` fork/join parallelism usage, untapped opportunities, test coverage, and the unused `wal` dependency.

---

## Summary

| ID | Severity | Finding | Location | Status |
|----|----------|---------|----------|--------|
| PAR-001 | High | `step()` task parameter was unused — removed from API | `src/node.zig` | **Resolved** |
| PAR-002 | High | `startElection()` vote broadcast is sequential — task param removed (no work to parallelize) | `src/node.zig` | **Resolved (Won't Do)** |
| PAR-003 | High | Zero test coverage of parallel code paths — 4 tests added with real ThreadPool | `tests/scenarios/parallel.zig` | **Resolved** |
| PAR-004 | Medium | Snapshot BLAKE3 checksum single-threaded — `task` param added for future use | `src/snapshot.zig` | **Resolved (Deferred)** |
| PAR-005 | Medium | `buildInstallSnapshotMessages()` had no task parameter — added | `src/snapshot.zig` | **Resolved** |
| PAR-006 | Medium | Doc comments referenced non-existent functions — corrected | `src/node.zig` | **Resolved** |
| PAR-007 | Low | `wal` dependency declared but never used — removed from build.zig.zon | `build.zig.zon` | **Resolved** |
| PAR-008 | Low | Benchmarks only measured sequential path — parallel variant added | `tests/bench.zig` | **Resolved** |
| PAR-009 | Info | `tick()` doc comment overstated parallel capability — corrected | `src/node.zig` | **Resolved** |

| Severity | Open | Resolved | Won't Do | Deferred |
|----------|------|----------|----------|----------|
| High | 0 | 2 | 1 | 0 |
| Medium | 0 | 2 | 0 | 1 |
| Low | 0 | 2 | 0 | 0 |
| Info | 0 | 1 | 0 | 0 |

**0 open gaps.** 7 resolved, 1 won't do, 1 deferred.

---

## Resolution Details

### PAR-001: `step()` task parameter removed (Resolved)

The `task: ?*spice.Task` parameter was accepted in the public `step()` API but explicitly discarded with `_ = task`. Analysis showed no parallelism opportunity: `step()` processes a single inbound message with sequential state transitions. No per-peer fan-out like heartbeats.

**Fix:** Removed the `task` parameter from `step()` entirely. Updated all callers across 8 files (node.zig, harness.zig, bench.zig, adversarial.zig, election.zig, snapshots.zig, file_storage.zig, basic.zig).

### PAR-002: Vote broadcasting stays sequential (Won't Do)

`startElection()` accepted `task: ?*spice.Task` but never used it. Analysis showed no work to parallelize: the RequestVote message is identical for all peers (same term, same candidate_id, same log position). The `sendMsg` call is a trivial transport queue push. Unlike `sendHeartbeats` where each peer gets a different AppendEntries payload constructed from different log positions, vote broadcast has no per-peer computation.

**Fix:** Removed `task` parameter from `startElection()`. Updated `tick()` doc comment to accurately state parallelism is for "building AppendEntries payloads" only.

### PAR-003: Parallel path tests added (Resolved)

**Critical finding: tests discovered and fixed a real bug.**

The parallel join loop in `sendHeartbeats()` joined futures in forward order (0..N), but spice's work queue is a LIFO stack — `pop()` asserts `tail.* == self`, meaning only the most recently pushed job can be popped. Forward-order joins caused assertion failures when futures remained `.queued` (not yet stolen by workers).

**Bug fix:** Changed join loop to reverse order (N-1..0) to match spice's LIFO semantics. Send loop remains forward order (message ordering is independent of join order).

**Tests added** (`tests/scenarios/parallel.zig`):
1. `parallel heartbeats: leader sends correct AppendEntries via fork/join` — verifies heartbeat messages reach all peers via the parallel path
2. `parallel heartbeats: messages match sequential path output` — compares parallel vs sequential output for equivalence
3. `parallel tick: full cluster round-trip with parallel heartbeats` — 5-node cluster with parallel leader ticks, verifies entry commitment
4. `parallel tick: safety invariants hold after parallel rounds` — Raft safety invariant verification via checker

### PAR-004: Snapshot BLAKE3 parallelism deferred (Deferred)

Zig's `std.crypto.hash.Blake3` does not expose a parallel tree-hashing API. The `update()/final()` interface is inherently sequential. Parallelizing would require reimplementing BLAKE3 tree hashing — complex, error-prone, and out of scope.

**Fix:** Added `task` parameter to `buildInstallSnapshotMessages()` (PAR-005) with documented reservation for future parallel hashing when Zig's Blake3 API supports it.

### PAR-005: Snapshot API accepts task parameter (Resolved)

`buildInstallSnapshotMessages()` now accepts `task: ?*spice.Task`. Currently sequential (task is unused) because: (a) Blake3 API doesn't support parallel tree hashing, (b) per-chunk struct construction is trivially cheap. The parameter enables future parallelism without another API change.

### PAR-006: Doc comments corrected (Resolved)

Removed references to non-existent `parallelSendVotes` and `parallelSendHeartbeats` from SEC-0067 doc comment. The allocator thread-safety documentation now accurately lists only `parallelBuildAE` as the concurrent allocation path.

### PAR-007: Unused `wal` dependency removed (Resolved)

The `wal` dependency in `build.zig.zon` was declared but never imported in `build.zig` or `@import`ed in any source file. `file_storage.zig` implements its own WAL via manual file I/O. Removed the dead dependency to eliminate unnecessary fetching.

### PAR-008: Parallel benchmark added (Resolved)

Added `bench: leader-tick-parallel` benchmark comparing sequential vs parallel tick for a 5-node cluster. Results show expected overhead for trivial workloads (~62 ns parallel vs ~13 ns sequential with no-op transport). Parallelism benefits emerge with real AppendEntries payload construction on larger clusters.

### PAR-009: `tick()` doc comment corrected (Resolved)

Updated to: "spice parallelism is used for building AppendEntries payloads in parallel during leader heartbeats." Removed false claim about parallel vote broadcasting.

---

## Files Modified

| File | Changes |
|------|---------|
| `build.zig.zon` | Removed unused `wal` dependency |
| `build.zig` | Added `parallel.zig` to scenario test list |
| `src/node.zig` | Removed `task` from `step()` and `startElection()`; fixed LIFO join order in `sendHeartbeats()`; corrected doc comments |
| `src/snapshot.zig` | Added `spice` import; added `task` param to `buildInstallSnapshotMessages()` |
| `tests/scenarios/parallel.zig` | **New** — 4 parallel path tests with real `spice.ThreadPool` |
| `tests/bench.zig` | Added parallel leader-tick benchmark |
| `tests/harness.zig` | Updated `step()` calls (removed `null` task arg) |
| `tests/scenarios/adversarial.zig` | Updated `step()` calls |
| `tests/scenarios/election.zig` | Updated `step()` calls |
| `tests/scenarios/snapshots.zig` | Updated `step()` and `buildInstallSnapshotMessages()` calls |
| `tests/scenarios/file_storage.zig` | Updated `step()` calls |
| `examples/basic.zig` | Updated `step()` comment |
