# Gap Analysis — Resolved Items

**Last updated:** 2026-03-21

Items are moved here from `GAP_ANALYSIS.md` when the gap is confirmed resolved with evidence. The corresponding GitLab issue is closed.

---

### GAP-0001: Spice parallelism integrated (RESOLVED)
- **GitLab issue:** [#1](https://gitlab.com/devnw/zig/raft/-/issues/1)
- **Category:** Feature
- **Severity:** High
- **Resolution:**
  - `tick()` and `step()` now accept `task: ?*spice.Task` parameter
  - Parallel path in `sendHeartbeats()` forks one `BuildAEFuture` per peer for parallel AppendEntries construction
  - Uses output-slot pattern (results written to shared array) because `rpc.Message` exceeds spice's 32-byte Future return limit
  - When `task` is null, behavior is identical to the original sequential path
  - `startElection()` accepts and threads the task parameter for future parallel vote broadcasting
  - `spice` re-exported from `root.zig` for consumer access
  - All callers updated: harness, scenarios (election, snapshots, file_storage), chaos tests, internal node tests
- **Evidence:** 150/150 tests pass (unit, integration, scenario, chaos)

---

### GAP-0002: Missing spec scenario tests added (RESOLVED)
- **GitLab issue:** [#2](https://gitlab.com/devnw/zig/raft/-/issues/2)
- **Category:** Test
- **Severity:** Medium
- **Resolution:** All 4 missing test scenarios implemented:
  - `tests/scenarios/snapshots.zig`: `snapshot_catches_up_laggard` — partitions follower, compacts log on leader, delivers InstallSnapshot chunks, verifies follower catches up
  - `tests/scenarios/snapshots.zig`: `chunked_snapshot_transfer` — 100-byte payload with chunk_size=7 produces 15 chunks, verifies CRC, offset progression, and reassembly
  - `tests/scenarios/file_storage.zig`: `torn_write_recovery` — writes valid entries, appends garbage bytes to WAL, re-opens storage, verifies truncation of garbage and survival of valid entries
  - `tests/scenarios/membership.zig`: `no_double_commit_during_reconfig` — proposes entries during joint consensus, verifies no duplicate index commits and correct entry kind counts
- **Evidence:** All 34 scenario tests pass

---

### GAP-0003: CI pipeline includes scenario and chaos tests (RESOLVED)
- **GitLab issue:** [#3](https://gitlab.com/devnw/zig/raft/-/issues/3)
- **Category:** CI-CD
- **Severity:** Medium
- **Resolution:** Added `test-scenarios` and `test-chaos` jobs to `.gitlab-ci.yml` test stage.
- **Evidence:** Both jobs present in CI config, use same pattern as existing test jobs

---

### GAP-0004: Documentation links fixed (RESOLVED)
- **GitLab issue:** [#4](https://gitlab.com/devnw/zig/raft/-/issues/4)
- **Category:** Docs
- **Severity:** Low
- **Resolution:** Updated `docs/README.md` to remove broken links to non-existent documentation files. Replaced with accurate links to existing project files (CLAUDE.md, design spec, source modules).
- **Evidence:** All links in docs/README.md resolve to existing files

---

### GAP-0005: Spice dependency has URL+hash for reproducible builds (RESOLVED)
- **GitLab issue:** [#5](https://gitlab.com/devnw/zig/raft/-/issues/5)
- **Category:** CI-CD
- **Severity:** Medium
- **Resolution:** Updated `build.zig.zon` spice dependency from path-only to URL+hash pointing to the GitLab repository. Zig 0.15 does not support both `.url` and `.path` in the same dependency entry; local development uses `zig build --override-dep` or a local `.zig.zon` override.
- **Evidence:** `zig build check` succeeds with URL-based dependency

---

### GAP-0006: Dead NotLeader struct removed (RESOLVED)
- **GitLab issue:** [#6](https://gitlab.com/devnw/zig/raft/-/issues/6)
- **Category:** Code Quality
- **Severity:** Low
- **Resolution:** Removed unused `NotLeader` struct from `src/node.zig`. Documented that consumers read `node.leader_id` directly after catching `error.NotLeader`.
- **Evidence:** No dead code; grep for NotLeader returns 0 results in src/

---

### GAP-0007: membership_under_chaos test added (RESOLVED)
- **GitLab issue:** [#7](https://gitlab.com/devnw/zig/raft/-/issues/7)
- **Category:** Test
- **Severity:** Medium
- **Resolution:** Added `chaos: membership_under_chaos` test to `tests/chaos.zig`. Test uses 5-node cluster with 10% message drop rate, adds node 6 via `proposeConfigChange`, ticks under faults for 300 rounds with periodic invariant checks, then removes back to 5 nodes with another 300 rounds of fault injection.
- **Evidence:** All 11 chaos tests pass (was 10, now 11)

---

### GAP-0008: Checker integrated in all Tier 2/4 tests (RESOLVED)
- **GitLab issue:** [#8](https://gitlab.com/devnw/zig/raft/-/issues/8)
- **Category:** Test
- **Severity:** Medium
- **Resolution:**
  - Added `checker_module` import to all scenario test targets in `build.zig` (persistence, file_storage, membership, replication)
  - Added `Checker.checkAll()` calls to all tests in: `membership.zig` (5 tests), `persistence.zig` (3 network tests), `file_storage.zig` (6 tests)
  - Replaced `checkSingleLeader()` with full `checkAllInvariants()` (4-invariant Checker) in all 11 chaos tests
  - Added dedicated build targets for `membership.zig` and `replication.zig` (were previously missing from build system entirely)
  - Scenario test count increased from 20 to 34
- **Evidence:** All 34 scenario tests and 11 chaos tests pass with full invariant checking
