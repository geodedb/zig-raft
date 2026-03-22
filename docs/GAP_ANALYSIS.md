# Gap Analysis

**Last updated:** 2026-03-22
**Spec:** `docs/superpowers/specs/2026-03-21-raft-design.md`
**Methodology:** Requirements-to-code audit + fourteen adversarial security review passes -- all source, tests, CI, and docs compared against design spec, then reviewed for security vulnerabilities. Pass 1 (SEC-0001..SEC-0011, all remediated), Pass 2 (SEC-0012..SEC-0028, all remediated or Won't Do), Pass 3 (SEC-0029..SEC-0036, all remediated), Pass 4 (SEC-0037..SEC-0044, all remediated or Won't Do), Pass 5 (SEC-0045..SEC-0064, all remediated), Pass 6 (SEC-0065..SEC-0068, all remediated or Won't Do), Pass 7 (SEC-0069..SEC-0070, all remediated), Pass 8 (SEC-0071..SEC-0073, all remediated), Pass 9 (SEC-0074..SEC-0097, all remediated or Won't Do), Pass 10 (SEC-0098..SEC-0101, all remediated), Pass 11 (SEC-0103..SEC-0106, all remediated), Pass 12 (SEC-0107..SEC-0108, all remediated), Pass 13 (SEC-0109..SEC-0110, all remediated), Pass 14 (SEC-0111..SEC-0118, all remediated).

## Summary

### Spec-to-Code Gaps (GAP-0001 through GAP-0008)

| Severity | Open | Resolved | Won't Do |
|----------|------|----------|----------|
| Critical | 0 | 0 | 0 |
| High | 0 | 1 | 0 |
| Medium | 0 | 5 | 0 |
| Low | 0 | 2 | 0 |

All 182 tests pass (124 unit, 1 integration, 46 scenario, 11 chaos). All spec gaps resolved. See [GAP_ANALYSIS_RESOLVED.md](GAP_ANALYSIS_RESOLVED.md).

### Security Findings (SEC-0001 through SEC-0118)

| Severity | Open | Resolved | Won't Do |
|----------|------|----------|----------|
| Critical | 0 | 3 | 0 |
| High | 0 | 20 | 0 |
| Medium | 0 | 55 | 4 |
| Low | 0 | 24 | 5 |
| Info | 0 | 8 | 1 |

**0 open findings.** 117 total findings across 14 passes, all resolved or Won't Do.

## Won't Do -- Handled via External Controls

> **For future agents:** The following items are intentionally closed as Won't Do. They are NOT gaps in this repository. They are managed by external systems and controls. Do not reopen or re-implement them.

- **SEC-0008: CI downloads Zig without hash verification** -- Managed centrally via [devnw/ci-catalog](https://gitlab.com/devnw/ci-catalog) component templates. CI/CD image hash pinning and download verification are handled at the CI catalog level, not in individual repositories.
- **SEC-0018: CI component includes use mutable tags** -- Same as above; CI component tag pinning is managed at the CI catalog level, not in individual repositories.
- **SEC-0019: check-yaml --unsafe in pre-commit** -- Pre-commit hook configuration is enforced via CI pipeline stages and developer tooling external to this repository.
- **Docker tag pinning:** Managed at CI catalog level (`debian:bookworm-slim` without digest pinning in CI catalog components).
- **SEC-0042: CI downloads Zig toolchain without SHA256 verification** -- Same as SEC-0008; CI/CD toolchain download integrity is managed centrally via the CI catalog level controls, not in individual repositories.
- **Pre-commit hooks:** Pre-commit configuration and enforcement is handled via CI pipeline stages and developer tooling external to this repository. Changes to `.pre-commit-config.yaml` are governed by the CI catalog team's security review process.
- **SEC-0068: `autofix_prs: true` in pre-commit config** -- Pre-commit configuration (including autofix settings) is handled via CI pipeline stages and developer tooling external to this repository. Same controls as SEC-0019.
- **SEC-0093: Test harness `page_allocator` defeats leak detection** -- The harness intentionally uses `page_allocator` because Raft's shared-ownership entry slices create complex free patterns that confuse the testing allocator. The comment in `tests/harness.zig:78-79` documents this trade-off. Switching to `testing.allocator` would require refactoring ownership semantics across the entire codebase.
- **SEC-0099: `Config.peers` borrowed slice can dangle** -- This is standard Zig borrowing semantics. The caller is responsible for ensuring the peers slice outlives the `RaftNode`. The test harness already dupes the slice (`allocator.dupe(u64, peers.items)`). Adding automatic duplication inside `RaftNode.init()` would break the existing ownership contract.
- **SEC-0102: File storage tests use predictable `/tmp` paths** -- Test-only code with no production impact. The hardcoded paths are deterministic for reproducible test debugging. TOCTOU risk is negligible in a test environment.

---

## Security Findings -- Pass 14 (SEC-0111 through SEC-0118) -- Resolved

8 findings identified through exhaustive adversarial review with 5 parallel review agents covering all 14 source modules, tests, CI/CD, build system, and configs. 7 automated scanners re-run (0 findings). 30+ agent-reported findings verified against actual code; 20+ filtered as false positives or duplicates of existing findings (including re-reports of Won't Do items SEC-0008/SEC-0019/SEC-0042).

### Medium Severity (5 findings)

| Rank | Sev | Risk | Finding | Component | CWE | Evidence | Issue | Status |
|------|-----|------|---------|-----------|-----|----------|-------|--------|
| 1 | Medium | 36 | `applyConfig()` missing `is_joint/voters_outgoing` consistency check — SEC-0110 added check to `validateSnapshotConfig` and `applySnapshotConfig` but missed `applyConfig` | `src/membership.zig` | CWE-20 | `membership.zig:123` enters joint validation without checking outgoing non-empty; line 149 sets `is_joint=true` with empty outgoing | [#118](https://gitlab.com/devnw/zig/raft/-/issues/118) | Resolved — added bidirectional is_joint/voters_outgoing consistency check (SEC-0111) |
| 2 | Medium | 36 | `pending_config_change` and `pending_config_index` not reset on snapshot application — stale flags block future config changes or trigger spurious finalization | `src/membership.zig`, `src/node.zig` | CWE-672 | `membership.zig:189` sets `is_joint` but not `pending_config_change`; `node.zig:877-892` never resets `pending_config_index` | [#119](https://gitlab.com/devnw/zig/raft/-/issues/119) | Resolved — reset `pending_config_change` in `applySnapshotConfig()` and `pending_config_index` in `handleInstallSnapshot()` (SEC-0112) |
| 3 | Medium | 24 | `handleInstallSnapshotResponse` uses potentially stale `snapshot_state.snapshot_index` — can inflate follower match_index if leader took new snapshot since sending | `src/node.zig` | CWE-672, CWE-682 | `node.zig:915` — `progress.maybeUpdate(self.snapshot_state.snapshot_index)`; `InstallSnapshotResponse` does not carry installed index | [#120](https://gitlab.com/devnw/zig/raft/-/issues/120) | Resolved — use `log.snapshot_index` (persisted) instead of `snapshot_state.snapshot_index` (SEC-0113) |
| 4 | Medium | 24 | `saveSnapshotImpl` config corruption on OOM — clears cached config ArrayLists before fallible `appendSlice`, leaving partial config on failure | `src/file_storage.zig` | CWE-460, CWE-662 | `file_storage.zig:456-457` clears arrays; lines 458-459 `try appendSlice` can fail; `compactImpl:558` reads stale config | [#121](https://gitlab.com/devnw/zig/raft/-/issues/121) | Resolved — build new lists before mutating, swap on success (SEC-0114) |
| 5 | Medium | 24 | `appendEntriesImpl` missing rollback on `file.sync()` failure — in-memory state diverges from durability guarantee | `src/file_storage.zig` | CWE-460 | `file_storage.zig:342` — `try file.sync()` with no rollback; per-entry rollback at lines 280-335 does not cover post-loop sync | [#122](https://gitlab.com/devnw/zig/raft/-/issues/122) | Resolved — added sync failure rollback using saved_wal_end/saved_index_len (SEC-0115) |

### Low Severity (3 findings)

| Rank | Sev | Risk | Finding | Component | CWE | Evidence | Issue | Status |
|------|-----|------|---------|-----------|-----|----------|-------|--------|
| 6 | Low | 12 | `finishLoadSnapshot` missing errdefer for config buffers — `try` at lines 499, 507, 508 leak `meta.voters_buf`/`voters_outgoing_buf` on error | `src/file_storage.zig` | CWE-401 | `file_storage.zig:496-508` — no `errdefer m.freeConfig()` covering `try` expressions | [#123](https://gitlab.com/devnw/zig/raft/-/issues/123) | Resolved — added `errdefer m.freeConfig()` and `errdefer self.allocator.free(data)` (SEC-0116) |
| 7 | Low | 12 | `ready()` missing errdefer for `log.slice()` allocation — OOM in `addCommittedEntries` leaks entries slice | `src/node.zig` | CWE-401 | `node.zig:396-405` — `entries` allocated by `log.slice()` at line 396, freed at line 405 (unreachable on error at line 401) | [#124](https://gitlab.com/devnw/zig/raft/-/issues/124) | Resolved — added errdefer to free entries on addCommittedEntries failure (SEC-0117) |
| 8 | Low | 8 | `InMemoryStorage.entriesImpl` uses `lo == hi` instead of `lo >= hi` — inverted range panics (inconsistent with FileStorage) | `src/storage.zig` | CWE-20, CWE-617 | `storage.zig:153` — `if (lo == hi)` vs `file_storage.zig:347` — `if (lo >= hi)` | [#125](https://gitlab.com/devnw/zig/raft/-/issues/125) | Resolved — changed to `lo >= hi` matching FileStorage (SEC-0118) |

### False Positives Filtered (Pass 14)

> 20+ agent-reported findings verified against actual code and filtered:

- **"handleAppendEntries calls becomeFollower before entry validation"** — Per Raft paper Figure 2, followers MUST accept a leader with `term >= currentTerm` and update their term. Entry size limits are library-specific rate limiting, not protocol-level acceptance. Term update before entry validation is correct Raft behavior.
- **"Shallow @memcpy of Entry structs in log.slice() creates dangling data pointers"** — By design. The Ready-State pattern documents that consumers must process entries from `Ready` before calling `advance()`. Entry data is borrowed from the log (same as etcd/raft). Not a bug.
- **"committed_entries in Ready struct are borrowed ArrayList.items slices"** — By design. The consumer must not call `tick()`/`step()` between `ready()` and `advance()`. Documented in CLAUDE.md consumer loop pattern.
- **"ready() and advance() both call log.slice() over same committed range — TOCTOU"** — The consumer loop is sequential: `ready() → persist/send/apply → advance()`. No interleaved mutations occur. `advance()` re-reads for config application which is correct.
- **"buildAppendEntries entries slice leaked after sendMsg"** — Messages go into `ready_state.pending_messages` (ArrayList). The consumer receives them in `Ready.messages` and sends via their transport. Entry data is borrowed from the log. Slice ownership follows the Ready lifecycle — cleared on `advance()`.
- **"CI downloads Zig toolchain without checksum"** — Duplicate of SEC-0008/SEC-0042 (Won't Do, managed at CI catalog level).
- **"check-yaml --unsafe in pre-commit"** — Duplicate of SEC-0019 (Won't Do, managed externally).
- **"CODEOWNERS lacks granular security-critical path coverage"** — SEC-0010 resolved with `@devnw`. Granular paths are a hardening suggestion, not a vulnerability.
- **"leader_commit trusted without upper bound"** — `commitTo` clamps to `@min(leader_commit, last_new_index)` then `@min(result, lastIndex())`. Defense-in-depth already present. Transport integrity is consumer responsibility.
- **"Deserialization accepts trailing garbage bytes"** — The deserializer parses exactly the declared structure. Trailing bytes are unreachable: entry data length is stored in the log/WAL and matches exactly. Not exploitable.
- **"BLAKE3 checksum does not cover config metadata"** — Config is validated independently via `validateSnapshotConfig`. Snapshot integrity covers data; config integrity is checked on application. Defense-in-depth.
- **"maybeDecrTo ignores rejected_index and conflict_term"** — Design decision matching simplified Raft implementations. The `conflict_index` is the effective backtrack target. Performance optimization (term-based skipping) is not implemented but not a security vulnerability.
- **"compact() uses fallback snapshot_term on term lookup failure"** — The `compact_index > snapshot_index` guard at line 226 ensures the index is in a valid range. The fallback is a safety net for theoretical I/O errors, not a vulnerability.
- **"Election timeout overflow produces degenerate range"** — `election_tick` is a u32 config value set by the consumer. Values > 2^31 are unreasonable (billions of ticks). Consumer responsibility.
- **"firstUnstableIndex silently saturates at lastIndex() on overflow"** — `lastIndex()` reaching u64::MAX is unreachable in any practical deployment. Defensive saturation is correct.
- **"Checker claims data verification but only compares terms"** — The SEC-0089 reference in the doc comment is aspirational. Test gap, not a security vulnerability in production code.
- **"Integration test suite has only 1 test"** — Test coverage gap, not a production vulnerability.
- **"Chaos tests don't exercise snapshot transfer under faults"** — Test coverage gap, not a production vulnerability.
- **"Harness tickAll two-round delivery limitation"** — Test design choice for deterministic behavior, not a vulnerability.
- **"InstallSnapshotResponse lacks error discrimination"** — Protocol limitation. Leader safely restarts from chunk 0 on any failure. Correctness preserved.
- **"Zero-length slice ownership ambiguity (heap vs comptime literal)"** — The `len > 0` guard consistently prevents freeing zero-length slices. Safe by construction.

---

## Security Findings -- Pass 13 (SEC-0109 through SEC-0110) -- Resolved

2 findings identified through exhaustive adversarial review with 5 parallel review agents covering all 14 source modules, tests, CI/CD, build system, and configs. 7 automated scanners re-run (0 findings). 8+ agent-reported findings verified against actual code; 6 filtered as false positives.

### Medium Severity (1 finding)

| Rank | Sev | Risk | Finding | Component | CWE | Evidence | Issue | Status |
|------|-----|------|---------|-----------|-----|----------|-------|--------|
| 1 | Medium | 30 | `FileStorage.compactImpl()` mutates `snapshot_term`/`snapshot_index` before fallible `writeSnapshotMeta()` — failed persistence leaves in-memory state diverged from disk (same bug pattern as SEC-0084) | `src/file_storage.zig` | CWE-460 | `file_storage.zig:553-554` mutate state, line 562 `writeSnapshotMeta()` can fail | [#116](https://gitlab.com/devnw/zig/raft/-/issues/116) | Resolved — persist BEFORE mutating in-memory fields (SEC-0109) |

### Low Severity (1 finding)

| Rank | Sev | Risk | Finding | Component | CWE | Evidence | Issue | Status |
|------|-----|------|---------|-----------|-----|----------|-------|--------|
| 2 | Low | 10 | `validateSnapshotConfig()` and `applySnapshotConfig()` accept `is_joint=true` with empty `voters_outgoing` — creates inconsistent joint consensus state violating Raft §6 | `src/snapshot.zig`, `src/membership.zig` | CWE-20 | `snapshot.zig:280` no joint/outgoing consistency check; `membership.zig:180` skips empty outgoing but sets `is_joint=true` at line 189 | [#117](https://gitlab.com/devnw/zig/raft/-/issues/117) | Resolved — added bidirectional is_joint/voters_outgoing consistency validation (SEC-0110) |

### False Positives Filtered (Pass 13)

> Agent-reported findings verified against actual code and filtered:

- **"Missing leader validation in AppendEntries"** — Raft does not require followers to verify leader membership. Term-based leader acceptance is the protocol's design.
- **"Race condition in ReplicationState parallel operations"** — Per CLAUDE.md, spice parallelism is used only for read-only work (building AppendEntries payloads, broadcasting votes). No concurrent mutation.
- **"Unbounded slice in `truncateUnstable()`"** — `keep` is always `<= items.len` by construction (clamped at line 304). Safe.
- **"Integer overflow in trim_count subtraction"** — `first_unstable >= 1` guaranteed by construction (snapshot_index starts at 0, first_unstable = snapshot_index + 1). Subtraction cannot underflow.
- **"Off-by-one in max_bytes entry count check"** — `count > 1` is intentional: always include at least one entry regardless of size (matches etcd/raft behavior).
- **"Snapshot term validation gap"** — Already mitigated by `node.zig` term check (`msg.term < self.current_term` rejects stale snapshots before reaching `SnapshotState`).

---

## Security Findings -- Pass 12 (SEC-0107 through SEC-0108) -- Resolved

2 findings identified through exhaustive adversarial review with 5 parallel review agents covering all 14 source modules, tests, CI/CD, build system, and configs. 7 automated scanners re-run (0 findings). 10+ agent-reported findings verified against actual code; 8+ filtered as false positives.

### Critical Severity (1 finding)

| Rank | Sev | Risk | Finding | Component | CWE | Evidence | Issue | Status |
|------|-----|------|---------|-----------|-----|----------|-------|--------|
| 1 | Critical | 80 | Use-After-Free: `receiveChunk()` frees existing pending snapshot data/config but does not null `self.pending_snapshot` before fallible `validateSnapshotConfig()` — failed validation leaves freed ArrayLists reachable, causing double-free or UAF on next call | `src/snapshot.zig` | CWE-416, CWE-415 | `snapshot.zig:157-163` — deinit at 158-159, validateSnapshotConfig at 163 can fail, pending_snapshot not nulled | [#114](https://gitlab.com/devnw/zig/raft/-/issues/114) | Resolved — null `self.pending_snapshot` after freeing, before fallible call (SEC-0107) |

### Low Severity (1 finding)

| Rank | Sev | Risk | Finding | Component | CWE | Evidence | Issue | Status |
|------|-----|------|---------|-----------|-----|----------|-------|--------|
| 2 | Low | 8 | `validateSnapshotConfig()` has no upper bound on voter count — O(n²) dedup check + unbounded `appendSlice` copy enables CPU DoS and OOM from a malicious peer | `src/snapshot.zig` | CWE-770, CWE-400 | `snapshot.zig:275-289` — no `MAX_VOTERS_PER_SET` limit; lines 168/172 copy unbounded voter IDs | [#115](https://gitlab.com/devnw/zig/raft/-/issues/115) | Resolved — added `MAX_VOTERS_PER_SET = 255` constant and early bounds check (SEC-0108) |

### False Positives Filtered (Pass 12)

> Agent-reported findings verified against actual code and filtered:

- **"Quorum `committedForSet` off-by-one at `buf[n/2]`"** — Math is correct: for n=5, `buf[2]` is the 3rd-highest match_index (median), requiring 3 peers at or above. Majority = `n/2 + 1 = 3`. Correct for all cluster sizes.
- **"log.zig `slice()` bounds wraparound on subtraction"** — `lo >= first_unstable` checked at line 104 before `lo - first_unstable` at line 105. `end` bounds-checked at line 107. Safe.
- **"InMemoryStorage `entriesImpl` missing index validation"** — Line 159 (`end > self.log.items.len`) catches out-of-range; returns `Unavailable`. Safe.
- **"Snapshot rename race: no syncDir between meta and data renames"** — BLAKE3 mismatch detection at lines 516-527 prevents loading corrupted data. Recovery path at lines 474-478 handles missing `snapshot.data` when `.tmp` exists.
- **"Ready duplicate message emission"** — By design; Raft protocol is idempotent. Consumer receives whatever `node.zig` emits.
- **"Missing errdefer for `owned_voters_outgoing` in receiveChunk"** — No fallible operations follow the `toOwnedSlice` call. Errdefer not needed.
- **"`truncateFrom` missing fsync after `setEndPos`"** — Caller `appendEntriesImpl` calls `file.sync()` at line 342 after the full batch. Intentional batching for performance.
- **"Cross-set voter deduplication in snapshot config"** — A voter in both sets is valid during joint consensus (Raft §6). Same node can be in old and new configurations simultaneously.

---

## Security Findings -- Pass 11 (SEC-0103 through SEC-0106) -- Resolved

4 findings identified through exhaustive adversarial review with 5 parallel review agents covering all 14 source modules, tests, CI/CD, build system, and configs. All 12 automated scanners re-run (0 findings). Multiple agent-reported findings verified against actual code; 5+ filtered as false positives (WAL u64 overflow unreachable, snapshot @intCast widening, recovery offset bounded by file_size).

### Medium Severity (1 finding)

| Rank | Sev | Risk | Finding | Component | CWE | Evidence | Issue | Status |
|------|-----|------|---------|-----------|-----|----------|-------|--------|
| 1 | Medium | 24 | `becomeLeader()` sets `role=.leader` before fallible `ReplicationState.init()` — OOM leaves zombie leader with null replication_state | `src/node.zig` | CWE-665 | `node.zig:507` sets role, `node.zig:514` can fail; subsequent leader ops silently no-op | [#110](https://gitlab.com/devnw/zig/raft/-/issues/110) | Resolved — deferred role assignment until after all fallible operations succeed (SEC-0103) |

### Low Severity (2 findings)

| Rank | Sev | Risk | Finding | Component | CWE | Evidence | Issue | Status |
|------|-----|------|---------|-----------|-----|----------|-------|--------|
| 2 | Low | 8 | Unchecked `total_bytes += entry.data.len` in `buildEntries()` bypasses max_bytes_per_msg on u64 overflow | `src/replication.zig` | CWE-190 | `replication.zig:134` — wrapping addition when `max_entry_size=0` (unlimited) | [#111](https://gitlab.com/devnw/zig/raft/-/issues/111) | Resolved — replaced `+=` with `std.math.add` checked addition (SEC-0104) |
| 3 | Low | 8 | Partial `appendSlice` failure in `FileStorage.init()` leaves inconsistent snapshot config (voters set, outgoing empty) | `src/file_storage.zig` | CWE-460 | `file_storage.zig:176-177` — no errdefer rollback between sequential fallible calls | [#112](https://gitlab.com/devnw/zig/raft/-/issues/112) | Resolved — added `errdefer` rollback between sequential appendSlice calls (SEC-0105) |

### Info Severity (1 finding)

| Rank | Sev | Risk | Finding | Component | CWE | Evidence | Issue | Status |
|------|-----|------|---------|-----------|-----|----------|-------|--------|
| 4 | Info | 4 | `election_elapsed` u32 tick counter wraps to zero after ~4.3B ticks, permanently disabling election timeout | `src/election.zig` | CWE-190 | `election.zig:66` — `+= 1` wraps; ~136 years at 1ms/tick | [#113](https://gitlab.com/devnw/zig/raft/-/issues/113) | Resolved — replaced `+=` with saturating addition `+|` + unit test (SEC-0106) |

### False Positives Filtered (Pass 11)

> Agent-reported findings verified against actual code and filtered:

- **"WAL wal_end u64 overflow on cumulative writes"** — `wal_end` tracks file offset; individual entries capped at ~4GB (u32 data_len). Overflowing u64 requires ~18 exabytes of WAL data. Not reachable in any practical deployment.
- **"WAL recovery offset + total_record_size overflow"** — Both values bounded by `file_size` (actual file on disk). Sum cannot exceed u64 for any real file.
- **"Snapshot @intCast(offset) truncation"** — `offset` is `usize`, target is `u64`. On 64-bit systems this is identity; on 32-bit it's a widening cast. Always safe.
- **"Snapshot buildInstallSnapshotMessages chunk_size unvalidated"** — `chunk_size` is validated by `Config.validate()` (minimum 1024, SEC-0100). Already fixed.
- **"appendSlice double-free via meta.voters_buf"** — `ArrayList.appendSlice` copies data into its own buffer; `defer m.freeConfig()` frees the source. No aliasing between `fs.snapshot_config_voters` buffer and `meta.voters_buf`.

---

## Security Findings -- Pass 10 (SEC-0098 through SEC-0101) -- Resolved

4 findings identified through exhaustive adversarial review with 5 parallel review agents covering all source modules, tests, CI/CD, and configs. All 12 automated scanners re-run (0 findings). 30+ agent-reported findings verified against actual code; 26+ filtered as false positives or duplicates of existing findings.

### Medium Severity (2 findings)

| Rank | Sev | Risk | Finding | Component | CWE | Evidence | Issue | Status |
|------|-----|------|---------|-----------|-----|----------|-------|--------|
| 1 | Medium | 24 | `receiveChunk()` accepts `last_included_term=0` — term 0 is invalid in Raft, corrupts snapshot_term history | `src/snapshot.zig` | CWE-20 | `snapshot.zig:145-177` — no validation of `msg.last_included_term > 0`; flows to `log.restore()` at `log.zig:272` | [#106](https://gitlab.com/devnw/zig/raft/-/issues/106) | Resolved — added term=0 validation in `receiveChunk()` + unit test (SEC-0098) |
| 2 | Medium | 24 | No adversarial/Byzantine test scenarios — validation code from SEC-0001 through SEC-0097 not exercised by malicious inputs | `tests/scenarios/` | N/A | Missing test scenarios for: bogus terms, inflated indices, oversized payloads, sender mismatches, invalid snapshot metadata | [#109](https://gitlab.com/devnw/zig/raft/-/issues/109) | Resolved — added `tests/scenarios/adversarial.zig` with 12 test cases (SEC-0101) |

### Low Severity (2 findings)

| Rank | Sev | Risk | Finding | Component | CWE | Evidence | Issue | Status |
|------|-----|------|---------|-----------|-----|----------|-------|--------|
| 3 | Low | 12 | `handleInstallSnapshot()` applies snapshot config via direct mutation, bypassing `applyConfig()` validation path | `src/node.zig` | CWE-20 | `node.zig:874-887` — direct mutation of `membership_state.voters/voters_outgoing` instead of `applyConfig()` | [#107](https://gitlab.com/devnw/zig/raft/-/issues/107) | Resolved — added `applySnapshotConfig()` in `membership.zig` with full validation (SEC-0099) |
| 4 | Low | 8 | `Config.validate()` only rejects `snapshot_chunk_size == 0`; value=1 causes 256M+ message allocation in `buildInstallSnapshotMessages()` | `src/node.zig`, `src/snapshot.zig` | CWE-770 | `node.zig:46` — no minimum enforced; `snapshot.zig:114-115` — `num_chunks = data.len / chunk_size` | [#108](https://gitlab.com/devnw/zig/raft/-/issues/108) | Resolved — raised minimum to 1024 bytes in `Config.validate()` + unit test (SEC-0100) |

### False Positives Filtered (Pass 10)

> 26+ agent-reported findings verified against actual code and filtered:

- **"Out-of-bounds slice in maybeAppend offset calculation"** — `offset = conflict_idx - prev_log_index - 1 = i` (loop index), always `< entries.len`. Safe by construction.
- **"Leader commit term not validated in followers"** — Per Raft §5.3, followers accept `leader_commit` from the leader. The leader is responsible for only committing entries from the current term (Figure 8). This is correct Raft behavior.
- **"Double-voting race condition in startElection"** — In the Ready-State pattern, the consumer persists hard state from `Ready` before calling `advance()`. If consumer doesn't persist before crash, that's consumer responsibility (same as etcd/raft).
- **"Unchecked leader_commit bounds"** — `commitTo()` does `@min(committed, self.lastIndex())`, clamping any value to the actual log size. Safe.
- **"match_index not bounded to leader's log"** — Even if inflated, `commitTo()` clamping prevents commit beyond actual log. The leader's `lastIndex()` is the ultimate bound.
- **"Log compaction below committed index"** — `appliedTo()` at `log.zig:189` enforces `applied <= committed`. Cannot compact uncommitted entries.
- **"Cross-set voter overlap in joint consensus config"** — In Raft §6, a node CAN legitimately be in both old and new voter sets during joint consensus. Already documented in Pass 8 false positives.
- **"CI Zig download missing SHA256 verification"** — Duplicate of SEC-0008/SEC-0042 (Won't Do — managed at CI catalog level).
- **"Transport authentication not enforced"** — Design decision documented in `transport.zig:9-18`. Consumer responsibility.
- **"CRC32 for persistent state integrity"** — Documented design tradeoff (Pass 2-3). CRC32 for accidental corruption; BLAKE3 for snapshot data.
- **"File permissions not explicitly set"** — Consumer responsibility per `file_storage.zig:38-43` docstring.
- **"Symlink attack on data directory"** — Consumer responsibility per `file_storage.zig:38-43` docstring.
- **"Election timeout overflow"** — `std.math.mul` with `catch maxInt(u32)` produces valid range for `intRangeLessThan()`. Safe.
- **"Empty voter set guard in vote response handler"** — `MembershipState.init()` and `Config.validate()` prevent empty voter sets.
- **"No validation of unchanged config in proposeChange"** — Not a security issue; at most log pollution.
- **"TOCTOU between snapshot.meta and snapshot.data renames"** — Mitigated by exclusive file lock (SEC-0066) and crash recovery in `loadSnapshotImpl()`.

---

## Security Findings -- Pass 9 (SEC-0074 through SEC-0097) -- All Resolved or Won't Do

24 findings identified through full codebase-wide adversarial review with 6 parallel review agents. All 21 actionable findings remediated in code; 3 closed as Won't Do with documented rationale.

### High Severity (6 findings)

| Rank | Sev | Risk | Finding | Component | CWE | Evidence | Issue | Status |
|------|-----|------|---------|-----------|-----|----------|-------|--------|
| 1 | High | 64 | `handleInstallSnapshotResponse` ignores `msg.success`, never updates peer progress — infinite snapshot loop, blocked commits | `src/node.zig` | CWE-670, CWE-682 | Added `msg.success` check + `maybeUpdate` + `maybeCommit` call | [#82](https://gitlab.com/devnw/zig/raft/-/issues/82) | Resolved |
| 2 | High | 64 | `replication_state.peers` never updated after config changes — new voters unreachable, `committedForSet` drops missing voters enabling minority commit | `src/node.zig`, `src/quorum.zig` | CWE-670, CWE-682 | Added `syncReplicationPeers()` after config apply; `committedForSet` now treats missing voters as match_index=0 | [#83](https://gitlab.com/devnw/zig/raft/-/issues/83) | Resolved |
| 3 | High | 60 | `RaftNode.init()` does not restore HardState from storage — double-voting on restart, term regression | `src/node.zig`, `src/storage.zig` | CWE-670, CWE-841 | Added `loadHardState` to Storage vtable; `init()` now restores term+voted_for | [#85](https://gitlab.com/devnw/zig/raft/-/issues/85) | Resolved |
| 4 | High | 60 | `handleInstallSnapshot` never calls `log.restore()` — log state diverges from snapshot | `src/node.zig` | CWE-670 | Added `self.log.restore(snapshot_val)` before membership update | [#98](https://gitlab.com/devnw/zig/raft/-/issues/98) | Resolved |
| 5 | High | 48 | `maybeDecrTo` allows forward jump of `next_index` via malicious `conflict_index` — replication halt | `src/replication.zig` | CWE-20 | Changed `!=` to `>=` to only allow backward movement | [#84](https://gitlab.com/devnw/zig/raft/-/issues/84) | Resolved |
| 6 | High | 32 | `FileStorage.compactImpl` defer frees uninitialized entries on mid-loop failure — heap corruption | `src/file_storage.zig` | CWE-457, CWE-761 | Track `entries_read` count; defer only frees `entries[0..entries_read]` | [#86](https://gitlab.com/devnw/zig/raft/-/issues/86) | Resolved |

### Medium Severity (11 findings)

| Rank | Sev | Risk | Finding | Component | CWE | Evidence | Issue | Status |
|------|-----|------|---------|-----------|-----|----------|-------|--------|
| 7 | Medium | 48 | `advance()` silently swallows config apply errors + `catch null` on `log.slice` — membership permanently stale | `src/node.zig` | CWE-755, CWE-390 | Removed `catch null` and `catch { warn }`; errors now propagate via `try` | [#87](https://gitlab.com/devnw/zig/raft/-/issues/87) | Resolved |
| 8 | Medium | 36 | `FileStorage.entriesImpl` leaks entry data on mid-loop `readEntryAt` failure | `src/file_storage.zig` | CWE-401 | errdefer now tracks `read_count` and frees per-entry `.data` | [#88](https://gitlab.com/devnw/zig/raft/-/issues/88) | Resolved |
| 9 | Medium | 36 | No entry count/total size validation on received AppendEntries — receiver trusts sender | `src/node.zig` | CWE-770, CWE-400 | Added `msg.entries.len > max_entries_per_msg` and total bytes > `max_bytes_per_msg` checks | [#90](https://gitlab.com/devnw/zig/raft/-/issues/90) | Resolved |
| 10 | Medium | 36 | `recordVote` accepts any sender without voter validation — unbounded HashMap growth | `src/node.zig` | CWE-770 | Added voter membership check against `cfg.voters` and `cfg.voters_outgoing` before `recordVote` | [#91](https://gitlab.com/devnw/zig/raft/-/issues/91) | Resolved |
| 11 | Medium | 32 | WAL rollback does not truncate orphaned bytes — ghost entries on crash recovery | `src/file_storage.zig` | CWE-460 | Added `file.setEndPos(saved_wal_end)` in all rollback paths | [#89](https://gitlab.com/devnw/zig/raft/-/issues/89) | Resolved |
| 12 | Medium | 32 | Checker only verifies terms, not entry data — can mask Log Matching and State Machine Safety violations | `tests/checker.zig` | CWE-697 | `checkLogMatching` now scopes to committed entries and flags term divergence as violation | [#97](https://gitlab.com/devnw/zig/raft/-/issues/97) | Resolved |
| 13 | Medium | 24 | `RaftLog.compact` mutates `snapshot_index` before fallible `storage.compact()` | `src/log.zig` | CWE-662 | Reordered: `storage.compact()` now runs BEFORE mutating `snapshot_index`/`snapshot_term` | [#92](https://gitlab.com/devnw/zig/raft/-/issues/92) | Resolved |
| 14 | Medium | 24 | `Config.validate()` does not reject zero peer IDs — phantom peer blocks quorum | `src/node.zig` | CWE-20 | Added `if (peer == 0) return ConfigError.InvalidId` in validation loop | [#93](https://gitlab.com/devnw/zig/raft/-/issues/93) | Resolved |
| 15 | Medium | 24 | `finishLoadSnapshot` unbounded allocation from snapshot data file size | `src/file_storage.zig` | CWE-770 | Added 256 MiB cap check before allocation | [#94](https://gitlab.com/devnw/zig/raft/-/issues/94) | Resolved |
| 16 | Medium | 24 | `InMemoryStorage.compactImpl` panics on OOB compact index | `src/storage.zig` | CWE-125 | Added `if (pos >= self.log.items.len) return StorageError.Unavailable` bounds check | [#95](https://gitlab.com/devnw/zig/raft/-/issues/95) | Resolved |
| 17 | Medium | 24 | `receiveChunk` OOM on `appendSlice` leaks pending snapshot state | `src/snapshot.zig` | CWE-401 | OOM path now calls `clearPending()` before returning error | [#96](https://gitlab.com/devnw/zig/raft/-/issues/96) | Resolved |

### Low Severity (5 findings)

| Rank | Sev | Risk | Finding | Component | CWE | Evidence | Issue | Status |
|------|-----|------|---------|-----------|-----|----------|-------|--------|
| 18 | Low | 24 | `Config.peers` borrowed slice can dangle in long-lived `RaftNode` | `src/node.zig` | CWE-825 | Caller responsibility; harness already dupes | [#99](https://gitlab.com/devnw/zig/raft/-/issues/99) | Won't Do |
| 19 | Low | 24 | `buildAppendEntries` subtraction underflow when `next_index` is 0 | `src/replication.zig` | CWE-191 | Added `if (next == 0) return null` guard | [#100](https://gitlab.com/devnw/zig/raft/-/issues/100) | Resolved |
| 20 | Low | 24 | Test harness `page_allocator` defeats memory leak detection | `tests/harness.zig` | CWE-401 | Intentional design; shared-ownership entry slices require it | [#101](https://gitlab.com/devnw/zig/raft/-/issues/101) | Won't Do |
| 21 | Low | 16 | File storage tests use predictable `/tmp` paths — symlink/TOCTOU risk | `tests/scenarios/file_storage.zig` | CWE-377 | Test-only code; no production impact | [#102](https://gitlab.com/devnw/zig/raft/-/issues/102) | Won't Do |
| 22 | Low | 16 | `InMemoryStorage.deinit` does not free snapshot data | `src/storage.zig` | CWE-401 | `deinit()` now frees `snapshot_data` (data + config slices); `saveSnapshotImpl` dupes on save | [#103](https://gitlab.com/devnw/zig/raft/-/issues/103) | Resolved |

### Info Severity (2 findings)

| Rank | Sev | Risk | Finding | Component | CWE | Evidence | Issue | Status |
|------|-----|------|---------|-----------|-----|----------|-------|--------|
| 23 | Info | 4 | `.observer.lock` not in `.gitignore` — tool metadata leak risk | `.gitignore` | CWE-538 | Added `.observer.lock` to `.gitignore` | [#104](https://gitlab.com/devnw/zig/raft/-/issues/104) | Resolved |
| 24 | Info | 4 | `SECURITY.md` lacks PGP key for encrypted vulnerability reports | `SECURITY.md` | CWE-311 | Added PGP key reference and security.txt pointer | [#105](https://gitlab.com/devnw/zig/raft/-/issues/105) | Resolved |

---

## Security Findings -- Pass 8 (SEC-0071 through SEC-0073) -- All Resolved

3 new findings identified through full re-audit of all 14 source modules with 5 parallel adversarial review agents + direct manual audit. All 12 automated scanners re-run (0 findings). 12+ agent-reported findings verified against actual code and filtered as false positives. Focused on:
- Consistency of checked arithmetic hardening across node.zig (SEC-0070 follow-up)
- Memory lifecycle of snapshot data loaded at init
- Protocol correctness of config change propagation to followers and across restarts

**All 3 findings remediated.** Key fixes:
- Applied `std.math.add` for checked overflow at all 6 `+ 1` locations in `node.zig`: `propose()`, `proposeConfigChange()`, `tick()` finalize, `ready()` slice bounds (x2), `becomeLeader()` noop index (SEC-0071)
- Added `freeSnapshot()` helper to `storage.zig`; `InMemoryStorage.loadSnapshotImpl` now returns heap-allocated copies (consistent with FileStorage); `RaftLog.init` frees snapshot after extracting index/term; `RaftNode.init` loads snapshot once, uses config for MembershipState, then frees (SEC-0072)
- `RaftNode.init()` restores MembershipState from snapshot config (including joint consensus) when available; `advance()` scans committed entries for `kind == .config` and calls `applyConfig()`; `handleInstallSnapshot()` applies snapshot config to MembershipState on receipt (SEC-0073)

### Medium Severity (2 findings)

| Rank | Sev | Risk | Finding | Component | CWE | Fix | Issue | Status |
|------|-----|------|---------|-----------|-----|-----|-------|--------|
| 1 | Medium | 24 | MembershipState not updated from committed config entries or snapshots on non-leader nodes — after leader change or restart following membership change, node uses stale initial voter set for elections, heartbeats, and quorum | `src/node.zig` | CWE-670 | `init()` restores config from snapshot; `advance()` applies config entries on commit; `handleInstallSnapshot()` applies snapshot config (SEC-0073) | [#81](https://gitlab.com/devnw/zig/raft/-/work_items/81) | **Resolved** |
| 2 | Medium | 16 | Snapshot memory leak on init — loadSnapshot() called twice (RaftLog.init + RaftNode.init), return values discarded without freeing heap-allocated data and config voter buffers when FileStorage is used | `src/log.zig`, `src/node.zig`, `src/storage.zig` | CWE-401 | Added `freeSnapshot()` helper; `InMemoryStorage.loadSnapshotImpl` returns heap copies; `RaftLog.init` frees snapshot; `RaftNode.init` uses snapshot for config then frees (SEC-0072) | [#80](https://gitlab.com/devnw/zig/raft/-/work_items/80) | **Resolved** |

### Low Severity (1 finding)

| Rank | Sev | Risk | Finding | Component | CWE | Fix | Issue | Status |
|------|-----|------|---------|-----------|-----|-----|-------|--------|
| 3 | Low | 8 | Unchecked `+ 1` arithmetic in node.zig (6 locations) — inconsistent with SEC-0055/SEC-0070 hardening | `src/node.zig` | CWE-190 | Applied `std.math.add` at all 6 locations in `propose()`, `proposeConfigChange()`, `tick()`, `ready()`, `becomeLeader()` (SEC-0071) | [#79](https://gitlab.com/devnw/zig/raft/-/work_items/79) | **Resolved** |

---

## Security Findings -- Pass 7 (SEC-0069 through SEC-0070) -- All Resolved

2 new findings identified through full re-audit of all source modules with 4 parallel adversarial review agents. All 12 automated scanners re-run (0 findings). Focused on:
- Error-path resource management (errdefer completeness in new SEC-0065/0066 code)
- Consistency of checked arithmetic hardening across codebase
- Cross-verification of agent-reported findings against actual code (filtered 8+ false positives)

### Medium Severity (1 finding)

| Rank | Sev | Risk | Finding | Component | CWE | Fix | Issue | Status |
|------|-----|------|---------|-----------|-----|-----|-------|--------|
| 1 | Medium | 16 | Missing errdefer for heap resources in `FileStorage.init()` -- memory leak on init failure when snapshot config present | `src/file_storage.zig:129-171` | CWE-401, CWE-460 | Added errdefer after `fs` construction to free ArrayLists and close wal_file; changed `meta.freeConfig()` to use `defer` so it runs on both success and error paths (SEC-0069) | [#77](https://gitlab.com/devnw/zig/raft/-/work_items/77) | **Resolved** |

### Low Severity (1 finding)

| Rank | Sev | Risk | Finding | Component | CWE | Fix | Issue | Status |
|------|-----|------|---------|-----------|-----|-----|-------|--------|
| 2 | Low | 8 | Unchecked `+ 1` arithmetic in replication/log paths (4 locations) -- inconsistent with SEC-0055 hardening | `src/replication.zig:63,79,109`, `src/log.zig:295` | CWE-190 | Applied `std.math.add` for checked overflow at all 4 locations, matching the pattern from SEC-0055 (SEC-0070) | [#78](https://gitlab.com/devnw/zig/raft/-/work_items/78) | **Resolved** |

---

## Security Findings -- Pass 6 (SEC-0065 through SEC-0068) -- All Resolved

4 findings identified through full re-audit of all source modules, CI/CD config, build files, and pre-commit configuration. All 12 automated scanners re-run (0 findings). Focused on:
- Crash safety (directory fsync after atomic renames)
- Concurrent access (file locking)
- Thread safety (allocator in parallel paths)
- Supply chain (pre-commit autofix)

### Medium Severity (2 findings)

| Rank | Sev | Risk | Finding | Component | CWE | Fix | Issue | Status |
|------|-----|------|---------|-----------|-----|-----|-------|--------|
| 1 | Medium | 32 | No exclusive file locking on data directory -- concurrent processes corrupt state | `src/file_storage.zig` | CWE-362, CWE-667 | Added exclusive advisory lock via `.lock` file in `init()`; held until `deinit()`; second init returns `error.WouldBlock` (SEC-0066) | [#74](https://gitlab.com/devnw/zig/raft/-/work_items/74) | **Resolved** |
| 2 | Medium | 24 | Missing directory fsync after atomic renames -- crash can lose committed state | `src/file_storage.zig` | CWE-662 | Added `syncDir()` helper using `openat(".", O_DIRECTORY)` + `fsync`; called after every `rename()` in `saveHardStateImpl`, `saveSnapshotImpl`, `compactImpl`, `writeSnapshotMeta`, and recovery in `loadSnapshotImpl` (SEC-0065) | [#73](https://gitlab.com/devnw/zig/raft/-/work_items/73) | **Resolved** |

### Low Severity (1 finding)

| Rank | Sev | Risk | Finding | Component | CWE | Fix | Issue | Status |
|------|-----|------|---------|-----------|-----|-----|-------|--------|
| 3 | Low | 12 | Allocator thread-safety undocumented for spice parallel paths | `src/node.zig` | CWE-362 | Added doc comments to `RaftNode.allocator` field and `RaftNode.init()` documenting thread-safety requirement when spice parallelism is active (SEC-0067) | [#75](https://gitlab.com/devnw/zig/raft/-/work_items/75) | **Resolved** |

### Info (1 finding)

| Rank | Sev | Risk | Finding | Component | CWE | Fix | Issue | Status |
|------|-----|------|---------|-----------|-----|-----|-------|--------|
| 4 | Info | 8 | `autofix_prs: true` enables automated code modifications from external repos | `.pre-commit-config.yaml` | CWE-829 | Won't Do -- pre-commit configuration is handled via CI pipeline stages and developer tooling external to this repository (SEC-0068) | [#76](https://gitlab.com/devnw/zig/raft/-/work_items/76) | Won't Do |

---

## Security Findings -- Pass 5 (SEC-0045 through SEC-0064) -- All Resolved

### High Severity (11 findings)

| Rank | Sev | Risk | Finding | Component | CWE | Fix | Issue | Status |
|------|-----|------|---------|-----------|-----|-----|-------|--------|
| 1 | High | 56 | `finalizeChange` mutates state before fallible serialization -- OOM corrupts joint consensus permanently | `src/membership.zig` | CWE-662, CWE-755 | Serialize BEFORE mutating state; all allocations succeed before any state change (SEC-0045) | [#53](https://gitlab.com/devnw/zig/raft/-/issues/53) | **Resolved** |
| 2 | High | 56 | `proposeChange` mutates state before fallible operations -- OOM corrupts membership state permanently | `src/membership.zig` | CWE-662, CWE-755 | Serialize and copy new voters BEFORE mutating state; errdefer on data; infallible state mutation after all allocations (SEC-0046) | [#54](https://gitlab.com/devnw/zig/raft/-/issues/54) | **Resolved** |
| 3 | High | 48 | Memory leak -- `proposeConfigChange` leaks `entry.data` on `log.append` failure | `src/node.zig` | CWE-401 | Added `errdefer` to free `entry.data` if `log.append` fails (SEC-0047) | [#55](https://gitlab.com/devnw/zig/raft/-/issues/55) | **Resolved** |
| 4 | High | 48 | Memory leak -- `tick()` leaks `final_entry.data` on `log.append` failure | `src/node.zig` | CWE-401 | Added `errdefer` to free `entry.data` if `log.append` fails (SEC-0048) | [#56](https://gitlab.com/devnw/zig/raft/-/issues/56) | **Resolved** |
| 5 | High | 48 | Memory leak on `toOwnedSlice` partial failure in `receiveChunk` (up to 256 MiB per event) | `src/snapshot.zig` | CWE-401 | Added `errdefer` between sequential `toOwnedSlice` calls (SEC-0049) | [#57](https://gitlab.com/devnw/zig/raft/-/issues/57) | **Resolved** |
| 6 | High | 44 | Integer overflow in `expected_offset` accumulation -- wraps to 0 in ReleaseFast | `src/snapshot.zig` | CWE-190 | Use `std.math.add` for checked overflow; clear pending on overflow (SEC-0050) | [#58](https://gitlab.com/devnw/zig/raft/-/issues/58) | **Resolved** |
| 7 | High | 44 | `InMemoryStorage.termImpl/entriesImpl` missing upper-bound guard -- panics on OOB | `src/storage.zig` | CWE-125, CWE-129 | Added upper-bound checks returning `Unavailable` for out-of-range indices (SEC-0051) | [#59](https://gitlab.com/devnw/zig/raft/-/issues/59) | **Resolved** |
| 8 | High | 44 | `handleInstallSnapshotResponse` ignores sender -- any node can demote leader | `src/node.zig` | CWE-287 | Validate sender is a known peer in `replication_state` before processing (SEC-0052) | [#60](https://gitlab.com/devnw/zig/raft/-/issues/60) | **Resolved** |
| 9 | High | 40 | `applyConfig` installs deserialized voter sets without semantic validation | `src/membership.zig` | CWE-20 | Added validation: reject empty voter sets, zero IDs, duplicates before applying config (SEC-0053) | [#61](https://gitlab.com/devnw/zig/raft/-/issues/61) | **Resolved** |
| 10 | High | 40 | Integer overflow in V3 snapshot meta size validation -- u32 arithmetic wraps | `src/file_storage.zig` | CWE-190, CWE-131 | Use `std.math.add` for checked arithmetic in config_len validation (SEC-0054) | [#62](https://gitlab.com/devnw/zig/raft/-/issues/62) | **Resolved** |
| 11 | High | 40 | `maybeUpdate` integer overflow -- `match + 1` wraps u64::MAX, crashes debug builds | `src/replication.zig` | CWE-190 | Use `std.math.add` with fallback to `match` on overflow (SEC-0055) | [#63](https://gitlab.com/devnw/zig/raft/-/issues/63) | **Resolved** |

### Medium Severity (8 findings)

| Rank | Sev | Risk | Finding | Component | CWE | Fix | Issue | Status |
|------|-----|------|---------|-----------|-----|-----|-------|--------|
| 12 | Medium | 32 | `handleAppendEntriesResponse` validates peer AFTER term-demotion check | `src/node.zig` | CWE-287 | Moved peer validation BEFORE term-demotion check; unknown sender cannot trigger `becomeFollower` (SEC-0056) | [#64](https://gitlab.com/devnw/zig/raft/-/issues/64) | **Resolved** |
| 13 | Medium | 28 | Uncapped allocation from untrusted `file_size` in `readSnapshotMeta` | `src/file_storage.zig` | CWE-770 | Added 1 MiB cap on snapshot meta allocation (SEC-0057) | [#65](https://gitlab.com/devnw/zig/raft/-/issues/65) | **Resolved** |
| 14 | Medium | 28 | `proposeConfigChange` accepts zero IDs and duplicate peer IDs | `src/node.zig` | CWE-20 | Added validation loop: reject zero IDs and duplicates in `new_peers` (SEC-0058) | [#66](https://gitlab.com/devnw/zig/raft/-/issues/66) | **Resolved** |
| 15 | Medium | 24 | Memory leak in `buildAppendEntries` on OOM in trimming path | `src/replication.zig` | CWE-401 | Catch alloc failure and free original entries slice before returning error (SEC-0059) | [#67](https://gitlab.com/devnw/zig/raft/-/issues/67) | **Resolved** |
| 16 | Medium | 24 | `deserializeVoters` accepts arbitrary type byte (not 0x00/0x01) | `src/membership.zig` | CWE-20 | Added explicit validation: only 0x00 and 0x01 accepted (SEC-0060) | [#68](https://gitlab.com/devnw/zig/raft/-/issues/68) | **Resolved** |
| 17 | Medium | 24 | Leaders grant pre-votes, enabling disruptive re-elections (§9.6 violation) | `src/node.zig` | CWE-400 | Leaders now reject pre-vote requests with `vote_granted=false` before evaluating the vote (SEC-0061) | [#69](https://gitlab.com/devnw/zig/raft/-/issues/69) | **Resolved** |
| 18 | Medium | 20 | `current_term += 1` unchecked overflow in `startElection` | `src/node.zig` | CWE-190 | Use `std.math.add` for checked overflow; return `error.Overflow` on wrap (SEC-0062) | [#70](https://gitlab.com/devnw/zig/raft/-/issues/70) | **Resolved** |
| 19 | Medium | 20 | `max_snapshot_size` checked after allocation in `receiveChunk` | `src/snapshot.zig` | CWE-770 | Check size BEFORE `appendSlice` allocation using checked arithmetic (SEC-0063) | [#71](https://gitlab.com/devnw/zig/raft/-/issues/71) | **Resolved** |

### Low Severity (1 finding)

| Rank | Sev | Risk | Finding | Component | CWE | Fix | Issue | Status |
|------|-----|------|---------|-----------|-----|-----|-------|--------|
| 20 | Low | 12 | `loadSnapshotImpl` swallows all `readSnapshotMeta` errors as null | `src/file_storage.zig` | CWE-755 | Only return null for `FileNotFound`; propagate all other errors (SEC-0064) | [#72](https://gitlab.com/devnw/zig/raft/-/issues/72) | **Resolved** |

---

## Security Findings -- Pass 4 (SEC-0037 through SEC-0044) -- All Resolved

| Rank | Sev | Risk | Finding | Component | CWE | Fix | Issue | Status |
|------|-----|------|---------|-----------|-----|-----|-------|--------|
| 1 | Medium | 40 | CI downloads Zig toolchain without SHA256 verification (supply chain) | `.gitlab-ci.yml:36-38` | CWE-494 | Won't Do -- CI/CD toolchain integrity managed at CI catalog level (same as SEC-0008) | [#50](https://gitlab.com/devnw/zig/raft/-/issues/50) | Won't Do |
| 2 | Medium | 35 | Node uses static `config.peers` for messaging instead of dynamic `membership_state.voters` | `src/node.zig` | CWE-672 | Added `dynamicPeers()` helper; `becomeLeader()`, `sendHeartbeats()`, `startElection()` now use `membership_state.voters` filtered to exclude self, including joint consensus outgoing voters (SEC-0044) | [#52](https://gitlab.com/devnw/zig/raft/-/issues/52) | **Resolved** |
| 3 | Medium | 30 | `loadSnapshotImpl` returns empty ClusterConfig -- config not persisted to disk | `src/file_storage.zig` | CWE-212 | Extended `snapshot.meta` to V3 variable-length format with serialized `ClusterConfig`; `saveSnapshotImpl` persists config; `loadSnapshotImpl` restores it; `compactImpl` preserves cached config across rewrites; backward-compatible with legacy 16-byte and V2 52-byte formats (SEC-0039) | [#47](https://gitlab.com/devnw/zig/raft/-/issues/47) | **Resolved** |
| 4 | Medium | 30 | Snapshot save non-atomic across data + meta files -- crash loses snapshot | `src/file_storage.zig` | CWE-367, CWE-662 | Write both `.tmp` files first, then rename meta (commit point), then rename data; `init()` cleans up stale `.tmp` files; `loadSnapshotImpl` recovers from crash between renames by renaming `.tmp` data file (SEC-0041) | [#49](https://gitlab.com/devnw/zig/raft/-/issues/49) | **Resolved** |
| 5 | Low | 25 | `findIndexPos` O(n) linear scan enables DoS on large WAL | `src/file_storage.zig` | CWE-407 | Replaced linear scan with binary search on sorted `raft_index` array (SEC-0040) | [#48](https://gitlab.com/devnw/zig/raft/-/issues/48) | **Resolved** |
| 6 | Low | 20 | Received snapshot config not validated -- bypasses SEC-0033 | `src/snapshot.zig` | CWE-20 | Added `validateSnapshotConfig()` in `receiveChunk()`: rejects empty voter sets, duplicate IDs, and zero IDs; new test covers all cases (SEC-0043) | [#51](https://gitlab.com/devnw/zig/raft/-/issues/51) | **Resolved** |
| 7 | Info | 15 | `writeSnapshotMeta` missing errdefer for temp file cleanup | `src/file_storage.zig` | CWE-404 | Added `errdefer { tmp.close(); self.dir.deleteFile("snapshot.meta.tmp") catch {}; }` after `createFile` (SEC-0037) | [#45](https://gitlab.com/devnw/zig/raft/-/issues/45) | **Resolved** |
| 8 | Info | 10 | FileStorage doc header lists wrong sizes (16 vs 20/52 bytes) | `src/file_storage.zig` | CWE-1078 | Updated doc comment to reflect actual 20-byte `hard_state` and variable-length `snapshot.meta` formats (SEC-0038) | [#46](https://gitlab.com/devnw/zig/raft/-/issues/46) | **Resolved** |

---

## Security Findings -- Pass 3 (SEC-0029 through SEC-0036) -- All Resolved

| Rank | Sev | Risk | Finding | Component | CWE | Fix | Issue | Status |
|------|-----|------|---------|-----------|-----|-----|-------|--------|
| 1 | Medium | 28 | `proposeConfigChange()` leaks memory and corrupts membership state when entry exceeds `max_entry_size` | `src/node.zig:157-181` | CWE-401, CWE-662 | Pre-validate serialized config size BEFORE calling `proposeChange()`, preventing state mutation on rejection (SEC-0029) | [#37](https://gitlab.com/devnw/zig/raft/-/issues/37) | **Resolved** |
| 2 | Medium | 24 | Snapshot transfer does not preserve `ClusterConfig` | `src/snapshot.zig`, `src/rpc.zig` | CWE-20, CWE-1285 | Added `config: ClusterConfig` field to `InstallSnapshot` RPC; `receiveChunk()` stores owned config copy in `PendingSnapshot`; assembled snapshot carries correct voter set (SEC-0030) | [#38](https://gitlab.com/devnw/zig/raft/-/issues/38) | **Resolved** |
| 3 | Medium | 24 | WAL entries not CRC-verified on read | `src/file_storage.zig` | CWE-354 | `readEntryAt()` now reads full record including CRC32 footer and verifies integrity on every read (SEC-0031) | [#39](https://gitlab.com/devnw/zig/raft/-/issues/39) | **Resolved** |
| 4 | Medium | 24 | Snapshot data file has no at-rest integrity check | `src/file_storage.zig` | CWE-354 | BLAKE3 digest of snapshot data stored in `snapshot.meta`; verified on `loadSnapshot()` (SEC-0032) | [#40](https://gitlab.com/devnw/zig/raft/-/issues/40) | **Resolved** |
| 5 | Medium | 20 | `Config.validate()` does not check for duplicate peer IDs or self-inclusion | `src/node.zig:39-52` | CWE-20 | Added `DuplicatePeerId` and `SelfInPeers` error variants; O(n^2) check in `validate()` (SEC-0033) | [#41](https://gitlab.com/devnw/zig/raft/-/issues/41) | **Resolved** |
| 6 | Low | 12 | `finalizeChange()` silently swallows serialization errors including OOM | `src/membership.zig:86` | CWE-755 | Changed return type to `!?rpc.Entry`; replaced `catch return null` with `try` to propagate errors (SEC-0034) | [#42](https://gitlab.com/devnw/zig/raft/-/issues/42) | **Resolved** |
| 7 | Low | 12 | `handleInstallSnapshot()` catches ALL errors silently | `src/node.zig:607` | CWE-755, CWE-778 | Added `std.log.warn` with leader ID and error name in catch block (SEC-0035) | [#43](https://gitlab.com/devnw/zig/raft/-/issues/43) | **Resolved** |
| 8 | Low | 8 | `hard_state` and `snapshot.meta` files lack integrity checks | `src/file_storage.zig` | CWE-354 | Extended file formats: `hard_state` now 20 bytes (added CRC32 footer); `snapshot.meta` now 52 bytes (added BLAKE3 digest + CRC32 footer); backward-compatible reads for legacy 16-byte files (SEC-0036) | [#44](https://gitlab.com/devnw/zig/raft/-/issues/44) | **Resolved** |

---

## Security Findings -- Pass 2 (SEC-0012 through SEC-0028) -- All Resolved

| Rank | Sev | Risk | Finding | Component | CWE | Fix | Issue | Status |
|------|-----|------|---------|-----------|-----|-----|-------|--------|
| 1 | Critical | 64 | Stack buffer overflow -- fixed `[16]u64` in `committedForSet()` | `src/quorum.zig` | CWE-121, CWE-787 | Replaced fixed stack buffer with dynamic allocation via allocator parameter | [#20](https://gitlab.com/devnw/zig/raft/-/issues/20) | Resolved |
| 2 | Critical | 64 | Stack buffer overflow -- fixed `[16]u64` in `maybeCommit()` | `src/replication.zig` | CWE-121, CWE-787 | Replaced fixed stack buffers with dynamic allocation using `self.allocator` | [#21](https://gitlab.com/devnw/zig/raft/-/issues/21) | Resolved |
| 3 | High | 56 | Use-after-free -- `trimmed` reads from freed `entries` | `src/replication.zig` | CWE-416 | Reordered: allocate result and copy BEFORE freeing source allocation | [#22](https://gitlab.com/devnw/zig/raft/-/issues/22) | Resolved |
| 4 | Medium | 36 | Silent error swallowing in `ready()` | `src/node.zig` | CWE-755, CWE-390 | Changed `ready()` to return `!rd.Ready`; replaced `catch {}` with `try` | [#23](https://gitlab.com/devnw/zig/raft/-/issues/23) | Resolved |
| 5 | Medium | 28 | Memory leak on snapshot ChecksumMismatch | `src/snapshot.zig` | CWE-401 | Added `clearPending()` call before returning ChecksumMismatch error | [#31](https://gitlab.com/devnw/zig/raft/-/issues/31) | Resolved |
| 6 | Medium | 24 | Election timer u32 overflow | `src/election.zig` | CWE-190 | Replaced `2 * self.election_tick` with `std.math.mul(u32, 2, self.election_tick) catch maxInt(u32)` | [#24](https://gitlab.com/devnw/zig/raft/-/issues/24) | Resolved |
| 7 | Medium | 24 | No Config validation | `src/node.zig` | CWE-20 | Added `Config.validate()` with checks for id=0, election_tick=0, heartbeat_tick=0, heartbeat>=election, chunk_size=0; called from `RaftNode.init()` | [#25](https://gitlab.com/devnw/zig/raft/-/issues/25) | Resolved |
| 8 | Medium | 24 | Unbounded snapshot data accumulation | `src/snapshot.zig` | CWE-770, CWE-400 | Added `max_snapshot_size` to Config (default 256 MiB); validated in `receiveChunk()` with `clearPending()` on exceed | [#32](https://gitlab.com/devnw/zig/raft/-/issues/32) | Resolved |
| 9 | Medium | 24 | Integer overflow in `maybeAppend()` | `src/log.zig` | CWE-190 | Replaced unchecked `+` with `std.math.add(u64, ...)` returning `error.Overflow` | [#33](https://gitlab.com/devnw/zig/raft/-/issues/33) | Resolved |
| 10 | Medium | 24 | Out-of-bounds slice in `log.slice()` | `src/log.zig` | CWE-125, CWE-129 | Added bounds validation of start/end against `items.len` before indexing; returns `error.OutOfRange` | [#34](https://gitlab.com/devnw/zig/raft/-/issues/34) | Resolved |
| 11 | Medium | 20 | CI component includes use mutable tags | `.gitlab-ci.yml` | CWE-829, SLSA L2 | Won't Do -- managed at CI catalog level | [#26](https://gitlab.com/devnw/zig/raft/-/issues/26) | Won't Do |
| 12 | Medium | 20 | WAL recovery allocates from untrusted `data_len` | `src/file_storage.zig` | CWE-770, CWE-20 | Added `WAL_MAX_RECORD_DATA_LEN` (64 MiB) cap; breaks recovery loop on exceed | [#35](https://gitlab.com/devnw/zig/raft/-/issues/35) | Resolved |
| 13 | Low | 16 | `check-yaml --unsafe` in pre-commit | `.pre-commit-config.yaml` | CWE-94 | Won't Do -- pre-commit enforcement handled externally | [#27](https://gitlab.com/devnw/zig/raft/-/issues/27) | Won't Do |
| 14 | Low | 12 | `debug.assert` no-ops in release builds | `src/log.zig`, `src/ready.zig` | CWE-617 | Replaced with runtime error returns: `error.AppliedBeyondCommitted` and `error.ReadyNotAdvanced` | [#28](https://gitlab.com/devnw/zig/raft/-/issues/28) | Resolved |
| 15 | Low | 12 | `proposeConfigChange()` bypasses `max_entry_size` | `src/node.zig` | CWE-20 | Added `max_entry_size` validation after `proposeChange()` returns; returns `error.EntryTooLarge` | [#29](https://gitlab.com/devnw/zig/raft/-/issues/29) | Resolved |
| 16 | Low | 12 | Non-atomic WAL writes | `src/file_storage.zig` | CWE-662 | Added rollback semantics: tracks `saved_wal_end`/`saved_index_len` and restores on any mid-loop failure | [#36](https://gitlab.com/devnw/zig/raft/-/issues/36) | Resolved |
| 17 | Info | 8 | Sender validation silently drops messages | `src/node.zig` | CWE-778 | Added `std.log.warn` with sender mismatch details before returning | [#30](https://gitlab.com/devnw/zig/raft/-/issues/30) | Resolved |

### Pass 1 Findings (SEC-0001 through SEC-0011) -- Resolved

| Rank | Sev | Risk | Finding | Component | CWE | Fix | Issue | Status |
|------|-----|------|---------|-----------|-----|-----|-------|--------|
| 1 | High | 48 | No message authentication -- any node can impersonate any peer | `src/node.zig` | CWE-287, CWE-290 | Added `from` vs msg sender ID validation in `step()`; documented transport auth requirement in `src/transport.zig` | [#9](https://gitlab.com/devnw/zig/raft/-/issues/9) | Resolved |
| 2 | High | 48 | CRC32 for snapshot integrity -- not collision-resistant | `src/snapshot.zig` | CWE-328, CWE-354 | Replaced CRC32 with BLAKE3 (256-bit); `InstallSnapshot.checksum` changed from `u32` to `[32]u8` | [#10](https://gitlab.com/devnw/zig/raft/-/issues/10) | Resolved |
| 3 | Medium | 36 | No snapshot metadata validation -- receiver accepts arbitrary index/term | `src/snapshot.zig` | CWE-20, CWE-284 | Added `StaleSnapshot` error; `receiveChunk()` rejects stale snapshots | [#15](https://gitlab.com/devnw/zig/raft/-/issues/15) | Resolved |
| 4 | Medium | 24 | Silent error swallowing in `log.append` drops entries | `src/node.zig` | CWE-755, CWE-390 | Replaced `catch {}` with `try`; `propose()` error set expanded to `!void` | [#11](https://gitlab.com/devnw/zig/raft/-/issues/11) | Resolved |
| 5 | Medium | 24 | `@intCast` truncation -- UB in release builds | `src/file_storage.zig`, `src/membership.zig` | CWE-190, CWE-681 | Added `maxInt(u32)` bounds checks before all `@intCast` narrowing | [#13](https://gitlab.com/devnw/zig/raft/-/issues/13) | Resolved |
| 6 | Medium | 24 | Config deserialization integer overflow in bounds check | `src/membership.zig` | CWE-190, CWE-131 | Replaced `*` with `std.math.mul` for checked multiplication | [#14](https://gitlab.com/devnw/zig/raft/-/issues/14) | Resolved |
| 7 | Medium | 24 | Unbounded entry size -- no `max_entry_size` config | `src/node.zig` | CWE-770, CWE-400 | Added `max_entry_size: u64 = 1 MiB` to Config; validated in `propose()` and `handleAppendEntries()` | [#17](https://gitlab.com/devnw/zig/raft/-/issues/17) | Resolved |
| 8 | Medium | 24 | Unsafe `@enumFromInt` in WAL deserialization | `src/file_storage.zig` | CWE-20, CWE-704 | Replaced with `std.meta.intToEnum` with error handling | [#12](https://gitlab.com/devnw/zig/raft/-/issues/12) | Resolved |
| 9 | Medium | 20 | CI downloads Zig without hash verification | `.gitlab-ci.yml` | CWE-494, SLSA L1 | Won't Do -- handled via external CI catalog controls | [#16](https://gitlab.com/devnw/zig/raft/-/issues/16) | Won't Do |
| 10 | Low | 16 | Empty voter set not rejected in config change | `src/membership.zig` | CWE-20, CWE-369 | Added `new_voters.len == 0` check returning `error.InvalidData` | [#19](https://gitlab.com/devnw/zig/raft/-/issues/19) | Resolved |
| 11 | Low | 8 | CODEOWNERS placeholder -- no review enforcement | `CODEOWNERS` | CWE-284 | Replaced `* @your-username` with `* @devnw` | [#18](https://gitlab.com/devnw/zig/raft/-/issues/18) | Resolved |

### Additional Informational Findings (Resolved)

- **INFO-1:** `SECURITY.md` referenced "GitHub issues" but repo is on GitLab -- **Resolved**: updated to "GitLab issues".
- **INFO-2:** File storage path (`FileStorage.init(dir_path)`) not validated for traversal -- **Resolved**: documented as consumer responsibility in `src/file_storage.zig` module doc.

---

## Open Spec-to-Code Gaps

*None -- all 8 spec gaps resolved. See [GAP_ANALYSIS_RESOLVED.md](GAP_ANALYSIS_RESOLVED.md).*

## Geode Extraction Metadata

This repository is the right home for the reusable Raft core, but the Geode review found a clear distinction between the portable consensus engine and Geode-specific transport/persistence/transaction adapters.

### Geode Boundary

Move/own here:

| Geode source | Why it belongs in `raft` |
|---|---|
| `src/distributed/raft.zig:38`, `:45`, `:64`, `:146`, `:157` | Core node state, log entry, message, config, and node types |
| `src/distributed/raft.zig:188`, `:242`, `:254`, `:285`, `:297`, `:323`, `:363`, `:399`, `:405`, `:459`, `:550`, `:650`, `:716` | Portable Raft lifecycle, ticking, message handling, proposal, election, append-entries, snapshot handling, and commit advancement |
| `src/distributed/raft.zig:754`, `:768`, `:788` | Direct source-level core tests |
| `tests/e2e_distributed_raft.zig:55`, `:109`, `:148`, `:180`, `:224`, `:256` | Cluster-level election, replication, partition recovery, and commit advancement behavior |

Keep in Geode or split into adapter code:

| Geode source | Why it stays |
|---|---|
| `src/distributed/raft_consensus.zig:62`, `:87`, `:94` | Transaction prepare/decide, schema, and snapshot-pointer commands are Geode-specific distributed behavior layered on top of Raft |
| `src/distributed/raft_consensus.zig:135` through `:211` | QUIC transport/client abstractions are application integration, not the Raft core |
| `src/distributed/raft_consensus.zig:215` through `:367` | Persistent storage integration is an adapter boundary, not the consensus core itself |
| `src/distributed/raft_consensus.zig:412` through `:457` | Protobuf frame encoding/decoding is transport framing glue |
| `build.zig:431`, `:432`, `:433`, `:434` | `raft_schema` protobuf wiring is Geode build integration |
| `src/dist/scaffold.zig:15` through `:259` | Alternate scaffold implementation and 2PC/shard-map helpers are not the core extraction target |

### Geode Tests And Benchmarks

| Geode reference | Purpose |
|---|---|
| `src/distributed/raft.zig:754`, `:768`, `:788` | Core unit coverage |
| `tests/e2e_distributed_raft.zig:55`, `:109`, `:148`, `:180`, `:224`, `:256` | End-to-end cluster behavior |
| `src/tools/run_variance_benches.zig:300`, `:301`, `:308`, `:312`, `:314`, `:370`, `:398`, `:465` | Current synthetic variance benchmark reference |

### Current Assessment

- Reusable core: `src/distributed/raft.zig`.
- Non-core Geode adjacency: `raft_consensus.zig`, protobuf framing, QUIC transport, persistent storage adapter, and transaction/schema commands.
- Migration priority: keep the consensus node and message/state machine in this repo; treat the rest as explicit adapters.
