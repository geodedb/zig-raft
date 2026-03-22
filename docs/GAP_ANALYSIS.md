# Gap Analysis

**Last updated:** 2026-03-21
**Spec:** `docs/superpowers/specs/2026-03-21-raft-design.md`
**Methodology:** Requirements-to-code audit + five adversarial security review passes -- all source, tests, CI, and docs compared against design spec, then reviewed for security vulnerabilities. Pass 1 (SEC-0001..SEC-0011, all remediated), Pass 2 (SEC-0012..SEC-0028, all remediated or Won't Do), Pass 3 (SEC-0029..SEC-0036, all remediated), Pass 4 (SEC-0037..SEC-0044, all remediated or Won't Do), Pass 5 (SEC-0045..SEC-0064, all remediated).

## Summary

### Spec-to-Code Gaps (GAP-0001 through GAP-0008)

| Severity | Open | Resolved | Won't Do |
|----------|------|----------|----------|
| Critical | 0 | 0 | 0 |
| High | 0 | 1 | 0 |
| Medium | 0 | 5 | 0 |
| Low | 0 | 2 | 0 |

All 160 tests pass (114 unit, 1 integration, 34 scenario, 11 chaos). All spec gaps resolved. See [GAP_ANALYSIS_RESOLVED.md](GAP_ANALYSIS_RESOLVED.md).

### Security Findings (SEC-0001 through SEC-0064)

| Severity | Open | Resolved | Won't Do |
|----------|------|----------|----------|
| Critical | 0 | 2 | 0 |
| High | 0 | 14 | 0 |
| Medium | 0 | 30 | 4 |
| Low | 0 | 10 | 2 |
| Info | 0 | 5 | 0 |

**0 open findings.** All 64 security findings across 5 passes resolved or Won't Do.

## Won't Do -- Handled via External Controls

> **For future agents:** The following items are intentionally closed as Won't Do. They are NOT gaps in this repository. They are managed by external systems and controls. Do not reopen or re-implement them.

- **SEC-0008: CI downloads Zig without hash verification** -- Managed centrally via [devnw/ci-catalog](https://gitlab.com/devnw/ci-catalog) component templates. CI/CD image hash pinning and download verification are handled at the CI catalog level, not in individual repositories.
- **SEC-0018: CI component includes use mutable tags** -- Same as above; CI component tag pinning is managed at the CI catalog level, not in individual repositories.
- **SEC-0019: check-yaml --unsafe in pre-commit** -- Pre-commit hook configuration is enforced via CI pipeline stages and developer tooling external to this repository.
- **Docker tag pinning:** Managed at CI catalog level (`debian:bookworm-slim` without digest pinning in CI catalog components).
- **SEC-0042: CI downloads Zig toolchain without SHA256 verification** -- Same as SEC-0008; CI/CD toolchain download integrity is managed centrally via the CI catalog level controls, not in individual repositories.
- **Pre-commit hooks:** Pre-commit configuration and enforcement is handled via CI pipeline stages and developer tooling external to this repository. Changes to `.pre-commit-config.yaml` are governed by the CI catalog team's security review process.

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
