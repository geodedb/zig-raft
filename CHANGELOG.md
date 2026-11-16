# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.4] - 2026-11-16

### Changed

- wal v0.1.24 -> v0.1.26 and otel v0.1.28 -> v0.1.29, so raft and its
  consumers resolve a single thread-pool (SONNY-6630).

## [0.2.3] - 2026-11-15

### Fixed

- `InProcessTransport` queued an InstallSnapshot with the leader's chunk
  data and configuration slices, which the leader frees as soon as `send`
  returns: the receiver read freed memory. It now copies them, and
  `freeMessage` frees them (SONNY-6636).
- A refused InstallSnapshot was re-sent on every heartbeat round. It now
  stays in flight and each consecutive refusal doubles the wait before the
  next send (`PeerProgress.snapshot_refusals`, `snapshotResendTicks`), from
  16 rounds up to `SNAPSHOT_RESEND_MAX_TICKS` (256); an install resets it.
  A snapshot lost without an answer is still re-sent after
  `SNAPSHOT_RESEND_TICKS` (SONNY-6636).

## [0.2.2] - 2026-11-15

### Fixed

- The leader counted entries it had not yet persisted toward commit, so an
  entry could commit while durable on less than a majority, and a
  single-node cluster committed an entry before it was on disk. The leader
  now counts only `RaftLog.stableLastIndex()` (new), and `advanceApply()`
  re-evaluates the commit index after stabilizing (SONNY-6611).
- A second `ready()` before `advance()` added the committed entries again;
  they were applied and counted twice. It now returns `ReadyNotAdvanced`
  before touching any state.
- A snapshot installed between `ready()` and `advance()` was persisted but
  never delivered in a Ready, and installed snapshots were leaked.
  `ReadyState` now owns the snapshot (freed at the `advance()` after the
  Ready that delivers it) and keeps one installed mid-cycle for the next
  Ready.
- A leader removed by a membership change stepped down as soon as it
  appended the new configuration; it now leads until that configuration
  commits (Raft §4.2.2).
- Fast backtracking: a rejection carries the rejected `prev_log_index` (in
  `match_index`), the follower hints `lastIndex + 1` (log too short) or its
  conflicting term and that term's first index, and the leader skips to
  its last entry of that term (`replication.backtrackHint`, new). Stale and
  duplicate rejections are ignored; rejections without a position (older
  followers, transports without the field) back off as before.
- Saving a snapshot dropped none of the log it covers, and the following
  `compact()` returned early: `FileStorage` never compacted its WAL or
  index and a follower kept a conflicting tail, and `InMemoryStorage` read
  misaligned entries after a snapshot. Both now apply Raft §7: keep the
  entries after the snapshot if the log holds its last entry with the same
  term, otherwise drop the whole log.
- `FileStorage.compact` left the snapshot point advanced when the WAL
  compaction failed; it is now rolled back (except on `WalPoisoned`).
- `FileStorage` conflict truncation over a damaged record silently cut the
  log at the damage; it now refuses with `StorageError.LogCorrupted`, maps
  wal's errors, and shrinks its index only after the WAL is truncated.
- `examples/basic.zig` did not build; it is now a runnable example of the
  consumer loop, and `zig build check` compiles the examples.

## [0.2.1] - 2026-11-15

### Changed

- Re-pin wal to v0.1.24 (SONNY-6622), which also moves thread-pool to
  v0.2.11. wal recovery and compaction now fail closed on data they cannot
  read and never truncate or rewrite valid records; only a torn tail is
  truncated on open.
- `StorageError` gains `LogCorrupted`, `LogAuthFailed` and
  `LogEncryptionMismatch`.

### Fixed

- `FileStorage` opened an entry log with a damaged record in the middle by
  truncating it there, silently losing every entry after it. It now refuses
  to open with `StorageError.LogCorrupted` and leaves the file untouched; an
  encrypted log, or one that does not authenticate, is refused with
  `LogEncryptionMismatch` / `LogAuthFailed` (SONNY-6622).
- A WAL compaction refused after a snapshot is logged rather than failing
  `compact`: the snapshot is durable and the WAL stays in use. Records the
  refused compaction leaves behind are skipped on recovery.

## [0.2.0] - 2026-11-15

### Changed (breaking)

- `Ready.messages` is now `[]const Outbound` (`{ to: u64, msg: Message }`,
  `raft.Outbound`) and `ReadyState.addMessage` takes the destination.
  RequestVote, AppendEntries and InstallSnapshot replies, and a candidate's
  RequestVote, are no longer sent through the `Transport` from inside
  `step()` / `tick()`; they are queued in the Ready for the consumer to send
  after `advancePersist()` and before `advanceApply()` (SONNY-6592). Leader
  replication (AppendEntries, InstallSnapshot) still goes through the
  `Transport` at once. A consumer that only calls `advance()` never sends
  votes or acks.
- `Storage.entries` and `RaftLog.slice` hand the caller both the slice and
  each entry's data; free them with the new `raft.freeEntries`.
  `InMemoryStorage.entries` now returns copies, as `FileStorage` does, and a
  custom `Storage` must do the same. A transport that keeps an AppendEntries
  past `send()` must copy its entries' data (`raft.dupeEntries`).

### Fixed

- Entry data read back through `Storage.entries` leaked: committed entries
  delivered by `ready()` and AppendEntries built from stable entries (and
  the entries trimmed off by `max_bytes_per_msg`) freed only the outer slice
  (SONNY-6592).
- Votes and AppendEntries / InstallSnapshot acks went out before the hard
  state and entries they announce were persisted; a crash in between could
  lose a granted vote or acked entries (SONNY-6592).
- A message queued between `ready()` and `advance()` was dropped by
  `advance()`; it now goes in the next Ready.
- A stale AppendEntries rejection delivered after a snapshot install could
  move the leader's `next_index` below the acked `match_index`, re-sending
  the snapshot forever (`PeerProgress.maybeDecrTo`).
- A node removed from the voter set kept starting elections and could be
  re-elected by peers still in the joint configuration.
- `InProcessTransport` aliased AppendEntries entry data and leaked the
  copies it queued; it now deep-copies and frees them (`freeMessage`).

### Added

- `raft.freeEntries`, `raft.dupeEntries`, `raft.Outbound`,
  `InProcessTransport.freeMessage`, `ReadyState.addOwnedCommittedEntries`.

## [0.1.22] - 2026-11-12

### Changed

- Converge wal v0.1.23, which keeps the LSN anchor when compaction empties
  the log (SONNY-6424). Geode, graph-storage and timeseries-storage pin the
  same wal revision.

## [0.1.16] - 2026-09-15

### Changed

- Converge grpc_zig v0.1.10 (GAP-0838 diamond sync).

## [0.1.15] - 2026-09-12

### Changed

- Converge thread_pool v0.2.7, fips v0.1.1, protobuf v0.1.5, config v0.1.13 (GAP-0838 diamond sync).

## [0.1.14] - 2026-09-12

### Changed

- Converge thread_pool v0.2.7, fips v0.1.1, protobuf v0.1.5 (GAP-0838 diamond sync).

## [0.1.9]

### Fixed

- **Candidate liveness backstop, part 2 (RC-B it.2): timer-based candidate
  demotion + backoff.** v0.1.8's `VoteLost` demotion only fires when a *quorum
  rejects* the candidate. A transport-degraded / isolated candidate — or a
  pre-vote *mutual-grant* lockstep where peers grant each other's pre-votes so no
  rejection quorum ever forms — collects neither a winning nor a rejecting
  quorum and, without a timer, stays a candidate indefinitely (the residual half
  of the geode k8s no-leader frozen-term state, observed on `v0.6.27-30`). Now,
  after `Config.candidate_demote_threshold` (default 4) consecutive failed
  candidate elections, the node steps down to follower and applies a growing,
  capped, randomized election backoff (`ElectionState.applyElectionBackoff`),
  desynchronizing lockstep peers so one campaigns ahead and wins. Liveness-only —
  no term/log change, no safety property affected; `candidate_demote_threshold=0`
  restores the prior re-elect-forever behaviour. New `Stats.candidate_demotions`.
  - Regression scenario `stuck candidate demotes to follower after consecutive
    failed elections` (RED before the fix: an isolated pre-vote candidate never
    demotes) + `applyElectionBackoff` unit test in `election.zig`.

## [0.1.8]

### Fixed

- **Candidate liveness backstop (RC-B): etcd-style demotion on quorum vote
  rejection.** `handleRequestVoteResponse` previously detected only a vote WIN
  (`hasJointQuorum`); a candidate denied by a quorum stayed in the election loop
  until its election timer fired, so a node frozen behind the cluster under
  pre-vote (or a split-vote loser) never stepped down to grant a competitor's
  vote. Now, mirroring etcd's `case quorum.VoteLost: r.becomeFollower(...)`, a
  candidate (pre-vote or real) rejected by a quorum demotes to follower at the
  current term and resets its randomized election timer. Consensus-layer half of
  the geode k8s no-leader all-candidate livelock (geode
  `docs/bugs/2026-06-04-cluster-raft-no-stable-quorum-writes-time-out.md`, RC-B;
  closes `ISSUE-candidate-liveness-backstop` Option A). Liveness-only —
  `becomeFollower` keeps the current term, so no safety property changes.
  - New `ElectionState.hasRejectQuorum` / `hasJointRejectQuorum` (loss-side dual
    of `hasQuorum` / `hasJointQuorum`), unit-tested in `election.zig`.
  - Regression scenarios `candidate_demotes_on_quorum_prevote_rejection` and
    `candidate_demotes_on_quorum_realvote_rejection` in
    `tests/scenarios/election.zig`.

## [0.1.0] - YYYY-MM-DD

### Added

- Initial release
- Basic functionality
- Documentation and examples

[Unreleased]: https://gitlab.com/devnw/zig/raft/-/compare/v0.1.0...HEAD
[0.1.0]: https://gitlab.com/devnw/zig/raft/-/releases/v0.1.0
