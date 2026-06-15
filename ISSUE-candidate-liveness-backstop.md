# hardening: candidate-side liveness backstop — same-term step-down requires inbound AppendEntries (pre-vote denial discards live-leader signal)

**Project:** devnw/zig/raft
**Type:** enhancement / defense-in-depth hardening
**Priority:** low (v0.1.7+ candidate)
**Filed:** 2026-06-12
**Relates to:** geode incident `docs/bugs/2026-06-04-cluster-raft-no-stable-quorum-writes-time-out.md`

> **RESOLVED 2026-06-14 (v0.1.8) — Option A implemented.** `handleRequestVoteResponse`
> now demotes a quorum-denied candidate (pre-vote OR real) to follower at the
> current term (etcd `quorum.VoteLost` parity), via new
> `ElectionState.hasRejectQuorum` / `hasJointRejectQuorum`. Regression scenarios
> `candidate_demotes_on_quorum_{pre,real}vote_rejection`. Option B
> (denial carries `leader_id`/`leader_term`) deferred — needs a wire-format
> change and is unnecessary for the live-leader-exists case Option A covers.
>
> **EXTENDED 2026-06-15 (v0.1.9) — timer-based demotion.** Option A only fires on
> a *quorum rejection*; a transport-degraded candidate or a pre-vote
> *mutual-grant* lockstep (peers grant each other's pre-votes → no rejection
> quorum ever forms) collects no quorum at all and stayed a candidate forever
> (geode `v0.6.27-30` k8s residual). Added `Config.candidate_demote_threshold`
> (default 4): after N consecutive failed candidate elections, step down to
> follower + `ElectionState.applyElectionBackoff` (growing, capped, randomized),
> desynchronizing lockstep peers. Liveness-only.

---

## Background — the geode incident that surfaced this

In `geodedb/geode` (which pins `devnw/zig/raft`), a sustained-churn k8s test (3
back-to-back 4-kill rounds with no settle time) stranded a candidate node at the
quorum's term for 70 s+. Root cause RC-A was **geode-side**: the inbound connection
pool (4 workers) accumulated zombie connections faster than they were reaped, starving
the node's `raft.handleMessage` path — it never received the leader's `AppendEntries`
and therefore never reached the raft lib's sole same-term step-down at `node.zig:937`.

RC-A is fixed in geode (`42c319e9` — accept-time idle clocks + independent inbound
sweeper). With the delivery path restored, the candidate steps down correctly on
inbound `AppendEntries` receipt.

**This issue is NOT about that fix.** It documents a **design gap in the raft library
itself** that became visible during triage: the library's same-term candidate step-down
requires an inbound `AppendEntries`, but pre-vote denial from a quorum — which carries
the information that a live leader exists — is silently discarded without acting on it.

---

## Design gap

### Sole same-term step-down path (`node.zig:937`)

```text
// node.zig ~line 937 (approximate — exact line may shift across versions)
// AppendEntries handler: if msg.term == self.term && self.state == .candidate
//   → step down to follower
```

A candidate steps down to follower (same term) **only** when it receives an inbound
`AppendEntries` from the current leader. This is the standard Raft paper behaviour.

### Pre-vote denial path (`node.zig:837-854`, approximate)

```text
// node.zig ~lines 837-854
// Pre-vote response handler: tally denials from peers
// If denied by quorum → ... (remains candidate, retries election)
```

When a candidate solicits pre-votes and a quorum denies it, the denial responses
currently carry no `leader_id` or `leader_term` field. The candidate tallies the
denials, sees it cannot win an election in this term, and **stays in the election loop
— it does not step down, and it does not learn who the current leader is**.

### Why this matters

In a healthy cluster with a functioning delivery path, the leader's `AppendEntries`
heartbeats arrive well within the election timeout, so the candidate quickly steps down
via `node.zig:937`. RC-A (geode inbound starvation) was the real blocker here.

However, in scenarios with **flaky directional links** — where a specific peer's
*outbound* (carrying `AppendEntries`) is dead while *inbound* (carrying votes/denials)
is alive — a stranded candidate may receive quorum pre-vote denials (proving a live
leader exists and has a quorum) yet remain stuck in the election loop indefinitely,
because the only step-down signal it knows about is a direct `AppendEntries` delivery
it will never receive.

This is a defense-in-depth gap. It does not cause incorrect behaviour (safety is
preserved), but it increases recovery latency in directional-link-failure scenarios.

---

## Candidate fixes

### Option A — etcd-style: demote pre-candidate to follower on quorum pre-vote rejection

etcd's implementation demotes a `pre-candidate` (which is effectively what a candidate
doing pre-vote is) back to `follower` state when it receives quorum pre-vote
rejections. The rationale: if a quorum says "no, don't start an election", a live
leader must exist; the right action is to stop candidating and wait for a heartbeat.

**Pros:** simple, no wire change, closes the gap.
**Cons:** requires tracking whether pre-vote denials form a quorum (similar to the
vote-grant tally already in place) and a state transition on that condition.

### Option B — denial response carries `leader_id` / `leader_term`

Extend the pre-vote denial response (or a new optional field) to carry the denier's
known `{leader_id, leader_term}` if it has one. The candidate, on receiving a denial
that carries a live-leader hint, can:

- adopt `leader_term` (step up its term if behind), and
- transition to follower and await `AppendEntries` from `leader_id`.

**Pros:** richer signal; helps a candidate that is also term-behind.
**Cons:** wire format change; denying peers may not always know the current leader
(e.g., they are themselves followers that haven't heard a heartbeat in a while).

### Option C — no change; fix at the consumer layer

Ensure the delivery path (inbound `AppendEntries`) is always reachable, so the
existing `node.zig:937` step-down fires promptly. This is what geode did with RC-A.
**Does not close the lib-level gap** for consumers with directional link failures.

---

## Recommendation

Option A is the most robust and lowest-risk change. It requires no wire format change
and closes the gap for the most common scenario (live leader exists, candidate's
inbound AppendEntries path is dead). It is a v0.1.7+ candidate — not urgent, and
should not be rushed.

Option B is a useful complement if/when the protocol is extended, but should wait
until the wire format implications are fully designed.

---

## Context for lib maintainers

- **geode's pin:** `devnw/zig/raft` currently at `v0.1.6`
- **Incident doc:** `geodedb/geode` repo,
  `docs/bugs/2026-06-04-cluster-raft-no-stable-quorum-writes-time-out.md`,
  section "2026-06-12 (later) — Sustained-churn stranded-candidate fixed"
- **geode-side fix:** commit `42c319e9` (accept-time idle clocks + independent inbound
  sweeper); this issue is the lib-level defense-in-depth complement, not a blocker for
  that fix.
- **Safety:** no safety property is violated by the current design; this is purely a
  liveness/recovery-latency improvement.
