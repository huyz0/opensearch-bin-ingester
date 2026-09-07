# M4 — Sequencer and commit log

Leases, epoch fencing, write-once deltas, checkpoints. The coordination layer,
built on compare-and-swap against the object store rather than on a consensus
cluster.

⚠️ **This is the highest-risk component in the system**, and both ADR-0002
(`:41-43`) and the research corpus say so in those words
([03-metadata-and-cas.md](../../../../research/30-design-space/03-metadata-and-cas.md)
carries `confidence: medium` for exactly this reason). It is the milestone where
"tested by hoping" would be least visible and most expensive.

## Completion condition

CAS leases, epoch fencing, write-once deltas and checkpoints exist, and
**invariants I1–I5 hold across 1,000 deterministic simulation seeds**.

⚠️ **I1–I5, not I1–I4, and the roadmap row is stale.**
[architecture.md](../../architecture.md) defines exactly five invariants, and
[testing.md](../../../standards/testing.md) rule 20 requires the simulation to
assert "I1–I5 (architecture.md defines exactly those five)". The `I1–I4`
citations in [roadmap.md](../../roadmap.md), ADR-0002 `:43`,
`40-implementation/03-benchmarking-plan.md:118` and
`30-design-space/08-failure-domains-and-resilience.md:213` — and
[`spec`](../../../../../.agents/skills/spec/SKILL.md)`:40` (the worked example
M5's spec author will copy) and
[`review`](../../../../../.agents/skills/review/SKILL.md)`:93` — all predate I5, which
[ADR-0011](../../decisions/0011-no-consensus-cluster.md) added on 2026-08-30 (a
day after ADR-0002). **Dropping I5 would drop the one invariant the corpus
singles out as "the one a plausible implementation violates by accident"**
([03-metadata-and-cas.md:219-221](../../../../research/30-design-space/03-metadata-and-cas.md)).
This milestone asserts five; the roadmap row is corrected in the same commit as
this spec. ⚠️ The ADR-0002 citation is deliberately **not** corrected — an ADR is
a dated record of what was believed then, and rewriting its body is what the
`adr` skill's staleness rule forbids.

## ⚠️ Deployment constraint this milestone introduces, and where it is resolved

**Read this before the scope table; round-1 review found it missing and it is
the single most consequential thing about M4.**

Today every ingester node calls `CommitLog.commit` directly and races on
`putIfAbsent`, retrying when it loses. That is safe for I1 — write-once *is* the
safety mechanism — but it has no leader, so nothing can be sealed and nothing can
be fenced. M4 introduces a lease so that **exactly one sequencer writes the
chain**. The other pods in an HA deployment (the minimum is six) then need a way
to get their commits to that sequencer. That way is **commit forwarding over the
peer mesh** ([architecture.md](../../architecture.md) `:48-53` — "Pods talk
directly — commit forwarding, directed push fan-out, and intra-AZ segment
fetch"), and **no milestone currently owns it**.

**Resolution, and it is a scoping decision this spec makes explicitly:**

- M4 defines the **`Sequencer` seam** with a contract designed for forwarding
  from the outset — the commit request carries `(podId, incarnationId, flushSeq, segments…)`
  precisely so that it is meaningful when it arrives from *another* pod, and so
  the record shape does not have to change when the remote implementation lands.
  M4.10's idempotency exists for exactly this reason.
- M4 ships the **local implementation**: the process holding the lease commits
  directly. A single-NODE deployment is fully correct at the end of M4 (under
  S = 1 every deployment has one sequencer; what a multi-node one lacks is the
  path from the other nodes to it).
- The **remote implementation (commit forwarding as an RPC over the peer mesh)
  is OUT of M4 and belongs with the peer mesh in M5**, which already builds the
  mesh for directed push fan-out and intra-AZ segment fetch. Building a
  pod-to-pod transport in M4 would pull the `EndpointSlice` watch, placement and
  an RPC surface into the coordination milestone.
- **Therefore, at the end of M4 a multi-pod deployment is not yet correct**, and
  saying so plainly is the point of this section. The simulation compensates: it
  drives **many logical pods through the seam** without a network, so multi-pod
  commit ordering, idempotency and fencing are all proven at M4 — what is
  deferred is the transport, not the correctness argument.
- ⚠️ M5's row in [roadmap.md](../../roadmap.md) is updated in the same commit as
  this spec to name commit forwarding, so the obligation is written down where
  the next milestone will be planned rather than living only here.

## Requirements

| ID | Requirement |
|---|---|
| FR-3 | Stable, monotonic `long` offset per `(index, partition)`, immutable once visible |
| FR-4 | Acknowledge a write only after the segment is durable **and** the offset is committed |
| FR-11 | Sequencer leadership by object-store CAS lease with epoch fencing and a write-once commit log |
| FR-10 | Consumers keep working without the ingester — M4 supplies the recovery path that is **tier 3** of the fallback ladder; the ladder itself is M5's |
| NFR-8 | RPO for acked writes = 0 |
| NFR-9 | RTO for offset visibility after sequencer loss < 5 s. ⚠️ **Not met by M4 alone** — see acceptance criterion 8 |
| NFR-11 | Offset stability across sequencer failover — absolute |
| NFR-1 | Write request rate < 0.30 requests per MiB (the commit chain is one writer, see *Cost impact*) |

## The invariants, verbatim

The simulation asserts these and nothing looser. Taken from
[architecture.md](../../architecture.md); where the research original
([03-metadata-and-cas.md:213-221](../../../../research/30-design-space/03-metadata-and-cas.md))
says more, the extra clause is kept because it is the testable part.

| # | Invariant | The testable clause |
|---|---|---|
| I1 | No commit-log sequence number is ever written twice | guaranteed by `putIfAbsent`; two writers racing one `seq` produce exactly one winner |
| I2 | A committed offset is never reassigned | ⚠️ load-bearing for **OpenSearch** consistency, not only our replay: under `all_active` two replicas converge only because record *R* is at offset *N* on both |
| I3 | A reader applies only deltas in a sealed prefix or a later epoch's chain | **"never in a discarded suffix"** — the research wording, kept |
| I4 | Uncommitted records may be reordered or dropped; committed ones may not | |
| I5 | No acknowledged commit exists beyond a `SEAL` in its own chain | a commit is acked only after every lower-numbered write is confirmed (ADR-0011) |

## Scope

**In:**

1. **The `Sequencer` seam and the `sequencer` module.** The module exists
   (`sequencer/build.gradle.kts`, `README.md`) with **zero Java sources**.
   `Sequencer` is one of the four declared I/O seams
   ([architecture.md](../../architecture.md) `:67`), so it gets an interface, a
   production implementation and a fake, "kept in step in the same commit". Its
   contract is stated explicitly: what a commit request carries, what is
   returned, which failures are retryable, and what a caller may assume once it
   returns.
2. **CAS lease with epoch fencing**, against `<prefix>/ctl/lease/<slot>.json`.
   ⚠️ **First acquisition is `putIfAbsent`, not `putIfMatch`** — round-2 review
   caught this against the SPI's own contract: `putIfMatch` on an ABSENT key
   *throws*, because "there is no version to have moved from", and that is
   deliberately not folded into the lost-race empty `Optional`. So the lease has
   two acquisition paths: `putIfAbsent` when no lease object exists yet, and
   `putIfMatch` to take over an existing expired one or to renew.
   Epoch is a **counter, not a clock**
   (ADR-0002; SlateDB's pattern). TTL and renew interval are **configuration**,
   not constants — measurement M1 in
   [50-open-questions.md](../../../../research/50-open-questions.md) settles the
   values at M8, and this milestone must not hardcode what M8 will measure.
3. **Epoch in the object path.** `ctl/log/<slot>/<epoch:016x>/<seq:016x>.delta`,
   so a fenced leader's in-flight PUT lands where readers of the new epoch never
   look. Fencing does not depend on catching the write in time.
4. **The seal protocol, both branches.** A new leader seals the old chain by
   racing for the next sequence number. **The two losers are different**: the
   fenced old leader stops, while the new leader — which holds a valid lease —
   **redrives**. See *Design*.
5. **Commit batching.** One delta carries every stream and every contributing
   pod flush in a `commitBatchInterval` window. ⚠️ This is the dimension the cost
   model's $52/month depends on, so it is built here rather than assumed.
6. **Checkpoints, with their key grammar and a bounded discovery path.**
7. **The `SEAL` and `CONTINUE` wire shapes**, with the version bump and the
    empty-runs guard they collide with. ⚠️ Called out as its own scope item
   because round-4 review found it had a task and a test row but no scope item
    and no acceptance criterion — so a variant that stopped parsing v0 could
    have shipped with every criterion green.
8. **Bounded recovery**: newest checkpoint + at most K deltas, replacing today's
   unbounded full-chain replay.
9. **Commit idempotency.** ⚠️ AMENDED BY ADR-0036 (2026-09-07): the key is
   `(podId, incarnationId, flushSeq)`, not the pair. The pair cannot
   discriminate a pod restart -- `flushSeq` restarts at 0 while `podShortId` is
   stable -- so dedup on it SUPPRESSES real commits. `lastAppliedFlushSeq`
   survives, carried in checkpoints beside the incarnation and a pointer.
10. **The ack-ordering rule (I5).** Pipelining for throughput is permitted;
   acknowledging out of order is not.
11. **Deterministic simulation**: injectable clock, fault-injecting store,
    delayed writes, reordered completions, partitioned leaders, duplicated
    in-flight PUTs, **and many logical pods driven through the seam**. 1,000
    seeds. Failing seeds become named regression tests.
12. **The compaction-trigger observable.** Q17 requires M4 to emit *commit-log
    index entries per stream* so M7's threshold is chosen from data
    ([50-open-questions.md](../../../../research/50-open-questions.md) `:99-103`,
    measurement M4). ⚠️ **Not as a metric label** — see *Design*.

**Out, explicitly:**

- **Commit forwarding as a transport** — the remote `Sequencer` implementation
  and the peer-mesh RPC it rides on. **M5**, with the rest of the mesh. See the
  deployment-constraint section above; this is the one deferral that changes what
  "done" means, so it is stated twice on purpose.
- **The degraded `ctl/inbox/` path** (a pod that cannot reach the slot leader
  writing an intent object). Specified at
  [03-metadata-and-cas.md:250-261](../../../../research/30-design-space/03-metadata-and-cas.md);
  it is the *degraded* counterpart to forwarding, so it cannot precede it.
- **Self-fencing on a commit-latency SLO breach** (F8, the gray-failure answer in
  [08-failure-domains-and-resilience.md](../../../../research/30-design-space/08-failure-domains-and-resilience.md)).
  It needs a commit-latency SLO, which **M9** measures; the *mechanism* it uses —
  voluntary release — ships here, so the milestone that has the number wires a
  policy rather than building a path. ⚠️ Round-4 review caught an earlier draft
  naming M8 and M9 in one sentence for the same dependency.
- **The early-challenge path** (an EndpointSlice watch triggering a lease
  challenge before TTL expiry). It needs the same membership signal as the peer
  mesh, so it goes with it in M5. ⚠️ Consequence, stated rather than hidden:
  **M4's failover is TTL-bound**, which ADR-0007 puts at ~10 s worst case — so
  NFR-9's < 5 s is *not* met by M4 alone. See acceptance criterion 8.
- **Raising S above 1.** ADR-0007 fixes S = 1; the slot dimension stays in every
  key so raising it is config, not a format change.
- **Piggybacked commits (W3).** Rejected for v1 by Q3 — it couples visibility to
  flush cadence and complicates recovery, to save the commit chain's cost, which
  the revised cost model puts at **$52/month** (the corpus's "$156" predates
  ADR-0007's single sequencer).
- **GCS/Azure conditional-write conformance.** Those backends do not exist.
- **The WAL invariant from ADR-0013.** Joins the simulation at M11, per
  testing.md rule 20's own ⚠️.
- **Deleting superseded deltas or checkpoints.** M7's GC.

## Design

### Two primitives, and everything is built from them

`putIfAbsent` for the delta chain, the seal and checkpoints; `putIfMatch` for
the lease (ADR-0008 — "use the primitive that matches the access pattern").
Both already exist in the SPI and both backends as of M2, and
`Capabilities.conditionalWrites` is already checked at startup. **M4 is
`putIfMatch`'s first real consumer**, as M2's own spec said it would be.

⚠️ **A lost CAS race is an empty `Optional`, not an exception and not a status
code.** ADR-0022 fixes that contract precisely so a caller can tell "lost the
race" from "store unreachable". This spec therefore says "loses the race"; it
never says "observes 412", because at this layer 412 is not observable.

⚠️ **Redrive, don't blind-retry.** On a lost race the loser re-reads the state,
rebuilds its intent, and resubmits — it does not resubmit the same bytes at the
same key. A race can surface as 409 on one attempt and 412 on the retry, which is
why the rebuild is mandatory rather than an optimisation.

### Why the chain path keeps `<slot>`, not `<podId>`

[03-metadata-and-cas.md:110](../../../../research/30-design-space/03-metadata-and-cas.md)
writes `ctl/log/<slot>/<epoch>/<seq>.delta`; `:164` and `:240` write
`ctl/log/<podId>/<epoch>/…`. The `<podId>` form belongs to the multi-sequencer-pod
design in `:142-165`, where the commit log is per pod so the commit rate does not
scale with slots.

**Decision: `<slot>`** — and this needs no new ADR, because **ADR-0007 already
binds it**: the slot dimension stays present in every path and every key, lease
paths and chain paths alike. Under S = 1 there is exactly one lease, therefore
exactly one sequencer writing exactly one chain, so the two forms collapse.
`<slot>` is also what `CommitLog.keyFor` already emits and what every object
written since M1 already uses, honouring `CommitLog.java:47-50`'s stated intent:
*"their SLOTS are in the grammar from the first object so that M4's leases and
epochs are not a key-grammar change."*

⚠️ Consequence to state plainly: with a per-slot chain, raising S makes the commit
rate scale with slots, which `:142-165` warns against. That is a real constraint
on raising S, and it belongs in whatever ADR proposes to raise it — not here,
where S is fixed at 1.

### The seal has two losing branches, and they are not the same

Round-1 review found this stated with one branch, which would have stopped the
cluster. Both must be implemented:

| Who loses the race at `N+1` | What it must do | Why |
|---|---|---|
| **The fenced old leader** | Treat the loss as **proof it is fenced**: stop sequencing, discard buffered commits, re-read the lease | Its lease is gone; anything it writes is in a discarded suffix (I3) |
| **The new leader** (holds a valid lease) | **Redrive**: re-read `N+1`, apply it, retry the seal at `N+2` | Stopping would surrender sequencing cluster-wide while holding the lease — a self-inflicted outage |

**Termination**: the new leader's redrive loop terminates because the only writer
that can beat it is the old leader, which stops on its own first lost race.
⚠️ **The bound is NOT the old leader's in-flight depth at the moment of fencing**
— round-2 review caught that, and it matters: a fenced leader does not know it is
fenced until a write loses or its **renew** fails, so it keeps originating commits
for up to one renew interval. The bound is therefore *the commits one leader can
originate in one renew interval* (order tens), and an implementation that caps
the redrive at in-flight depth gives up early and surrenders sequencing
cluster-wide — the exact outage this branch exists to prevent.

**Consecutiveness is load-bearing**: a fenced leader must claim `N+1` before
`N+2`, so it *necessarily* collides with the seal. A timestamp-keyed or
randomly-keyed log would not have this property. This is also the corpus's
general lesson about absence: **the seal protocol never asks whether `seq N+1` is
empty — it claims it**
([04-discovery-and-tailing.md:309-319](../../../../research/30-design-space/04-discovery-and-tailing.md)).

### Checkpoint key grammar, and finding "newest" without a scan

⚠️ Round-1 review found this unspecified, and it matters: ADR-0022 removed
`lastModifiedMillis`, so **nothing may reason about object timestamps**, and
`list` returns one ascending page — so "find the newest" naively means paging the
whole `ckpt/` prefix, which grows with uptime because nothing deletes checkpoints
until M7.

**Grammar**: `ctl/log/<slot>/<epoch:016x>/ckpt/<seq:016x>.ckpt`. Zero-padded
lowercase hex, so **lexicographic order is numeric order** — the same property
the delta keys already rely on, and the reason `<epoch>` is given an encoding
here rather than left as a bare number.

**Discovery — the constraint, deliberately NOT the mechanism.** ⚠️ Three review
rounds each found a defect in a *mechanism* specified here, and the third
diagnosed why: this spec was designing something it does not need to contain.
So it states the bound and stops.

**The bound**: recovery must find the newest checkpoint **without a `list`**.
That is not a preference — ascending-only paging plus ADR-0022's removal of
`lastModifiedMillis` means a scan is O(n) in a prefix that grows with uptime,
and recovery is **tier 3 of M5's fallback ladder**, run by up to 300 plugin
nodes against cost.md R15's ~1 LIST/s ceiling. A scan breaches that ceiling at
fan-out, not at M4's own scale.

**The mechanism is M4.8's to choose, and it records the choice in an ADR** —
it is a durable key-grammar-adjacent decision, so it earns one. Candidates the
reviews surfaced, none adopted here: a pointer carried in the lease object
(already read at recovery, but it puts a second writer on the one CAS object
whose lost race is the fencing signal); a separate `LATEST` pointer written with
`putIfMatch` after each checkpoint; or deriving the cursor from the chain head
the recovering process already has. Each has a cost and a failure mode, and
choosing between them belongs with the code that has to make it work.

⚠️ **Cold start** — no lease, no checkpoint, no chain — is exempt from the bound
and tested separately; there is nothing to scan yet.

⚠️ Recovery is also **tier 3 of M5's fallback ladder**, run by up to 300 plugin
nodes against cost.md R15's ~1 LIST/s ceiling. A discovery path that scanned
would breach that ceiling at fan-out, not at M4's own scale — exactly the kind of
cost bug that surfaces two milestones later.

### `SEAL` is a wire-format change, and it collides with an existing guard

`CommitDelta` today rejects an empty run list, with a stated reason: *"an empty
delta would consume a sequence number and commit nothing, so a replay would see a
gap it cannot explain"*. A `SEAL` commits no runs **by construction**. So M4 must
either make `SEAL` a `CommitDelta` variant (bumping `VERSION` from 0, relaxing
that guard for exactly the seal case) or introduce a separate object type at the
same key. ⚠️ **`CONTINUE` is a THIRD shape and belongs to the same task** —
round-2 review found it demanded by a test-plan row while owned by no scope
item, criterion or task. It opens the new epoch's chain with `{prevEpoch,
prevSeq}` so a reader can follow across the boundary, it commits no runs either,
and it needs its own golden file. **Either way it is a
[`wire-format-change`](../../../../../.agents/skills/wire-format-change/SKILL.md)**:
version bumped with the old shape still parsing, every reader and writer updated
in the same commit, fakes updated, golden files for **every** shape — v0, `SEAL` and
`CONTINUE` — and an ADR travelling with it.

### The compaction-trigger observable is not a metric label

Q17 wants *commit-log index entries per stream*. `stream` is on
[observability.md](../../../standards/observability.md) rule 1's **"Never a
label"** list, enforced by `check-metric-cardinality.sh` on every commit. So the
shape is rule 2's: a **histogram** of index-entries-per-stream (bounded series,
no per-stream identity) plus the **periodic top-K structured log event** that
rule 2 already prescribes for "who is doing this?". That answers Q17 — the
threshold is chosen from the distribution, not from a per-stream time series.

### Rejected alternatives

- **Consensus (Raft/KRaft) for ordering.** Rejected by ADR-0011, and on
  *correctness* as much as cost: a quorum-acked but un-checkpointed offset lives
  only on pod disks, a worse RPO than a conditional PUT that is in a regional
  bucket the instant it succeeds. KRaft commits in ~1–2 ms against a conditional
  PUT's ~30–60 ms — real, and bought with a cluster we refuse to run.
- **Read-modify-write on a shared manifest** (the Quickwit shape). Rejected with
  their own published warning: a JSON metastore on object storage *"does not
  handle concurrent writers well and you should move to PostgreSQL before scaling
  indexing out."* Throughput *decreases* with concurrency.
- **A wall-clock node epoch** (AutoMQ's shape). Rejected: they have KRaft to
  reject stale epochs and we do not, so our epoch must be a counter whose
  uniqueness the store itself guarantees.
- **An ETag-CAS'd `HEAD` pointer** to skip the speculative 404 probe. ⚠️
  **Genuinely still open** in the corpus (`:280-281`); it costs one extra CAS per
  commit. Not adopted: the 404 probe is one GET, and this milestone should not add
  a per-commit *write* to optimise a read path M5 has not yet measured.

## Cost impact

⚠️ **The commit chain is written by exactly ONE writer**, at
`1 / commitBatchInterval`. Per the cost model's own ⚠️ **Revised 2026-08-30**
banner, which supersedes the research doc's `:160-161`:

| Commit batch interval | Commit PUT/s | Cost | Added visibility latency |
|---|---|---|---|
| **250 ms (default)** | 4 | **$52/month** | 0–250 ms |
| 50 ms | 20 | $259/month | 0–50 ms |

The commit batch interval **is** the post-PUT latency dial, which is why
`commitBatchInterval` is built in this milestone (scope item 5) rather than
assumed by the cost table.

⚠️ **The number that must not regress is commits per second, not commits per
stream.** One delta carries every stream *and every contributing pod flush* in
its window. Nothing may make the commit rate scale with records, shards,
partitions, indices — or **pods** (non-negotiable 6). Acceptance criterion 9
asserts the pod dimension specifically, because the stream dimension already
holds in today's code and a test asserting only that would pass without
constraining the change.

The lease costs ~0.33 PUT/s (renew at TTL/3). Checkpoints cost one PUT per K
deltas or T seconds — negligible, and they *reduce* cost by bounding recovery
reads.

⚠️ R2 permits a LIST on recovery, because it runs once at startup and never on a
hot path. This milestone spends less than that permits: the bound above keeps
recovery free of LISTs **entirely** outside cold start — a cost improvement, not
only a latency one.

## Acceptance criteria

1. **I1–I5 hold across 1,000 deterministic simulation seeds** on
   `MemoryBinStore`, with an injectable clock and a fault-injecting store
   covering: delayed writes, reordered completions, partitioned leaders,
   duplicated in-flight PUTs, and **multiple logical pods committing through the
   seam**. ⚠️ **On `MemoryBinStore`, not MinIO** — ADR-0008's addendum `:47-56`
   and testing.md `:83-91` both say MinIO's conditional writes are not stable
   enough to test the commit protocol against. Every failing seed becomes a named
   regression test. ⚠️ Each injected fault class must have **at least one seed
   where it changes the outcome**, reported by the harness, or the suite proves
   the simulator rather than the system.
2. **Exactly one winner under lease contention**: with N concurrent contenders
   racing an unheld or expired lease, exactly one acquires it and every other
   receives an empty `Optional` and backs off. Asserted over ≥100 seeds.
3. **The seal's two branches are both correct**, asserted separately:
   (a) a **fenced** leader that loses at `N+1` stops, and **no delta it wrote
   after fencing is ever applied by a reader** (I3 — asserted on the reader, not
   merely that its call failed); (b) a **new leader** that loses at `N+1` because
   the old leader's write landed first **redrives** to `N+2` and completes the
   seal, rather than stopping. ⚠️ (b) must fail against an implementation that
   stops on any lost race — the one-branch design round-1 review caught.
4. **Offsets are stable across failover** (NFR-11, I2): for a workload committed
   across an induced failover, every `(stream, offset) -> record` mapping
   observed before the failover is identical after it, for **every** seed.
5. **Recovery issues ZERO `list` calls, and replays at most K deltas**: with a
   chain of ≥5,000 deltas and **≥5,000 checkpoints** present, recovery issues
   **no `list` at all** and at most K delta GETs, asserted by request count
   through `CountingBinStore`, and reconstructs state identical to a full
   replay. ⚠️ **≥5,000 checkpoints, not ≥50**: round-2 review showed a bound in
   *pages* stays green against the naive `list`-and-take-last, and round 3
   showed ≥50 does too — 50 keys fit inside one 1,000-key page. The workload
   must exceed a page for the assertion to discriminate at all. Cold start (no
   lease, no checkpoint) is exempt and tested separately.
6. **Commit is idempotent**: replaying a
   `(podId, incarnationId, flushSeq)` already applied assigns no new offsets --
   including when the replay arrives after a checkpoint recorded it -- and
   returns the original assignment. ⚠️ THE POINTER IS RECORDED FOR EVERY COMMIT
   THE LEADER OBSERVES APPLYING, and a replay of a pod's most recent such
   `flushSeq` is answered. ⚠️ AN AMBIGUOUS COMMIT IS NOT ONE OF THEM:
   `LocalSequencer` calls `observe` only after `commitAll` RETURNS, and the
   `IOException` means the PUT may have landed — so the commit applies and the
   in-memory window never learns of it. M4.10d must reconcile that case from
   the CHAIN, which carries the triple after M4.10c, rather than treating the
   in-memory map as authoritative; a dedup tested only after successful commits
   is a no-op against exactly the ambiguity M4.10 exists for. Below that
   watermark, a replay the pointed delta DOES carry is answered
   too; refusal is permitted ONLY when the pointed delta does not carry the
   triple. A dedup that records the pointer and never populates it, or that
   refuses below the watermark without reading, satisfies neither this
   criterion nor the T0 row. ⚠️ AMENDED BY ADR-0036 (2026-09-07), which supplies both
   the incarnation and that bound; a restarted pod reissuing `flushSeq 0` is a
   NEW commit and must be ACCEPTED, which the pair-keyed wording forbade.
7. **The ack-ordering rule (I5) is enforced, and its violation is detectable**: a
   commit is acknowledged only after every lower-numbered write in its chain is
   confirmed. ⚠️ The test must **fail** against a deliberately naive pipelining
   implementation that acks out of order. Today's `CommitLog` is strictly serial
   and satisfies I5 *by accident*, so a test written against it would pass while
   constraining nothing — this criterion is met by a green test **plus a recorded
   red** against the naive implementation, not by the green alone.
8. **Failover time is measured and labelled**: on the simulated clock, from lease
   expiry to the new leader's first committed delta, the time is **within one TTL
   plus one commit batch interval**. ⚠️ **This is a MODELLED number, not a
   measurement** (performance.md rule 7 — "label models and say what would
   falsify them"), and it is **not** NFR-9's < 5 s: without the early-challenge
   path (Out, M5) failover is TTL-bound, which ADR-0007 puts at ~10 s worst case.
   **NFR-9 is met at M5, and this spec says so rather than claiming it here.**
   Falsified by a real-clock chaos run at M8.
9. **Commit cost does not scale with pods or streams**: over a fixed window in
   which every batch is saturated, the number of commit-chain PUTs is
   **bounded by `window / commitBatchInterval`** and does not change when the
   workload goes from 1 pod to 6, or from 1 stream to 1,000 — asserted through
   `CountingBinStore`.
   ⚠️ **AND NOR DOES THE REPLAY-ANSWERING GET** (ADR-0036), which this clause
   does not count: a batch may hold several replays whose pointers name ONE
   shared delta, so at most one GET per distinct `(epoch, sequence)` per
   `commitAll`. N GETs of one object is the pod axis M4.7 removed, returning as
   reads.
   ⚠️ The saturation clause matters: "unchanged" alone is
   not true of a correct batcher at trickle rates, where a window may carry one
   commit or none.
   ⚠️ The **pod** dimension is the one that matters — the stream dimension
   already holds in today's code, so asserting only it would constrain nothing.
10. **The compaction-trigger observable exists**: a histogram of commit-log index
    entries per stream plus the periodic top-K log event, with
    `check-metric-cardinality.sh` green — i.e. **no `stream` label**.
11. **`SEAL` and `CONTINUE` round-trip, and v0 still parses**: golden files for
    all three shapes, the version bumped, every reader and writer updated in the
    same commit, and the ADR travelling with it — the
    [`wire-format-change`](../../../../../.agents/skills/wire-format-change/SKILL.md)
    checklist, observed rather than assumed.
12. **`sequencer` compiles with no Helidon on the classpath**;
    `GATE_SCOPE=full ./scripts/check-module.sh` green.

## Test plan

⚠️ Every new test below must have a red record (`scripts/tdd-red.sh`) before its
production code exists, and the mutation column is what makes each worth having.

| Tier | Behaviour | *Fails first against* |
|---|---|---|
| T0 | Lease acquire/renew/release; epoch increments by exactly one per acquisition | an epoch derived from a clock, or one that can repeat |
| T0 | Renewal failure stops sequencing immediately | a holder that keeps sequencing after a failed renew (F4) |
| T0 | Seal branch (a): fenced leader loses at `N+1` and stops | a fenced leader that redrives to `N+2` — the mirror of the bug below |
| T0 | Seal branch (b): new leader loses at `N+1`, redrives, completes at `N+2` | an implementation that stops on **any** lost race, surrendering the cluster |
| T0 | `CONTINUE` header lets a reader follow across an epoch boundary | a reader that stops at the sealed prefix and silently misses the new epoch |
| T0 | Checkpoint round-trip; contents omit the full offset→segment index | a checkpoint that grows with the retention window |
| T1 | Newest-checkpoint discovery issues **zero** `list` calls with ≥5,000 checkpoints present | a discovery that pages the `ckpt/` prefix — ⚠️ the workload must exceed one 1,000-key page, or `list`-and-take-last passes |
| T1 | Bounded recovery reconstructs state identical to full replay | recovery that reads the whole chain anyway — caught by request count, not by result |
| T0 | Idempotent commit on `(podId, incarnationId, flushSeq)` replay, including post-checkpoint | a sequencer that assigns fresh offsets to a retry — silent duplication; and one keyed on the bare `(podId, flushSeq)`, which dedups a retry within an incarnation but SUPPRESSES a restarted pod's new commit |
| T1 | Ack-ordering: acked commits never exceed the confirmed prefix | naive pipelining that acks `N+2` before `N+1` confirms |
| T1 | Commit batching: PUT count unchanged from 1 pod to 6 and 1 stream to 1,000 | a commit rate that scales with pods |
| T1 | Replay after an AMBIGUOUS commit (the PUT landed, `commitAll` threw, `observe` never ran) is detected and answered from the CHAIN | a dedup that consults only the in-memory window — green against every successful-commit fixture |
| T1 | Replay answering: EXACTLY ONE GET per `commitAll` when 6 replaying pods' pointers share one delta, and every replay answered WITH ITS OWN original offsets | N GETs of one shared delta; answering all six from the pointed delta's first run — the pod axis returning as reads; and an always-refuse dedup, which 0 == 0 would otherwise satisfy |
| T1 | Offset stability across induced failover | an implementation that re-assigns after failover (I2/NFR-11) |
| **T1** | **1,000-seed deterministic simulation** asserting I1–I5, many logical pods through the seam | any of the five; failing seeds pinned as named regressions |

⚠️ The simulation is **T1, not T2**: testing.md's tier table puts `MemoryBinStore`
at T1 and `LocalFsBinStore` at T2, and this suite runs on `MemoryBinStore` by
ADR-0008's addendum. It runs on **every commit**, so it needs a stated time
budget — **< 60 s for 1,000 seeds**, with the seed count configurable so a
developer can run 10,000 locally when hunting a failure.

**Coverage and mutation.** ⚠️ `check-mutants.sh` **does not exist**, and
`check-coverage.sh` exists but is unwired. The strongest automated quality gates
are therefore absent for the highest-risk component in the system. The simulation
is the compensating control, and it is only a control if its mutations are real —
which is why criteria 1, 3 and 7 each demand a recorded red.

## Risks

| Risk | What would reveal it |
|---|---|
| **The GC-pause seal race**: a leader paused by a long GC resumes several epochs later. The corpus says *"believed safe by I3; needs proof"* (`:282-283`) — the single most SPEC-worthy open item | A simulation seed that pauses a leader across ≥2 epoch changes and then lets its writes land. ⚠️ If it cannot be shown safe, that is an ADR, not a bug fix |
| **I5 asserted vacuously** — today's `CommitLog` is strictly serial, so it satisfies I5 by accident | Criterion 7's required red against a naive pipelining implementation |
| **The simulation proves the simulator** — a fault-injecting store that never injects the fault that matters | Criterion 1's requirement that each fault class have a seed where it changes the outcome |
| **The multi-pod gap becomes invisible** — every test in the tree is single-pod, so a missing forwarding path stays green until M6/M8 | The deployment-constraint section above, the seam contract carrying `(podId, incarnationId, flushSeq)` from the start, and criterion 9's pod dimension |
| **Moving `CommitLog` from `ingest` to `sequencer`** crosses a gate-enforced module boundary | `check-module.sh` at `GATE_SCOPE=full`; done as its own commit (M4.2), not folded into a behaviour change |

## Tasks

One commit each, decomposed in [backlog.md](../../backlog.md).

| ID | Task |
|---|---|
| M4.0 | This spec; the roadmap's stale `I1–I4` corrected to `I1–I5`; M5's row named as commit forwarding's owner |
| M4.1 | `Sequencer` seam interface and its fake, with the contract stated — the commit request carries `(podId, incarnationId, flushSeq, …)` so it is meaningful from another pod |
| M4.2 | Move `CommitLog` from `ingest` to `sequencer` — **pure move, no behaviour change** |
| M4.3 | `Lease` — the record, its validation and its JSON codec, in `format`, mirroring the `IndexRegistry`/`IndexOrdinalRegistry` split |
| M4.3b | `LeaseManager` in `sequencer`: the key grammar and acquire/renew/release — ⚠️ `putIfAbsent` for FIRST acquisition (`putIfMatch` throws on an absent key), `putIfMatch` to take over or renew; TTL and renew interval as configuration. ⚠️ Split out of M4.3, which was two changes in two modules; M1's own table carries `M1.3b`/`M1.16b`/`M1.19b`, so the convention is established |
| M4.4 | Epoch fencing: epoch in the path; a fenced writer's PUT is unreadable |
| M4.5 | ADR + wire-format change for `SEAL` **and `CONTINUE`**, and the empty-runs guard they collide with; golden files for **every** shape — v0, `SEAL`, `CONTINUE` |
| M4.6 | The seal protocol, **both losing branches**, with the termination argument |
| M4.7 | Commit batching on `commitBatchInterval` — one delta per window across pods and streams |
| M4.8 | Checkpoints: contents, key grammar, cadence (K/T as configuration), and newest-checkpoint discovery with **zero `list` calls** — mechanism chosen here and recorded in an ADR |
| M4.9 | Bounded recovery: newest checkpoint + ≤K deltas, asserted by request count |
| M4.10 | Commit idempotency on `(podId, incarnationId, flushSeq)` (ADR-0036) |
| M4.11 | The ack-ordering rule, with the naive-pipelining mutation as its red record |
| M4.12 | The fault-injecting store decorator and the seeded simulation harness |
| M4.13 | The 1,000-seed run asserting I1–I5; failing seeds pinned as regressions |
| M4.14 | The compaction-trigger observable — histogram + top-K log event, no `stream` label |
| M4.15 | `VERIFIED.md` and the milestone review |
