# Metadata, ordering and object-store CAS

**Status:** proposal · **Confidence:** medium — this is the highest-risk component in the system
and the biggest departure from prior art. Everything here needs adversarial review, and §7 deserves
a TLA+ or at minimum a deterministic-simulation test. · **Last updated:** 2026-08-29

**Read this if:** you are implementing the sequencer, the commit log, leader election, or recovery.
**One-line takeaway:** replace WarpStream's hosted metadata store with **leased sequencers fenced by
epochs, appending to a write-once commit log** (`If-None-Match: *`). Never read-modify-write a
shared manifest — that is exactly the pattern Quickwit warns collapses under concurrency.

---

## 1. The requirement

For each stream, produce a stable total order: `offset -> (segmentKey, byteRange, recordIndex)`.

- **Stable:** once a record is visible at offset *N*, it is at *N* forever
  ([SPI](../20-opensearch/01-pull-based-ingestion-spi.md) §5).
- **Gapless:** a consumer reading sequentially must never block on a hole that never fills.
- **Cheap:** commit rate must be independent of index count and partition count
  ([cost-model](../00-problem/02-cost-model.md) rule R6).
- **No external dependency:** object store only (constraint C3).

## 2. CAS primitives, per provider

| Provider | Create-if-absent | Compare-and-swap | Version token |
|---|---|---|---|
| **S3** | `PutObject` + `If-None-Match: *` | `PutObject` + `If-Match: "<etag>"` | ETag |
| **GCS** | `x-goog-if-generation-match: 0` | `x-goog-if-generation-match: <generation>` | generation (**not** ETag — GCS does not apply `If-Match` to PUT) |
| **Azure Blob** | `If-None-Match: *` | `If-Match: "<etag>"` | ETag |
| **Local FS** | `O_CREAT\|O_EXCL` | write temp + atomic `rename` guarded by a version file | mtime+ctr |

Errors to handle (S3, and the shape generalises):
- **412 `PreconditionFailed`** — `If-Match`: someone else wrote; `If-None-Match`: the object exists.
- **409 `ConditionalRequestConflict`** — concurrent conditional writes raced.
- Guidance: **redrive, don't blind-retry** — re-read current state (`HeadObject` for the ETag),
  rebuild the request, resubmit. A race can return 409 first and 412 on retry.
- **Multipart:** the condition is evaluated at `CompleteMultipartUpload`, not on the parts. On 412
  you must abort the upload and restart. Keep CAS objects small enough to be single PUTs and this
  never arises.

**SPI shape** (see [pluggable-store-abstraction](07-pluggable-store-abstraction.md)):
```java
Optional<Version> putIfAbsent(String key, ByteBuffer body);            // empty => already exists
Optional<Version> putIfMatch(String key, ByteBuffer body, Version v);  // empty => version moved
record Versioned<T>(T value, Version version) {}
Versioned<ByteBuffer> get(String key);
```
Two operations. Everything else in this document is built from them.

## 2b. Why this is not a consensus implementation

⚠️ Read this before proposing Raft, KRaft, or an external quorum — the question
recurs and is settled in
[ADR-0011](../../internal/product/decisions/0011-no-consensus-cluster.md).

**Compare-and-swap has consensus number ∞** in Herlihy's wait-free hierarchy: a
CAS register can solve consensus for any number of processes, where plain
read/write registers cannot solve it for two. An object store offering
conditional writes therefore *is* a consensus primitive; the practical limits are
latency and per-key throughput, not capability.

We are **not implementing consensus** — each decision is a *single* conditional
write to a *unique* key, performed atomically by the store. No ballots, no
rounds, no replicated state machine. That is what separates this from
hand-rolled consensus, which is the usual and correct reason to reach for Raft.

An embedded Raft would cut commit latency from ~30–60 ms to ~1–2 ms, and would
make durability **worse**: a quorum-acked but not-yet-checkpointed offset lives
only on pod disks, so losing two of three AZs loses committed offsets. With CAS
a committed offset is in a regional bucket at the moment of commit.

## 3. Why a sequencer at all

Data placement is deliberately leaderless: any pod writes any partition's bytes into its own
segment ([object-layout](01-object-layout-and-format.md) §2). That is what makes the write path
cheap and the pods stateless — but it means the byte layout carries no order.

Something must impose the order after the fact. Every comparable system agrees:
WarpStream's metadata store, AutoMQ's KRaft controller, KIP-1150's **Batch Coordinator**
([diskless-kafka](../10-prior-art/03-diskless-kafka-and-cas-systems.md) §1). Ours is a **leased
sequencer** whose durable state is the commit log in the bucket.

## 4. Leases and epoch fencing (SlateDB pattern)

Streams are hashed to **slots**: `slot = consistentHash(indexUUID) mod S` (default S=64).
Grouping by *index* rather than by *(index, partition)* means a consumer follows one chain per
index and per-index ordering is trivially coherent; a manual override exists for hot indices.

Each slot has a lease object:

```
<prefix>/ctl/lease/<slot>.json   ->  { epoch, holderPodId, holderEndpoint, expiresAtMillis }
```

**Acquire / renew:**
1. `GET` the lease, keep its `Version`.
2. If unheld or expired: `putIfMatch(lease, {epoch+1, me, now+TTL}, version)`.
   Exactly one contender wins; losers get 412 and back off with jitter.
3. The holder renews with `putIfMatch` at TTL/3. Renewal failure ⇒ **immediately stop sequencing**.

**Epoch is a counter, not a clock** (SlateDB is explicit about this) — no clock-skew assumptions in
the safety argument. Clocks are used only for liveness (when may a challenger try), and a wrong
clock costs availability, never correctness.

**The epoch goes in the path**, not just in a field:

```
<prefix>/ctl/log/<slot>/<epoch>/<seq:016x>.delta
```

A fenced-out leader whose PUT was already in flight lands in a path that readers of the new epoch
never look at. Fencing therefore does not depend on catching the write in time.

## 5. Why write-once appends, not manifest CAS

The obvious design — one manifest object per slot, updated with `If-Match` — fails for three
reasons:

1. **Quickwit's warning, verbatim:** a file-based metastore on object storage *"does not handle
   concurrent writers well and you should move to PostgreSQL before scaling indexing out."*
2. **Read-modify-write cost.** Every commit becomes GET + PUT, and the manifest grows with the
   retention window, so you re-upload megabytes 4× per second.
3. **Contention collapse.** Under contention, throughput *decreases* with concurrency: each 412
   forces a re-read and re-upload, which lengthens the window in which the next writer conflicts.

**Write-once append fixes all three:**

```
putIfAbsent("<prefix>/ctl/log/<slot>/<epoch>/<seq:016x>.delta", body)
```

- Constant-size writes; no read before write.
- **Zero contention in steady state** — one leader owns the chain, and each `seq` is claimed exactly
  once.
- The write-once property *is* the safety mechanism: if two processes believe they lead, only one
  can claim a given `seq`, and the loser learns it is fenced (§7).
- Sequential numbering makes "is there anything new?" a single speculative `GET` of `seq+1`
  (404 ⇒ nothing new) — no LIST on any path.

## 6. Commit rate must not scale with anything

If each slot's leader wrote its own delta stream at 4 Hz, 64 slots would cost 256 PUT/s ≈
**$3,300/month** — violating rule R6.

**Fix: the commit log is per *sequencer pod*, not per slot.** A pod holding leases on many slots
writes **one** delta object per flush containing a section per slot it owns:

```
delta = header{ podId, epochPerSlot[], flushSeq }
        section[slot]{ per-stream: streamId, firstOffset, recordCount,
                       segmentKey ref (interned), byteStart, byteLen }
```

> ⚠️ **Revised 2026-09-05 — what shipped groups by SEGMENT, not by slot, and
> carries less than this sketch.** [ADR-0032](../../internal/product/decisions/0032-a-batched-delta-carries-many-segments-under-the-reserved-kind.md)
> is the shape in the tree: `CommitDelta(sequence, List<SegmentCommit>)` with
> `SegmentCommit(segmentKey, List<RunCommit>)`, written under the v1 kinded
> header at the kind ADR-0028 reserved for exactly this.
>
> The conclusion this section reaches is unchanged and is the reason for the
> change: **one delta per window, not one per flush**, so commit PUT/s stays
> independent of pods, streams, partitions and indices. What differs is the
> grouping and the contents. Grouping is by segment because that is the pairing
> a consumer needs — a run's offsets are meaningless without the object holding
> the records, and grouping by slot would leave that pairing to a separate
> interning table nothing type-checks. ⚠️ **ADR-0007 fixes S = 1**, so a
> per-slot section list would today always have exactly one entry: the
> dimension this sketch sections on is not yet a dimension.
>
> Not carried, deliberately: `podId` and `flushSeq` are on the *request*
> (`CommitRequest`) rather than the entry, and the interning, `byteStart` and
> `byteLen` are absent — a consumer reads the whole segment object today.
> ⚠️ **`podId`/`flushSeq` in the entry is what M4.10's idempotency needs**, so
> that part of this sketch is deferred rather than rejected; it is not in the
> entry yet, and the `Sequencer` contract says so in as many words.

```
commit PUTs/s = (number of sequencer pods) x (commit flush rate)
```

Independent of index count, partition count and slot count. With 3 sequencer pods at 4 Hz:
**12 PUT/s ≈ $156/month**, exactly as budgeted in [cost-model](../00-problem/02-cost-model.md) §5.

Slot→chain resolution is the lease registry: a reader looks up `ctl/lease/<slot>.json`, learns
`(holderPodId, epoch)`, and follows `ctl/log/<podId>/<epoch>/…`. Leases change rarely and are
cached.

> **Optimisation W3 — piggybacked commits (zero extra PUTs).** A sequencer pod is also an ingester
> pod. Appending the delta as a trailing section of the segment it was already going to write drops
> commit PUTs to **zero** and halves the write path. Constraints: the delta may only reference
> segments already durable when the flush began (adds ≤1 flush interval of visibility latency), and
> the segment key must then be discoverable as a log entry — so the *chain* still needs a
> write-once claim on `seq`. Practical compromise: keep a tiny `seq` claim object (a few bytes
> pointing at the segment) and put the bulk in the segment. Measure whether the saved PUT is worth
> the coupling before adopting.

## 7. Failover safety: sealing by racing for the next sequence number

> ⚠️ **Implemented in M4.5 (ADR-0028), with ONE change to step 3.** `SEAL{
> continuedAt }` is built exactly as written here, behind a sealed
> `ChainEntry` interface, with the version distinguishing layouts rather than
> releases: v0 *is* a bare delta and still parses, v1 *is* a kinded entry.
>
> ⚠️ **Step 3 is overturned in shape, not in content.** This section makes
> `CONTINUE` a *header on* `new_epoch/<0>.delta`, riding on the first delta at
> no extra object. M4's SPEC made it a THIRD standalone shape that commits no
> runs, so it consumes seq 0 by itself and the new epoch's first delta is seq 1.
> The price is **one extra PUT per failover** — per failover, not per record, so
> no rate changes. The reason is that a header-on-a-delta needs a delta to ride
> on: a leader that seals and then has nothing to commit could not write the
> link at all, and the boundary would be unmarked until traffic arrived.
>
> ⚠️ `continuedAt` itself was nearly lost and is recorded because of how: a
> first draft of ADR-0028 rejected it on the reasoning that the two links "are
> written by different terms at different moments". They are not — step 2 has
> the NEW leader writing the seal into the old chain, so it stamps the epoch it
> already holds and writes the `CONTINUE` moments later itself. The design here
> was right and the re-derivation was wrong.

The dangerous window: an old leader is fenced but its in-flight write still lands.

Suppose the new leader has recovered the chain through `seq = N`. The old leader may still attempt
`seq = N+1`. Resolution, using only write-once semantics:

1. New leader (epoch *e+1*) recovers state through `seq = N` in the **old** epoch's chain.
2. It attempts `putIfAbsent(old_epoch/<N+1>.delta, SEAL{ continuedAt: e+1 })`.
   - **New leader wins** ⇒ the chain is sealed. The old leader's next `putIfAbsent` at `N+1` returns
     412, which it must treat as **proof it is fenced**: stop, discard buffered commits, re-read the
     lease.
   - **Old leader wins** ⇒ the new leader's `putIfAbsent` returns 412. It **re-reads `N+1`**,
     applies that delta to its recovered state, and retries the seal at `N+2`. This terminates:
     the old leader's lease is expired, so it makes at most a bounded number of further attempts,
     and every attempt is either applied or rejected — never lost, never double-applied.
3. Once sealed, the new leader starts `new_epoch/<0>.delta` with a `CONTINUE{prevEpoch:e, prevSeq}`
   header so readers can follow the chain across the epoch boundary.

### Why the barrier holds, and the constraint it imposes

Sequence numbers are **consecutive**, which is what makes a `SEAL` an impassable
barrier rather than a hint: a fenced leader must claim *N+1* before *N+2*, so it
necessarily collides with the seal and learns it is fenced. It can never write
past one. (A timestamp-keyed or randomly-keyed log would not have this property —
consecutiveness is load-bearing, not cosmetic.)

⚠️ **This forbids naive pipelining of the commit chain.** A leader that issues
*N+1*, *N+2* and *N+3* concurrently could have *N+2* succeed while *N+1* loses to
a seal, leaving an **acked record beyond a barrier** — durable in the bucket but
invisible to every reader, which is silent data loss and an NFR-8 violation.

**Rule: a commit may be acknowledged only after every lower-numbered write in the
chain has been confirmed.** Pipelining for throughput is permitted;
acknowledgement out of order is not. See
[ADR-0011](../../internal/product/decisions/0011-no-consensus-cluster.md).

**Invariants to test:**
- I1 No `seq` is ever written twice (guaranteed by `putIfAbsent`).
- I2 A committed offset is never reassigned.
- I3 Every delta a reader applies is either in the sealed prefix of an old epoch or in the chain of
  a later epoch — never in a discarded suffix.
- I4 Uncommitted records may be reordered or dropped; committed ones may not.
- I5 No acknowledged commit exists at a sequence number beyond a `SEAL` in its
  own chain. (The pipelining rule above; the one invariant a plausible
  implementation violates by accident.)

**Test method:** deterministic simulation with an injectable clock and a fault-injecting store
(delayed writes, reordered completions, partitioned leaders, duplicate in-flight PUTs). This is a
correctness-critical protocol; the local-FS store makes such a simulator cheap to build, which is
another reason constraint C1 exists.

## 8. Commit idempotency

A pod's commit request carries `(podId, podFlushSeq, segmentKey, runs[])`. The sequencer keeps
`podId -> lastAppliedFlushSeq` (also in checkpoints) and **ignores replays**. Without this, a pod
that retries after a timeout gets its records assigned offsets twice — silent duplication that
OpenSearch's `_id` dedup would mask in some mappers and not in others.

## 9. Checkpoints and bounded recovery

Every *K* deltas (default 1,000) or *T* seconds (default 60), the leader writes:

```
<prefix>/ctl/log/<podId>/<epoch>/ckpt/<seq:016x>.ckpt
```

containing, per stream: `nextOffset`, `oldestRetainedOffset`, and per pod `lastAppliedFlushSeq`.
It deliberately does **not** contain the full `offset -> segment` index for the retention window —
that stays in the deltas, so the checkpoint stays small (1,600 streams × ~24 B ≈ **38 KiB**).

Recovery = newest checkpoint + replay of subsequent deltas. Bounded by *K*. Old deltas become
deletable once a checkpoint covers them **and** the retention window has passed.

## 10. What crosses an AZ boundary

Only commit metadata: pod → slot leader, ~30 B per `(segment, stream)` pair. For Scenario A that is
~576 KB/s gross, ~⅔ of it cross-AZ ≈ **$20/month** — 0.4% of ingested bytes. Delta-encode sorted
stream IDs and intern segment keys to get under the 0.1% target
([cost-model](../00-problem/02-cost-model.md) §5).

**Degraded path:** if a pod cannot reach a slot leader, it must not block ingestion. It writes a
commit-intent object into `<prefix>/ctl/inbox/<slot>/<podId>-<flushSeq>.intent`
(`putIfAbsent`, idempotent) and the leader drains the inbox. Costs one extra PUT per
(pod, slot, flush) while degraded — acceptable, and it means a network partition delays visibility
rather than stopping writes.

## 11. Failure modes

| Failure | Effect | Recovery |
|---|---|---|
| Ingester node dies after PUT, before commit | Segment is orphaned; records never visible | Producer retries (at-least-once); GC sweeps unreferenced segments |
| Ingester node dies mid-PUT | Partial/absent object | Never referenced; multipart abort via lifecycle rule |
| Sequencer dies | Slot unsequenced until lease TTL expires | Challenger acquires at epoch+1, seals, continues (§7) |
| Sequencer partitioned but alive (zombie) | May attempt stale writes | Fenced by epoch path + seal race |
| CAS 409/412 storm | Throughput dip | Redrive with jittered backoff; steady state has a single writer so this is a failover-only condition |
| Object store unavailable | Writes fail, consumers idle | Producers retry; consumers block harmlessly in `readNext` |
| Clock skew | Premature/late failover | Liveness only; safety rests on the epoch counter |

## 12. Open questions

- **Q:** Do we need per-slot leases at all, or is a single cluster-wide sequencer lease adequate for
  the target scale? Fewer moving parts; caps throughput and widens the blast radius. **Strongly
  consider starting with S=1 and adding slots only when measurement demands it.**
- **Q:** Is `putIfAbsent` on the delta chain sufficient, or do we also want an ETag-CAS'd `HEAD`
  pointer to skip the 404 probe on the fallback poll path? (Costs one extra CAS per commit.)
- **Q:** How do we bound the seal race in the pathological case of a leader paused by a long GC
  pause that resumes after several epochs? (Believed safe by I3; needs proof.)
- **Q:** GCS generation semantics vs S3 ETag semantics under multipart/resumable uploads — verify
  the `putIfAbsent` contract holds identically on all three providers with a conformance test suite.

---

**Sources:** [AWS: multi-writer applications on S3 using native controls](https://aws.amazon.com/blogs/storage/building-multi-writer-applications-on-amazon-s3-using-native-controls/) ·
[S3 conditional writes announcement](https://aws.amazon.com/about-aws/whats-new/2024/11/amazon-s3-functionality-conditional-writes) ·
[SlateDB RFC-0001](https://slatedb.io/rfcs/0001-manifest/) ·
[GCS preconditions](https://docs.cloud.google.com/storage/docs/request-preconditions) ·
[Quickwit metastore config](https://quickwit.io/docs/configuration/metastore-config) ·
[KIP-1150](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1150:+Diskless+Topics)
