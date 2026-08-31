# 0013. Fast mode: an opt-in quorum-replicated WAL

Status: accepted
Date: 2026-08-30
Requirements: FR-17, NFR-14
Research: docs/research/30-design-space/12-fast-mode-wal-and-quorum.md

## Context

The default path acks and makes a record visible in 42–708 ms, because ordering
is assigned after the write and both the segment and the commit delta are
object-store round trips. Some workloads need read-after-write in milliseconds.

[ADR-0011](0011-no-consensus-cluster.md) recorded the trigger: *"if a requirement
for sub-50 ms visibility ever appears, the answer is a WAL or an embedded Raft,
and it is a v2 architecture, not a tweak."* It has appeared.

## Decision

Add an **opt-in fast tier**: a write-ahead log, replicated to a quorum of AZs,
carrying both the record and its sequencer-assigned offset. Ack and visibility at
**~1.5–6 ms**.

**Two orthogonal settings, per index, carried as OpenSearch index settings and
pushed to the ingester by the plugin** ([ADR-0015](0015-routing-registration-and-aliases.md)):

```
index.ingestion_source.param.flush_timer   duration   (default: 5s)
index.ingestion_source.param.wal           bool       (default: false)
index.ingestion_source.param.wal_quorum    1 | 2 | 3  (default: 2, only when wal=true)
```

⚠️ **`flush_timer` is a per-index *deadline*, not a per-index bundling guarantee.**
Segments are shared, so the tightest timer among the indices on a pod sets the
cadence for all of them — a cost externality of the same shape as the lane-deadline
rule. ⚠️ It carries no `R<n>`: cost.md numbers rules 1-18 and none of them is
the lane deadline, so citing one here would be a wrong number rather than a
vague one. Cap how tight a timer may be, and report the *effective* cadence.

⚠️ **`wal=true` is what makes a long timer safe.** With `wal=false` the timer bounds
ack latency because the PUT is the ack; with `wal=true` the ack comes from the
quorum and the timer only delays arrival in S3, so it can be long — recovering
~26% of the quorum-2 cross-AZ bill.

⚠️ **Not per record.** See the alternative below — a per-record selector is not
merely worse operationally, it does not work.

Invariant **I6**: every WAL-committed offset appears in a committed segment at the
same offset. The WAL *is* the flush buffer, so the two cannot diverge; any quorum
member can upload it if the writer dies.

The WAL lives on `emptyDir`, not a PersistentVolume — a restarted pod recovers
from its quorum peers, so pods stay reschedulable.

## Alternatives considered

- **Fast mode as the default.** Rejected on cost: at 100 MiB/s, replicating every
  record cross-AZ costs **$5,436/month at quorum 2**, against ~$400/month for the
  entire normal-mode system — **13×**. At 1% of traffic it is $54/month. The
  selector must therefore be per record; a deployment that fast-modes everything
  has chosen a different and far more expensive product.
- **A per-record header selecting fast mode** (this ADR's first draft).
  ⚠️ **Rejected as technically unsound, not just operationally awkward.** A
  partition is a single totally ordered log, and a consumer must process offset
  *N* before *N+1*. Suppose R1 (durable) takes offset 100 and R2 (fast) takes 101:
  R2 is WAL-committed and *would* be visible in 5 ms, but the consumer cannot
  reach it until R1 becomes visible at ~300 ms. **A fast record behind a durable
  record is only as fast as the record ahead of it.** Mixing modes in one
  partition therefore either produces head-of-line blocking or degenerates into
  "the whole partition is fast from here on". Per-index is the smallest
  granularity that actually delivers the latency it promises.

  Three operational consequences follow and all point the same way: the index
  owner — not the producer — owns the durability guarantee that fast mode weakens;
  an operator can *see* which indices are downgraded by reading index settings,
  rather than sampling traffic; and no producer can accidentally commit the
  cluster to $5,436/month by setting a header.

- **Ack fast without moving visibility.** Rejected: it does not deliver
  read-after-write, which is the actual requirement. Visibility needs an offset,
  and an offset needs the sequencer.
- **Pre-allocated offset ranges**, so the writer can assign without a round trip.
  Rejected again (ADR-0001): a failed write leaves a hole every consumer of that
  partition blocks on.
- **Embedded Raft for the metadata**, AutoMQ/KRaft style. Rejected: our need is
  *replication for durability*, not *election*. Leader election stays the CAS
  lease; the quorum only holds copies. Raft would make pods stateful in the
  stronger sense and reintroduce quorum-loss failure modes for the default path.
- **PersistentVolume for the WAL.** Rejected: it makes pods non-reschedulable for
  a log that is recoverable from peers by construction.

## Consequences

- ⚠️ **NFR-8 becomes conditional.** In `wal` mode an acked record lives on
  `quorum` pods' disks until the segment upload completes (~250 ms – 1 s). Lose
  the quorum in that window and an acked record is lost — and if a consumer
  already indexed it, **the index contains a record the log cannot reproduce.**
  The registration must record the mode so an operator can see it; a cluster that
  quietly enabled fast mode everywhere has silently downgraded its durability.
- ⚠️ **Scoped reversal of ADR-0011.** The default path is unchanged: object-store
  CAS, no WAL, no quorum. Only the opt-in tier takes the trade.
- Pods become **stateful in effect** for the upload window: one holding
  un-uploaded WAL entries cannot terminate without flushing or handing off.
  Shutdown ordering and `terminationGracePeriodSeconds` must cover it. This, not
  the disk, is the real complexity cost.
- Disk: ~5.9 GiB per pod for 60 s of buffered 100 MiB/s. Sized by the upload
  interval, not by retention.
- The sequencer is now in the synchronous path for fast records. Its replication
  fan-out is a new, unmeasured load.
- Because `index.ingestion_source.param.*` is `Property.Dynamic`, the mode can be
  changed on a live index. `durable → wal` takes effect for new records;
  ⚠️ `wal → durable` must **drain the in-flight WAL first**, or records acked
  under the old mode lose their quorum guarantee before reaching S3.
- ⚠️ **Fast mode is a latency feature only; it never pays for itself in PUTs.**
  Having a WAL invites deferring segment uploads AutoMQ-style (they upload at
  100 MB, triggered by memory pressure). The arithmetic forbids it: cross-AZ
  replication costs **7–37× the PUT saving**, and both terms are linear in bytes
  so the ratio never flips. See [automq §10.3](../../../research/10-prior-art/02-automq.md).
  A `quorum=1` local-disk WAL *would* reproduce their economics — and their
  single-AZ durability posture with it.
- **Milestone placement: after M9.** This is a second write path with its own
  failure modes; it should be built once the default path is measured and proven,
  not alongside it.
