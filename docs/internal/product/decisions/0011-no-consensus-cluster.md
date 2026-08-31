# 0011. No Raft/KRaft cluster; the object store's CAS is the consensus primitive

Status: accepted; 2026-08-30: the "sub-50 ms visibility" trigger below arrived and
was taken up as an **opt-in tier** in [ADR-0013](0013-fast-mode-wal-and-quorum.md).
The default path is unchanged — no WAL, no quorum, no consensus.
Date: 2026-08-30
Requirements: FR-11, FR-12, NFR-8, NFR-9, NFR-11
Research: docs/research/30-design-space/03-metadata-and-cas.md

## Context

Every comparable system has a consensus layer: AutoMQ has KRaft, Kafka has KRaft,
WarpStream has a hosted metadata store, Quickwit ends at PostgreSQL, OpenSearch
itself has a cluster-manager quorum. The question is whether we need our own.

## Decision

**No.** No master node, no embedded Raft, no external quorum. Coordination is
compare-and-swap on the object store.

### Why that is sufficient, precisely

Consensus is needed for five things. Ours are covered without a quorum:

| Need | How we meet it |
|---|---|
| Leader election with fencing | CAS lease + epoch counter, epoch in the object path (ADR-0002) |
| A totally ordered log | consecutively numbered, write-once commit entries (`putIfAbsent`) |
| Linearizable shared state | CAS on the lease and the ordinal registry (ADR-0008) |
| Membership | Kubernetes endpoints. **No quorum is involved**, so members may disagree; the worst outcome is a duplicated GET |
| Durability | S3 gives ~11 nines across AZs, on the write itself |

The formal grounding: **compare-and-swap has consensus number ∞** in Herlihy's
wait-free hierarchy — a CAS register can solve consensus for any number of
processes, unlike plain read/write registers (1) or test-and-set (2). An object
store that offers conditional writes therefore *is* a consensus primitive. The
practical limits are latency and per-key throughput, not capability.

⚠️ **We are not implementing consensus; we are using a consensus primitive.**
Each decision is a **single** conditional write to a **unique** key, which the
store performs atomically. There is no multi-round protocol, no ballot, no
replicated state machine. That distinction is the whole safety argument, and it is
why "hand-rolled consensus" — the usual reason to reach for Raft — does not apply.

### The barrier property that makes failover safe

Because sequence numbers are **consecutive**, a `SEAL` is an impassable barrier:
a fenced leader must claim *N+1* before *N+2*, so it collides with the seal and
learns it is fenced. It can never write past one.

⚠️ **This forbids naive pipelining of the commit chain.** A leader that issues
*N+1*, *N+2*, *N+3* concurrently could have *N+2* succeed while *N+1* loses to a
seal — leaving an acked record beyond a barrier, invisible to readers. **A commit
may be acknowledged only after every lower-numbered write in the chain has been
confirmed.** Pipelining is permitted for throughput; acknowledgement is not.

## Alternatives considered

### Embedded Raft in the ingester nodes

| | Object-store CAS (chosen) | Embedded Raft |
|---|---|---|
| Pods | **stateless** | stateful: stable identity + persistent disk |
| Commit latency | +30–60 ms | +1–2 ms |
| Commit cost | ~$156/month | ~$0, plus checkpoints |
| Durability of a committed offset | **in S3, multi-AZ, at commit** | on pod disks until checkpointed |
| Failure modes | lease expiry, seal race | quorum loss, split brain, membership change |
| Ops burden | none | real |

Rejected on two grounds, one of which is correctness rather than taste:

1. **It makes the pods stateful**, forfeiting the trivial rescheduling and
   scale-to-zero that the whole architecture is built on (FR-12).
2. ⚠️ **A quorum-acked but not-yet-checkpointed offset lives only on pod disks.**
   Losing the quorum — two of three AZs — loses committed offsets, violating
   NFR-11 (offset stability) and threatening NFR-8 (RPO 0). With CAS, a committed
   offset is durable in a regional bucket the instant it is committed. **Raft
   would make our durability story worse, not better**, for a ~50 ms latency gain
   we do not need.

### An external metadata store (DynamoDB, etcd, Postgres)
Rejected in ADR-0002: it is the dependency the project exists to avoid.

### "But AutoMQ and WarpStream both chose consensus"
They did, and **neither could have chosen otherwise**: S3 `If-None-Match` reached
GA in **August 2024** and `If-Match` in **November 2024**, after both systems were
designed in 2023. A grep of AutoMQ's `s3stream` finds **no conditional-write usage
at all**.

Their controllers also carry work we do not have — a global object-ID allocator
(`PrepareS3Object`), per-stream leadership (`OpenStreams` with node and stream
epochs), consumer groups, topic lifecycle. Only *commit ordering* overlaps with
our needs. **The controller is not there to order the log; it is there to be
Kafka.** See [automq §9](../../../research/10-prior-art/02-automq.md).

### Reusing OpenSearch's cluster manager
Rejected. It would couple the ingester to OpenSearch credentials and topology
(rejected in ADR-0005 and ADR-0006), and it is unavailable to an ingester that must
run before, and independently of, any particular cluster.

## Consequences

- Pods stay stateless; a pod is cattle and rescheduling costs nothing.
- Visibility latency is bounded by object-store write latency (~30–60 ms per
  conditional PUT), not by network RTT. Accepted: the latency budget is seconds.
- **Ack ordering constraint** (above) is now a hard implementation rule and a
  test target.
- If a requirement for **sub-50 ms visibility** ever appears, the answer is a WAL
  (AutoMQ's model) or an embedded Raft — and it is a **v2 architecture, not a
  tweak**. Recording that here so the trade is re-opened deliberately.
- We remain dependent on the store's conditional-write guarantee.
  `Capabilities.conditionalWrites` is checked at startup and fails loudly; there
  is no degraded mode.
