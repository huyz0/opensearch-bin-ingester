# 0075. Every durable delta reaches every pod: pushed within an AZ, hinted across

Status: accepted
Date: 2026-09-26
Requirements: FR-5, FR-6, NFR-4, NFR-5, NFR-2
Research: docs/research/30-design-space/05-az-topology-and-data-flow.md#metadata-is-not-exempt--the-195-kib-crossover;
  docs/research/30-design-space/05-az-topology-and-data-flow.md#directed-fan-out-not-broadcast

## Context

[ADR-0012](0012-peer-mesh-without-gossip.md) planned a directed push of
committed deltas to the pods with subscribers. **It was never built.** M10's
exploration measured the consequence on RustFS: three pods in three AZ labels,
writes spread across them, one consumer per AZ served by its own pod -- **each
consumer received only the 1,800 of 5,400 records its own pod wrote**, with 11
gaps nothing repaired.

The cause is structural. A pod publishes only the `CommitDelta` returned to its
own flush (`DefaultIngest.pushLoop`). `BatchingSequencer` returns a whole batch
to every commit in a window, so a pod also publishes other pods' segments that
happened to share its window, and never otherwise. Forwarded commits and the
ADR-0058 inbox drain reach `LocalSequencer.commitAll` without batching, and
nobody publishes them.

Two constraints shape the fix, both from research 05:

- **Cross-AZ bytes are the budget.** A commit delta is ~150 KiB at scale,
  7.7× the ~19.5 KiB crossover above which moving bytes across an AZ costs
  more than a GET: "*'It's only metadata' does not license cross-AZ chatter.*"
  Pushing whole deltas across AZs to interested pods -- the first draft of
  this record -- costs 12/s × 150 KB × 2 remote AZs ≈ 3.6 MB/s cross-AZ at
  100 MB/s ingested: **3.6%, 36× NFR-5's 0.1%**, even directed. M10.16's
  review measured that against the corpus and refused it.
- **Intra-AZ bytes are free.** Research 05 prices a same-AZ peer as always
  preferable. Broadcast was rejected there for its cross-AZ two-thirds, not for
  its intra-AZ third.

## Decision

**The leaseholder publishes every durable delta in chain order. Within its own
AZ it pushes the delta to every ready pod. Across AZs it sends only a key-sized
hint to one relay pod per remote AZ, which reads the delta from the store and
pushes it to every ready pod in its AZ. Every pod publishes what it receives to
its own subscribers, and nothing else publishes in an assembled node.**

1. **The hook.** `LocalSequencer` reports each delta it makes durable to an
   `onCommitted(delta, epoch)` listener: once when `CommitLog` puts it, and
   once when `reconcileAmbiguousAppend` finds that an append whose response was
   lost did land (M10.16 review F3) -- never for a replayed or recovered entry.
   Own flushes, forwarded commits and inbox-drain commits all pass through it.
2. **Leaseholder, same AZ.** The delta is published locally (`ChainPublisher`)
   and POSTed to `/ctl/push` on every other ready pod of the leaseholder's AZ,
   each through a **single ordered sender** that retries a failed POST and
   drops only when the peer leaves the ready set or its queue overflows.
3. **Leaseholder, other AZs.** For each remote AZ with a ready pod, a
   `DeltaHintFrame` -- epoch and sequence, a fixed 24 bytes -- goes to that
   AZ's **relay**, through the same kind of single ordered, retrying sender: the ready pod with the
   lowest pod id, computed from the same `EndpointSlice` view by every pod, so
   no election and no registration.
4. **Relay.** Hints are handled **one at a time, in arrival order**, on one
   queue: for each, it reads the delta object named by `(epoch, sequence)`
   (`DeltaReader.ifWritten`) -- **one GET per (delta, remote AZ)** -- publishes
   it locally, and pushes it to every other ready pod of its AZ as in (2).
   **A failed read is retried with bounded backoff (about 30 s) before the next
   hint is looked at**, so a transient store error delays the AZ rather than
   reordering it; a read that still fails is a lost delta for that AZ, counted.
   A hint whose object does not exist (a stale or forged hint) is dropped
   without advancing anything.
5. **Receiving.** `/ctl/push` accepts a delta only from a ready pod of the
   receiver's own AZ; `/ctl/hint` only from a ready pod, and only when the
   receiver is its AZ's relay. Everything received goes to the pod's
   `ChainPublisher`, which drops any `(epoch, sequence)` not after the last one
   it published, so a duplicate is harmless. The pod's `SubscriptionHub`
   delivers only to runs with local subscribers, so a pod with none publishes
   nothing and reads nothing.
6. **Held bytes.** A writer holds the bytes of the segment it just PUT *before*
   committing it, in a byte-bounded map that evicts oldest first, and forgets
   them if the commit fails or is deferred. Because every delta reaches every
   ready pod, a writer's own segment always comes back to it; the publisher
   uses the bytes once and drops them, so the writer serves its own segment at
   zero GETs (M10.15). An evicted entry publishes cold.
7. **`DefaultIngest` stops publishing its own commits** when the assembly wires
   the `ChainPublisher` (`publishThroughChain`); built without one, as unit tests
   build it, it publishes directly as before.
8. **Counted.** Pushes are `CrossAzBytes.Transport.DELTA_PUSH` (same-AZ by
   construction) and hints `DELTA_HINT`, both against the peer's AZ from the
   view.

## Alternatives considered

- **Push whole deltas to interested pods in every AZ** (this record's first
  draft). ≈3.6% of ingested bytes cross-AZ at scale; refused above.
- **Push once per remote AZ, relay within it.** Cross-AZ bytes then scale with
  AZs rather than pods, but each is still the whole delta: 3.6 MB/s at the
  corpus's rate, the same 36× over. The hint keeps only the key on the
  expensive link and pays one GET per (delta, remote AZ) -- 24 GETs/s at 12
  deltas/s and three AZs, ~$25/month, a rate in deltas and AZs, which
  non-negotiable 6 allows.
- **Directed intra-AZ push with interest registration** (ADR-0012's "explicit
  registration with the sequencer"). Saves intra-AZ bytes, which are free, and
  costs a registry, a handshake, and a race in which a commit lands before its
  subscriber's registration and is never delivered (M10.16 review F4). Not
  worth it until an intra-AZ link is shown to saturate; the pushed delta is
  ~1.8 MB/s per pod at the corpus's rate.
- **Every pod tails the chain from the store.** A GET per delta per pod whether
  or not anyone subscribes -- $75-224/month at 6-18 pods (research 05) and a
  rate in pods × deltas.
- **Each writer pushes its own segments.** Runs of one stream come from many
  writers and only the chain orders them; out-of-order arrival reads to a
  consumer as a gap, then a duplicate, then lost records.

## Consequences

- A consumer served by any ready pod receives every record of its streams in
  chain order. Cross-AZ: one hint per (delta, remote AZ), bytes independent of
  the delta's size. Store: one GET per (delta, remote AZ). Intra-AZ: the delta
  per ready pod.
- **NFR-5 at any rate, not only the corpus's.** The hint is a fixed cost per
  delta: 24 bytes × (AZs − 1) = 48 bytes at three AZs, so it stays under 0.1%
  of ingested bytes whenever a delta carries more than 48 KB -- true of every
  size-triggered flush and of any interval flush above ~48 KB. In the low-rate
  regime (ADR-0072), where a delta may carry a few KB, the hint and the
  existing per-flush `COMMIT_FORWARD` term have the same shape and together can
  exceed 0.1% with no defect; M10.5 reports both terms separately so a reader
  can tell a rate effect from a leak.
- Idle: no commits, no pushes, no hints, no GETs. The leaseholder's lease and
  checkpoint cadence is unchanged and remains the ingester's idle rate (M9).
- ⚠️ **A LOST PUSH IS NOT REPAIRED ON A FOLLOWER IN M10.** Senders retry, so a
  push is lost only when its receiver or the relay dies, restarts, a queue
  overflows, a relay's store read fails past its retry budget, or a term is
  fenced or closed before an ambiguous append is reconciled. The consumer then sees a gap, and gap repair (M8.24's catch-up)
  is served only by the leaseholder (`Assembly.respondCatchUp`: "this node has
  no serving committed chain"). A plugin consumer on a follower pauses live
  delivery for a repair that cannot come. M10.22 carries catch-up served from
  a follower; until it lands, this is a known failure mode, not a claim.
- Leaseholder loss pauses pushes until the next term; deltas committed by the
  old term before it lost the lease and not yet pushed are the same gap.
- A relay change (a pod joins or leaves) can deliver a hint to a pod that is no
  longer the relay; it drops it, and the AZ misses that delta -- the same gap.
