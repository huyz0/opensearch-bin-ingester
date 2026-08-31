# Discovery and tailing: how the plugin learns there is new data

**Status:** proposal · **Confidence:** high (the cost argument is arithmetic; the protocol is
conventional) · **Last updated:** 2026-08-29

**Read this if:** you are implementing the tail channel, the subscription protocol, or the
consumer's fallback path.
**One-line takeaway:** discovery must be **pushed**, not polled. A push channel makes an idle
cluster cost **$0** instead of $16,600–$207,000/month, and it removes discovery latency from the
end-to-end budget entirely.

---

## 1. Why polling loses, in one table

From [poller-semantics](../20-opensearch/02-poller-semantics-and-cost.md) §2 — Scenario A,
1,600 shards, idle cluster:

| Discovery mechanism | Object-store requests/s | Cost/month (idle!) | Discovery latency |
|---|---|---|---|
| LIST the data prefix per shard | 16,000 | $207,000 | ~100 ms |
| GET a marker object per shard | 16,000 | $16,600 | ~100 ms |
| GET marker, node-level, 1 Hz | 9 | $9 | ~1 s |
| **Push over a persistent connection** | **0** | **$0** | **~1 ms** |

The third row is the "reasonable compromise" that a hurried implementation would ship. Note it is
both more expensive *and* 1000× slower than the push channel. There is no trade-off here — push
dominates on every axis. The only cost is a persistent connection per node.

## 2. The tail channel

One **HTTP/2 connection per OpenSearch data node** to an ingester node **in the same AZ**
([az-topology](05-az-topology-and-data-flow.md)). Not one per shard — see
[poller-semantics](../20-opensearch/02-poller-semantics-and-cost.md) §4.

```
POST /v1/subscribe            (request body: subscription, kept open; response: event stream)

  -> { "subscriptions": [ {"index":"<uuid>", "partitions":[0,3,7], "fromOffset":[912,44,0]} ],
       "az": "us-east-1a", "nodeId": "..." }

  <- event: batch
     { "index":"<uuid>", "partition":3,
       "firstOffset":45, "recordCount":128,
       "segment":"data/2026/08/29/22/1756503412345-p7-01J…-h76800-ZAAA….bseg",
       "byteStart":184320, "byteLen":65536, "codec":"zstd" }

  <- event: tail        { "index":"<uuid>", "partition":3, "endOffset":173 }   # for lag, no data
  <- event: reassign    { "index":"<uuid>", "partition":3, "chain":"ctl/log/pod-7/4/" }
  <- event: heartbeat   { "t": 1756503412345 }
```

Properties that matter:

- **Metadata only.** Record bytes never traverse the ingester. The plugin fetches
  `segment[byteStart, byteStart+byteLen)` from the object store directly — free in-region, and it
  removes the ingester from the bandwidth path entirely. This is a real advantage over WarpStream,
  whose agents proxy fetches ([warpstream.md](../10-prior-art/01-warpstream.md) §6).
- **Coalescable.** Events for many partitions from one segment arrive together, so the plugin's
  `FetchCoalescer` can turn them into **one** ranged GET (rule R4).
- **`tail` events keep `getPointerBasedLag()` and `latestPointer()` free** — they are called every
  10 s per shard and must never touch the store
  ([poller-semantics](../20-opensearch/02-poller-semantics-and-cost.md) §5).
- **Heartbeats** distinguish "idle" from "dead" so the plugin knows when to fail over rather than
  silently stalling — an important detail, since a stalled subscription looks exactly like a quiet
  cluster.
- **Resumable:** the subscription carries `fromOffset` per partition, so reconnection is exact and
  needs no re-listing.

### Transport choice
HTTP/2 with a streamed response (chunked NDJSON events) over Helidon SE's blocking API is the
simplest thing that works and multiplexes all partitions on one connection. SSE is a reasonable
alternative with better proxy compatibility and worse framing efficiency. WebSocket buys
bidirectionality we do not need. **Recommendation: HTTP/2 + NDJSON events**, with the subscription
sent as the request body and updated by re-issuing the call when the node's shard set changes.

## 2a. Who fetches the bytes? — decided: the ingester, not the plugin

This is the single highest-leverage read-side decision, and at ~300 data nodes it is not close.

### The vice that kills direct reads
A data node hosts ~400 of ~120,000 streams, so its share of any segment is ~0.33%. Its runs are
scattered through the segment (which is sorted by `(index, partition)`, not by node). That leaves
two bad options and one good one:

| Strategy | GET/s @250 ms flush | Cost/month | Bandwidth |
|---|---|---|---|
| **A1** plugin reads S3, whole object coalesced | 3,600 | **$3,732** | **29.3 GiB/s aggregate — 100 MiB/s *per node* for 0.33 MiB/s of useful data (300× amplification)** |
| **A2** plugin reads S3, precise per-run ranges | 1,440,000 | **$1,492,992** | 0.10 GiB/s (useful only) |
| **B** plugin reads from a same-AZ ingester node | **24** | **$25** | **0.10 GiB/s total — 0.33 MiB/s per node** |

A1 and A2 are the two horns: coalescing (rule R4) is correct at 9 nodes and becomes a 300×
bandwidth tax at 300 nodes, while precise ranges trade that for an absurd request count. **The
proxy escapes the vice entirely** because the ingester node fetches once and fans out only the bytes
each node actually wants.

**150× fewer requests and 300× less bandwidth.** At a 2 s flush the same comparison is
$467 / $186,624 / **$3**.

### Why the ingester's cache hits and the plugin's does not
- The ingester node that **wrote** the segment still has it in RAM
  ([§2b](#2b-serve-tail-bytes-from-the-write-buffer-adopted-from-automq)) ⇒ **0 GETs in the writing
  AZ**; one GET per other AZ ⇒ **2 GETs per segment, independent of node count**.
- An ingester node's cache is shared across **~100 data nodes and all their shards** in its AZ. A data
  node's local cache holds a segment of which ~0.33% was ever relevant to it, and it will not read
  that segment again — the hit rate is close to zero. **Caching only pays where fan-in is high, and
  the fan-in point is the ingester node, not the data node.**

### Second-order benefits
- **It buys back the latency budget.** Read cost stops depending on the flush interval
  ($25/month at 250 ms vs $3/month at 2 s — both negligible), so the 2 s flush recommended purely to
  suppress read cost is no longer needed. **Keep 250 ms and take the low latency**; see
  [cost-model §5b](../00-problem/02-cost-model.md).
- **It makes placement-aware segment assembly unnecessary** ([50-open-questions](../50-open-questions.md) Q18
  can be closed).
- **It shrinks the plugin dramatically**: no AWS SDK in the OpenSearch JVM, no object-store
  credentials in cluster state, no block cache competing with the data node's heap, far less
  plugin-security-policy surface ([plugin-packaging §2](../20-opensearch/03-plugin-packaging.md)).
- Latency *improves* — RAM or same-AZ hop instead of an S3 round trip.

### The costs, stated honestly
| Cost | Mitigation |
|---|---|
| The ingester is now on the read critical path | It is stateless and HA. An outage causes **indexing lag, not data loss** — the log is durable in the bucket. Keep the fallback ladder (§3) and make failover automatic |
| Serving capacity must be sized | ~33 MiB/s in + ~33 MiB/s out per pod at 100 MiB/s ingest across 3 pods. Small, but it is new capacity: budget ~$150–300/month of compute, against $3,700/month of saved GETs |
| A slow subscriber can back up the pod | Bounded per-subscriber queues; degrade to coordinates-only ("here is the key and range, fetch it yourself") rather than dropping progress |
| Catch-up/backfill bursts can starve live traffic | **Redirect catch-up reads to the object store when fan-out is 1** ([fetch-modes §4](10-client-library-and-fetch-modes.md)) — removes the risk by construction rather than by rate limiting |

### Decision
**Primary path: the ingester serves reads** — but not exclusively. The ingester also issues signed
grants so a client can read the object store **directly** when fan-out is ~1 (catch-up replay) or the
pod is under pressure. The pod must **stream through, never buffer-then-forward** (that alone costs
6% on a slice). Full treatment of the three modes and the consumer library:
**[10-client-library-and-fetch-modes.md](10-client-library-and-fetch-modes.md)**.

 Live tail is delivered on the subscription (inline when
small, §2c); catch-up is a ranged fetch from the same-AZ ingester node.
**Fallback: direct S3 from the plugin** — whole-object reads, no cache, no cleverness — used only
when no ingester node is reachable or the data is older than the cache window. It stays in the plugin
as break-glass, not as the hot path.

> ⚠️ **This corrects [comparison-matrix §2](../10-prior-art/04-comparison-matrix.md), item 2**, which
> claimed that the plugin reading the object store directly is an advantage over WarpStream's
> agent-serves-fetches model. That is true at ~9 nodes and false at ~300: we converge on WarpStream's
> design for exactly the reason they chose it. The advantage we *do* keep is that our serving tier is
> the same process that buffered the write, so the writing AZ needs no GET at all.

### Which pod owns a segment within an AZ
The segment key embeds `<podShortId>` ([object-layout §5](01-object-layout-and-format.md)):
- **In the writing AZ:** the owner is the writing pod itself, so the bytes are already in its buffer.
- **In other AZs:** consistent-hash the segment key onto that AZ's pods; the owner does the single
  GET and siblings proxy from it (intra-AZ, free).


## 2b. Serve tail bytes from the write buffer (adopted from AutoMQ)

**The pod that wrote a segment still has its bytes in RAM.** AutoMQ exploits exactly this: after
uploading, `LogCache.markFree()` only *marks* a block free and `tryRealFree()` reclaims it only
above 90% of `walCacheSize`, so tail reads are served from the write buffer and never touch S3
([automq §8.2](../10-prior-art/02-automq.md)). Copy the policy verbatim.

Applied to our topology, the plugin fetches ranges from a **same-AZ ingester node**:

| Case | Object-store GETs |
|---|---|
| Serving pod wrote the segment (writing AZ) | **0** — RAM hit |
| Another AZ | **1** per segment, to populate that AZ's cache |
| **Total, 3 AZs** | **2 per segment — independent of data-node count** |

At a 2 s flush that is 1.5 segments/s × 2 = **3 GET/s ≈ $3/month**, versus **$3,732/month** for
naive per-node fetching at ~300 nodes ([cost-model §5b](../00-problem/02-cost-model.md)). It also
removes an S3 round trip from the tail latency path.

**Buffer policy to implement:**
- After a successful segment PUT, keep the buffer resident and registered in a
  `segmentKey -> buffer` map. Do **not** release it.
- Reclaim oldest-first only when the pool exceeds a high-water mark (AutoMQ uses 90%).
- Track hit rate; a low hit rate means the retention window is shorter than consumer lag and the
  pool should grow.
- The write buffer and the read cache are the **same memory**. Do not build two.

## 2c. Inline small deltas in the notification (adopted from AutoMQ)

AutoMQ's subscription response carries `ConfirmWalDeltaData`, described in the schema as *"the
confirm WAL delta data between two end offsets. It's an optional field. If not present, the client
should read the delta from WAL."* When the delta is small, **the notification is the data** and the
subscriber issues no fetch at all.

This matters disproportionately at our scale: with ~9,990 trickle indices flushing every
`maxStreamLatency` ≈ 5 s ([object-layout §7b](01-object-layout-and-format.md)), a typical batch is a
few KB. Sending coordinates and then fetching them is two round trips for something that fits in the
event.

```
<- event: batch
   { "index":"…", "partition":3, "firstOffset":45, "recordCount":128,
     "segment":"…", "byteStart":184320, "byteLen":65536, "codec":"zstd",
     "inline": "<base64 or length-prefixed binary frame>"   // optional
   }
```

**Rule for when to inline** — this is a cost decision, not a size heuristic:
- Bytes already in the **serving pod's AZ** (its own buffer, or its AZ cache): intra-AZ transfer is
  **free**, so inline up to a generous cap (default 256 KiB) and save the plugin a fetch entirely.
- Bytes **only in another AZ**: compare against the **~19.5 KiB crossover**
  ([cost-model R12](../00-problem/02-cost-model.md)) — below it, shipping the bytes costs less than
  the GET it saves; above it, send coordinates. In bulk this matters enormously: inlining
  100 MiB/s cross-AZ would be ~$340/day. ⚠️ **The refined rule is a size test, not a blanket ban**:
  a trickle index's 8 KiB batch is 0.4× a GET and is worth inlining even cross-AZ, while anything
  past ~20 KiB is not.
- Always send coordinates alongside `inline`, so a subscriber that drops or distrusts the inline
  payload can fetch normally. The inline field is an accelerator, never the source of truth
  (consistent with §6's best-effort push decision).

## 2d. Session-based incremental subscription (adopted from AutoMQ)

`AutomqGetPartitionSnapshot` carries `SessionId` + `SessionEpoch` with per-partition
`ADD | PATCH | REMOVE` operations — Kafka's incremental fetch session pattern (KIP-227), so responses
carry *changes* rather than full state.

At 120,000 streams over ~300 nodes, a node subscribing to ~400 streams cannot afford to re-send full
subscription state on every reconnect, shard relocation or index creation. The subscription protocol
must therefore be:

```
POST /v1/subscribe  { sessionId: 0, epoch: 0, subscriptions: [...] }   // full, first time
   <- { sessionId: 42, epoch: 1, ... }
POST /v1/subscribe  { sessionId: 42, epoch: 1,
                      add: [...], remove: [...] }                       // incremental thereafter
```

- The server may invalidate a session at any time (restart, eviction) by returning a "reset" signal;
  the client then re-sends full state. Do not assume the server remembers forever.
- `epoch` orders concurrent requests within a session and makes retries idempotent.
- Shard relocation becomes an `add`/`remove` pair, not a resubscribe.


## 3. The fallback ladder

The plugin must survive the ingester being unavailable — an ingester outage must degrade latency, not
correctness or availability.

| Tier | Trigger | Mechanism | Cost |
|---|---|---|---|
| 0 | normal | push subscription | 0 req |
| 1 | connection lost | reconnect (jittered backoff) to another **same-AZ** pod, then any pod | 0 req |
| 2 | no pod reachable | poll the commit chain: speculative `GET ctl/log/<pod>/<epoch>/<seq+1>.delta` (404 = nothing new), once per **node** per interval | 1 GET/node/interval |
| 3 | chain unreadable / cold start | resolve leases, read newest checkpoint, replay deltas | tens of GETs, once |
| 4 | full disaster | `LIST` the data prefix over a bounded time window, filter keys by the embedded membership filter, read segment headers | 1 LIST/1000 keys ([bloom-in-key](02-partition-bloom-in-key.md) §2) |

Tier 2 at a 5 s interval costs 9 nodes × 0.2/s = 1.8 GET/s ≈ **$2/month** — a perfectly acceptable
degraded mode. **Tier 4 must never run automatically on a hot path**; gate it behind an explicit
recovery action or a long timeout.

## 3b. Why peers can tell you data *exists*, but never that it *doesn't*

⚠️ Read this before proposing "ask a peer instead of the object store, and reach
consensus on whether there is new data". The first half works and is adopted; the
second half cannot work, and — more usefully — **is not needed**.

### The asymmetry

| Claim | Kind | Can a peer establish it? |
|---|---|---|
| "There is a delta at seq *N+1*" | **positive** | **Yes.** And it is self-verifying: deltas are write-once, immutable and CRC-covered, so you fetch it and check. A peer cannot fabricate one that survives verification |
| "There is **no** delta at seq *N+1*" | **negative** | **No.** A delta may have landed a microsecond ago that no peer has seen. "None of us has seen one" is not "none exists" |

Asking more peers does not help, and a quorum does not either: **a majority vote
over non-authoritative replicas of a fact does not make the fact authoritative.**
Consensus about whether data exists is only meaningful when the consensus group
*owns* the data. Ours does not — the object store does. Making the peers own it is
[ADR-0011](../../internal/product/decisions/0011-no-consensus-cluster.md), rejected.

A `404` on `chain/<seq+1>.delta` **is** authoritative, because S3 has been
read-after-write consistent since December 2020 and the store is the sole writer's
arbiter. That is the difference: the store is in the write path, a peer is not.

### The better observation: nobody needs to establish absence

You never have to prove "nothing is there". You have to be **woken when something
arrives** — which is what the push channel does, and why R3 demands zero requests
while idle. **Polling to confirm absence is the thing the design already
eliminated**, not a thing to make cheaper.

What that probe costs when it does run:

| | Rate | Cost |
|---|---|---|
| Steady state (push) | 0 | **$0/mo** |
| Ingester nodes probing `seq+1` every 3 s | 2 GET/s | $2/mo |
| 300 plugin nodes on the tier-2 fallback, 5 s | 60 GET/s | $62/mo |
| 300 plugin nodes polling at 1 s | 300 GET/s | $311/mo |

So peer-asking would be optimising a **$2/month** path in the case where peers are
reachable at all — and in the case where they are not (an ingester outage, which is
precisely when the fallback runs), there are no peers to ask.

### What we do adopt: positive facts only

Piggybacked digests carry `chainPosition: N`
([ADR-0012](../../internal/product/decisions/0012-peer-mesh-without-gossip.md)).
A pod that sees a **same-AZ** peer ahead of it pulls seq *N+1…N* from that peer
rather than the store — a positive, verifiable, free-to-transfer fact.

A peer reporting the *same* position tells you nothing and must **not** be treated
as "nothing new". The only correct responses to "I might be behind" are: wait for a
push, or ask the store.

### The one place absence genuinely matters — and it is resolved by writing

Sequencer failover must establish that the old chain ended at *N*. That is exactly
a negative fact, and it is safety-critical.

⚠️ **The seal protocol never asks whether seq *N+1* is empty. It claims it.**
A conditional write both establishes and creates the fact, atomically, in the one
place that arbitrates. This is why the protocol needs no consensus layer, and it
is the general lesson: **when you must know that nothing is there, take the slot —
do not survey opinions about it.**


## 4. Ingester-side fan-out

The sequencer knows the moment offsets are assigned. It must fan out to subscribers efficiently:

- Subscribers register interest as `(indexUUID, partitionId) -> connection`. Keep a
  `Map<StreamId, List<Subscriber>>` updated on (re)subscribe.
- Fan-out happens **after** the commit is durable — never notify about uncommitted offsets, or a
  consumer could read a record that a failover later un-assigns
  ([metadata-and-cas](03-metadata-and-cas.md) §7 I4).
- A subscriber on a **different pod** from the sequencer must be reached by an internal hop. Two
  options: (a) the sequencer forwards to peer pods (metadata only, small), or (b) every pod tails
  the commit chain and fans out locally. **(a) is cheaper** (no extra object-store reads) and uses
  the same peer channel the commit path already needs; (b) is more robust. Start with (a) plus (b)
  as the fallback — they share the event schema.
- Backpressure: a slow subscriber must not stall the sequencer. Bounded per-connection queue;
  on overflow drop to a `tail` event ("you are behind, offsets up to X exist") and let the consumer
  catch up by reading the chain. **Dropping detail but never dropping the fact of progress** is the
  correct degradation.

## 5. End to end: what the client learns, when, and what it costs

**Question this answers:** does the plugin learn about data it cares about
immediately, without reading or listing the object store?

**Answer: yes for discovery, always. Yes for the bytes too, in the common case.**
The floor is not the notification — it is *durability plus ordering*, because
notifying earlier would let a consumer read a record a failover could un-assign
(invariant I4).

### The path, with no object-store access by the plugin

```
producer -> ingester node (same AZ)        buffered by (index, partition)
   -> run joins a segment                 at minRunBytes or maxStreamLatency
   -> segment PUT                         durable
   -> commit RPC -> sequencer             metadata only
   -> delta putIfAbsent                   ordered; offsets now immutable
   -> push to the pod holding the subscriber   (registered interest; metadata)
   -> push to the plugin                  same-AZ, persistent HTTP/2
   -> readNext returns                    the parked thread wakes
```

**No LIST. No GET by the plugin.** The subscription already carries the plugin's
`(index, partition)` set, so the sequencer pushes only what that node wants —
there is no client-side filtering either.

### After the segment PUT — what still has to happen

⚠️ **The client cannot be told anything the instant the segment lands in S3.** An
offset does not exist until the commit, because ordering is assigned at commit and
not at write ([ADR-0001](../../internal/product/decisions/0001-segments-carry-no-absolute-offsets.md)).
Telling a consumer about an uncommitted record would let it index something a
failover could un-assign (invariant I4).

So after the PUT completes there is still **one object-store round trip**:

```
segment PUT done
  -> commit RPC reaches the sequencer          (metadata, ~1 ms)
  -> sequencer batch wait                      0 - commitBatchInterval
  -> delta putIfAbsent                         20 - 100 ms      <-- the floor
  -> push, two hops                            1 - 3 ms
```

**21 – 353 ms at the default 250 ms commit batching.** That is the price of
assigning order after writing, and it is the same trade WarpStream makes — their
~400 ms p99 write is a segment PUT plus a metadata commit, for the same reason.

The commit batch interval is a dial, and because `S = 1`
([ADR-0007](../../internal/product/decisions/0007-one-sequencer-slot-by-default.md))
exactly one sequencer writes the chain, so the cost is simply `1 / interval`:

| Commit batch | Post-PUT latency | Commit PUTs | Cost |
|---|---|---|---|
| **250 ms (default)** | **21 – 353 ms** | 4/s | **$52/mo** |
| 100 ms | 21 – 203 ms | 10/s | $130/mo |
| 50 ms | 21 – 153 ms | 20/s | $259/mo |
| 25 ms | 21 – 128 ms | 40/s | $518/mo |

⚠️ **The delta PUT's 20–100 ms is a hard floor** — it is a small object, so it is
TTFB-dominated and no amount of batching removes it. Beating it needs a fast
metadata path, i.e. a WAL or consensus, which is
[ADR-0011](../../internal/product/decisions/0011-no-consensus-cluster.md)'s stated
reversal condition for a sub-50 ms requirement.

**Safe optimisation, no consistency cost:** an ingester node should **prefetch a
segment when it becomes durable, not when it is committed**. Prefetching is
cache warming with no visibility implication, so it can start 20–350 ms earlier
and be resident well before the push arrives.

### Latency, busy index

| Stage | ms |
|---|---|
| buffer → segment flush trigger | 0 – 250 |
| segment PUT | 20 – 100 |
| **sequencer batch wait** | **0 – 250** |
| **delta PUT** | **20 – 100** |
| push (two hops) | 1 – 3 |
| poller wake + index | 1 – 5 |
| **Total** | **~42 – 708 ms** |

⚠️ **Corrected 2026-08-30.** An earlier version of this table said 42–458 ms: it
carried the delta PUT but omitted the **sequencer batch wait**, which at the
default interval is up to another 250 ms. The commit is two steps, not one.

**Discovery itself is ~1–3 ms of that.** Everything else is making the data
durable and ordered, which must happen first.

### Latency, trickle index — the honest number

For the ~9,990 low-traffic indices, `maxStreamLatency` dominates and everything
above is noise:

```
wait for minRunBytes (64 KiB) or maxStreamLatency (5 s)   0 – 5,000 ms   <-- the floor
then the pipeline above                                      42 –   708 ms
TOTAL                                                     ~5.0 – 5.7 s
```

⚠️ **This is the number to put in front of an operator**, not the 42 ms one. A
busy index sees a few hundred milliseconds; a trickle index sees ~5 s, and that is
a deliberate trade for segment shape and key-filter feasibility
([object-layout §7b](01-object-layout-and-format.md)). It is surprising if
undocumented.

### Do the bytes cost a fetch?

| `via` | Plugin reads the object store? | When |
|---|---|---|
| `inline` | **No** — the bytes are in the event | small batches; the common case for trickle indices, which are under the ~19.5 KiB crossover even cross-AZ |
| `proxy` | **No** — same-AZ pod serves from RAM (it wrote the segment, or prefetched on commit) | the default for busy streams |
| `direct` | Yes, via a signed URL | catch-up with fan-out 1, or the pod shedding load |

So in steady state **the plugin touches the object store for neither discovery nor
data**. The only S3 GETs on the read path are the pod's prefetch — one per segment
per non-writing AZ, issued *before* the request arrives.

### The poller's own contribution

`DefaultStreamPoller` sleeps 100 ms after an *empty* poll, and that sleep is
hard-coded. Since our `readNext` blocks on the queue for the full `pollTimeout`, a
push during the block returns immediately; the sleep only bites if data arrives
inside that 100 ms window.

| `index.ingestion_source.poll.timeout` | Chance of hitting the sleep | Expected added latency |
|---|---|---|
| 1 s (default) | 9.1% | 4.6 ms |
| 5 s | 2.0% | 1.0 ms |
| **30 s** | **0.33%** | **0.17 ms** |

Raising `poll.timeout` costs nothing and shrinks the window. Do it.

### Where "right away" does not hold

Push is **best-effort** by design (§6) — the commit chain is the source of truth.
The four cases where the client learns late, and what covers each:

| Case | Delay | Covered by |
|---|---|---|
| Push event dropped (slow subscriber, pod restart) | up to the tier-2 poll interval | chain poll, ~$2/month |
| Shard relocated; new node has not registered interest yet | registration round trip | chain poll |
| Sequencer failover in progress | lease TTL, < 5 s target | resumes on its own |
| Object store degraded | until it recovers | producers retry; consumers block harmlessly |

In every case the guarantee is *eventually, via the chain*; push is the fast path,
not the correctness path. That is what makes it safe to keep push simple.

## 6. Open questions

- Should the subscription be per node (all indices) or per index? Per node minimises connections;
  per index simplifies authorization. **Leaning per node with an index allow-list.**
- Do we need at-least-once *delivery* of push events, or is the chain always the source of truth
  and push a pure accelerator? **Strongly prefer the latter** — it makes the push path
  best-effort and therefore simple, and makes tier-2 fallback provably correct.
- How does the plugin discover service endpoints, and how does it learn its own AZ? See
  [az-topology](05-az-topology-and-data-flow.md) §4.
