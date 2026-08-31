# Compaction, retention and garbage collection

**Status:** proposal · **Confidence:** medium-high · **Last updated:** 2026-08-29

**Read this if:** you are deciding what to build in v1, or sizing storage cost.
**One-line takeaway:** we can **defer compaction entirely** — a luxury WarpStream and AutoMQ do not
have, because we have exactly one consumer per partition and it normally sits at the live edge.
Retention and GC, however, are v1 requirements: GC is a correctness matter, not just a cost one.

---

## 1. Why prior art needs compaction and we (mostly) do not

WarpStream compacts because a lagging consumer reading historical data gets no scan-sharing benefit
and can double GET counts; AutoMQ compacts because Kafka consumers routinely replay days of history
([warpstream.md](../10-prior-art/01-warpstream.md) §4, [automq.md](../10-prior-art/02-automq.md) §6).

Our profile is different:

- **One consumer per partition** — the shard that owns it. No fan-out, no independent consumer
  groups replaying at different positions.
- **The consumer is normally at the live edge**, and at the live edge every segment is dense with
  useful data — read amplification is naturally low.
- **Data is transient.** Retention exists to survive consumer downtime, not to serve replay.

Catch-up reads only happen after: a node restart, a shard relocation, a long ingestion pause, or a
`pointer.init.reset` to an earlier position.

## 2. The read-amplification question, quantified

A catching-up shard reading a 250 ms segment that holds 1,600 streams gets
`1/1600` of the object relevant to *its* partition — but it **coalesces with all the other shards on
the same node** (~178 of 1,600 streams ⇒ ~11% relevant), and the whole-object GET is one request
either way. Amplification costs **bandwidth, which is free in-region**; it costs a request only if
you split it into more requests.

So: catching up over 1 hour of Scenario A backlog = 12 segments/s × 3,600 s = **43,200 segments**,
one GET each per node = 43,200 GETs ≈ **$0.017 per node**. Compaction would reduce this to perhaps
$0.002. **Compaction is not economically justified by the read path here.**

Where it *would* pay:
- A shard that must replay **days**, not hours (then retention is the real question).
- Storage cost: merging 250 ms segments does not reduce bytes, but it does reduce per-object
  overhead and the number of objects to enumerate during GC.
- **Commit-log size**: this is the real one — 12 segments/s × 1,600 runs each is a lot of index
  entries in the delta chain. Compaction that rewrites a partition's data into one object collapses
  many index entries into one.

**Conclusion:** treat compaction as a **v2, metadata-driven** feature. Trigger it on *commit-log
index size*, not on read amplification. Note this is a genuine divergence from prior art, made
possible by a constraint (single consumer) we should not quietly give up later.

## 3. Retention as the primary cost dial

From [cost-model](../00-problem/02-cost-model.md) §5: Scenario A stores 8.64 TB/day.

| Retention | Storage cost/month | Survives |
|---|---|---|
| 1 h | $8 | a pod restart |
| **6 h** | **$50** | a bad deploy, a node replacement |
| 24 h | $199 | an overnight outage |
| 7 d | $1,392 | a disaster |

**Storage exceeds API cost past ~3 h of retention** — an unusual property worth stating plainly,
because it inverts the intuition that object storage is "cheap to keep, expensive to touch."

**Default: 6 hours**, configurable per index. Retention must be expressed as
`max(timeRetention, unconsumedData)` — **never delete data no consumer has read.**

The full treatment, including why we must *not* build a Kafka-style offset store and the five ways a
naive watermark silently loses data, is in
**[09-consumer-position-and-watermarks.md](09-consumer-position-and-watermarks.md)**. The rule in one
line: **a consumer watermark may only extend retention, never shorten it below the time floor**,
because the position OpenSearch reports is the in-memory pointer, not the committed one.

## 4. Garbage collection — a v1 requirement

Three classes of garbage:

| Class | Cause | Detection | Action |
|---|---|---|---|
| **Orphan segments** | pod died after PUT, before commit | in the data prefix, not referenced by any delta/checkpoint after a grace period (≫ commit latency) | delete |
| **Expired segments** | past retention and below every consumer's low watermark | commit log + progress | delete |
| **Superseded deltas** | covered by a newer checkpoint and past retention | checkpoint seq | delete |
| **Abandoned multipart uploads** | mid-PUT crash | not visible via LIST | **bucket lifecycle rule** (`AbortIncompleteMultipartUpload`, 1 day) — do not hand-roll this |

Mechanics:
- **DELETE requests are free**, and S3 `DeleteObjects` batches 1,000 keys per call — GC is
  essentially free if driven from the commit log rather than from LIST.
- Run GC from the commit log (cheap, exact) and use a **LIST sweep only as a periodic
  reconciliation** (e.g. hourly, over one time-partition prefix at a time) to catch orphans the log
  by definition cannot know about. Bound it: 1 LIST per 1,000 keys, so a 43,200-object hour costs
  43 LIST ≈ $0.0002.
- GC is a **leased role**, same lease mechanism as the sequencer — never run it from multiple pods.
- **Grace period before deleting an orphan must exceed the maximum possible commit delay**,
  including the degraded inbox path. Getting this wrong deletes live data. Default generously
  (1 hour) and make it configurable.

## 5. If/when compaction is built

Design it after AutoMQ's model ([automq.md](../10-prior-art/02-automq.md) §6):

- **Merge**: many small mixed segments → one larger mixed segment. Streaming, one GET per input,
  one PUT per output, bounded memory.
- **Split**: once a partition has accumulated a threshold of contiguous data (AutoMQ:
  `streamSplitSize = 16 MiB`), promote it into its own single-stream object so catch-up is one
  sequential scan.
- **Atomicity**: compaction publishes a commit-log entry that *remaps* offset ranges to the new
  object. Because segments carry no absolute offsets
  ([object-layout](01-object-layout-and-format.md) §2), this is a pure metadata swap. Old objects
  are deleted only after the remap is durable **and** a grace period has elapsed — readers may hold
  cached references to the old key.
- **Never rewrite offsets.** Compaction changes *where* a record lives, never *which offset* it has.
  This invariant is what keeps the Lucene `_offset` field valid across compaction.

## 6. Open questions

- How does the consumer's low watermark reach the ingester? Push from the plugin, or does the ingester
  read `_cat`/ingestion-state APIs? (Push is simpler and avoids the ingester needing OpenSearch
  credentials.)
- What happens when a shard is relocated to a node in another AZ mid-backlog — does it re-read
  segments the new node's AZ has not cached? (Yes; a one-off cost, worth measuring.)
- Should retention be enforced by object-store **lifecycle rules** as a backstop against a GC bug?
  Cheap insurance, but it can delete unconsumed data — it must be set well beyond the configured
  retention, as a safety net only.
