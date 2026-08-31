# The consumer library and the three fetch modes

**Status:** proposal · **Confidence:** medium-high (latency figures are modelled, not measured —
see §1; the cache-destruction argument in §2 is structural) · **Last updated:** 2026-08-30

**Read this if:** you are implementing the plugin's fetch path, the ingester's serving path, or
deciding what the consumer library owns.
**One-line takeaway:** the proxy's latency penalty is **~0.5 ms if the pod streams through**, so
latency is not the reason to redirect. The real reasons are **fan-out ≈ 1** (catch-up reads) and
**load shedding**. Ship a consumer library that hides three modes — `inline`, `proxy`, `direct` — with
the **service choosing**, because only it knows cache state and fan-out.

---

## 1. The latency claim, quantified

Modelled with S3 TTFB 25 ms, 90 MB/s per connection, same-AZ RTT 0.5 ms, 1.25 GB/s intra-AZ:

| | 64 KiB slice | 8 MiB segment |
|---|---|---|
| Cache **hit** via pod | **0.6 ms** | **7.2 ms** |
| Miss, pod **streams through** | 25.5 ms first byte / 26.2 ms complete | 25.5 ms first byte / 118.7 ms complete |
| Miss, pod **buffers then forwards** | 26.3 ms | **125.4 ms** |
| **Direct** from S3 | 25.7 ms | 118.2 ms |
| **Proxy penalty (streaming)** | **+0.5 ms** | **+0.5 ms** |
| Hit speedup vs S3 | **46.6×** | 16.4× |

Three conclusions:

1. **The proxy penalty is ~0.5 ms** — noise against a 25 ms S3 TTFB. The concern is real but small.
2. **Buffer-then-forward costs 6% on a slice and 6 ms on a segment.** So: **the pod must stream
   through, never buffer-then-forward.** That is an implementation rule, not an architecture change,
   and it recovers most of what the redirect was meant to win.
3. **A cache hit is 17–47× faster than any S3 path.** The leverage is in *raising the hit rate*, not
   in optimising the miss path. That reframes the whole question.

> On a 64 KiB slice — the common case — transfer time is negligible and it is all TTFB. So caching
> is worth ~25 ms per fetch, and a redirect that lowers the hit rate is a latency *regression*, not
> an improvement.

## 2. The trap: naive redirect-on-miss destroys the shared cache

If the pod redirects the client to S3 whenever it misses, the fetch that *would have populated the
shared cache* instead populates a node-local cache nobody else can use. The pod's cache stays cold,
so the next node also misses and is also redirected — and so on for all ~100 nodes in the AZ.

**The result is exactly the design we rejected**: ~100 GETs per segment per AZ instead of 1, i.e.
back to $3,732/month and 300× bandwidth amplification
([discovery-and-tailing §2a](04-discovery-and-tailing.md)).

Any redirect design must therefore either (a) also trigger a cache fill, or (b) redirect only when
the pod knows the fan-out is ~1. (b) is better — it costs no extra GET.

## 3. On the tail path, misses are predictable — so prevent them

The pod does not have to wait to be asked. It follows the commit stream, so **the instant a segment
is committed it knows which streams it contains** and whether any of its AZ's subscribers want them.

**Prefetch on commit.** The fetch (≈25 ms) runs concurrently with commit → push event → plugin →
request, so the data is usually resident before the first request arrives.

- The pod that **wrote** the segment already has it — 0 GETs
  ([discovery-and-tailing §2b](04-discovery-and-tailing.md)).
- Each other AZ prefetches once — 1 GET, exactly the budgeted `2 GETs per segment for 3 AZs`.
- At ~100 nodes per AZ essentially every segment is wanted by someone, so speculative prefetch is
  almost never wasted. For small deployments, gate it on "at least one interested subscriber".

**On the live tail, the steady state should be ~100% hit rate.** A miss there is a bug or a cold
start, not a normal condition — instrument it and alert on it.

## 4. When to redirect: the fan-out rule

The pod knows how many of its AZ's subscribers want a given segment. That makes the decision
computable rather than a guess:

```
proxy cost   = 1 GET + pod bandwidth + 0.5 ms per consumer
direct cost  = N GETs, no pod bandwidth
=>  redirect iff N == 1 and the segment is not already cached
```

| Situation | Fan-out | Mode | Why |
|---|---|---|---|
| Live tail, cached/prefetched | high | **proxy** | 0.6 ms, 0 GETs |
| Live tail, cold (shouldn't happen — §3) | high | **proxy** + fill | preserves the shared cache |
| **Catch-up: one node replaying a backlog** | **1** | **direct** | no pod bandwidth, no starvation risk, GETs bounded by 1 consumer |
| AZ recovery: many nodes replaying the same backlog | high | **proxy** | N GETs would explode |
| Pod serving queue deep / overloaded | any | **direct** | **load shedding** — redirect instead of queueing |

The last row generalises the rule already in
[discovery-and-tailing §4](04-discovery-and-tailing.md) that a slow subscriber degrades to
coordinates-only. A redirect *is* coordinates-only, made useful.

The catch-up row is the one that matters most in practice: it removes the "restarting node starves
live tail" risk ([Q22](../50-open-questions.md)) by construction rather than by rate limiting.

## 5. The three delivery modes

Every batch event carries **coordinates**; the `via` field says how the ingester expects the client to
get the bytes:

```
<- event: batch
   { "index":"…", "partition":3, "firstOffset":45, "recordCount":128,
     "segment":"data/2026/08/30/…-h76800-Bx….bseg",
     "byteStart":184320, "byteLen":65536, "codec":"zstd",
     "via":"inline" | "proxy" | "direct",
     "inline":"<frames>",                       // when via=inline
     "grant":"https://…?X-Amz-Signature=…"      // when via=direct
   }
```

| Mode | Bytes come from | Chosen when |
|---|---|---|
| `inline` | the event itself | small batch already in the serving pod's AZ ([§2c](04-discovery-and-tailing.md)) — trickle indices, zero fetches |
| `proxy` | a same-AZ ingester node, **streamed through** | the default; cached or high fan-out |
| `direct` | the object store, via a **short-lived signed URL** | fan-out ≈ 1, cold data, or pod under pressure |

**The ingester decides.** The client must not guess, because only the ingester knows cache residency,
fan-out and its own load. The client *may* fall back (§7).

## 6. Signed URLs: keeping credentials out of the OpenSearch JVM

`direct` mode would normally require object-store credentials and an SDK inside the data node —
undoing the packaging simplification proxying bought us
([plugin-packaging §2](../20-opensearch/03-plugin-packaging.md)).

**Use a presigned/signed URL instead.** The ingester mints it locally (HMAC, microseconds, no API
call) and the client fetches with a plain `HttpClient`:

| Store | Mechanism |
|---|---|
| S3 | presigned GET, `Range` in the signed headers so the grant is scoped to one byte range |
| GCS | V4 signed URL |
| Azure Blob | SAS token, read-only, short expiry |
| Local FS | a loopback URL served by the pod |

Properties: short TTL (seconds), read-only, scoped to one key and ideally one range. **The plugin
needs no credentials, no cloud SDK, and no object-store IAM identity** — it holds only bearer grants
the ingester chose to issue.

### The multi-tenancy caveat, and the rule it implies
A segment bundles many indices. A grant for a whole segment therefore exposes other indices' bytes
to whoever holds it. Within a single OpenSearch cluster this crosses no boundary — shards relocate
anywhere, so every data node is already inside that trust domain. Across clusters it does.

> **Rule: the bundling universe is a trust domain.** Never co-bundle segments across OpenSearch
> clusters or tenants. If the ingester fronts *n* clusters, that is *n* segment streams
> (~$156/month each — see [failure-domains §3](08-failure-domains-and-resilience.md) for the
> per-stream PUT arithmetic). Cheap, and it makes `direct` mode safe by construction.

This is the first concrete constraint to come out of the security gap tracked as
[Q10](../50-open-questions.md).

## 7. What the consumer library owns

One library, used by the plugin and by any future consumer. Consolidating here is the point: two
hand-written fetch paths will diverge and one of them will be wrong.

```java
public interface BinStoreClient extends Closeable {
    Subscription subscribe(SubscribeRequest req, RecordHandler handler);  // session + epoch (§2d)
    List<Record>  fetch(StreamId stream, long fromOffset, int maxRecords, Duration timeout);
    long          tailOffset(StreamId stream);   // served from pushed state, never I/O
    void          reportProgress(Map<StreamId, Long> watermarks);         // GC watermark feed
}
```

Responsibilities:
- **Mode dispatch** — follow `via`; the caller never knows which path was used.
- **Endpoint discovery and AZ affinity** — prefer a same-AZ pod, fail over in-AZ then cross-AZ,
  and **log loudly on cross-AZ** ([az-topology §4](05-az-topology-and-data-flow.md)).
- **Read coalescing** — merge adjacent ranges before requesting, in every mode.
- **Frame decode + decompression** — one implementation of the segment format
  ([object-layout](01-object-layout-and-format.md)).
- **A small node-local cache** — sized for retries and `forcedShardPointer` re-reads
  ([poller-semantics §6](../20-opensearch/02-poller-semantics-and-cost.md)), **not** as a primary
  cache; the primary cache is the pod's, where fan-in is high.
- **The blocking-queue bridge** so `readNext(maxMessages, timeoutMillis)` parks with zero I/O.
- **The fallback ladder** (§8) and the cost counters.

Deliberately *not* in the client: offset storage (OpenSearch owns it —
[consumer-position](09-consumer-position-and-watermarks.md)), and any decision about *where* to
fetch from.

## 8. Fallback and failure

| Failure | Client response |
|---|---|
| `proxy` request fails | retry another same-AZ pod, then cross-AZ; then `direct` if it holds a valid grant |
| `grant` expired / 403 | ask the ingester for a fresh grant; do not cache grants across retries |
| No pod reachable at all | fallback ladder tier 2+ ([discovery-and-tailing §3](04-discovery-and-tailing.md)) — break-glass direct object-store reads, whole-object, no cleverness |
| Inline payload corrupt | ignore it and fetch by coordinates — inline is an accelerator, never the source of truth |

Because every event carries coordinates regardless of mode, **every mode can degrade to every other
mode**. That is the property that makes shipping all three safe.

## 9. Open questions

- Measure §1 against real S3 and MinIO — the table is modelled. TTFB variance (p99) may change the
  fan-out threshold.
- ✅ **`direct` grants are per-object**, which is safe under the §6 trust-domain rule and avoids one
  grant per coalesced range. Per-range where the provider supports signing `Range`.
- Does the fan-out rule need hysteresis? A segment can go from fan-out 1 to many as more nodes catch
  up; flapping between modes mid-fetch would be bad.
- Is prefetch-on-commit ever wasteful enough to need gating in large deployments, or is
  "some subscriber wants it" always true at ~100 nodes/AZ?
- Should the consumer library be published separately so non-OpenSearch consumers can use it, or stay
  internal to the plugin? Affects API stability commitments.
