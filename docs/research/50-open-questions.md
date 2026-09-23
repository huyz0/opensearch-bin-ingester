# Open questions — all answered

**Status:** every question raised during the research phase is now decided.
**Last updated:** 2026-08-30

Decisions that are expensive to reverse became ADRs in
[`docs/internal/product/decisions/`](../internal/product/decisions/). The rest are
recorded here with their evidence. What remains is a short list of constants that
**cannot** be settled without running code — §3.

---

## 1. Decided by ADR

| Q | Question | Decision | ADR |
|---|---|---|---|
| **Q1** | Who decides a record's partition? | Ours to choose. OpenSearch's own ingestion path **already breaks the routing invariant** — `RawPayloadIngestionMessageMapper` sets `_id = shardId + "-" + pointer`, and `MessageProcessorRunnable` performs no routing at all. Three modes: `explicit` (default), `routing_key`, `os_routing`. Reject out-of-range partitions, never fold | [0006](../internal/product/decisions/0006-partition-assignment-and-the-routing-invariant.md) |
| **Q2** | How many sequencer slots? | **S = 1**, with the slot dimension present in every key path so raising it is config, not a format change. ~60,000 assignments/s is a map lookup and an add | [0007](../internal/product/decisions/0007-one-sequencer-slot-by-default.md) |
| **Q5** | Do we need `putIfMatch` at all? | **Yes, for the lease and registry only.** Write-once for the commit log. The reason to avoid read-modify-write is *contention at rate*, which a lease rewritten every 3 s does not have. The `putIfAbsent`-only fallback is written down for the day a backend needs it | [0008](../internal/product/decisions/0008-cas-primitives-write-once-log-cas-lease.md) |
| **Q23** | `all_active` vs segment replication | **`SEGMENT` + `all_active=false`** (the OpenSearch default). The validator enforces a strict XOR; the setting is `Final`. One consumer per partition ⇒ 1× serving bandwidth | [0009](../internal/product/decisions/0009-replication-mode-segment-replication-primary-only-ingest.md) |
| **Q9** | Multi-tenant isolation and quotas | Rate limit, fair-share buffers, per-index segment share cap. Promotion deferred with a stated threshold | [0010](../internal/product/decisions/0010-multi-tenancy-and-security-model.md) |
| **Q10** | Security model | Trust domain = bundling universe; mTLS/token at both edges; ambient identity to the store; signed URLs to clients; SSE-S3 default, **SSE-KMS only with Bucket Keys** | [0010](../internal/product/decisions/0010-multi-tenancy-and-security-model.md) |
| **Q4** | Per-index prefixes vs shared bundles | **Single shared prefix per trust domain.** Promotion is an *isolation* mechanism, not a discovery one, and its threshold (~192 MiB/s for one index) is not reached at target scale | [0010](../internal/product/decisions/0010-multi-tenancy-and-security-model.md) |
| **Q6** | Consumer low-watermark reporting | Plugin pushes it on the subscription; watermarks may only **extend** retention | [0005](../internal/product/decisions/0005-no-consumer-offset-store.md) |

## 2. Decided here

### Q3 — Piggybacked commits (W3): **not in v1**
Folding the commit delta into the sequencer's own segment saves $156/month of
Scenario B's ~$500 and couples commit visibility to the leader's flush cadence
while complicating recovery. Build separate delta objects, measure at M9, adopt
only if the write path is a material fraction of a real bill.
**$156/month is cheap; a subtle recovery bug is not.**

### Q7 — Ordinal registry: **required**
Forced by ADR-0003: the exact bitmap encoding needs dense ordinals, and 16-byte
UUIDs in directory entries cost ~160 KB per segment at 10,000 indices versus
~40 KB for 2-byte ordinals. Ordinals are **never reused**; a deleted index's
ordinal is tombstoned, so a recreated index gets a new one and stale filters
cannot alias it.

### Q8 — Push-event delivery: **best-effort**
The commit chain is the source of truth; push is an accelerator. This makes the
push path simple and makes the tier-2 fallback provably correct, because a
dropped event costs latency and nothing else. Every event carries coordinates
even when it also carries inline bytes, so a subscriber can always fall back.

> ⚠️ **REVISION 2026-09-14 (ADR-0044): the ANSWER stands, the last sentence's reason does not.**
> Push is still best-effort and tier-2 fallback is still correct, but it is not the coordinates
> that make it so — **no reader in the tree consumes the event's `byteStart`/`byteLen`**, and
> every fallback path is a WHOLE-OBJECT read that finds its run through the segment's own
> DIRECTORY. ⚠️ A consumer-side bounded read is TWO expressible requests -- the header range `[0, PREAMBLE + headerLen)`, which is what `SegmentKey`'s `h<headerLen>` is for, then the run's slice -- and what is missing is only a `SegmentReader` entry point taking a directory and a slice separately. What is NOT expressible is `[0, objectLen)`, since nothing the consumer holds carries the object length, and any prefix `SegmentReader.open` would accept, since it checks the FOOTER magic first. ⚠️ **THIS MATTERS HERE MORE THAN ELSEWHERE**: this file is the
> decision log AGENTS.md routes a reader to first, and **M5.66** — the row that decides whether
> these fields come off the wire — would otherwise open Q8, find a DECIDED question saying the
> coordinates are what makes fallback work, and close its own second arm on a reason ADR-0044
> had already killed.

### Q11 — Format evolution: **readers lag writers by one release**
Every format carries a version or tag byte. Adding a field to a versioned struct
is not a contract change; changing the meaning of an existing one is. The policy:
1. Ship the **read** side first, in an earlier release.
2. Enable the **write** side only once every possible reader can parse it.
3. Readers keep handling the old shape until every writer of it has aged out —
   at least the retention window, and forever for a stalled tenant.
4. **An unknown tag or version means "read the header", never "no match".**
Enforced by procedure in [`wire-format-change`](../../.agents/skills/wire-format-change/SKILL.md).

### Q12 — Directory compression: **no**
The directory is ~0.9% of a segment and 100% of the recovery-path read.
Delta-encoding would shrink it ~4× but forfeits fixed-width binary search, which
is what makes a lookup free. Recovery is a cold path; a lookup is not. Revisit
only if recovery time is measured to hurt.

### Q13 — Cache compressed or decompressed: **compressed, and serve compressed**
Blocks never span runs ([object-layout §4](30-design-space/01-object-layout-and-format.md)),
so an ingester node can slice **compressed** blocks and forward them, letting the
client decompress its own. That is more cache entries per byte, less pod CPU, and
less bandwidth than caching decompressed. AutoMQ's block cache holds data blocks
for the same reason.

### Q14 — Structured concurrency: **do not build on it**
JEP 505 is the *fifth* preview in JDK 25; JEP 525 is the sixth in JDK 26;
finalisation is expected in **JDK 27**. Use an explicit `ExecutorService` for
fan-out and revisit at JDK 27.
⚠️ Note the API was **reshaped** in the fifth preview (static factory + `Joiner`
instead of subclassing `StructuredTaskScope`), which is exactly why building on a
preview would have cost a rewrite.

### Q15 — Plugin security policy: **unchanged declaration, new enforcement**
OpenSearch 3.8.0 still reads `plugin-security.policy`
(`PluginInfo.OPENSEARCH_PLUGIN_POLICY`, `bootstrap/Security.java`), but
enforcement moved to a Java agent — `jvm.options` carries
`21-:-javaagent:agent/opensearch-agent.jar` and `libs/agent-sm/agent-policy`
ships its own `PolicyParser`. **Write the policy file as before.** Thanks to
ADR-0004 the plugin's surface is now just outbound connections to the ingester.

### Q16 — S3 Express One Zone: **rejected, and the numbers say so**
| | API | Storage (6 h) | Total |
|---|---|---|---|
| S3 Standard | $336/mo | $50/mo | **$386/mo** |
| S3 Express One Zone | $72/mo | $238/mo | **$310/mo** |

Express is 4.4× cheaper per PUT and 13× per GET, but **4.8× more expensive per
GB**, and once the API bill is already small the storage penalty eats the saving:
a 20% difference, not an order of magnitude. It is **single-AZ**, which violates
NFR-8 (RPO 0) and NFR-10. A per-AZ-bucket hybrid would reintroduce the cross-AZ
replication this project exists to avoid. **Not worth it at any layer.**

### Q17 — Compaction trigger: **deferred with the feature; collect the metric now**
Compaction stays deferred ([compaction §1](30-design-space/06-compaction-and-retention.md)).
When it is built the trigger is **commit-log index entries per stream**, not read
amplification. Emit that metric from M4 so the threshold is chosen from data
rather than guessed at the time.

### Q20 — `maxStreamLatency`: **5 s default, per-index override, automatic promotion**
5 s is what makes the segment shape and the key filter work at 10,000 indices
([object-layout §7b](30-design-space/01-object-layout-and-format.md)). A stream
that sustains `minRunBytes` per flush interval is **automatically promoted** to
per-segment flushing, so a busy index sees ~250 ms without configuration and a
trickle index sees ~5 s. ⚠️ Document it as the visible latency SLO — it is
surprising if undocumented.

### Q21 — Break-glass direct-S3 path: **yes, minimal; revised by ADR-0064**
Ship it through a **node-local read broker**, not through a cloud SDK or
credential in the plugin JVM. The broker owns ambient identity and streams one
coalesced whole-object recovery read per missing segment per OpenSearch node;
the plugin sends only a protected local request. This works when no ingester
can mint a short-lived grant, without turning a grant chain into a long-lived
bearer credential. `direct` while an ingester is reachable still uses the
short-lived signed capability in ADR-0041; the broker is for fallback tiers 2
and 3. See [ADR-0064](../internal/product/decisions/0064-node-local-read-broker-for-store-fallback.md).

### Q22 — Serving capacity: **~1× ingest in, ~1× out per consuming copy**
With ADR-0009 (one consuming copy), six pods at 100 MiB/s ingest:
**16.7 MiB/s in and 16.7 MiB/s out per pod (~133 Mbps)**. At 500 MiB/s,
83 MiB/s each way (~667 Mbps). Both comfortable on any modern NIC; pod count
stays HA-driven, not capacity-driven. Size for 2× headroom and treat a sustained
breach as the signal to add pods.
The starvation half of this question was closed by
[fetch-modes §4](30-design-space/10-client-library-and-fetch-modes.md):
catch-up reads with fan-out 1 are redirected to the object store.

### Q18 — Placement-aware segment assembly: **not needed**
Serving reads from the ingester nodes removes the fan-out problem entirely, so
coupling to OpenSearch shard placement buys nothing.

### Q19 — Where the per-AZ read cache lives: **the ingester nodes**
$25/month and 0.33 MiB/s per node, versus $3,732/month and 300× bandwidth
amplification for direct reads. See
[0004](../internal/product/decisions/0004-the-service-serves-reads.md).

---

## 3. Deferred to measurement

These are **not** open design questions — the design is decided. They are
constants that cannot honestly be chosen without running code, and each names
where it will be settled.

| # | Constant | Settled by |
|---|---|---|
| M1 | Lease TTL and challenge policy under realistic GC pauses | M8 chaos suite — ⚠️ **MEASURED at the shipped 10 s TTL / 3 s renew** (M8.27, `LeaseTtlMeasurementIT`): `SIGSTOP` pauses of 2, 4, 6, 8 and 9 s caused no takeover; 11, 12 and 20 s caused exactly one, 10.28–10.33 s after the stop over two runs; nothing acked was lost. The absorbed pause is the TTL itself, whatever the renew phase, because a follower elects only when a forwarded commit times out and that timeout is the TTL (M8.12). The defaults stand. ⚠️ **The challenge half is NOT measured, and M8.55 owns it**: an early challenge needs `EndpointSlice` evidence (M8.13), which nothing in this measurement produces; in a cluster, a pause longer than the readiness probe's failure window would be taken over BEFORE the TTL |
| M2 | Block size and compression codec | M9, benchmark B3 |
| M3 | Cost-only threshold settled at 1 by [M9.14 / ADR-0067](../internal/product/decisions/0067-direct-threshold-cost-parity-boundary.md): at N=1, both modes cost one GET; above 1, direct costs N GETs. Real S3 TTFB variance remains **NOT-RUN** and can reopen the threshold only with a cloud distribution. The §1 table in fetch-modes is modelled, not measured. | M9.14 RustFS matrix; cloud TTFB NOT-RUN |
| M4 | Compaction trigger threshold (Q17) | after M9, from the metric collected from M4 |
| M5 | `safetyMargin` for the GC watermark — sized above the observed Lucene commit interval | M7 — ⚠️ **MEASURED AS A FLOOR ONLY** (M7.14, `CommitIntervalProbeIT`): one shard, sixty records, the pointer advancing 0 → 59 across the only two commits observed, and **no advance at all within three ten-second waits** because a commit follows the translog flush policy rather than ingestion. `RetentionRule.DEFAULT_SAFETY_MARGIN = 10_000` is an upper bound on a guess, and a deployment at Scenario A's rate must size its own |
| M6 | Whether the plugin can observe the *committed* pointer rather than the in-memory one, shrinking M5 | M7 — ⚠️ **ANSWERED: YES**, and it is [ADR-0051](../internal/product/decisions/0051-the-plugin-can-read-the-committed-pointer-and-the-error-moves-to-the-safe-side.md). `batch_start` is in the shard's last Lucene commit user data and a plugin inside the node reads it. ⚠️ **BUT IT SHRINKS M5 LESS THAN THIS ROW ASSUMED**: the flush window is still in the staleness. What it buys is the error's DIRECTION — behind-durable rather than ahead — so being wrong costs storage instead of records |

⚠️ **Do not let a measurement constant become a design excuse.** If a task is
blocked on one of these, the answer is to build the thing that measures it, not
to defer the design.
