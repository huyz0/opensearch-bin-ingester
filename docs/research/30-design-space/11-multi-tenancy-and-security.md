# Multi-tenancy isolation and the security model

**Status:** decided — see [ADR-0010](../../internal/product/decisions/0010-multi-tenancy-and-security-model.md) ·
**Confidence:** medium-high (the promotion threshold is arithmetic; the threat model is conventional) ·
**Last updated:** 2026-08-30

**Read this if:** you are implementing admission control, buffer allocation, or
anything that crosses a tenant boundary.
**One-line takeaway:** the bundling universe is a **trust domain**, and within
one domain isolation is bought with rate limits, fair-share buffers and a
per-index share cap — **not** with per-index segment streams, because at target
scale nothing reaches the throughput that makes a dedicated stream free.

This document closes the two gaps the corpus previously acknowledged (Q9, Q10).

---

## 1. What one noisy index can do

At 10,000 indices in one service, a single index can:

| Failure | Mechanism |
|---|---|
| Delay everyone's flush | dominates segment bytes |
| Apply backpressure to everyone | exhausts the shared slab pool (`maxUnflushedBytes` is global) |
| Inflate everyone's read amplification | makes segments larger without making them more relevant |
| Delay all commits | dominates the sequencer's per-flush work |
| Saturate serving | dominates a pod's egress |

## 2. Isolation mechanisms, in order of how often they bind

1. **Per-index admission rate limit** — token bucket on bytes/s and records/s;
   `429` + `Retry-After`. Checked *before* buffering, so a rejected write costs
   no memory. Must be a map lookup, not a network call.
2. **Fair-share buffer allocation** — weighted fair queuing over the slab pool,
   not first-come. No index holds more than a multiple of its fair share while
   others wait. Without this, mechanism 1 alone still lets a burst inside the
   limit starve everyone during the burst.
3. **Per-index share cap on a segment** — an index contributes at most ~25% of a
   segment's bytes; beyond that its records wait for the next flush. This is what
   bounds the read amplification one tenant inflicts on another's readers.
4. **Promotion to a dedicated segment stream** — deferred, see §3.

## 3. The promotion threshold, and why promotion is not used

A dedicated stream is **free only when the index fills its own segments**:

```
promotionThreshold = segmentSize / flushInterval        per pod
                   = 8 MiB / 250 ms = 32 MiB/s per pod
                   ≈ 192 MiB/s across 6 pods
```

Below that, the dedicated stream is time-triggered and costs an extra
`pods × flushRate` PUTs ≈ **$312/month per promoted index**.

At Scenario B's 100 MiB/s aggregate, **no index reaches the threshold**, so
promotion is not implemented and mechanisms 1–3 carry isolation instead.

> ⚠️ This also settles the older "per-index prefixes vs shared bundles" question
> ([bloom-in-key §6](02-partition-bloom-in-key.md)): promotion is an **isolation**
> mechanism, not a discovery optimisation, and its threshold is a throughput
> number rather than a policy argument. LIST-as-seek was never worth $312/month
> for a path that runs during recovery only.

## 4. Trust domains

**The bundling universe is a trust domain.** A segment interleaves many indices
and is readable as a unit — `direct` mode hands a client a signed URL for an
object ([fetch-modes §6](10-client-library-and-fetch-modes.md)) — so segments are
never co-bundled across OpenSearch clusters.

*n* clusters ⇒ *n* segment streams, *n* bucket prefixes, *n* credential sets,
*n* ordinal registries. Within one cluster no boundary is crossed: shards
relocate anywhere, so every data node is already inside that domain.

**This is the only place in the design where isolation is bought with bundling
efficiency**, and it is deliberate.

## 5. The security model

| Boundary | Mechanism |
|---|---|
| Producer → service | mTLS or bearer token scoped to a set of indices; authorized at admission |
| Plugin → service | mTLS or token bound to a cluster identity; a node subscribes only within its trust domain |
| Service → object store | ambient identity (IRSA / workload identity / managed identity), scoped to the domain's prefix |
| Client → object store | short-lived signed URL (≤60 s), read-only, scoped to key and where possible byte range. **No object-store credentials in the OpenSearch JVM** |
| In transit | TLS everywhere, intra-AZ included |
| At rest | SSE-S3 by default; SSE-KMS **only with S3 Bucket Keys** (§6) |

**Never:** log, trace or place in an error message any credential, signed URL, or
document payload. **Never** size a buffer from client input without a bound.
**Never** fold an out-of-range partition — reject it (ADR-0006).

## 6. The SSE-KMS cost trap

SSE-KMS charges a KMS request per encrypt and per decrypt. Applied to our access
pattern that is a KMS call per segment PUT and per GET, on top of the S3 request
— and it lands on the **KMS** bill, so the cost meter (which counts store
operations) will not see it.

[S3 Bucket Keys](https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucket-key.html)
derive a short-lived bucket-level key and **reduce KMS request traffic by up to
99%**. Enabling SSE-KMS without them is a cost regression invisible to every gate
we have.

Per-index keys are incompatible with bundling (a segment spans indices).
**Per-trust-domain keys** are the granularity that works.
