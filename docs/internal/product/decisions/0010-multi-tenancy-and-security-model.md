# 0010. Multi-tenancy isolation and the security model

Status: accepted
Date: 2026-08-30
Requirements: FR-1, FR-6, FR-12, NFR-6
Research: docs/research/30-design-space/10-client-library-and-fetch-modes.md §6 (Q9, Q10)

## Context

One service fronts up to 10,000 indices. Two gaps were tracked as open questions:
isolation between tenants (Q9) and a security model (Q10). Both were
acknowledged as **absent from the research corpus**, not merely undecided.

## Decision

### Trust domains
**The bundling universe is a trust domain.** A segment is readable as a unit —
`direct` mode hands a client a signed URL for a whole object — so segments are
never co-bundled across OpenSearch clusters. *n* clusters means *n* segment
streams and *n* bucket prefixes, each with its own credentials and its own
ordinal registry. Within one cluster, no boundary is crossed: shards relocate
anywhere, so every data node is already inside that trust domain.

### Isolation between indices in one trust domain
Four mechanisms, in order of how often they bind:

1. **Per-index admission rate limit** — token bucket on bytes/s and records/s,
   `429` with `Retry-After` when exceeded. The first and cheapest defence.
2. **Fair-share buffer allocation** — the slab pool is allocated by weighted fair
   queuing, not first-come. No index holds more than a multiple of its fair share
   while others are waiting. Without this, one index exhausting the pool applies
   backpressure to everyone (the `maxUnflushedBytes` ceiling is global).
3. **Share cap on a segment, at *two* granularities** — an index contributes at
   most ~25% of a segment's bytes, **and a single stream `(index, partition)`
   contributes at most ~25% of its index's share.** ⚠️ The second was added
   2026-08-30: with routing-based tenancy, millions of tenants live inside one
   index and the unit that goes hot is a *stream*, not an index. A tenant that is
   40% of its index's traffic makes its partition 7× the even share
   ([SPI §4b](../../../research/20-opensearch/01-pull-based-ingestion-spi.md)), and a
   per-index cap does not see it at all.
4. **Promotion to its own segment stream** — deferred, with a stated threshold
   (below).

### Promotion threshold, and why it is not used yet
Promotion is **free only when the index fills its own segments**, i.e. sustains
`segmentSize / flushInterval` per pod — 32 MiB/s per pod, ~192 MiB/s across six
pods at 8 MiB/250 ms. Below that, a dedicated stream is time-triggered and costs
an extra `pods × flushRate` PUTs (~$312/month per promoted index).

At Scenario B's 100 MiB/s aggregate, **nothing reaches that threshold**, so
promotion is not implemented. Mechanisms 1–3 carry isolation instead.
⚠️ This also settles Q4: promotion is an **isolation** mechanism, not a discovery
optimisation, and its threshold is a throughput number rather than a policy
argument.

### Security
| Boundary | Mechanism |
|---|---|
| Producer → service | mTLS or a bearer token scoped to a set of indices; authorization checked at admission, before any buffering |
| Plugin → service | mTLS or token bound to a cluster identity; a node may subscribe only to indices in its own trust domain |
| Service → object store | ambient identity (IRSA / workload identity / managed identity), scoped to that trust domain's bucket prefix |
| Client → object store (`direct`) | short-lived signed URL, ≤60 s, read-only, scoped to key and where the provider allows to a byte range. **No object-store credentials in the OpenSearch JVM** |
| In transit | TLS everywhere, including intra-AZ |
| At rest | **SSE-S3 by default.** SSE-KMS is optional and **requires S3 Bucket Keys** |

### The SSE-KMS cost trap
SSE-KMS charges a KMS request per encrypt and per decrypt. Applied naively to our
access pattern that is a KMS call per segment PUT and per GET, on top of the S3
request. **S3 Bucket Keys reduce KMS request traffic by up to 99%** by deriving a
short-lived bucket-level key. Enabling SSE-KMS without Bucket Keys is a cost
regression the cost meter will not see, because the charge lands on KMS, not S3.

### Never
- Never log, trace, or place in an error message: credentials, signed URLs, or
  document payloads. The ingester does not parse payloads and must not print them.
- Never size a buffer from client-supplied input without a bound — frame lengths,
  batch counts, key lengths, queue depths.
- Never reject-by-folding: a partition or partition-count mismatch is an error
  (ADR-0006), never a modulo.

## Alternatives considered

- **A single global bundle across all clusters**, maximising bundling efficiency.
  Rejected: it makes every `direct` grant a cross-tenant data leak, and no
  practical grant scoping fixes it because a segment interleaves tenants.
- **Per-index segment streams for isolation.** Rejected on cost: ~$312/month per
  index in time-triggered PUTs at scales where nothing fills a segment.
- **Encrypting per index with distinct keys.** Rejected: a segment spans indices,
  so per-index keys and bundling are mutually exclusive. Per-*trust-domain* keys
  are compatible and are what we use.

## Consequences

- Closes Q9 and Q10 as **decided**, and the research corpus gains the section it
  was missing.
- Bundling efficiency is capped at the trust-domain boundary — a deliberate
  cost-for-isolation trade, and the only one in the design.
- Adds an admission-time authorization step on the hot path; it must be a map
  lookup, not a network call.
