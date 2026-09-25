# 0064. Use a node-local store reader for plugin fallback

Status: accepted — clarified 2026-09-24 by M9.49 on request permissions, naming and local process identity; clarified 2026-09-25 by M9.50 on allowed recovery object keys; clarified 2026-09-25 by M9.41 on carrying the commit cursor through live and catch-up events; clarified 2026-09-25 by M9.44 that Tier 3's 30-GET ceiling bounds request spend, not wall-clock latency: ordered replay latency grows with serial object-store TTFB, plus one pointer STAT; clarified 2026-09-25 by M9.45 that the canonical RustFS episode costs five Tier 3 GETs (pointer, three deltas, one shared segment), while a mid-replay restoration episode cost six; 30 remains the hard cap, not the measured typical cost
Date: 2026-09-22
Requirements: FR-10, NFR-4
Research: docs/research/30-design-space/04-discovery-and-tailing.md §2a, §3; docs/research/30-design-space/10-client-library-and-fetch-modes.md §4; docs/research/50-open-questions.md Q21
Supersedes: ADR-0057's tier-2/3 deferral only; ADR-0004's ingester-served hot path remains accepted

## Context

The normal read path is deliberately ingester-served. At the target scale, a
plugin reading the object store directly costs about $3,732/month and repeats
whole-object bandwidth at roughly 300 data nodes, while a shared ingester
cache costs about $25/month. The plugin therefore has no object-store SDK,
credentials or cache on its hot path.

The fallback ladder still needs a break-glass path when no ingester node is
reachable. A plugin-side SigV4 implementation would put cloud credentials and
a large signing surface in the OpenSearch JVM. A chain of pre-issued grants
would avoid the SDK but would either expire during a long outage or become a
long-lived bearer capability. Asking an ingester for a short-lived grant fails
in the exact failure case the fallback is meant to cover.

The fallback is explicitly a recovery path, not a second hot path: it may read
the whole segment, it must coalesce all runs for one segment on one OpenSearch
node, and its GET/STAT counts must be measured by M9.21. Tier 3 must resolve the
newest checkpoint pointer before it can fetch the checkpoint and ordered deltas;
that resolution uses one checkpoint-pointer STAT per recovery episode. The
episode is bounded to at most 30 GETs total (checkpoint, deltas and segments),
and 64 MiB of aggregate object bytes read and retained, including projected
inline-event copies; the STAT and GET counts are reported separately.
Ingester availability is rechecked before each Tier 3 STAT/GET and before
replay delivery; if an ingester answers mid-episode, local recovery stops and
the gap remains held for the normal catch-up path.

## Decision

Use a **node-local store reader** (`NodeLocalStoreReader`) beside each OpenSearch node. The plugin sends
the reader a structured recovery request containing the configured store
namespace, one allowlisted recovery object key, and the requested delivery
window. The reader owns
the object-store SDK, ambient workload identity and endpoint configuration; it
streams the whole object back without exposing credentials or a signed URL to
the plugin.

The reader contract is deliberately narrow:

- for automatic Tier 2, only GET of keys under the configured bucket/prefix is
  permitted; automatic Tier 3 permits GET plus one STAT of the newest checkpoint
  pointer before checkpoint/delta/segment GETs. All GET and STAT operations are
  counted; no other STAT is permitted. The operator-entered Tier 4 LIST is a
  separate recovery tool/path, not exposed by `NodeLocalStoreReader`;
- the complete automatic GET allowlist is (a) the checkpoint pointer at
  `<prefix>/ctl/log/0/<epoch:016x>/ckpt/LATEST` (STAT then GET; its GET body is
  the checkpoint per ADR-0034), (b) a commit delta at
  `<prefix>/ctl/log/0/<epoch:016x>/<sequence:016x>.delta`, and (c) a segment at
  `<prefix>/data/<yyyy>/<MM>/<dd>/<HH>/<timestampMillis:019d>-<podShortId>-<sequence:016x>-h<headerLen>-<filterEncoded>.bseg`.
  The hex fields are lowercase, zero-padded, and canonical; the segment time
  fields and `podShortId`/filter follow `SegmentKey`. The configured bucket and
  prefix are fixed at assembly, so callers cannot select them. Validate the
  entire key against its canonical grammar and that exact namespace: no arbitrary
  key, URI, bucket, prefix, traversal, extra path component, or seq-keyed `.ckpt`
  object is accepted. STAT is refused for delta and segment keys;
- automatic Tiers 2 and 3 issue no LIST; only the separate, operator-entered
  Tier 4 data-prefix scan may LIST, outside M9.21 and separately counted and
  paced under cost.md R15;
- each recovery episode represents one node-scoped operation, never one request
  per record, shard, partition or run; Tier 2 polling is likewise coalesced once
  per OpenSearch node and poll interval;
- the reader streams with bounded buffers, enforces an object-size limit and
  reports store failures without logging credentials or object payloads; and
- the plugin and companion run under the same dedicated OS service account;
  the per-installation authentication secret is in a file readable only by
  that account, and the actual plugin-side client reads it through that
  owner-only path. OS ownership/permissions protect the local channel; the
  secret is never in index settings or cluster state.

The plugin uses this reader only for fallback tiers 2 and 3. Inline and proxy
delivery remain unchanged, and a reachable ingester remains preferred. If both
the ingester fleet and the local reader/store are unavailable, the consumer
stays in the ladder's visible recovery state: indexing is delayed, but durable
records are not skipped or acknowledged as consumed.

M9.21 owns the executable `NodeLocalStoreReader` seam and the RustFS request counts. Its result
must replace `FallbackLadder`'s modelled `TENS_OF_GETS` with the measured
number, or amend that estimate with a new ADR if the result is not acceptable.

The subscription cursor is the commit-chain sequence, not a stream offset or
either event epoch. Version 4 appends that non-negative sequence to both live
and catch-up events. Its grant-presence byte permits the same cursor on inline,
proxy and direct deliveries; v1-v3 remain readable and decode with an absent
sequence. The live publisher takes it from `CommitDelta.sequence()`, and
catch-up takes it from the source delta that yielded each run. A consumer with
an older event can continue to use stream offsets, while Tier 2 starts from the
exact commit cursor as soon as a v4 event arrives.

For the pinned sequence 9,876, v4 adds three encoded bytes over the matching
v2 inline or v3 direct event: one grant-presence byte plus a two-byte unsigned
varint. A sequence up to `Long.MAX_VALUE` adds at most ten bytes. This changes
subscription bandwidth only; it adds no object-store operation and does not
change requests per MiB or the node-scoped Tier 2/3 request budgets.

## Alternatives considered

- **Put a SigV4 signer and cloud SDK in the plugin.** Rejected. It violates the
  dependency boundary, puts bucket credentials in the OpenSearch JVM, expands
  the plugin security-policy and licence surface, and duplicates retry,
  endpoint and credential-refresh logic already owned by the backend.

- **Pre-issue a long-lived chain of signed grants.** Rejected. It turns a
  short-lived capability into a durable bearer credential, and the chain is
  either finite (so a long outage still fails) or long-lived (so revocation and
  leakage become unacceptable). It also requires changing ADR-0041's explicit
  short-TTL contract.

- **Ask an ingester for a short-lived signed URL.** Rejected as the tier-2/3
  fallback. It is the right mechanism for `direct` while an ingester is
  reachable, but it cannot operate when the ingester fleet is the failed
  component.

- **Discover checkpoints or the commit chain with LIST.** Rejected. Automatic
  recovery must issue zero LISTs; LIST costs 12.5 GETs under cost.md and is not
  acceptable for a node-local automatic path. Tier 3 instead pays exactly one
  counted checkpoint-pointer STAT per recovery episode, plus bounded GETs.

- **Keep tiers 2 and 3 deferred.** Rejected. It leaves the consumer unable to
  make progress during an ingester outage even though the only durable state is
  in the object store. The node-local store reader adds a stateless companion, not a
  new durable dependency, and keeps the failure mode visible when the store
  itself is unavailable.

## Consequences

- The plugin remains free of cloud SDKs, object-store credentials and signed
  URL handling. The reader becomes the only new deployment artifact and must
  be upgraded with the plugin contract.
- Tier 2 and Tier 3 requests are outage-recovery work, coalesced per node. Each
  Tier 3 episode adds exactly one checkpoint-pointer STAT and at most 30 GETs;
  aggregate bytes read and retained are capped at 64 MiB per Tier 3 episode;
  all operations are counted separately, with zero automatic LISTs. If the
  byte ceiling is reached, or an ingester answers mid-episode, recovery stops
  and leaves the gap held. This does not change steady-state request rates,
  which remain governed by the ingester cache and prefetch path.
- Recovery correctness depends on the reader's bounded streaming and on the
  catch-up path's coalescing. M9.21 must exercise both and count them rather
  than treating `TENS_OF_GETS` as a design constant.
- A reader outage is an indexing-latency incident, not a data-loss path. The
  fallback ladder must keep the gap visible and resume from the durable
  pointer once either the reader/store or an ingester becomes available.
