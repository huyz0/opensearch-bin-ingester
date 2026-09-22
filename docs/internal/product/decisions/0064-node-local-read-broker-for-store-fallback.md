# 0064. Use a node-local read broker for plugin store fallback

Status: accepted
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
node, and its GET/LIST counts must be measured by M9.21.

## Decision

Use a **node-local read broker** beside each OpenSearch node. The plugin sends
the broker a structured recovery request containing the configured store
namespace, one segment key, and the requested delivery window. The broker owns
the object-store SDK, ambient workload identity and endpoint configuration; it
streams the whole object back without exposing credentials or a signed URL to
the plugin.

The broker contract is deliberately narrow:

- only GET of keys under the configured bucket/prefix is permitted;
- LIST is refused except for the explicitly entered recovery tier, where it is
  separately counted and paced;
- one request represents one coalesced segment per OpenSearch node, never one
  request per record, shard, partition or run;
- the broker streams with bounded buffers, enforces an object-size limit and
  reports store failures without logging credentials or object payloads; and
- the local channel is protected by OS ownership/permissions and a
  per-installation authentication secret, never by index settings or cluster
  state.

The plugin uses this broker only for fallback tiers 2 and 3. Inline and proxy
delivery remain unchanged, and a reachable ingester remains preferred. If both
the ingester fleet and the local broker/store are unavailable, the consumer
stays in the ladder's visible recovery state: indexing is delayed, but durable
records are not skipped or acknowledged as consumed.

M9.21 owns the executable broker seam and the RustFS request counts. Its result
must replace `FallbackLadder`'s modelled `TENS_OF_GETS` with the measured
number, or amend that estimate with a new ADR if the result is not acceptable.

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

- **Keep tiers 2 and 3 deferred.** Rejected. It leaves the consumer unable to
  make progress during an ingester outage even though the only durable state is
  in the object store. The node-local broker adds a stateless companion, not a
  new durable dependency, and keeps the failure mode visible when the store
  itself is unavailable.

## Consequences

- The plugin remains free of cloud SDKs, object-store credentials and signed
  URL handling. The broker becomes the only new deployment artifact and must
  be upgraded with the plugin contract.
- The fallback can add one whole-object GET per missing segment per OpenSearch
  node, plus only the explicitly entered recovery LISTs. It does not change
  steady-state request rates, which remain governed by the ingester cache and
  prefetch path.
- Recovery correctness depends on the broker's bounded streaming and on the
  catch-up path's coalescing. M9.21 must exercise both and count them rather
  than treating `TENS_OF_GETS` as a design constant.
- A broker outage is an indexing-latency incident, not a data-loss path. The
  fallback ladder must keep the gap visible and resume from the durable
  pointer once either the broker/store or an ingester becomes available.
