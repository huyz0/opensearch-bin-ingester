# M9.21 fallback test plan

Status: approved before implementation by M9.48; execution is owned by M9.41–M9.45
Requirements: FR-10, NFR-2, NFR-3, NFR-4
Decision: [ADR-0064](../../decisions/0064-node-local-store-reader-for-plugin-fallback.md)
Specification: [M9 SPEC](SPEC.md) criterion 18 and tasks M9.41–M9.45

## Scope and invariants

This plan covers only automatic Tier 2 and Tier 3 fallback through
`NodeLocalStoreReader`. The normal ingester-served path remains preferred.
Automatic fallback uses the exact checkpoint-pointer, sequenced-delta and
`SegmentKey` grammars from ADR-0064, never LISTs, and never accepts a key outside
the assembly-selected bucket and prefix. Tier 3 is one node-scoped recovery
episode with exactly one checkpoint-pointer STAT and at most 30 GETs total,
including the checkpoint-pointer GET, deltas and segments. Tier 2 performs at
most one GET for the unchanged cursor per OpenSearch node per poll interval,
including repeated 404 responses.

Every unrecoverable Tier 3 outcome has the same safety result: the missing range
remains visible as a gap; the consumer cursor does not advance; and no record
after the gap is delivered. A later successful retry may close that same gap.
Fallback requests stop as soon as any ingester answers.

## Test matrix

| Order / task | Tier and named test | Arrange and force | Required assertions | Mutation this test must kill |
|---|---|---|---|---|
| 1 / M9.41 | T0 `SubscriptionEventTest` plus v1–v4 golden compatibility cases | Decode existing v1–v3 live/catch-up frames; round-trip a v4 frame with the commit-chain sequence | Old versions still decode; v4 preserves the exact sequence through every live and catch-up writer/reader; malformed or absent v4 sequence is refused | Drop the sequence field, substitute the event sequence, or decode v4 as v3 |
| 2 / M9.42 | T0 `NodeLocalStoreReaderKeyPolicyTest` | Exercise canonical pointer, delta and segment keys built from the configured namespace; vary hex case/padding, segment fields, path depth, prefix, bucket, URI and traversal; try STAT on every key class | Only canonical keys pass; only `ckpt/LATEST` permits STAT; every rejected key causes zero store calls | Replace canonical reconstruction with `startsWith`, `endsWith`, or prefix-only validation; allow STAT on a delta/segment; normalize traversal into an accepted key |
| 2 / M9.42 | T1 `NodeLocalStoreReaderTest` | Inject missing/wrong authentication secrets, wrong namespace, oversized object, and store-call recording; capture logs | Authentication and namespace failures are refused before I/O; size is bounded while streaming; credentials and payload bytes do not appear in logs or errors | Accept a missing/bad secret, trust a caller bucket, buffer without a size limit, or log the secret/body |
| 2 / M9.42 | T2 `NodeLocalStoreReaderProcessIT` | Launch the separate reader process and use the actual plugin-side client under the same dedicated OS account; attempt access as a different OS account; bind an alternate reader to a non-loopback address | The actual client reads the owner-only secret and completes a bounded request; another OS identity cannot read the secret; non-loopback binding is refused | Inject a test token instead of reading the production secret path, make the secret world-readable, or accept a wildcard listener |
| 3 / M9.43 | T1 `TierTwoChainPollTest` | Put at least 16 subscriptions/shards on one OpenSearch node with the same current chain cursor. Use an injected interval and counting/faulting store; hold the cursor unchanged across polls with 404s, then make the next delta available; toggle ingester reachability | Across all 16 subscribers, at most one GET for that node/cursor/interval, including each 404 interval; no LIST or STAT; delta is consumed once; polling stops when an ingester answers | Poll once per subscription/shard, retry a 404 within the interval, list for discovery, or keep polling in the normal path |
| 4 / M9.44 | T1 `TierThreeRecoveryTest` success and fault matrix | Put at least 16 subscriptions/shards on one OpenSearch node; make multiple streams reference the same missing segment. Seed a checkpoint pointer and ordered deltas; independently fail/corrupt pointer STAT, pointer GET/body, delta GET/body and segment GET/body; exercise the cap at 30 and just beyond it | Success emits the complete missing range to every matching subscriber contiguously. Each shared segment key is fetched once per node, not per subscriber. Each failure or insufficient cap returns incomplete, leaves the gap visible, leaves every affected cursor unchanged, emits nothing past the gap, uses exactly one pointer STAT, applies the 30-GET total node-episode cap across subscribers, and issues zero LISTs | Fetch once per subscriber/shard; advance a cursor before replay completes; skip a missing/malformed object; continue after the cap; retry a missing segment within the episode; issue another STAT or any LIST |
| 5 / M9.45 | T3 `LadderStoreTiersIT` against RustFS | Create a real consumer gap shared by at least 16 subscriptions on one node, make every ingester endpoint unreachable, then restore an ingester after recovery is underway; observe store counters and delivered offsets | Tier 2 precedes Tier 3; each shared missing segment is fetched once per node, not per subscription; the gap closes contiguously for all matching streams without skips; GET/STAT counters match actual operations (one checkpoint-pointer STAT and its GET, at most 30 GETs total); LIST count is zero; fallback request rate returns to zero after restoration; replace `TENS_OF_GETS` with the measured GET count or a correcting ADR | Enter Tier 3 while an ingester answers, issue per-subscription reads, skip/index past a gap, undercount a store call, exceed the cap, LIST, leave the modelled constant, or continue fallback after restoration |

## Failure-case assertions

`TierThreeRecoveryTest` runs each row as an independent case so one failure
cannot mask another. The faulting store records verb, key, count and purpose.
For every row, assert the gap is still reported, the pre-recovery cursor is
unchanged, the post-gap event queue is empty, and no operation outside the
permitted key/verb set occurred.

| Injected condition | Expected result |
|---|---|
| Checkpoint-pointer STAT throws or reports unavailable | Incomplete recovery; no checkpoint/delta/segment GET; cursor unchanged |
| Checkpoint-pointer GET throws or returns malformed bytes | Incomplete recovery; cursor unchanged; no delta or segment delivery |
| A required delta is missing, unreadable or malformed | Incomplete recovery; cursor unchanged; no later delta/segment can make the gap appear closed |
| A referenced segment is missing, unreadable or malformed | Incomplete recovery; cursor unchanged; no later record is delivered |
| The next required GET would exceed the 30-GET episode cap | Refuse before that GET; incomplete recovery; cursor unchanged |
| Key is malformed, noncanonical, out of prefix/bucket, or an unsupported in-prefix object | Reject before store I/O; no GET, STAT or LIST |
| Ingester becomes reachable during or after fallback | Cancel/stop automatic fallback; subsequent intervals issue zero store requests |

## Test-first and suite expectations

For each implementation task, write its named test first and record an observed
red run with `scripts/tdd-red.sh` before production changes. Keep parsing,
canonical-key policy, counters and recovery state transitions injectable and at
T0/T1. T2 is reserved for the actual local process boundary and OS identity;
T3 is reserved for the real RustFS store path. The default test task must not
start a container. Do not use sleeps for polling; inject interval/clock seams,
and use ephemeral ports for the process test.

The named mutations above are the minimum explicit test-quality bar; run the
available module and repository gates, and run mutation testing where its gate
and module support exist. No coverage percentage or RustFS latency figure is
claimed by this plan. Request counts are backend-independent; only RustFS
latency is a lower bound, not production latency.
