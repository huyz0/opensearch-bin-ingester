<!-- SPDX-License-Identifier: Apache-2.0 -->
# M6 — verified

One line per acceptance criterion in [SPEC.md](SPEC.md), naming what
demonstrated it. ⚠️ **It forces enumeration; it cannot make the evidence true**
— AGENTS.md non-negotiable 4, and nothing checks it. Everything below was run
on this tree: `./gradlew build` green at **1,498 tests, 0 failures, 0 errors**,
`./gradlew :plugin:clusterTest` green, and `GATE_SCOPE=full
./scripts/check-module.sh` green at 8 modules.

⚠️ **TWO criteria are met in a different place than the SPEC named, and each
says so.** Criterion 1's refusal happens at index CREATION rather than "when
ingestion starts", because a booting node showed the later refusal is caught
and retried forever; criterion 11's subscriber count is counted as subscriber
OBJECTS rather than transport calls, because the default multi-key subscribe
passes one listener per key and collapses the count by construction — which is
what the SPEC's criterion 11 already REQUIRED ("counted at the transport, as
subscriber identities opened"), so it is the first implementation that deviated,
not this one; and criterion 11's work lives in `RoutedSearchIT` rather than in
the `MultiNodeIT` the TEST PLAN names, folded in rather than written.
⚠️ **AND CRITERION 5 IS MET AT THE CLASS AND NOT IN A DEPLOYMENT** — the
milestone review found it, and § *What M6 does NOT close* says what that costs.
A milestone whose evidence document is uniformly affirmative is the one to
distrust.

1. **A valid `mapper_type` this plugin cannot serve fails visibly** — `MapperRefusalIT#testAnUnservedMapperTypeIsREFUSEDAtIndexCREATION`
   (`raw_payload`, a valid member of OpenSearch's closed enum, so core does not
   refuse it first) and `MapperRefusalIT#testTheSERVEDMapperTypeStillCreates` as the control;
   `MapperTypeRefusalTest` pins the factory's own refusal, its scope to this
   plugin's `ingestion_source.type`, and that an ordinary index is none of its
   business.
   ⚠️ **NOT "WHEN INGESTION STARTS", AND THAT IS THE FINDING.** The first
   version refused in `createShardConsumer`, which `DefaultStreamPoller`
   CATCHES, logs at WARN and retries forever — MEASURED on a booting node with
   the index GREEN, the shard started and nothing indexed, which is ADR-0020's
   own failure mode one level up. The refusal an operator meets fails the
   CREATE request.
2. **Placement matches OpenSearch's own** — `RoutingPartitionerParityTest.aTHOUSANDRoutingValuesLandWhereOPENSEARCHWouldLookAtFOURShardCounts`
   (shard counts 1, 3, 5, 16), `.aSPLITIndexAgreesToo` (`routingNumShards` ≠
   `numShards`), and `.theHASHIsOPENSEARCHSHash` against
   `StringHelper.murmurhash3_x86_32` at lengths 0–37 — asserted against
   `OperationRouting.generateShardId` and `Murmur3HashFunction` themselves, not
   a re-derivation.
3. **Registration carries what placement needs, and round-trips** — `IndexRegistrationTest` (17 cases: refusals for a truncated frame, a forward
   version, a wrong magic, an alias count bigger than the frame, trailing
   bytes, oversized varints) and `GoldenIndexRegistrationTest` over
   `golden/index-registration-v1.bin` and `index-registration-split-v1.bin`;
   [ADR-0047](../../decisions/0047-index-registration-is-a-format-type-on-the-subscription-channel.md)
   is the wire-format record.
4. **`os_routing` is refused where it cannot be correct, and the MODE is what is refused** — `IndexCatalogTest.aROUTEDWriteToAPartitionedIndexIsREFUSEDAndAnEXPLICITOneIsNot`
   and `.aPartitionSizeOfTWOIsAlreadyUNROUTABLE` (the boundary is `> 1`, not
   `> 2` — every refusal case used 4 and every accepting case 1, so moving it
   left nine cases green);
   `RoutedIngestTest.aROUTEDWriteToAPartitionedIndexIsREFUSEDImmediately`.
5. **Neither parameter, both parameters, and an out-of-range partition are 400** — `BulkRoutingParamTest.NEITHERParameterIsFOURHUNDRED`,
   `.BOTHParametersAreFOURHUNDRED`, `.anEMPTYRoutingValueIsFOURHUNDRED`,
   `.aPARTITIONTheIndexDoesNotHaveIsFOURHUNDREDAndNotFIVEHUNDRED`;
   `RoutedIngestTest.anEXPLICITPartitionOutsideTheRegisteredCountIsREFUSED` and
   `.theFIRSTPartitionPASTTheEndIsREFUSEDToo` (the boundary itself: `>=` → `>`
   left the rest green). The unregistered-index half is
   `BulkRoutingParamTest.aROUTINGValueReachesTheINGESTERUnaltered`, which answers
   **202** for an index the adapter cannot resolve — it holds no catalog, so
   `logs` is exactly that — and reds against a `400 UNKNOWN_INDEX` adapter, as
   does the 503 case below it (MEASURED in review: replacing the routed branch
   with a `400 UNKNOWN_INDEX` fails 2 of that file's 8 cases). ⚠️ An earlier
   draft of this clause said "the only case in the tree", which understated its
   own coverage — the same over-precise enumeration as the citation it replaced;
   `RoutedIngestTest.aROUTEDWriteToAnUNREGISTEREDIndexWAITSAndIsRELEASED` is the
   pooling, and
   `BulkRoutingParamTest.anINDEXWhoseRegistrationNeverArrivedIsFIVEHUNDREDANDTHREE`
   is the eventual refusal, **503** rather than 400 because the state ends on its
   own.
   ⚠️ An earlier draft of this line cited a case that DOES NOT EXIST
   (`aWriteToANINDEX…PASSEDDOWN…`) and attached a 202 claim to it. Round 1 of
   M6.12's review caught it. An evidence document whose citations are not
   resolved against the tree is worth less than no evidence document, and
   `check-milestone-verified.sh` cannot check this — it forces enumeration only.
   ⚠️ FR-13's defining clause is served here for the first time: `?partition=99`
   on a 3-shard index returned **202** before this milestone.
   ⚠️ **TRUE OF THE CLASS, NOT OF A DEPLOYMENT.** `RoutedIngest` is constructed
   in tests only; the sole production `Ingest` is `DefaultIngest`, which does not
   override `appendRouted`. In an assembled server the out-of-range partition
   would still be accepted and `?routing=` would answer 500. Nothing can reach
   either today — no production `main()` existed when this was written (M5.91a; ⚠️ **M8.1 and M8.4 own it since ADR-0052**) — and **M6.19** is the
   row. Found by M6's milestone review, not by any gate.
6. **An alias resolves to the current concrete index, and a rollover moves new records without moving old ones** — `AliasRolloverIT#testARolloverMovesNewRecordsAndLeavesOldOnesWhereTheyAre`,
   which holds the plugin's push to make ADR-0046's window deterministic:
   records land in the PREVIOUS index before the push and in the NEW one after,
   and the previous index still holds its records afterwards.
   `IndexCatalogTest.aROLLOVERMovesTheAliasAndLeavesTheOldIndexReadable` and
   `.anORDINARYRePushNeverMakesTheAliasUNRESOLVABLE` are the unit halves.
7. **A delivery the consumer could not take is reported, not skipped** — `DeliveryGapTest` (9 cases: the queue is forced full through the capacity the
   client is constructed with, the gap names the offsets and says whether the
   drop was local, a contiguous stream never reports one, and EVERY gap is
   counted rather than the first); `ShardConsumerGapTest.aGapDoesNotSTOPTheShardAndDoesNotLOSETheRecordsAroundIt`.
   ⚠️ The consumer COUNTS AND LOGS rather than throwing, because research doc 02
   §6 says any exception out of `readNext` pauses that shard until an operator
   intervenes.
8. **`error_strategy` behaves as configured** — `ErrorStrategyIT#testUnderDROPThePoisonIsSkippedAndTheShardKeepsIngesting`
   (a good record indexed AFTER the poison, and the poller still `POLLING`) and
   `ErrorStrategyIT#testUnderBLOCKTheShardSTOPSRatherThanSkipping` (the poller `PAUSED`,
   through the same `_ingestion/_state` an operator reads).
   ⚠️ The BLOCK half asserts the poller's STATE and not only an absence: review
   MEASURED a consumer mutated to deliver once and never again leaving an
   absence-only case green.
9. **A shard count reaching the ingester late does not lose records** — `RoutedIngestTest.aROUTEDWriteToAnUNREGISTEREDIndexWAITSAndIsRELEASED`,
   `.aROUTEDWriteThatWAITSTooLongIsREFUSEDRatherThanFolded` (the refusal is
   distinct from the pool-full one and the elapsed wait is asserted),
   `.TWOConcurrentRoutedWritesKeepTheirOWNRecordsAndTheirOWNPartition`.
10. **The pending pool is bounded, per index** — `PendingPoolTest.theBoundIsInBYTESAndPERIndex` (records of 16 and 480 bytes, so
    a COUNT bound is distinguishable from a byte bound; a second index's record
    survives the flood), `.theBYTECountIsEXACTAndCOUNTSTheRoutingValue`,
    `.aPARTIALExpiryLeavesTheREMAININGBytesExact`, and
    `RoutedIngestTest.aROUTEDWriteIsREFUSEDWhenTheINDEXSPoolIsFull`, which
    asserts the bytes are RELEASED after the refusal.
    ⚠️ **THE EXPIRY HALF EXERCISES A METHOD NOTHING CALLS IN PRODUCTION.**
    `PendingPool.expire()` has no production caller — every routed write settles
    its own batch in a `finally`, so the pool does not leak, but nothing bounds a
    wait whose caller died, and two mutations survive behind it. **M6.24**, and
    the same caveat criterion 12 carries for `onReconnect()`.
11. **Multi-node: shards spread across nodes, searchable by routing value, one subscription per node** — `RoutedSearchIT#testARoutedWriteToAnAliasIsSearchableByThatRoutingValue`
    on two data nodes: the shards are asserted to be on more than one node, the
    document is found BY ITS ROUTING VALUE, and each node opens one subscriber
    identity. Three mutations red it — shifting the partition by one shard,
    registering per shard, and pushing `numShards` where `routingNumShards` and
    `routingFactor` belong.
    ⚠️ **COUNTED AS SUBSCRIBER OBJECTS, NOT TRANSPORT CALLS**, and the first
    version was wrong: `SubscriptionTransport`'s default multi-key form passes
    the SAME listener once per key, so an identity counter over listeners read
    "1 subscriber" while two subscriptions were open on a two-shard node.
    ⚠️ **AND THE TEST PLAN'S `MultiNodeIT` WAS FOLDED IN RATHER THAN WRITTEN**:
    both halves of this criterion are in `RoutedSearchIT`, and a reader following
    the plan finds a file that does not exist.
    ⚠️ **AND M6.13 IS WHAT MADE IT MEASURABLE** — `PerNodeInstallTest`, 8 cases,
    including eight nodes constructed concurrently off one latch.
12. **The registration push is per NODE and per CHANGE, and a failed push is retried** — `RegistrationPushTest` (17 cases): one push for a one-shard and an
    eight-shard index in the same case, zero for an irrelevant change, the
    shape and aliases carried, an index this node does not host or this plugin
    does not ingest never pushed, three attempts in all — two retries — and a carry to the
    next change. `RegistrationWiringTest` holds the wiring, the two threads and
    the ordering.
    ⚠️ **THE "ON CONNECT" HALF IS `onReconnect()`, WHICH NOTHING CALLS IN
    PRODUCTION** — no production `SubscriptionTransport` exists (M5.6e, M8), so
    what is verified is the method and its effect, not a deployed reconnect.
    **M6.15** is the row that wires it.
13. **Zero object-store requests attributable to idle consumers still holds, with registration wired** — `IdleConsumerCostTest.theREGISTRATIONPathAndThePENDINGPoolCostZEROOfBothNumbersWhenIDLE`:
    1,000 consumers, 3,000 intervals, 300 registrations arriving DURING the
    loop, batches held while the pool is swept, zero requests and zero grants.
    ⚠️ **THAT COUNT CANNOT DIE TO A REGISTRATION DEFECT**, because no class on
    the path holds a `BinStore` — the by-construction non-proof M1 was withdrawn
    for. Its recorded red is the polling flush, which proves the meter is live.
    The property itself is `RegistrationPathStoreReachTest`, and **M6.18** is
    the deterministic gate it is not.

## What M6 does NOT close

⚠️ **THE ROUTED PATH WAS NOT WIRED INTO ANY DEPLOYABLE SERVER** when this was
written; M8.32 wired it, with `RoutedWriteTest` as the evidence. `RoutedIngest`
holds the routing, the catalog lookup, the pending pool and FR-13's
out-of-range refusal, and it is constructed by six call sites, all of them
tests. `BulkService` calls `Ingest.appendRouted`, whose default throws
`IllegalStateException`, which `BulkService` does not catch — so an assembled
server answers **500** to every `?routing=` write, which a producer retries
forever. It is a latent trap rather than a live defect because no production
`main()` existed at all when this was written (M5.91a, then unowned; ⚠️ **owned by M8.1 and M8.4 since ADR-0052**, and M6.19's own routed path by M8.32), and it is **M6.19**. This is the
milestone's one substantive delivery gap and the milestone review is what found
it.

⚠️ **The subscription transport is still a fixture.** Every criterion above
that crosses the ingester↔plugin boundary does so over a test transport; no
production `SubscriptionTransport` exists (M5.6e, owned by M8). So
`SubscriptionTransport.register`'s default REFUSES rather than accepting
silently, and `IndexRegistrar.onReconnect()` has no caller — **M6.15**.

⚠️ **The registration-path store check is a test, not a gate.** It reads
declared signatures and is blind to method bodies: a static holder called from
`register()` passes. **M6.18** specifies the scanner that closes it, including
auditing the hand-named seed list, which a reflective walk cannot do.

⚠️ **A virtual-shard or mid-split index is unplaceable and undetectable.**
`generateShardId` branches on `index.number_of_virtual_shards` and on a
mid-split index, and `IndexRegistration` carries neither — **M6.14**.

⚠️ **A node with the jar installed and no factory does not boot**, and the
diagnostic points at the wrong thing (`NullPointerException: subscriptions`).
Unchanged by M6.13 — the old static was null on that path too — **M6.16**.

⚠️ **`RestartResumeIT`'s note about the multi-node harness is stale.** M6.9
measured `OpenSearchIntegTestCase` needing no build change at all, which also
makes a real node-process restart reachable — **M6.17**.

⚠️ **`./gradlew build` does not run `clusterTest`.** Criteria 1, 6, 8 and 11 —
the mapper refusal, the rollover, both error strategies and the multi-node
routed search — are demonstrated only in a tier the default suite and (as far
as this tree shows) CI do not invoke. **M6.20** owns wiring it.

Open rows carried forward, each with its own backlog entry: M6.14, M6.15,
M6.16, M6.17, M6.18, M6.19, M6.20, M6.21, M6.22, M6.23 and M6.24 — the last
six harvested by M6's milestone review. ⚠️ Enumerated from the file rather than
as a range: round 1 of this commit's review found M6.24 missing from an earlier
draft of this line while the roadmap said ten.
