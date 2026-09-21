<!-- SPDX-License-Identifier: Apache-2.0 -->
# M6 — the OpenSearch plugin, beyond the skeleton

What M1's walking skeleton deferred: the placement decision a producer cannot
make, the registration that feeds it, and the paths that run when something is
wrong. M1 already makes a document searchable and survives restart; this
milestone is about being **correct for an index whose shape the producer does
not know**, and **honest when a delivery is lost**.

⚠️ **THE HEADLINE RISK IS A SILENT WRONG ANSWER.** OpenSearch never sets
`_routing` on an ingested document — `MessageProcessorRunnable` has no such key
and `IngestPipelineExecutor` forbids mutating it — so a routed search finds a
document **only** because our placement reproduces OpenSearch's own hash
([ADR-0015](../../decisions/0015-routing-registration-and-aliases.md),
[SPI §4b](../../../../research/20-opensearch/01-pull-based-ingestion-spi.md)).
A placement error is not an exception and not a failed gate: the query simply
misses the document. Every routing criterion below is therefore pinned against
OpenSearch's own `OperationRouting`, in the tier where that class is real.

## Completion condition

A producer writes to an **alias** with a **routing value**, never naming a
partition; the record lands in the shard OpenSearch's own routing would choose;
it is searchable **by that routing value** on a **multi-node** cluster; and a
delivery the consumer could not take is **reported rather than skipped**.

## Requirements

| ID | Requirement | Served by |
|---|---|---|
| FR-7 | An OpenSearch plugin implementing `IngestionConsumerPlugin` for 3.8.0+ | scope 1, 5, 6; criteria 1, 8-11 |
| FR-13 | Producers supply the partition (`explicit` \| `routing_key` \| `os_routing`); the ingester validates against a registered partition count and rejects mismatches | scope 2-4; criteria 2-6 |
| FR-16 | Index registration, pushed by the plugin over its existing subscription: shard counts, aliases, replication mode, lanes, ack mode | scope 2, 5; criteria 3, 4, 9, **12** ⚠️ **THE ROUTING SUBSET ONLY.** ADR-0015 §2's payload also carries `replicationMode`, `allActive`, `lanes[]`, `ackMode` and `quorum`; M6 carries what PLACEMENT needs, because the rest serves FR-17 and FR-18, which are out of scope here and would be fields with no reader. Adding them later is another wire-format change, and M6.2's ADR says so where the next author reads it |
| FR-19 | Producers send a routing value; the ingester computes the partition. Aliases resolved ingester-side to the current concrete index | scope 3, 4; criteria 2, 4, 6 |
| FR-12 | Multi-AZ Kubernetes deployment, ≥2 ingester nodes per AZ | scope 6; criterion 11 ⚠️ the CONSUMER half only -- a multi-node OpenSearch cluster, not a multi-AZ ingester fleet, which is M8's |
| FR-10 | Consumers keep working without the ingester | scope 5; criterion 7 ⚠️ the DETECTION half only: M5.18 shipped the ladder as a policy nothing executes |
| NFR-2 | Idle cost: zero object-store requests from consumers | criterion 13 -- **re-asserted, because M6 adds a registration path and a pending pool and neither may buy a request** |

## Scope

1. **The mapper surface, finished and checked.** ADR-0020's `DEFAULT` envelope
   is already assembled in the consumer (M1/M2). What M6 adds is the **refusal**:
   an index configured with a `mapper_type` this plugin cannot serve must fail
   where an operator sees it, not produce zero documents silently.
2. **Index registration (FR-16)**, pushed by the plugin over the subscription it
   already holds: `indexUUID`, `indexName`, `aliases[]`, `numShards`,
   `routingNumShards`, `routingFactor`, `routingPartitionSize`. ⚠️ **A wire
   format change**, so it carries the
   [`wire-format-change`](../../../../../.agents/skills/wire-format-change/SKILL.md)
   obligations and an ADR of its own.
3. **`os_routing` placement (FR-19)**: `floorMod(murmur3_32(routing),
   routingNumShards) / routingFactor`, matching
   `OperationRouting.calculateScaledShardId`, **pinned against that class**.
4. **Aliases and the pending pool (ADR-0015 §3, §4)**: an alias resolves to the
   current concrete index; a record for an index whose shape is not yet known
   waits in a **bounded** pool and is placed when registration arrives, or is
   **rejected** after `pendingTimeout` — never folded into partition 0.
   ⚠️ **THE BOUND IS PER INDEX, NOT GLOBAL**, and saying which matters: one
   unregistered index flooding a global bound would refuse the records of every
   other index waiting on an ordinary plugin reconnect, which is the blast
   radius ADR-0010's per-index share cap exists to prevent elsewhere. A
   single-index flood is green under either reading, so criterion 10's case
   floods one index and asserts a SECOND index's pending records survive.
5. **Error and back-pressure paths**: a delivery the consumer cannot take is
   **detected as a gap**, not dropped silently; `error_strategy: BLOCK` and
   `DROP` are asserted rather than assumed.
6. **A multi-node cluster**: shards spread over more than one node, one
   subscription per node, documents searchable cluster-wide.

### Out of scope, and where each lives

- **Fast mode (FR-17), priority lanes (FR-18), per-index admission limits
  (FR-15).** Each has its own ADR and none is a plugin concern.
- **A production `SequencerTransport` and the pod-to-pod mesh** — M5.6e, M8.
- **A production `main()`** — ⚠️ **owned by M8.1 and M8.4 since ADR-0052**; it was still unowned when M6 was specified, and M5.91a records it.
- **Compaction (FR-14)** — proposed, unscheduled.
- **The ingester's own multi-AZ deployment (FR-12's fleet half)** — M8.
- **Changing the shard count of an ingesting index.** ADR-0015 §4: resize needs
  a read-only source, so it is incompatible with ingestion regardless of us.
  **Roll over instead**, which §4 supports and criterion 6 asserts.

## Design

### Registration travels the channel that already exists

The plugin runs inside OpenSearch, is handed `IndexMetadata` in
`createShardConsumer`, can hold a `ClusterStateListener`, and already maintains
an authenticated subscription. So it pushes registration over that — **no new
endpoint, no new credential, no unauthenticated surface** (ADR-0015 §2).

⚠️ **The seam, not the socket.** `client` may not import `io.github.huyz0.os.biningester.binstore`
(ADR-0023) and `plugin` may not import `ingest`; the registration therefore
travels as a type in `format` over a method on the transport seam, exactly as
subscription does. The production transport is still M8's (M5.6e), so M6 wires
it end to end through the in-process bridge the cluster tests already use, and
says so rather than implying a deployed hop.

### Placement is computed where the shard count is known

`partition = floorMod(murmur3_32(routing), routingNumShards) / routingFactor`.
⚠️ **Reproduced against `OperationRouting`, never from memory** — ADR-0015 says
so in as many words, and the failure mode is a silent miss. `routingFactor`
exists because a split index keeps `routingNumShards` at its original value;
for an unsplit index it is 1 and the expression reduces to
`floorMod(murmur3_32(routing), numShards)`.

**Rejected:** deferring routing to read time (each shard reads everything and
keeps the 1/S that hashes to it). ADR-0015 prices it at S× serving
amplification — 100 GiB/s for a 2,000-shard index — which is the mistake
[ADR-0004](../../decisions/0004-the-service-serves-reads.md) exists to prevent.

**Rejected:** having the producer compute the partition for a routing value.
That needs the shard count, which needs either an unauthenticated endpoint on
the plugin or OpenSearch credentials in every producer. ADR-0015 rejects both;
`explicit` remains for producers that genuinely know their partition.

### A buffered record has no offset yet, so it can still be moved

[ADR-0001](../../decisions/0001-segments-carry-no-absolute-offsets.md) assigns
ordering at commit, not at write, which is what makes the pending pool possible
at all: a record waiting for registration has not been placed yet, so placing
it late costs nothing and needs no key. The pool is FIFO per index, **bounded**,
and counted against the same memory budget as any other buffer.

### A dropped delivery is a gap, and a gap is reportable

`ConsumerClient.deliver` offers into a bounded queue and **drops on full** —
correct, because blocking would stall the ingester's commit path for every
stream on the node. What is missing is the other half: nothing notices. The
shard then reads offset 41 after offset 12 and indexes it, so the documents in
between are missing with no exception anywhere. M6 makes the consumer **see the
gap** and surface it; recovering from it is the ladder's, which is M8's to
execute (M5.18 shipped it as a policy nothing runs).

## Cost impact

⚠️ **M6 adds no object-store request at all**, and that is the whole of its cost
story. Every mechanism here is CPU or bounded memory:

| Rule | What M6 does | Budget |
|---|---|---|
| **R1-R2** *never LIST, never scale with indices* | Registration travels the existing subscription; no discovery, no listing | **0** |
| **R3** *idle consumers issue zero requests* | Unchanged, and **re-asserted** (criterion 13) because the registration push and the pending pool are both new paths that could have bought one | **0**, asserted at fan-out |
| **R5** *one fetch per object per node* | Unchanged — M5.45h's `NodeSegmentSource` is the sharing unit | unchanged |
| **R15** *LIST ceiling* | No LIST added | **0** |
| — | Routing costs one murmur3_32 of a short string, ~50 ns, on the write path. It is **not** document parsing, which stays forbidden ([streaming-io §3](../../../../research/40-implementation/02-streaming-io-and-memory.md)) | CPU only |
| — | The pending pool holds records for at most `pendingTimeout` (default 5 s) and is bounded; an unregistered index cannot consume memory indefinitely | bounded, NFR-6 |
| — | Registration is ONE message per NODE per relevant cluster-state change (criterion 12), never object-store requests, never per shard and never per record | no requests |

## Acceptance criteria

1. **An index configured with a VALID `mapper_type` this plugin cannot serve
   fails visibly**: `RAW_PAYLOAD` or `FIELD_MAPPING` — both accepted by
   OpenSearch's closed `MapperType` enum, and neither able to express what
   ADR-0020's envelope carries — produces an error from THIS plugin's own
   refusal, naming the setting, when ingestion starts. ⚠️ **A bogus value proves
   nothing**: OpenSearch core rejects it first, so the case would red with this
   plugin ignoring the setting entirely. The failure mode being prevented is
   "zero documents indexed and nothing in the log".
2. **Placement matches OpenSearch's own**: for ≥1,000 routing values across
   shard counts {1, 3, 5, 16} and a split index (`routingNumShards` ≠
   `numShards`), the ingester's partition equals
   `OperationRouting.calculateScaledShardId`'s, **asserted against that class**
   rather than against a re-derivation.
3. **Registration carries what placement needs, and round-trips**: golden file,
   every reader and writer and fake updated in one commit
   (`wire-format-change`), and a decoder that refuses a truncated or
   forward-version frame.
4. **`os_routing` is refused where it cannot be correct**: a **routed** write to
   an index whose registration carries `routingPartitionSize > 1` is refused
   with a message naming the setting, while an **explicit-partition** write to
   that same index still succeeds. ⚠️ **THE MODE IS REFUSED, NOT THE INDEX** —
   SPI §4b says "reject `os_routing` when `routing_partition_size > 1`" —
   because with built-in tenant partitioning the shard depends on the routing
   value AND the document `_id` (`OperationRouting:587`), which the ingester
   does not have; explicit placement needs no hash and is unaffected. Refusing
   the registration outright would take a working mode away, and not refusing
   at all is a silent miss per record.
5. **A producer that names no partition and no routing value is refused**, one
   that names both is refused, and **an explicit partition outside the
   registered count is refused** — 400 in each case, never a default of 0 and
   never a 202. ⚠️ **AN UNREGISTERED INDEX IS NOT ONE OF THESE CASES**: it is
   accepted and waits in the pending pool (criterion 9), because ADR-0015's
   Consequences require that "a producer that starts before the plugin connects
   is not punished for a race it cannot see" — a 400 there is a 400 storm on
   every plugin reconnect. An unregistered index is refused only when
   `pendingTimeout` expires, and the refusal says so. ⚠️ **The third is FR-13's defining clause** ("validates against
   a registered partition count and rejects mismatches") and nothing has served
   it before, because until M6.3 no count existed in the ingester:
   `?partition=99` on a 3-shard index returns 202 today and lands in a stream
   nothing polls.
6. **An alias resolves to the current concrete index, and a rollover moves new
   records without moving old ones**: once the plugin's push has arrived,
   records written to the alias land in the NEW index's streams; records already
   committed keep their offsets in the previous index's streams, and nothing is
   repartitioned.
   ⚠️ **AND THE STALENESS WINDOW IS
   [ADR-0046](../../decisions/0046-the-pending-pool-covers-an-unknown-index-not-an-unpushed-rollover.md)'s,
   WHICH AMENDS ADR-0015 §3 RATHER THAN RE-READING IT.** §3 named "a rollover
   the plugin has not yet pushed" as a pending-pool trigger and claimed the
   window shrank to "records wait a few milliseconds and land in the right
   one". That trigger is not observable: the alias still resolves to the
   previous concrete index, whose shape IS registered and IS correct for that
   index, and nothing in the ingester distinguishes a current mapping from one
   that was current a millisecond ago. Detecting it means asking OpenSearch per
   write or on a timer, which ADR-0015 §2 exists to avoid. So ADR-0046
   withdraws the shrink claim: records land in the previous index, the window
   is bounded by the plugin's push latency, and §4 already calls that normal
   for time-series rollover. ⚠️ An earlier draft of this criterion asserted the
   same behaviour while citing §4 as if §3 agreed -- the correction is an ADR,
   not a spec sentence.
7. **A delivery the consumer could not take is reported, not skipped**: with a
   queue forced full, the shard consumer surfaces a gap naming the offsets, and
   does **not** hand the engine the later records as if contiguous.
8. **`error_strategy` behaves as configured**: under `DROP` a poison record
   leaves the shard ingesting **and the records after it still arrive** — a
   shard that keeps polling and hands the engine nothing more is "still
   ingesting" by every weaker wording while indexing zero documents; under
   `BLOCK` it stops rather than skipping past the poison record. Both observed
   on a real node rather than assumed from the setting's name.
9. **A shard count reaching the ingester late does not lose records**: records
   written before registration are placed when it arrives; records still pending
   after `pendingTimeout` are **rejected** with a message, never folded into
   partition 0.
10. **The pending pool is bounded, per index**: a flood for one unregistered
    index is refused once **that index's** pool holds its configured maximum,
    with the refusal naming the index, a **second** unregistered index's pending
    records survive the flood, and **the pool's own retained byte count never exceeds that
    maximum** — asserted from the pool, not from the heap. ⚠️ An earlier draft
    said "service memory stays flat", which no test in the plan measures; the
    heap claim belongs to a soak, and this is a counter.
11. **Multi-node**: on a cluster of ≥2 nodes with an index whose shards are
    **asserted** to be spread across them, every document written to the alias
    with a routing value is searchable **by that routing value**, and each node
    holds **one** subscription regardless of how many shards it hosts.
    ⚠️ **THE SUBSCRIPTION COUNT IS COUNTED AT THE TRANSPORT, as subscriber
    identities opened, never as `clientFor` calls**: keying the node's clients
    by shard instead of by run leaves a per-node client count of 1 and opens one
    subscriber per shard, which is the M5.62 shape wearing this criterion's
    clothes.
    ⚠️ **AND IT NEEDS PER-NODE STATE THAT DOES NOT EXIST YET**:
    `BinStorePlugin` holds its `NodeSubscriptions` in a `private static
    volatile` field, and `InternalTestCluster` runs every node in ONE JVM, so
    all of them would share one. M6.13 is that task, and it comes before M6.9.
12. **The registration push is per NODE and per CHANGE, never per shard and
    never on a timer**: a node hosting K shards of an index pushes **once** on
    connect and **once** per relevant cluster-state change (shard count, alias
    set, index added or removed), and **zero** times for an irrelevant one.
    ⚠️ **Counted, at K = 1 and K = 8**: a listener built in
    `createShardConsumer` pushes once per shard per change and would be
    invisible to every other criterion here — FR-16 was otherwise served by
    three criteria, none of which observes the push at all.
    ⚠️ **AND A FAILED PUSH IS RETRIED.** A push that throws once and is never
    retried satisfies every count above exactly, and then every index on that
    node falls out of the pending pool at `pendingTimeout` — a sustained stream
    of refusals for a fault that lasted one message. Asserted here rather than
    left in a mutation cell, because M6.12 harvests evidence per CRITERION.
13. **Zero object-store requests attributable to idle consumers still holds**,
    re-asserted with registration wired: ≥1,000 idle consumers over ≥3,000
    intervals of an injected clock cause zero requests and zero grants, with the
    registration path live.

## Test plan

| Tier | Behaviour | Fails first against | Mutation it catches |
|---|---|---|---|
| T1 | `RoutingPartitioner` vs `OperationRouting`, 1,000 values × 4 shard counts + a split index | `RoutingPartitionerParityTest` | `floorMod` → `%` (negative hashes land on shard 0); dropping `/ routingFactor`; `murmur3_32` seeded 1 rather than 0 |
| T0 | registration encode/decode + golden | `IndexRegistrationTest`, `GoldenIndexRegistrationTest` | a reordered field; a truncated frame accepted; a forward version accepted |
| T0 | `IndexCatalog` alias → concrete index, and a ROUTED write to a `routingPartitionSize > 1` index refused | `IndexCatalogTest` | refusing the whole index rather than the routing MODE, which takes explicit placement away; accepting a routed write and hashing anyway, which is the silent miss |
| T0 | pending pool: placed on registration, rejected after timeout, bounded IN BYTES | `PendingPoolTest` | an unbounded pool; a bound on the record COUNT rather than on bytes, which the case distinguishes by holding records of DIFFERENT sizes -- a count bound is not unbounded, refuses the flood and names the index, and still holds N x maxRecordSize; a timeout that rejects the wrong index's records; folding to partition 0 |
| T0 | `_bulk` refuses neither/both of `partition` and `routing`, and an out-of-range partition | `BulkRoutingParamTest` | defaulting to partition 0; accepting both and silently preferring one; accepting `?partition=99` on a 3-shard index, which is what happens today; ⚠️ **answering `400 UNKNOWN_INDEX` on a catalog miss** -- which an earlier draft of this spec SPECIFIED, is green against every other row in this plan, and is a 400 storm on every plugin reconnect. The pool's own row is a T0 on the pool OBJECT and never crosses the HTTP path, so only this row can red it |
| T1 | consumer gap detection | `DeliveryGapTest` | dropping silently (today's behaviour); reporting a gap where the offsets are contiguous. ⚠️ The queue is forced full through the CAPACITY the client is constructed with -- `NodeSubscriptions` already takes it and `ConsumerClient` refuses a non-positive one -- so no new seam is needed and the case drives the production path |
| T4 | mapper refusal (a VALID unserved type), `error_strategy` DROP/BLOCK | `MapperRefusalIT`, `ErrorStrategyIT` | an index that ingests zero documents while reporting healthy; a refusal that is OpenSearch core's rather than this plugin's |
| T1 | per-node plugin state | `PerNodeInstallTest` | an install-order STACK rather than a node-keyed map: distinct instances per node, reflective constructor intact, `SingleNodeBootIT` green, criterion 11 green under deterministic boot -- and node B answering with node A's subscription under a concurrent or restarting boot |
| T4 | alias + routing searchable, multi-node, one subscription per node | `RoutedSearchIT`, `MultiNodeIT` | placement that puts the document on the wrong shard (the silent miss); a subscription per shard, counted at the TRANSPORT so keying clients by shard cannot hide it; a fixture whose shards all land on one node |
| T4 | rollover moves new records only | `AliasRolloverIT` | repartitioning old records; writing new ones into the old index AFTER the push arrived. ⚠️ The window BEFORE the push is ADR-0046's decided behaviour and is asserted POSITIVELY -- records in it land in the previous index and are readable there -- rather than left as an absence |
| T1 | the push is per node and per change, and a FAILED push is retried | `RegistrationPushTest` | a listener per shard (K pushes per change at K = 8); a push on an irrelevant change; a timer; ⚠️ **a push that fails once and is never retried**, which passes the count criterion exactly and turns into a sustained stream of pending-timeout refusals for every index on that node |
| T1 | NFR-2 re-assertion with registration wired | `IdleConsumerCostTest` (extended) | a registration push that reads the store; a pending pool that persists |

⚠️ **T4 boots a real in-process OpenSearch node** and is the only tier that can
answer "searchable" — it is a property of Lucene and the ingestion engine, not
of our seams. It runs under `./gradlew :plugin:clusterTest`, not under
`./gradlew test` (build.md § tiers).

⚠️ **Coverage and mutation**: `check-coverage.sh` exists but is unwired;
`check-mutants.sh` does not exist. Both facts are stated in every report this
milestone produces rather than implied away.

## Risks

| Risk | What would reveal it |
|---|---|
| **The placement hash diverges from OpenSearch's** — the silent miss this milestone exists to prevent | Criterion 2 compares against `OperationRouting` itself, at four shard counts and a split index. ⚠️ A test that re-derives the expected value from our own formula would pass while wrong, which is why the real class is on the fixture side |
| **Registration arrives late in production and the pool masks it** | Criterion 9 asserts the rejection at timeout, and the refusal names the index. A pool that silently absorbed a permanently-unregistered index would look healthy while the producer's writes were never placed |
| **A dropped delivery still reaches the engine as contiguous** | Criterion 7 forces the queue full and asserts the gap is named. Today nothing notices, so the test must red against the CURRENT behaviour |
| **The multi-node test passes for a single-node reason** | Criterion 11 asserts shards on more than one node AND one subscription per node; a fixture that happened to place every shard on one node would satisfy the first half only |
| **M6 quietly buys an object-store request** | Criterion 13 re-runs the idle-cost case with registration wired. ⚠️ Re-asserted rather than inherited: M5's own SPEC records why "it held before" is not evidence |

## Tasks

One commit each, in dependency order.

| ID | Task |
|---|---|
| M6.0 | This spec, its backlog rows, and the spec review |
| M6.1 | A dropped delivery is a GAP the consumer reports, not a silent skip |
| M6.2 | `IndexRegistration` in `format` + ADR + golden file (wire-format change) |
| M6.3 | `IndexCatalog` in `ingest`: registrations, alias resolution, refusal of `routingPartitionSize > 1` |
| M6.4 | `RoutingPartitioner`, pinned against `OperationRouting` |
| M6.5 | The pending pool: buffer-and-reroute, bounded, `pendingTimeout` |
| M6.6 | `_bulk` takes a routing value; `partition` and `routing` are mutually exclusive |
| M6.7 | The plugin pushes registration on connect and on cluster-state change |
| M6.8 | `error_strategy` and the mapper refusal, on a real node |
| M6.13 | Per-node plugin state, so a cluster of nodes in ONE JVM is possible |
| M6.9 | Routed search end to end, on a multi-node cluster ⚠️ depends on M6.13 |
| M6.10 | Alias rollover: new records move, old ones do not |
| M6.11 | NFR-2 re-asserted with registration wired |
| M6.12 | M6's `VERIFIED.md` and the milestone review |
