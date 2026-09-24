# M9 — Cost and performance proof: the numbers measured, not modelled

## Completion condition

From [roadmap.md](../../roadmap.md): *"RustFS/S3-compatible benchmarks; the cost/latency
curve published; NFR-1/4/5 measured, not modelled"*, plus **NFR-7** (end-to-end
p99 < 3× the flush window, reassigned from M5), measurement **M3** (the
fan-out threshold selecting `direct`), and the rows M8 handed over under their
M8 IDs: the catch-up read path and the ninth chaos row (M8.24), the ladder's
store tiers and gap re-reads ([ADR-0057](../../decisions/0057-the-store-reading-fallback-tiers-and-gap-re-reads-wait-for-m9.md),
whose deferral M9 discharges and whose decision is superseded in part),
the prefetcher (M8.82, M8.83, M8.56), address-matched challenges (M8.58), and M8's
milestone-review harvest (M8.60–M8.81). NFR-7's harness must measure the
[ADR-0058](../../decisions/0058-a-partitioned-pod-acks-on-a-durable-commit-intent.md)
path, where a 202 may precede its offset.

⚠️ **THE ROADMAP ROW SAYS "RUSTFS/S3-COMPATIBLE BENCHMARKS" AND THERE IS NO AWS S3.** A
completion condition this milestone cannot meet is not a condition, so **M9.22
amends the row** to RustFS benchmarks with the S3 halves named NOT-RUN — the
tail latency in NFR-7, the TTFB half of measurement M3, and a dollar figure
from a bill. It is an amendment of a plan, not of a requirement: no FR or NFR
moves, and the numbers the row asks for are still taken.

⚠️ **"PUBLISHED" MEANS A COMMITTED DOCUMENT IN `docs/`**, nothing external:
`docs/internal/product/measurements/cost-latency-curve.md`, generated from a
committed results file (criterion 16).

⚠️ **ONE RIG IS AVAILABLE, AND IT IS NOT A CLOUD.** Every measurement in this
milestone runs on one workstation: WSL2, Docker, RustFS from
`docker-compose.test.yml` (a test fixture, testing.md rule 19a), and the
assembled ingester processes the M8 chaos harness already starts. There is no
AWS account. So this milestone can prove three kinds of thing and must label
which it is proving:

| Kind | Example | Holds on S3? |
|---|---|---|
| **A request count** | PUTs per MiB, GETs per segment, LISTs per hour | ✅ **Yes.** The count is a property of this project's code, not of the store; RustFS answers the same verbs the same number of times |
| **A byte count** | cross-AZ bytes, allocation per record | ✅ **Yes**, for the same reason — provided the AZ is a label the pods carry, not a network the rig has |
| **A latency, a throughput, or a dollar figure** | p99 visibility, MiB/s per core, $/TiB | ❌ **No.** RustFS on loopback has no S3 tail latency and no bill. A latency is a measurement of THIS RIG; a dollar figure is a measured count multiplied by a published price table, which is a model and is labelled one |

⚠️ **AND THE RIG IS PINNED AS FAR AS IT CAN BE.** Research 40/03 §0 names five
environment controls; this rig honours three and cannot honour two, and the
curve document records which (criterion 16):

| §0 control | On this rig |
|---|---|
| Fixed JDK build | ✅ honoured — the toolchain the build pins |
| `-XX:+AlwaysPreTouch` | ✅ honoured — set on every measured JVM |
| Record the environment with the result | ✅ honoured — kernel, JDK, CPU model, core count, container limits and the RustFS digest go in the results file |
| Fixed CPU governor | ❌ **cannot** — WSL2 does not expose it; run-to-run variance is measured instead and published beside every number |
| No co-tenancy | ❌ **cannot** — one workstation runs Docker, RustFS and every JVM. So each latency is a lower bound carrying noise, never a clean p99 |

⚠️ **NFR-1, NFR-4 and NFR-5 are counts, so M9 can MEASURE them.** NFR-7 is a
latency, so M9 measures it on RustFS and says in the same sentence that S3's
PUT tail is not in the number. What a production-cloud run would add is stated
per criterion, and is § *What a cloud run adds*.

## Requirements

| ID | What M9 does to it |
|---|---|
| **NFR-1** (< 0.30 requests per MiB written) | Measured on the assembled fleet against RustFS, counted by `CountingBinStore`, turned into a gate |
| **NFR-2** (zero idle requests) | Already measured by M8's `IdlePodCostSoakTest`; M9 puts it in CI (M8.75) so it cannot regress unseen |
| **NFR-3** (no LIST on the hot path) | Asserted in every macro run; M8.66 states the backfill's costs |
| **NFR-4** (read rate scales with segments, AZs, nodes) | Measured at 16 vs 1,600 shards and 1 vs 3 vs 9 consumer nodes, with the prefetcher wired (M8.56, closed by M8.84 and M8.85) |
| **NFR-5** (cross-AZ bytes < 0.1% of ingested) | Measured, which first needs pods to carry an AZ label and the peer sockets to count bytes by it (M9.2) |
| **NFR-6** (memory bounded) | Not re-proved; the allocation gate (M9.6) is its leading indicator |
| **NFR-7** (p99 < 3× the flush window) | Measured on RustFS, producer 202 to consumer-visible, at three windows; one point to searchable in OpenSearch |
| **NFR-9**, **NFR-11**, **FR-9**, **FR-10**, **FR-11**, **FR-12**, **NFR-13** | Carried by the inherited M8 rows, each citing its own |

## Scope

1. **The measurement machinery first**: the mutation gate (M0.14, named in
   AGENTS.md as not existing), the cost meter (research 02 §7), the AZ label and
   cross-AZ byte counter, a load generator, the macro harness, a profile, and a
   JMH harness with an allocation gate.
2. **The four NFRs measured** (1, 4, 5, 7) and two deferred constants settled:
   **M2** (block size and codec, benchmark B3, assigned to M9 by
   `50-open-questions.md` §3 and missing from the roadmap row) and **M3**.
3. **The cost/latency curve**, generated into `docs/`.
4. **Every inherited row**, under its M8 ID. ⚠️ **NOTHING IS RE-DEFERRED**: the
   ladder's store tiers are designed and built here (M9.20, M9.21), and M8.58
   carries a lease-format change under `wire-format-change`.

### Inherited rows

| Row | Disposition |
|---|---|
| M8.24a — the catch-up protocol decision | **Done.** [ADR-0065](../../decisions/0065-node-scoped-catch-up-with-live-tail-priority.md) |
 | M8.24b — bounded replay primitives | **Done.** The source and coordinator are covered by focused ingest tests; the HTTP/control-frame wiring and T4 evidence remain M8.24. |
| M8.24c — versioned catch-up control frames | **Done.** Request, event and end frames are format-owned, versioned, golden-pinned, and reject unknown versions before decoding. M8.24d carries them onto the HTTP seam. |
| M8.24d — HTTP catch-up control seam | **Done.** A bounded POST decodes one request, delegates to a replay responder, and returns length-framed response frames; malformed requests are refused before delegation. Production registration is M8.24f. |
| M8.24e — durable catch-up responder | **Done.** `batch_start` is translated to the source's exclusive offset, shared segment bytes are read once per response, inline event frames are emitted, and a request-matched end frame closes the response. Production assembly wiring is split across M9.25 and M9.26; consumer integration is M8.24g; T4 evidence is M8.24h. |
| M8.24f — production catch-up registration and route proof | **Parent, closed only after M9.25 and M9.26.** M9.25 registers the responder and proves normal replay, shared-segment request cost and early refusal; M9.26 proves the composed route preserves a positive persisted `batch_start`. |
| M8.24 — the catch-up read path and the ninth chaos row | **A parent task, closed only after M8.24f–h.** M8.24a–e settle the decision and build protocol/components; M8.24f (M9.25–M9.26) registers and verifies durable replay in the assembled ingester, M8.24g connects the OpenSearch consumer's node-scoped replay and priority queues, and M8.24h proves recovery and live-tail bounds in T4. M9.13 and M9.21 remain after the parent. |
| M8.28's tiers 2 and 3 and gap re-reads | **Three tasks, DESIGNED AND BUILT IN M9** (the user's decision, 2026-09-20). (a) The gap RE-READ over a reachable ingester, with `TENS_OF_GETS` counted for it, is **M9.13**. (b) **M9.20 is an ADR choosing a THIRD approach** to plugin-side store access, both earlier candidates having been rejected on 2026-09-20 -- a SigV4 signer in the plugin (a credential in the OpenSearch JVM) and long-lived pre-issued chain grants (amending ADR-0041's short TTL). (c) **M9.21 EXECUTES tiers 2 and 3** on a real gap with NO ingester reachable, re-reading the missing window through M8.24's catch-up-from-offset read path and counting the GETs and LISTs each tier costs. ⚠️ **ADR-0057 IS SUPERSEDED IN PART** -- its decision that tiers 2 and 3 wait, and its consequence that an outage longer than the backoff leaves consumers indexing nothing -- and M9.20 says so in its own text, leaving ADR-0057's tier-0/1 half standing |
| M8.82 — expose ready EndpointSlice peers | **A task**, prerequisite to constructing the live peer ring |
| M8.83 — versioned durable-segment hint | **A component task**, prerequisite to signalling the per-AZ cache owner; adds a new cross-AZ transport to criterion 6's accounting. Its sender and receiver are independently tested, but neither is registered or invoked by the production assembly here |
| M8.56 — wire `SegmentPrefetcher` | **An integration parent after M8.82 and M8.83**, closed by M8.84's production assembly/real-HTTP RustFS proof and M8.85's multi-candidate-per-AZ owner proof. The original three-round test review found that one candidate per AZ could not falsify a membership regression; NFR-4 measurement must wait for both child rows. M5.91b remains owned until the parent closes |
| M8.58 — address-matched challenges | **A task, and its shape is decided** (the user's decision, 2026-09-20): **the Kubernetes pod UID goes IN THE LEASE**, and `EndpointSliceView` matches `targetRef.uid` rather than the endpoint's address. ⚠️ **THAT IS A WIRE-FORMAT CHANGE**, so the task follows [`wire-format-change`](../../../../../.agents/skills/wire-format-change/SKILL.md): the format document, every reader, every writer, the fakes, the golden files and the ADR in ONE commit. Exercised against `FakeKubeApi`; a real cluster stays unavailable and the row says so |
| M8.60–M8.81 — M8's milestone-review harvest | **Twenty-one tasks**, each as written, including M8.76/.77 split from the combined M8.73 review fix, M8.79/.80 split from M8.78 after round-three review, and M8.81's canonical source digest on Windows. M8.61 and M8.62 name many commits and may be split at take-up by `next-task` if one commit's worth turns out to be less |
| measurement M3 (`direct` fan-out threshold) | **A task (M9.14)**, and half of it is NOT-RUN on this rig: see criterion 12 |
| M5.43, M5.45d | **Already done** (`directEnabled` exists; `publishSegment` serves `DIRECT`). Nothing inherited |

### Not in scope

- **Real S3, GCS or Azure runs.** No account exists. § *What a cloud run adds*
  says what each would change.
- **Optimisation for its own sake.** Profiling (M9.5) may produce an
  optimisation task; it is written as a new row with before/after numbers
  (performance.md rule 11), not folded into a measurement row.
- **The cost governor's REFUSING half** (FR-21, research 30/15; cost.md rules
  12–17). M9 builds and asserts the COUNTING half only. ⚠️ **FR-21 is owned by
  no milestone once M9 ends** — counting catches a regression in review, only
  refusing catches one in production — and `VERIFIED.md` records that rather
  than letting the meter read as a governor.
- **Compaction** (FR-14): after M9 by the roadmap.

## Design

**The macro harness is the M8 chaos harness pointed at a workload.**
`NodeProcess` already starts assembled ingester processes against RustFS and
asserts at the store. M9 adds a seeded load generator (doc sizes 200 B,
1 KiB, 10 KiB and a mixed long-tail distribution, research 40/03 §0), wraps the
store in `CountingBinStore` inside each process, and exports the counts plus a
latency histogram as one JSON results file per run. Rejected: a separate
harness process tree — two ways to start a fleet is how M8's harness and
its first draft disagreed about what a kill was.

**The cost meter is arithmetic over `StoreCounts`.** Requests per MiB written
and read, idle request rate, and `estimated_usd_per_tib_ingested` from a price
table held as configuration (research 02 §1 AWS us-east-1 prices as the
default). It lives beside `CountingBinStore` in `binstore-spi`, touches no clock
or socket, and is T0.

**Cross-AZ bytes are counted at the peer sockets by label.** The rig has one
network, so an "AZ" is a setting each pod carries (`pod.az` or similar — the
settings list has none today) and each peer transport counts bytes sent to a
peer whose label differs. NFR-5's denominator is producer bytes accepted.
Rejected: Docker networks per AZ with `tc` shaping — it would add latency
realism the rig cannot calibrate against S3 anyway, and the requirement is a
byte ratio.

**Latency is measured from the producer's 202 to the record's delivery at a
consumer**, stamped by the load generator and read back through the consumer
library, with HdrHistogram. One operating point is also measured to
"searchable" in OpenSearch at T4 (`clusterTest`), because that is what research
40/03 §2 defines and what an operator sees. The flush window NFR-7 divides by
is the configured **CEILING** of the adaptive interval (ADR-0017; the divisor
decision is recorded in ADR-0063), the worst case — **decided by the user on
2026-09-20**. It resolves an ambiguity in a REQUIREMENT's text rather than
choosing an implementation: NFR-7 says "the configured flush window" and the
interval has both a floor and a ceiling, so every future reading of that row
rests on this answer.

**NFR-1 has two regimes, and both are asserted.** Above the rate at which
flushes are size-triggered, the PUT rate tracks bytes ÷ segment size (R14) and
the bound is < 0.30 requests per MiB. Below it the interval, not the byte
count, sets the rate, so a per-MiB ratio measures the workload rather than the
design; the bound there is **≤ 2 write requests per pod per INTERVAL
CEILING** — one data PUT and one commit PUT per interval — and it is asserted
rather than merely published. ⚠️ Never per second: a per-second number
describes the configured dial, not the code (criterion 3). NFR-1's text is amended
to carry both (M9.18), because today it reads as one unconditional bound.

**Tiers 2 and 3 read the object store from the plugin side, which nothing lets
it do today.** M9.20 chooses how — architecture rule 2 keeps a cloud SDK out of
the plugin, and both earlier candidates were rejected — and M9.21 executes the
tiers and counts their requests, re-reading the missing window through M8.24's
path. The ladder is then executed end to end for the first time, which is the
point: a policy nothing runs is a policy that has never been wrong.

**Store reads are flat in consumer-node count, and that is the claim under
test.** cost.md rules 10 and 11 say consumers read THROUGH the ingester, whose
cache absorbs the fan-in, so the object store sees requests per SEGMENT and per
AZ — not per consumer node. A build whose ingester cache never hits would still
serve every read correctly and would cost one store GET per node, which is the
$3,732/month design ADR-0004 rejected. So criterion 5 bounds STORE requests as
near-constant in consumer nodes and lets only INGESTER-SERVED requests grow
with them. ⚠️ `direct` mode is the deliberate exception — the consumer fetches
for itself, which is what measurement M3's threshold exists to ration — so it
is measured separately (criterion 12) and excluded from the flat bound.

**The prefetch signal is a directed hint, not gossip.** M8.82 exposes the
ready, AZ-labelled peer set already supplied by Kubernetes. M8.83 implements a
sender that sends only a versioned frame containing writer identity, writer AZ
and the segment key to the rendezvous-ring owner in each other AZ, and a
receiver that authenticates it; no payload bytes or commit facts cross on this
path. These are independently testable components, not yet production-wired.
M8.56 registers the receiver and invokes the sender after durability from the
assembled node. The receiver binds the claimed writer identity and AZ to
the request's `remotePeer().host()` and the current ready `EndpointSlice`
address set before any store read; a known-writer claim from a different peer
is refused, and the segment key's encoded pod ID must equal the claimed writer
ID. The hint may be lost or stale without affecting correctness: the
existing object-store path remains the fallback. Its maximum frame is 1,550
bytes (the 8-byte header, bounded fields, and length prefixes); at the default
8 MiB segment size, two cross-AZ copies contribute at most 0.037% of producer
bytes. The M9.10 run counts it with every other transport against the full
NFR-5 limit, and the signal fan-out is capped at AZs−1 per segment rather than
growing with nodes, streams or consumers. Its reader, writer, fake, golden and
ADR travel together as a `wire-format-change` task.

**Profile, then JMH** (performance.md rule 1). JFR over the macro harness names
the top three hot spots; JMH benchmarks are written for those plus B3 (needed
by M2 regardless). Only allocation per record is a gate (performance.md rule
9); ns/op and MiB/s trend in the curve document.

**The curve document is generated** (gate-design rung 5): a script renders
`docs/internal/product/measurements/cost-latency-curve.md` from the committed
results files, and a `--check` mode fails when the document and the results
disagree. Rejected: a hand-written table — the M7/M8 evidence documents
measured what a hand-copied number costs.

## Cost impact

| Rule | Effect |
|---|---|
| **R1, R14, R1b** (bundle; PUT rate tracks bytes ÷ segment size; the interval is the dial) | **Measured for the first time.** Criterion 3 is NFR-1 counted |
| **R2, R15 / NFR-3** | Every macro run asserts 0 LIST on the hot path. M8.66 writes the backfill's LIST+GET cost into cost.md rule 2b |
| **R3 / NFR-2** | Unchanged; moved into CI (M8.75) |
| **R5, R10, R13 / NFR-4** | Measured: GETs per segment per AZ with the prefetcher wired (M8.84/M8.85 under parent M8.56), flat in shards; budget remains at most one prefetch GET per non-writing AZ plus cold misses. The durable hint itself adds no object-store requests |
| **NFR-5 cross-AZ byte budget** | M8.83 adds the durable-signal frame to byte accounting; at most AZs−1 hints per segment. At 8 MiB, the maximum two frames contribute 0.037% of producer bytes, leaving the rest of the `<0.1%` budget for the other named transports; criterion 6 measures all of them together |
| **M8.24's catch-up path** | ⚠️ **NEW READS**, the only production change here that adds requests: a catch-up must cost GETs per SEGMENT in the backlog, shared per node (R5), never per shard. Budget: ≤ 1 GET per backlog segment per consumer node and zero LIST, asserted by M8.24f and M8.24h |
| **M9.13's gap re-read** | Replaces `TENS_OF_GETS` (modelled) with a counted number |
| **M9.21's tiers 2 and 3** | ⚠️ **NEW READS, AND THEY ARE THE PLUGIN's OWN**: tier 2 polls the commit chain, tier 3 is an explicit recovery LIST of the data prefix. Both run only when NO ingester is reachable, so they are a recovery path, which is where R2 permits a LIST and nowhere else. Budget: tier 2 ≤ 1 GET per chain delta per NODE (R5 — never per shard), tier 3 ≤ 1 LIST per recovery action per node, both counted rather than modelled, and the rate returns to zero the moment an ingester answers |
| **NFR-1's low-rate bound** (M9.18) | No request change: a second asserted bound over the same counts, ≤ 2 write requests per pod per INTERVAL CEILING |
| **Everything else** | None. Harness, meter, gates, ADRs and documents add no production requests |

## Acceptance criteria

Every criterion runs on the local rig unless it says otherwise. "RustFS" means
the digest-pinned container in `docker-compose.test.yml`.

1. **`check-mutants.sh` exists and gates.** It drives jzap's
   `mutationTestDiff` (ADR-0045), fails below 80% killed on the staged diff, is
   wired in `.pre-commit-config.yaml`, and its own test shows it red on a
   seeded diff with an unkilled mutant and green once a test kills it. (M0.14)
2. **The cost meter computes what research 02 §7 names**, T0: requests per MiB
   written and read, idle request rate, and $/TiB from a price table. ⚠️ Pinned
   against the EXACT RATIONAL rather than a rounded one: Scenario A's 24
   requests per second against 100 MiB/s is 24/100 = **0.24 exactly**, asserted
   within 1e-9, and the $/TiB case asserts `requests × pricePerRequest`
   exactly. Research 02's two-significant-figure prose is the SOURCE of the
   inputs, never the expected value. (M9.1)
3. **NFR-1, counted, in BOTH regimes, and bounded PER INTERVAL.** Three
   assembled pods with three AZ labels, RustFS, the mixed doc-size workload,
   ≥5 minutes per point.
   **Above the size-triggered rate:** **requests per MiB written < 0.30** at
   every such rate the rig sustains (at least 3 rates, the highest being the
   most the rig holds, recorded).
   **Below it:** **≤ 2 write requests per pod per INTERVAL CEILING** — one data
   PUT and one commit PUT per interval, which is what ADR-0017 buys. ⚠️ **PER
   INTERVAL, NEVER PER SECOND**: a per-second bound describes the configured
   dial rather than the code — at a 250 ms ceiling correct behaviour is 8
   requests per pod per second, and at a 5 s ceiling an implementation flushing
   five times too often passes. Asserted at ceilings of 250 ms and 5 s, so the
   bound is shown to be independent of the dial.
   **LIST is zero everywhere except the three recovery paths named here**, so
   the assertion enumerates them rather than saying "hot path":

   | LIST that is permitted | Bound | Why |
   |---|---|---|
   | the takeover chain backfill | once per TAKEOVER, 1 per 1,000 chain keys | cost.md rule 2b (M8.66) |
   | the retention sweep's hour prefixes | paced to `RetentionLoop.MAX_SWEEP_HOURS_PER_TICK` per tick | cost.md rule 8, and R15's ~1/s ceiling |
   | the inbox drain | once per heal event and once per term | ADR-0058 |

   Every flush, every commit, every read and every idle tick is asserted at
   **zero LIST**, counted by `CountingBinStore` and attributed by purpose, so a
   new LIST anywhere else is red rather than absorbed into an allowance.
   ⚠️ Counts, so all of it holds on S3. (M9.8, then M9.18)
4. **NFR-2 stays zero, in CI, and the job's existence is SCRIPTED.**
   `IdlePodCostSoakTest` runs in a CI job — which does not exist today:
   `.github/workflows/ci.yml` runs L0 gates only and says the L1 job lands with
   **M0.27**, pulled into this milestone ahead of M8.75 and M9.16. A script
   asserts that the workflow names `soakTest`, so deleting the job is RED
   rather than merely reviewable (gate-design rung 3, not rung 7).
   (M0.27, M8.75)
5. **NFR-4, counted — STORE requests FLAT in consumer-node count.** With the
   prefetcher wired, over `inline` and `proxy` (the modes cost.md rules 10–11
   describe):
   **(a)** object-store requests per segment are **≤ (AZs − 1) prefetch GETs
   plus ≤ 1 cold GET per serving ingester node**, and change by **≤ ±10% from 1
   to 9 consumer nodes** at equal bytes — near-constant, not linear. ⚠️ A build
   whose ingester cache never hits costs one store GET per consumer node and
   must be RED here; that is what this criterion exists to catch.
   **(b)** INGESTER-SERVED requests (consumer to ingester, no store request)
   may grow linearly with consumer nodes, and are counted separately.
   **(c)** store requests at 1,600 shards are **< 1.2×** those at 16 shards for
   the same bytes, through real `NodeSubscriptions` subscribers.
   ⚠️ `direct` mode is excluded and measured by criterion 12. Counts, so they
   hold on S3. (M8.56, M9.9)
6. **NFR-5, counted, on every transport that can cross an AZ.** Bytes sent to
   a peer carrying a different AZ label are **< 0.1%** of producer bytes
   accepted. ⚠️ **THE INSTRUMENTED TRANSPORTS ARE NAMED**, not left as "the
   peer sockets": the proxy read path a consumer takes to an ingester in
   another AZ (cost.md rules 10–11, the largest term), `inline` push payloads,
   the commit forward (`SequencerTransport`), the inbox drain's reads, and the
   key-only durable-segment prefetch signal (M8.83).
   ⚠️ **AND AT LEAST ONE RUN FETCHES ACROSS AN AZ BOUNDARY**: a steady-state
   write-only run has near-zero cross-AZ bytes by construction and an
   uninstrumented build would pass it, so the criterion requires a run with
   consumers deliberately served from another AZ, reported beside the
   steady-state run. Holds on a real cloud to the extent the label matches the
   pod's real zone, which is a deployment fact.
   ⚠️ **THE LARGEST TERM IS NOT COUNTABLE YET, AND THE NUMBER MUST SAY SO.**
   M9.2 instrumented the sockets that exist: the consumer's poll answer (its
   `inline` payloads, its `direct` grants and the answer's own framing), the
   commit forward and the inbox drain. M8.83 adds the prefetch-signal byte
   count to the node-wide report; its frame is bounded to at most 1,550 bytes and
   at most one is sent per non-writing AZ per segment. **A segment served in `proxy` mode does
   not travel over any wired route today** — a `proxy` push carries no inline
   bytes and no ingester endpoint serves the segment to a consumer — so the
   `PROXY_READ` column counts the EVENT FRAME that names the segment, not the
   segment. The instrumentation is in place on the socket that will carry
   those bytes, and the moment that route lands they are counted by the same
   call. ⚠️ **Until then a measured "cross-AZ bytes < 0.1%" is a statement
   about a build in which the largest term cannot occur**, not about the
   design, and M9.10's report and `VERIFIED.md` say that in the same sentence
   as the ratio. A number that omits the term cost.md rules 10–11 call the
   largest one, quoted without that clause, is the failure mode this
   paragraph exists to prevent. (M9.2, M9.10)
7. **NFR-7 on the rig.** p99 from producer 202 to consumer delivery is **< 3×
   the interval ceiling** at ceilings of 250 ms, 1 s and 5 s, each over
   ≥10,000 records at a steady rate. ⚠️ **RustFS on loopback, NOT S3**: S3's PUT
   p99 is absent, so this number is a lower bound on production latency and is
   labelled that way in the curve. (M9.11)
8. **NFR-7 to searchable, one point.** At a 1 s ceiling in `clusterTest`, p99
   from 202 to a document being returned by a search is **< 3 s** over ≥1,000
   documents. (M9.11)
9. **NFR-7 on the ADR-0058 path, with the DRAIN ITSELF bounded.** During an AZ
   partition, every record acked on a durable intent is visible **after at most
   one drain**, and the drain is bounded: for an inbox of M intents it
   completes within **max(2, ceil(M / 100)) interval ceilings** of the heal
   event, asserted at M = 10 and M = 1,000. ⚠️ "Reported as a number" is green
   for any number and is not a criterion. Outside a partition the criterion-7
   bound holds, and the partition's duration is excluded from NFR-7 with the
   curve saying so. (M9.12)
10. **Allocation is a gate, over a MEASURED noise floor** — and this is the
    only criterion M9.5's profile serves. ⚠️ **THE PROFILE IS EVIDENCE, NOT A
    CRITERION**: performance.md rule 1 requires it before a micro-benchmark
    exists, so it selects the benchmarks named below and is cited by them; it
    is not separately checkable and no criterion claims it is. A committed
    profile naming three hot spots, with its rig block, is what lets a reader
    ask why these benchmarks and not others. A named set of ≤5
    JMH benchmarks (the M9.5 hot spots plus B3), run with 1 fork inside L1's
    budget. ⚠️ **THE THRESHOLD IS DERIVED, NOT ASSUMED**: the task first
    measures run-to-run variance of `gc.alloc.rate.norm` on this rig over ≥10
    repetitions per benchmark and records it, and the gate's threshold is the
    larger of 5% and 3× that measured variance **capped at 25%**, with the
    number and its derivation in the baseline file. ⚠️ **IF 3× THE MEASURED
    VARIANCE EXCEEDS 25%**, the benchmark is not gateable on this rig: it is
    dropped from the named set (the set may then be smaller than five), the
    measured variance is recorded with it, and the curve says the rig is too
    noisy for it — never a 60% gate, which is a gate in name only. A gate tighter than the rig's own noise is
    a gate people re-run until it is green. The full suite (`@Fork(2)`,
    ≥5 warmups, `-prof gc`) runs by hand. (M9.6)
11. **M2 settled.** B3 plus a macro run compare zstd 1/3/6, lz4 and none at
    64 KiB / 256 KiB / 1 MiB blocks on the mixed workload; the table (ratio,
    MiB/s per core, B/op) is in the curve document and the default is either
    confirmed in writing or changed by ADR with a `wire-format-change` commit.
    ⚠️ CPU and ratio are rig-independent enough to decide on; the rig is
    recorded with the result. (M9.7)
12. **M3, the half the rig can measure.** For fan-outs 1–16, GETs per segment,
    ingester bytes served and ingester CPU under `proxy` and `direct` are
    counted on RustFS, and the default threshold is chosen by cost and written
    into the curve document. ⚠️ **THE TTFB HALF IS NOT-RUN**: the question as
    `50-open-questions.md` poses it is "given real S3 TTFB variance", and RustFS
    has none. The document says the threshold is cost-chosen and names the
    cloud run that would move it. (M9.14)
13. **M8.24's row, as written.** An OpenSearch node killed with a backlog of
    ≥100 segments resumes from `batch_start` with every acked record indexed,
    catch-up GETs **≤ 1 per backlog segment per node**, and **EVERY record
    written after the kill visible within 3× the interval ceiling OF ITS OWN
    202**, measured per record over ≥1,000 post-kill records while the catch-up
    runs. The producer keeps live work continuously pending for the replay
    window, and the scheduler trace shows a catch-up turn after each bounded
    live quantum; this is the falsifier for the no-starvation rule. ⚠️ A p99
    taken over one record is not a bound; the deadline is per-record and the
    assertion fails on the first breach. T4. (M8.24)
14. **A gap is re-read over a REACHABLE ingester, and its cost is counted.**
    A forced delivery gap is filled through the catch-up path with no record
    skipped and none indexed past, and the requests are counted: the consumer
    issues **zero store requests** (it holds no store) and the ingester issues
    **≤ 1 GET per missing segment per node**. ⚠️ **NOTHING HERE PRICES
    `TENS_OF_GETS`**, which is TIER 3's constant — the plugin reading the store
    itself — and this path issues no store request from the consumer at all, so
    it could only ever "confirm" the constant by measuring something else.
    That constant is criterion 18's. (M9.13)
15. **Every other inherited row is closed by the test or gate § *Test plan*
    NAMES FOR IT** — M8.60–M8.83, each with a row there giving its tier, the
    test that must fail first by name, and the mutation it catches.
    ⚠️ **ONE EXCLUSION, BY NAME: M8.64**, a prose sweep, whose claims are
    sentences no predicate over the tree can judge (M7.35 measured this); it is
    closed by `check-links.sh`, `check-javadoc-cites.sh` and the review reading
    the sentences, and `VERIFIED.md` says so rather than naming a test that
    does not exist. Every other row names one. And
    `check-wired.sh` stays green with M8.56 done; criterion 5 covers M8.56's
    production prefetch behavior. `VERIFIED.md` carries one
    line per row, citing that test. ⚠️ An earlier draft said "closed by its own
    named test or gate" and named nothing, which was green for any work at
    all.
16. **The curve is published.** `cost-latency-curve.md` is generated from the
    committed results files; its `--check` mode is wired as a pre-commit gate
    and red when a number in the document disagrees with a results file. It
    plots requests per MiB (measured), $/TiB (modelled from the measured count
    and the AWS price table, labelled so) and p99 (measured on RustFS, labelled
    so) against the interval ceiling, and records the rig. (M9.15)
17. **The cost assertions gate CI, in a NAMED execution layer.** ⚠️ **THEY DO
    NOT FIT L1–L3.** Criterion 3 alone is ≥3 rates × 2 ceilings × ≥5 minutes,
    which is over 25 minutes against build.md's caps of 5 min (L1), 10 min (L2)
    and 15 min (L2+L3 together), and criterion 4's `soakTest` is L2S. Naming no
    layer makes the cheapest route to a green milestone relaxing
    `check-suite-time.sh` or cutting "≥5 minutes per point" — the move
    non-negotiable 2 forbids. So M9.16 **CREATES a measurement job**:

    | | Where | Budget | Contents |
    |---|---|---|---|
    | **Per push** | the existing L1 job | inside 5 min | the fast subset: criteria 3, 5 and 6 at ONE rate, ONE ceiling, ONE node count, 60 s per point. ⚠️ **THAT RATE IS ABOVE THE SIZE-TRIGGERED RATE**, so the point sits in the regime where `< 0.30` requests per MiB applies; a fast point below it would red correct code against a bound criterion 3 says does not hold there — **the same thresholds**, fewer points. A threshold is never loosened to fit; only the sampling shrinks |
    | **Nightly / on demand** | a NEW workflow, `measurement`, explicitly outside L1–L3 and outside `check-suite-time.sh`'s budgets | unbounded, as L4–L6 are | every point of criteria 3, 5 and 6; criterion 4's `soakTest` (L2S, nightly per build.md); and the non-gating latency runs behind criteria 7, 9, 11 and 13, which trend in the curve |

    build.md's execution-layer table gains the row in the same commit, so the
    standard and the workflow cannot disagree. A per-push run that cannot
    complete the fast subset inside 5 min is a finding, not a reason to trim a
    bound. (M0.27, M9.16, M8.75)
18. **Tiers 2 and 3 EXECUTE, and their cost is counted.** With no ingester
    reachable, a consumer holding a real gap enters tier 2 and then tier 3, in
    order, read off the tiers it reports entering; the missing window is
    re-read through M8.24's catch-up path with no record skipped and none
    indexed past; and the GETs and LISTs each tier costs are **counted** by
    `CountingBinStore`, replacing `FallbackLadder`'s modelled `TENS_OF_GETS`
    with the measured number or correcting it to it. The request rate returns
    to zero once an ingester answers. T3 against RustFS — a count, so it holds
    on S3.
    ⚠️ **OR THE OTHER BRANCH, WHICH IS CHECKABLE TOO.** No approach is named
    today: a SigV4 signer in the plugin (a credential in the OpenSearch JVM)
    and long-lived pre-issued grants (amending ADR-0041) were both rejected on
    2026-09-20, and architecture rule 2 keeps a cloud SDK out of the plugin.
    **If M9.20 finds no acceptable approach, it records a RE-DEFERRAL ADR**
    naming every candidate considered and why each was refused, superseding
    ADR-0057 only as to where the question lives, and stating the consequence
    that stands meanwhile (an ingester outage longer than the backoff costs
    latency, not loss). Criterion 18 is then met BY THAT ADR, `VERIFIED.md`
    records tiers 2 and 3 as NOT-BUILT with the ADR cited, M9.21 is not
    started, and its backlog row is closed by the ADR — never by an execution
    that did not happen. (M9.20, then M9.21 or the re-deferral)
19. **M8.58's lease change is complete in one commit.** The pod UID is in the
    lease and `EndpointSliceView` matches `targetRef.uid`; the format document,
    every reader and writer, the fakes, the golden files and the ADR land
    together; a lease in the previous format is handled as that document
    states; and a replacement pod on a dead holder's IP is **not** challenged
    early — red before the change. (M8.58)

### What a cloud run adds

Nothing here needs a cloud to be CORRECT; three things need one to be COMPLETE:
the S3 PUT/GET tail in NFR-7 (criterion 7 is a lower bound until then), the
TTFB half of M3 (criterion 12), and a dollar figure read off a bill rather than
multiplied from a price table (criterion 16). Each is a named NOT-RUN in
`VERIFIED.md`, not a pass.

## Test plan

| Task | Tier | Fails first | Catches |
|---|---|---|---|
| M9.0 | doc | — (this spec; `check-links.sh` and the spec review carry it, sdd.md rule 7) | a spec whose criteria are opinions |
| M0.14 | script test | `check-mutants` green on a diff with a surviving mutant | a gate that never fails |
| M9.1 | T0 | `CostMeterTest.scenarioAIsPointTwoFour` | per-MiB divided by MB, not MiB; LIST priced as GET |
| M9.2 | T0 + T3 | `PeerBytesByAzTest` — same-AZ bytes counted as cross-AZ | a counter keyed by pod, not by label |
| M9.3 | T0 | `LoadGeneratorTest` — same seed, same byte stream | a non-reproducible workload |
| M9.4 | T3 (RustFS) | `MacroHarnessIT` — a run whose results file lacks the counts | a harness reporting latency without cost |
| M9.5 | L6 (manual) + doc | — **no test can fail first for a profile**; it is a measurement, and its falsifiability is that the three hot spots it names are reproducible on a second run, recorded. Evidence for criterion 10 only | a benchmark set chosen on a hunch (performance.md rule 1) |
| M9.6 | JMH + script | the allocation gate green with a baseline halved | a gate reading the wrong column |
| M9.7 | JMH (B3) + T3 | `CodecComparisonBenchmark` reporting ratio without B/op | a codec chosen on speed alone |
| M9.8 | T3 | `WriteRequestRateIT` red with the size trigger disabled | per-flush-per-index PUTs (R1) |
| M9.9 | T3 | `ReadRequestRateIT` red with the ingester's segment cache disabled | R5, R10, R11 — store GETs growing per consumer node |
| M9.14 | T3 | `DirectThresholdMeasurementIT` red with `proxy` and `direct` counted into one total | a threshold chosen from a number that mixes the modes |
| M9.10 | T3 | `CrossAzBytesIT` red with inline forced across AZs | R12 |
| M9.11 | T3 + T4 | `VisibilityLatencyIT` red with a sleep in the flush path | a latency measured from the wrong stamp |
| M9.12 | T3 (chaos) | `PartitionVisibilityIT` red with the heal drain disabled | ADR-0058's drain not running |
| M8.24c | T0 | `CatchUpControlFrameTest` and golden tests red with an unknown version accepted | a mixed-version peer partially decodes a catch-up request |
| M8.24d | T1 | `CatchUpServiceTest` red with a 501 seam or malformed request delegated | the HTTP adapter does not carry the versioned control frames or invokes replay on invalid input |
| M8.24e | T1 | `DurableCatchUpResponderTest` red with `batch_start` passed as an inclusive offset or shared segment reads repeated | replay resumes at the wrong record or store GETs scale with streams rather than segments |
| M9.25 | T2 | `CatchUpAssemblyIT` red when the assembled server does not register POST on the existing subscription URL or repeats a GET for distinct streams sharing one segment | a component route that is never reachable in production assembly, recovery cost growing per stream, or a LIST on replay |
| M9.26 | T2 | `CatchUpAssemblyIT` red when a positive `batch_start` is reset to zero or treated inclusively | a resumed stream duplicates an already-indexed record or skips the record at its persisted boundary |
| M9.27 | — (parent) | M9.31–M9.34 pass the cursor, responder, and HTTP streaming criteria below as separate reviewed commits | a parent closes on aggregate evidence while one layer still buffers or reorders replay |
| M9.31 | — (parent) | M9.35 passes cursor behavior and M9.34 proves one-segment-at-a-time memory within a delta | delta-level laziness mistaken for bounded memory when one delta batches many segments |
| M9.32 | T1 | `DurableCatchUpResponderTest` fails if the cursor advances or the next segment GET occurs before every requested run from the active shared segment is emitted; it also fails if shared runs repeat GETs, any hot-path LIST occurs, or a streaming frame exceeds its individual byte bound | duplicate reads, segment payload release before all its runs are streamed, unbounded frame allocation, or discovery cost growing with streams |
| M9.33 | T1 + T2 | `CatchUpServiceTest` fails if the first frame is not delivered while the responder is held open, or a response above 8 MiB is rejected/buffered; `CatchUpAssemblyIT` proves the assembled URL streams the large multi-segment replay | response-sized heap growth or an unreachable component-only streaming path |
| M9.35 | T1 | `CommittedDeltaSourceTest` rejects eager legacy multi-stream fallback; `SnapshotCommittedDeltaSourceDefaultOrderTest` fails if the production constructor reverses deltas, while `SnapshotCommittedDeltaSourceTest` fails if `openSegments` drains future deltas, reverses segment order within one delta, includes an unrequested stream, or emits a run whose last offset equals its exclusive offset | wrong-stream replay, duplicate records, or a later committed segment emitted first |
| M9.34 | T1 | `SnapshotCommittedDeltaSourceSegmentLazinessTest` fails if the cursor traverses/materializes all matching runs from a multi-segment delta before returning its first replay segment | per-delta backlog growth in cursor working memory despite delta-level laziness |
| M9.28 | T1 | `HttpCatchUpExchangeTest` red when a wrong request ID/key is dispatched, an unmatched end completes the request, or 404/501 is treated as a live-poll failure | partial/cross-request replay contaminates a stream, or an old peer disrupts live delivery |
| M9.29 | T0/T1 | `CatchUpDeliveryLaneTest` red when catch-up exceeds its bound, a matching live queue starves catch-up past the configured quantum, or an end marker acknowledges before all replay records are consumed | heap growth, tail starvation, or premature completion |
| M9.39 | T0 | `CatchUpOfferContractTest` rejects timed/blocking queue methods, fails if availability is published before a gated insertion completes, and uses a release-gated shared semaphore plus a competing reader to catch shared-before-private permit publication | capacity refusal waits for consumer progress, or a consumer consumes the shared permit before its lane permit and strands a visible delivery |
| M9.40 | T1 | `CatchUpOfferTest` refuses a full bounded lane without corrupting multi-record pending accounting or catch-up-only availability; after capacity is freed, the same exchange accepts the retry and completes | fixed-count rollback leaves phantom pending records, or refusal leaks private permits |
| M9.30 | T1 | `NodeCatchUpDeliveryTest` red when catch-up starts before all assigned local plugin shards are started/readable, an uncommitted shard does not contribute `batch_start=0`, a late-starting shard is omitted, an unreadable position is not retried on the existing progress interval, requests are issued per shard instead of once for a complete snapshot, unmatched events reach clients, unsupported replay drops the live subscription, or the client issues a store request | a node restart loses backlog, a barrier waits forever, request amplification, or mixed-version rollout leaves a stream unconsumed |
| M9.37 | T1 | `NodeCatchUpDeliveryTest`, `CatchUpReleaseBackpressureTest`, `CatchUpSchedulingWiringTest`, `NodeCatchUpCoordinatorContractTest`, `ShardPositionsCatchUpLimitTest`, and `CatchUpRetryContinuityTest` fail when the started/readable shard barrier, missing-`batch_start` zero, progress-interval retry, one bounded V1/V2 request through 120,000 streams, explicit refusal above the bound, old-peer 400/413 live continuity, held-client routing, stable-request retry, non-blocking shard release, held-client readiness, post-completion request transition, or zero-store-call requirements are violated | a node restart loses backlog, a barrier waits forever, request amplification, shard release blocks, a consumer misses replay, or mixed-version rollout interrupts live delivery |
| M8.24g | — (parent) | M9.27–M9.30 green; the original catch-up request/response, queue bounds and fallback criteria all hold | a consumer integration parent falsely closed by transport-only evidence |
| M8.24h | T4 | `KillNodeMidBacklogIT` red with the tail queued behind catch-up or one post-kill record exceeding 3× its own interval ceiling | starvation, an unbounded live quantum, or a backlog that resumes from the wrong `batch_start` |
| M8.24 | — (parent) | M8.24f (M9.25 and M9.26), M8.24g and M8.24h all green; the T4 test covers ≥100 backlog segments and ≥1,000 post-kill records | an end-to-end criterion falsely closed by component-only evidence |
| M9.13 | T3 | `GapRereadIT` red with the re-read skipped | a gap indexed past |
| M9.21 | T3 (RustFS) | `LadderStoreTiersIT` red with tier 3 answered by a reachable ingester | a tier that never runs; an uncounted recovery LIST |
| M8.58 | T0 + T3 | `LeaseHolderUidTest` — a replacement pod on a dead holder's IP challenged early | matching a holder by address |
| M9.15 | script test | `--check` green on a doctored table | a generated document nobody regenerates |
| M9.16 | script test | the CI gate green with the cost job removed from the workflow | a CI job that silently stops running |
| M9.17 | script + milestone review | `check-milestone-verified.sh` green with a criterion's evidence line deleted | ⚠️ **THE SCRIPT FORCES ENUMERATION AND NOTHING MORE** — its own header says it cannot verify an evidence line is TRUE, its check is a regex, and M7.31 MEASURED six overstated lines all passing it. What catches an overstated line is the milestone review reading `VERIFIED.md` against the tree, as M8.19 did; this row's second half is that review, and its finding list is the record |
| M9.18 | T3 + script | `LowRateWriteBudgetIT` red at a 250 ms ceiling with a per-second bound | a rig-dependent bound; two NFR-1 statements in the tree |
| M9.19 | doc | — (an ADR; `check-adr-refs.sh` and `check-links.sh` carry it) | a divisor nobody wrote down |
| M9.20 | doc | — (an ADR; either branch of criterion 18) | a mechanism assumed buildable |
| M9.22 | doc | — **no test can fail first for a roadmap edit**; it changes one prose row and no code. `check-links.sh` and the M9.0 spec review carry it | a completion condition the milestone cannot meet |
| M9.23 | doc | — **no test can fail first for a task decomposition edit**; this row's criterion is the reviewed SPEC decomposition and its cost/test accounting | an integration task hiding two unmodelled prerequisites |
| M9.24 | doc | — **no test can fail first for a decomposition edit**; the independent spec review checks requirement traceability, acceptance boundaries, test tiers and request budget | a parent marked complete without all three integration layers or a child with unbounded store requests |
| M0.27 | script test | the L1 job green having executed zero tests | `check-harness-tests.sh`'s antidote not inherited |
| M8.82 | T1 | `EndpointSliceViewTest#readyPeerEndpointsExposeTheirAZAndExcludeDrainingNodes` red with ready endpoint extraction returning empty | guessing an AZ or including a draining endpoint in the peer ring |
| M8.83 | T1 + T2 | `DurableSegmentSignalFrameTest` golden/unknown-version cases and `DurableSegmentSignalServiceTest` — valid known-writer/source/key triple accepted; a different source claiming that writer and a key naming another pod are refused before any GET; `DurableSegmentSignalSenderTest` constrains one owner per remote AZ and byte attribution; maximum fan-out/frame byte count is ≤0.037% for an 8 MiB segment. The format definition, reader, writer, fake, golden and its ADR travel together under `wire-format-change`. Tests exercise the sender seam and stand-alone HTTP service; no assembly registration/invocation is claimed | unversioned/ambiguous bytes, spoofed identity, or arbitrary external keys triggering cache GETs |
| M8.84 | T1 + T3 (RustFS) | `DurableSegmentListenerFailureTest#aThrowingDurableHintDoesNotSuppressTheAlreadyCommittedPush`; `SegmentPrefetchAssemblyIT#oneRemoteAzOwnerWarmsTheSharedCacheBeforeTheFirstRead` | a listener exception aborting a committed push; missing production registration/callback, missing real HTTP send, or an extra GET on first consumer read |
| M8.85 | T1/T2 with a counting store | multi-candidate-per-AZ prefetch integration test: deliver one signal to at least two candidates in the same AZ and assert exactly one GET, then assert the selected owner's cached read adds zero | a local-AZ membership view exposing only self, or all candidates fetching rather than only the deterministic ring owner |
| M8.60 | T0 + script | `check-metric-cardinality.sh` on a per-index label; a metric test red with the counter unexported | a counter only tests can read |
| M8.61 | T3 (chaos) | ⚠️ **TEN NAMED ASSERTIONS, EACH RED AGAINST ITS OWN DEFECT** — the row cannot be closed by tightening three of them: (1) `AzPartitionIT` intents per flush (unbounded → red with an extra intent per record); (2–4) `OrphanAfterKillIT`'s contiguity at the store, "a NEW segment", and the orphan's content (red with the orphan's bytes replaced, a reused key, a hole in the sequence); (5) `ChallengeResumeIT`'s takeover count read mid-run (red with a second takeover after the read); (6) `IdlePodCostSoakTest`'s lease bounds (red with a doubled renew rate); (7) `RollingRestartIT`'s herd budget per restart (red with every reconnect in one 100 ms window); (8–10) the three the M8 milestone review lists (5bc9672, 08cb808, 60796af), each named in the commit body with its own mutation | an assertion that passes on the very defect its row names |
| M8.62 | `check-mutants.sh` | the gate red on the recorded surviving mutants | mutants recorded and never killed |
| M8.63 | T3 | `check-suite-time.sh` and the Awaitility-converted tests red with the condition never satisfied | a sleep that hides a race |
| M8.64 | doc | — **no test can fail first for a prose sweep**: the claims are sentences, and no predicate over the tree distinguishes a true one from a false one (M7.35 measured exactly this). `check-links.sh` and `check-javadoc-cites.sh` carry the citations; the review reads the sentences | prose asserting a mechanism that no longer exists |
| M8.65 | T0 + T3 | `InboxDrainSerialisationTest` — two entry points on two locks | M8.48/49's two-commit-paths bug class |
| M8.66 | T3 | `BackfillStopsOnDepositionTest` — overlapping walks under a flapping leader | a sweep starved for a term |
| M8.67 | T0 + bench | `NodeSegmentSourcePerKeyLockTest` — one slow GET blocking another key | a node-wide lock on a real GET |
| M8.68 | T0 | `MalformedClassificationTest` — a throwing listener classified `MALFORMED` | a diagnosis that misnames the fault |
| M8.69 | T0 | `ServerPropertiesTest` — a blank duration taking the default | a silent default |
| M8.70 | T3 | `ShutdownDrainIT` — 503s served before readiness flips | a rollout that drops requests |
| M8.71 | T0 + T3 | **four named cases, each red first**: `NodeProcessKillResumesPausedTest` (a paused node killed and never reaped), `RetryFloorCallerTest` (`DEFAULT_RETRY_FLOOR` unread by production), `RootClosesOnlyOwnedTransportTest` (an injected transport closed by the root), `ShardPositionsCloseTest` (removal by shard id alone, evicting another index's entry) | four unpinned behaviours, each invisible to coverage |
| M8.72 | T4 (`clusterTest`) | `PluginLoggingIT` — the lines absent from the log4j output | logs that reach nobody |
| M8.76 | JVM and Python gate tests | explicit, same-package, and unique wildcard imports resolve to their declared type; ambiguous wildcard imports and malformed predicates are refused; declarations in another method do not shadow a valid receiver | a correct call is falsely treated as unwired, or an ambiguous/malformed call is accepted |
| M8.77 | JVM and Python gate tests | `new pkg.Widget.Nested(...)` does not satisfy `new Widget` | a nested-type construction falsely wires the outer type |
| M8.79 | Python unit and Bash integration tests | Windows-style paths and Python paths with spaces work; CRLF selector reaches scoped Gradle without `\\r`; failure red hashes source bytes, but a passing test produces no red | the selected test is not run, a passing test is recorded red, or its red is not bound to the source |
| M8.80 | Python unit and Bash integration tests | prior JUnit reports are removed; cleanup failure exits before Gradle and cannot mint red evidence | stale JUnit failure is recorded although the selected test never ran |
| M8.81 | Python unit and Bash integration tests | CRLF and LF source checkouts produce the same canonical Git-content digest, and red evidence matches the staged source bytes checked by `checkTdd` | Windows red records are stale against the staged LF blob |
| M8.74 | T0 + T3 | `PeerReplyTooLargeTest` — a 400 treated as unknown | blaming the producer for the peer |
| M8.75 | script test | the workflow check green with `soakTest` removed | NFR-2 regressing unseen |

⚠️ **EVERY TASK IN § *Tasks* HAS A ROW HERE** — spec/SKILL.md requires a tier,
the test that must fail first BY NAME, and the mutation it catches, per task.
⚠️ **SEVEN ROWS NAME NO TEST AND SAY WHY IN THE ROW** — M9.0, M9.5, M9.19,
M9.20, M9.22, M9.23 and M8.64 are a spec, a profile, two ADRs, two plan edits and a
prose sweep: none is a behaviour a test can pin, and writing a plausible test
name for them would be the weaker failure. Criterion 15 reads this table and
excludes M8.64 by name.

Coverage: the existing gate on changed production code. Mutation: M0.14's 80%
on every M9 commit once it lands. Suites extended: the cost assertions (new),
the chaos matrix (M8.24, M9.12), store conformance (none). Harness and load
generator live in test source sets and are excluded from the coverage gate with
that reason stated.

## Risks

- **Peer authentication depends on direct pod-to-pod source addresses.** A
  service-mesh sidecar, NAT or proxy can make `remotePeer().host()` differ from
  the EndpointSlice pod address; in that deployment every hint is safely
  refused and the object-store fallback costs extra GETs. Do not authenticate
  from a forwarded header. The T2 test uses a direct socket; deployment must
  preserve that source identity for prefetch to be effective.
- **The rig cannot reach the size-triggered regime.** WSL2 plus RustFS may top
  out below the rate at which three pods fill segments. Then criterion 3 is
  measured with fewer pods or a smaller `maxSegmentBytes`, and the curve says
  which — never with a relaxed budget (cost.md rule 18).
- **RustFS latency flatters NFR-7.** Stated in criterion 7; revealed only by a
  cloud run.
- **The Docker memory ceiling** (`check-test-budget.sh`) caps fleet size. A run
  that needs more pods than fit is a finding, not a reason to raise the cap.
- **Profiling finds the hot spot in the harness.** Then the harness is fixed
  first and the profile re-run.
- **M9.20 may find no acceptable way for the plugin to read the store.** Then
  criterion 18 is met by a re-deferral ADR and M9.21 does not run; the
  milestone still completes, and `VERIFIED.md` says NOT-BUILT.
- **The catch-up path is a protocol change** and may take several review
  rounds; it is ordered after the measurement machinery so it does not block it.

## Decisions

⚠️ **The four decisions the user settled on 2026-09-20 are recorded or have
an owning ADR task. The rest are open, and their ADR comes before the task
starts.**

**Decided, ADR owed:**

1. **NFR-7's divisor is the interval CEILING**, endpoints producer 202 to
   consumer delivery. An ADR because it resolves an ambiguity in a
   requirement's text rather than choosing an implementation. → [ADR-0063](../../decisions/0063-nfr-7-uses-the-adaptive-interval-ceiling.md)
2. **NFR-1 is amended**: < 0.30 requests per MiB above the size-triggered
   rate, and **≤ 2 write requests per pod per INTERVAL CEILING** below it.
   ⚠️ **THE PER-SECOND FORM IS REJECTED AND THE ADR MUST SAY WHY**: it
   describes the configured dial rather than the code, so at a 250 ms ceiling
   correct behaviour is 8 requests per pod per second and fails, while at a 5 s
   ceiling an implementation flushing five times too often passes. A bound that
   changes meaning when an operator turns a dial is unfalsifiable. The ADR is
   written from M9.8's measured counts, and moves `requirements.md`, cost.md
   § Enforcement and performance.md's gate table in the same commit. → M9.18
3. **Ladder tiers 2 and 3 are built in M9**, on a third approach the ADR
   chooses, superseding ADR-0057 in part. → M9.20, then M9.21
4. **M8.58 puts the pod UID in the lease** and matches `targetRef.uid` — a
   `wire-format-change`. → M8.58

**Open, ADR before the task:**

5. **Where the load generator and macro harness live** — a new `bench` module
   or a source set in `server` (code-structure.md; `check-module.sh`'s surface).
6. **The catch-up read path** (M8.24, after M8.24a) — resuming a subscription
   from an offset changes the subscription protocol: `wire-format-change`.
   The decision is [ADR-0065](../../decisions/0065-node-scoped-catch-up-with-live-tail-priority.md).
7. **A codec or block-size default change**, if M2's measurement moves it.
8. **Measurement M3's threshold default** — chosen on COST alone, while the
   variable `50-open-questions.md` names as deciding it (real S3 TTFB variance)
   is NOT-RUN on this rig. The ADR states the cost-only basis, the fan-out at
   which the two terms cross under the price table, and what a cloud run would
   change — so a later measurement moves a recorded decision rather than
   discovering an unexplained constant. → M9.14

## Tasks

⚠️ **Dependency order**: gates and meters, then the harness, then the
measurements, then the inherited rows that change what is measured, then the
document. Inherited rows keep their M8 IDs; `check-wired.sh` needs M8.56 open
until it lands. M8.82 and M8.83 are prerequisites added after the missing
EndpointSlice peer view and durable-signal boundary were confirmed; M8.56 is
the final assembly task and depends on both.

| ID | Task | Serves |
|---|---|---|
| M9.0 | This spec and the decomposition | — (planning) |
| M9.22 | **QUALIFY RUSTFS AND AMEND THE ROADMAP's M9 ROW**: RustFS benchmarks, with the S3 halves named NOT-RUN, so the completion condition is one this milestone can meet | — (planning) |
| M9.23 | Decompose M8.56 from the discovered peer-view and durable-signal prerequisites; update criterion, cost and test plan | FR-10, NFR-4, NFR-5 |
| M9.24 | Split the unfinished M8.24 server wiring, consumer replay path, and T4 kill-node proof into ordered, testable child rows without weakening its original acceptance condition | FR-9, NFR-13 |
| M0.14 | `check-mutants.sh`: jzap, 80% killed on the staged diff | — (gate) |
| M9.1 | The cost meter: requests per MiB, idle rate, $/TiB from a price table | NFR-1, NFR-4 (R9) |
| M9.2 | An AZ label on each pod and cross-AZ bytes counted at the peer sockets | NFR-5 |
| M9.3 | A seeded load generator with realistic doc sizes | NFR-1, NFR-7 |
| M9.4 | The macro harness: the assembled fleet on RustFS, counts and a latency histogram per run | NFR-1, NFR-7 |
| M9.5 | Profile the macro harness with JFR and record the top three hot spots | NFR-7 |
| M9.6 | The JMH harness and the allocation gate over a named set | NFR-6 |
| M8.66 | The backfill stops on deposition, and its costs are stated in cost.md 2b | NFR-3 |
| M8.65 | The inbox drain's serialisation made literal | FR-11 |
| M8.67 | `NodeSegmentSource`'s node-wide lock split per key, measured | NFR-4, FR-10 |
| M8.82 | Expose ready, AZ-labelled peers from the `EndpointSlice` view | FR-10, NFR-4, NFR-9 |
| M8.83 | Add the independently tested versioned durable-segment signal components, ADR and format/research documentation; authenticate the sender, pin golden bytes, and count cross-AZ bytes | FR-10, FR-12, NFR-4, NFR-5 |
| M8.56 | Wire the post-durability sender and authenticated receiver to `SegmentPrefetcher` in the assembled ingester and prove the path against RustFS | FR-10, NFR-4 |
| M9.7 | Measurement M2: block size and codec | NFR-1 |
| M9.8 | NFR-1 counted on the assembled fleet: the < 0.30 bound asserted above the size-triggered rate, and the low-rate counts REPORTED | NFR-1 |
| M9.18 | **NFR-1 AMENDED, FROM M9.8's COUNTS**: the two regimes, the per-interval low-rate bound asserted, `requirements.md`, cost.md § Enforcement, performance.md's gate table and the ADR in one commit | NFR-1 |
| M9.9 | NFR-4 counted: shards, nodes, AZs | NFR-4 |
| M9.10 | NFR-5 counted | NFR-5 |
| M9.19 | **NFR-7's DIVISOR**: the ADR fixing the flush window as the interval ceiling, and the endpoints | NFR-7 |
| M9.11 | NFR-7 on the rig at three ceilings, and one point to searchable | NFR-7 |
| M9.12 | NFR-7 on the ADR-0058 path: visibility bounded by the drain | NFR-7, FR-4 |
| M8.24b | Bounded replay primitives for the catch-up path | FR-9, NFR-13 |
| M8.24c | Versioned catch-up request, event and end frames | FR-9, NFR-13 |
| M8.24d | HTTP catch-up control seam with bounded request and response framing | FR-9, NFR-13 |
| M8.24e | Durable catch-up responder with one GET per shared segment | FR-9, NFR-13 |
| M8.24f | Parent closes after M9.25 and M9.26 establish production registration as POST on the existing subscription URL, node-scoped replay, shared-segment cost and positive-offset resume without a second URL or credential boundary | FR-9, NFR-13 |
| M9.25 | After M8.24e, register bounded POST replay on the existing `/sub/{indexUuid}/{partition}` subscription URL in production server assembly and prove two committed streams sharing one segment replay from `batch_start=0` with matching frames and payloads. Count ≤1 GET per shared backlog segment per consumer node and 0 LIST; malformed/unknown-version requests reach no replay/store read | FR-9, NFR-13 |
| M9.26 | After M9.25, exercise the production route with a positive persisted `batch_start` inside committed runs; each event begins at that offset with the exact remaining record count, retains the correct segment data, and closes with the matching end frame. Count ≤1 GET per shared segment and 0 LIST | FR-9, NFR-13 |
| M9.27 | Parent: close only after M9.31 proves the lazy ordered/filtering cursor and within-delta memory bound, M9.32 proves segment-at-a-time responder streaming and shared GETs, and M9.33 proves flushed HTTP streaming over 8 MiB on the existing URL. Together replay working memory is O(max segment size + requested streams), independent of backlog size | FR-9, NFR-13 |
| M9.31 | Parent: close only after M9.35 proves delta-lazy ordering/filtering/exclusive offsets and M9.34 proves that one delta is processed one replay segment at a time | FR-9, NFR-13 |
| M9.32 | The responder fetches one active segment, emits every matching run carried by it before asking for the next segment, then releases that payload; each shared segment costs one GET | FR-9, NFR-13 |
| M9.33 | The HTTP adapter writes and flushes each complete frame immediately; total response bytes can exceed 8 MiB without aggregate buffering, while each individual frame remains capped | FR-9, NFR-13 |
| M9.35 | The committed snapshot cursor streams in commit/delta segment order, including multiple segments within one delta, and only consumes the next delta needed to produce the next segment. It strictly applies exclusive offsets, filters to requested streams, and rejects multi-stream replay when a source implements only the legacy per-stream cursor | FR-9, NFR-13 |
| M9.34 | For a delta containing many committed segments, the cursor traverses and retains only the current segment's requested runs before returning it, rather than materializing replay output for the whole delta | FR-9, NFR-13 |
| M9.28 | The consumer sends one bounded node request on the existing subscription URL and incrementally validates/dispatches frames into the catch-up lane; only requested-stream events with the matching request ID are accepted, only a matching end completes the exchange, and an old peer leaves live polling untouched | FR-9, NFR-13 |
| M9.29 | Live and catch-up lanes are independently bounded; live records are served first up to a finite quantum, then pending catch-up receives a turn; only a matching end after all matching replay records are consumed completes the request | FR-9, NFR-13 |
| M9.30 | Parent: close only after M9.40 proves bounded client admission/accounting, M9.39 proves queue-capacity refusal and queue-before-notification ordering, and M9.37 integrates it with the all-local-shards routing snapshot, retries, held-client routing, unsupported-peer continuity and zero object-store calls | FR-9, NFR-13 |
| M9.36 | Parent: close only after M9.40 proves bounded offer accounting, M9.39 proves the try-offer concurrency contract, and M9.37 integrates it with routing, retries and shard lifecycle | FR-9, NFR-13 |
| M9.38 | Parent: close only after M9.40 proves bounded client admission/accounting and M9.39 proves the try-offer queue-capacity and notification-order contract | FR-9, NFR-13 |
| M9.39 | Prove the try-offer queue-capacity contract separately: use the queue's immediate offer operation, refuse full lanes without timed or blocking queue calls, and publish shared consumer availability only after the delivery is visible in the queue; a gated-queue test detects premature notification | FR-9, NFR-13 |
| M9.40 | Add bounded client catch-up offer admission without changing the blocking transport API. Refused events roll back pending-record accounting and leave no catch-up-only semaphore residue; after the consumer makes room, the same request can admit the next event | FR-9, NFR-13 |
| M9.37 | Wait until every active local plugin shard copy known from cluster routing is started/readable before node-scoped catch-up; an uncommitted shard contributes `batch_start=0`, late-starting shards join, and unreadable positions retry at the existing progress-report interval. Send one node-scoped request: V1 through 1,024 streams and bounded V2 through 120,000 streams (at most 4 MiB); explicitly refuse larger snapshots. Route only to held clients using non-blocking lane admission, retry the stable request when a lane is full, and preserve live subscriptions on unsupported peers including old-peer V2 refusal. Prove one request still costs at most one GET per shared backlog segment per consumer node, zero LIST, and no consumer/plugin store call | FR-9, NFR-13 |
| M8.24g | Parent closes only after M9.27–M9.30: send one node-scoped replay request from the streams' persisted `batch_start`s; route only matching response events, acknowledge only the matching end frame, and keep replay/live queues bounded with a live quantum that yields to catch-up each bound. On an old peer or `UNSUPPORTED_CATCH_UP_VERSION`, do not acknowledge/advance the live range; preserve the live subscription and use its existing reconnect/fallback path per ADR-0065. The client issues no store request | FR-9, NFR-13 |
| M8.24h | After M8.24f/g, kill an OpenSearch node with ≥100 segments outstanding; recover every acked record from `batch_start`; while ≥1,000 post-kill records remain continuously pending, every one becomes visible within 3× its own interval ceiling and scheduler trace shows a catch-up turn after every bounded live quantum. Store GETs ≤1 per backlog segment per consumer node; LISTs = 0 | FR-9, NFR-13 |
| M8.24 | Parent closes only when M8.24f–h pass the original end-to-end criterion | FR-9, NFR-13 |
| M9.13 | A gap re-read through the catch-up path, and `TENS_OF_GETS` counted | FR-10 |
| M9.20 | **THE ADR CHOOSING HOW THE PLUGIN READS THE STORE** for ladder tiers 2 and 3 -- a third approach, superseding ADR-0057 in part | FR-10 |
| M9.21 | **TIERS 2 AND 3 EXECUTE** on a real gap with no ingester reachable, the window re-read through M8.24's path, GETs and LISTs counted | FR-10, NFR-4 |
| M9.14 | Measurement M3: the `direct` fan-out threshold, by cost | FR-6, NFR-4 |
| M8.58 | The watch matches the holder by identity, not address | NFR-9, FR-11 |
| M8.60 | Export M8's test-only counters as bounded-label metrics | NFR-11 |
| M8.61 | Tighten M8's chaos assertions that pass on their named defect | NFR-8, NFR-9 |
| M8.62 | Kill M8's recorded surviving mutants | — (tests) |
| M8.63 | Replace `Thread.sleep` polling with Awaitility | — (tests) |
| M8.64 | Sweep M8's stale prose | — (prose) |
| M8.68 | `MALFORMED` reclassified | — (observability) |
| M8.69 | A blank duration, byte or boolean setting is refused | — (config) |
| M8.70 | A readiness-to-refusal delay in graceful shutdown | NFR-9 |
| M8.71 | Harness and composition-root loose ends | — (harness) |
| M8.72 | Verify the three `System.getLogger` lines reach the OpenSearch log | NFR-11 |
| M8.76 | `check-wired` resolves package-qualified calls, including wildcard imports | — (gate) |
| M8.77 | `check-wired` excludes qualified nested-type construction | — (gate) |
| M8.79 | TDD recorder resolves Windows source paths and selector line endings | — (gate) |
| M8.80 | TDD recorder fails closed when stale-result cleanup cannot finish | — (gate) |
| M8.81 | TDD recorder hashes canonical source bytes across Windows line endings | — (gate) |
| M8.74 | `BodyTooLargeException` blames the right side; 400 is a known outcome | FR-12 |
| M0.27 | The L1 CI job (Docker, `soakTest` and the cost jobs need it; it lands with the first product test, which exists now) | — (CI) |
| M8.75 | Put `soakTest` in the nightly `measurement` workflow (L2S, nightly per build.md), with a script asserting the workflow names it | NFR-2 |
| M9.15 | The cost/latency curve, generated into `docs/`, with its `--check` gate | NFR-1, NFR-7 |
| M9.16 | **CREATE the nightly `measurement` workflow** (outside L1-L3 and their budgets) and put every point of criteria 3, 5 and 6 in it; the per-push L1 job runs the fast subset at the same thresholds; build.md's layer table gains the row | NFR-1, NFR-4, NFR-5 |
| M9.17 | M9's `VERIFIED.md` and the milestone review | — (evidence) |
