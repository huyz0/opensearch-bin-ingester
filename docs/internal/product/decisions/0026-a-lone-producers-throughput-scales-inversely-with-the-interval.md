# 0026. A lone producer's throughput scales inversely with the flush interval

Status: accepted
Date: 2026-09-02
Requirements: NFR-1, NFR-6, FR-4
Cost rule: R1b, defined in docs/internal/standards/cost.md

## Context

M3.3 made the flush interval adaptive: 250 ms floor, 5 s ceiling, per pod, per
its own `fillRatio` (ADR-0016 §2, carried forward by ADR-0017). M3.5 then set
out to re-prove NFR-6 (memory bounded and independent of request size) with the
interval at its ceiling, by copying M1.18's `MemoryFlatUnderTenXBodySizeTest`:
one producer streaming a 200 MB `_bulk` body under `-Xmx256m`.

It did not finish. Two runs were killed at 8 and 20 minutes, both still short
of the 200 MB target. The cause is not GC pressure, and it is not a test defect.

`BulkService.appendBulkBody` parses a body into chunks of
`APPEND_CHUNK_RECORDS = 1000` and calls `Ingest.append` **once per chunk,
blocking** until that chunk's segment and commit delta are durable. That
chunking is deliberate and correct — it bounds both retained memory and the
per-pod accumulator lock's hold time (M1.7b, round-1 review). But it has a
consequence nobody wrote down: **a lone producer advances exactly one chunk per
flush**, so its throughput is `chunkBytes ÷ interval`, and the interval is now
a variable.

Measured directly (2 MiB body, interval warmed to its 5 s ceiling, real
Helidon/`LocalFsBinStore` stack under `-Xmx256m`):

| Interval | Bytes/flush | Seconds/flush | Throughput | 200 MB body |
|---|---|---|---|---|
| 250 ms floor (M1.18, measured) | ~223 KB | 0.25 | ~806 KB/s | ~254 s |
| 5 s ceiling (M3.5, measured) | ~223 KB | **5.15** | **~44 KB/s** | **~79 min** |

The 20.3x throughput ratio is the 20x interval ratio. Ten appends carried
2,097,174 bytes in 51.5 s — one chunk each, one per ceiling interval, exactly.
⚠️ The measured run's 40.7 KB/s average is slightly below the steady-state
figure because its tenth append was a ~190-record tail, not a full chunk; the
steady-state number is `1000 × ~228 B ÷ 5 s`.

Two things follow, and the second is the reason this is an ADR rather than a
test-plan note.

**The interval does not self-correct for a lone producer.** Each flush carries
one ~223 KB chunk against an 8 MiB target, so `fillRatio ≈ 0.026` — deep in the
low band. Sustained low is precisely the condition that *keeps* the interval at
its ceiling (M3.3). A single producer therefore pins itself at ~44 KB/s and the
adaptive rule, working exactly as designed, never rescues it. The measured run
confirms this: 5.15 s/flush held for the whole run, never shortening.

**And the single-producer shape cannot prove what M3.5 was written to prove.**
A blocking producer never has more than one chunk outstanding, so the
accumulator holds ~223 KB regardless of the interval — at the ceiling *and* at
the floor. M3 SPEC acceptance criterion 5 names the risk as "segments held open
for up to 5 s, potentially accumulating more before a flush than the
fixed-250ms path ever did". In this shape that accumulation never happens, so
the test would have passed while exercising nothing.

## Decision

Two parts, one descriptive and one prescriptive.

**Record the property.** A lone producer's ingest throughput is
`APPEND_CHUNK_RECORDS × recordBytes ÷ currentInterval`, and at the 5 s ceiling
that is ~44 KB/s — a **per-connection** cap, not a per-pod one.

⚠️ **This is NOT confined to idle pods, and an earlier draft of this ADR
claimed it was.** That claim was wrong and round-1 review caught it. The
ceiling is reached after two minutes of `fillRatio <= 0.4`, which at the
250 ms floor is any pod under `0.4 × 8 MiB ÷ 0.25 s` ≈ **13.4 MB/s** — most
pods, not idle ones. The SPEC's own worked load in criterion 4 (1 MiB/s per
pod) settles at the ceiling *by design*, and that operating point is where
ADR-0017's headline $15.55/month comes from. So the honest statement is: on a
normally-loaded pod running at its designed steady state, any one connection
streaming a large body is capped at ~44 KB/s, while the pod as a whole is
doing fine.

It is accepted **for M3**, whose completion condition is the adaptive interval
and the memory proof, and deliberately not fixed here — every candidate fix
below is a real design change needing its own measurement. It is recorded
rather than left implicit because it is invisible at the call site, and
because "the adaptive interval made one large upload 20x slower" is a support
ticket someone will eventually file. ⚠️ **Carried as a backlog row for a
later milestone, not closed by this ADR** — see *Consequences*.

**Prove NFR-6 at the ceiling with concurrent producers landing in the MIDDLE
band.** M3.5's test uses enough concurrent producers that one ceiling
interval's arrivals put `fillRatio` strictly between the two thresholds
(0.4 < r < 0.9), which is `adaptInterval`'s `else` branch: it resets both
streaks and **leaves the interval untouched at its ceiling**. The timer then
flushes an accumulator holding 3.2–7.2 MiB, against the ~223 KB a lone
producer ever holds — 15–33x more accumulation, which is exactly the risk
criterion 5 names.

⚠️ **NOT "enough producers to exceed the 8 MiB target", which an earlier
draft of this ADR specified and round-1 review caught as self-defeating.**
Exceeding the target makes `fillRatio >= 1.0`, which is the **high** band, and
`DEFAULT_INTERVAL_SHORTEN_DELAY` is `Duration.ZERO` — so the interval drops to
the floor on that very drain. The ceiling state would survive exactly one
flush and the rest of the run would be M1.18's 250 ms regime wearing a
different name. The size trigger is a *bound*, not the state under test;
putting the run where the size trigger fires destroys the state the test
exists to hold.

Concretely: at ~228 B/record and 1000 records per blocking chunk, each
producer contributes ~223 KB per ceiling interval, so the band 0.4–0.9 is
**15 to 33 producers**. M3.5 uses 24 (~5.2 MiB/flush, `fillRatio` ≈ 0.65),
centred with real margin to both thresholds. Aggregate ~1.04 MiB/s — which is
the SPEC's own criterion-4 worked load, reached here as a consequence of the
band rather than as a separate assumption.

## Alternatives considered

**Keep the single-producer shape, shrink the body to ~20 MB.** Fits in ~8
minutes at the ceiling and is the smallest change to the SPEC's literal
wording. Rejected: it proves strictly less than M1.18 already does. The
accumulator still never holds more than one ~223 KB chunk, so the run would be
green without ever entering the state criterion 5 describes — a passing test
that constrains nothing, which testing.md rules 8–10 exist to prevent.

**Keep the single-producer shape and the 200 MB body, accept ~79 minutes.**
Rejected on two counts: it still exercises no accumulation (same defect as
above), and a 79-minute gate is one nobody runs, so in practice it would
protect nothing while appearing to.

**Raise `APPEND_CHUNK_RECORDS` so a lone producer advances faster.** A 10x
chunk would restore ~400 KB/s at the ceiling. Rejected: the constant's own
javadoc explains it bounds the shared per-pod accumulator lock's hold time, so
raising it trades one slow producer for every producer on the pod waiting
longer. It also raises retained memory per chunk — against NFR-6, which is the
requirement this whole task serves.

**Make the interval shorten on a *blocked producer* rather than on
`fillRatio`.** Would fix the lone-producer case directly. Rejected as
out-of-scope for M3 and under-designed: it adds a second, independent input to
a control loop ADR-0016/0017 deliberately specified on one signal, and the
interaction between the two is exactly the kind of thing that needs its own
measurement before it needs an implementation. Recorded as a future option in
*Consequences*, not chosen here.

**Pipeline chunk appends so a producer has several in flight.** Would decouple
throughput from the interval entirely. Rejected for M3: `Ingest.append`'s
contract is "returns when durable" (FR-4), and overlapping appends from one
connection would either break that contract or need a per-connection
completion-tracking scheme that does not exist. A real option later; a large
change now.

## Consequences

**Makes easy.** M3.5's test becomes both faster and stronger: 24 concurrent
producers in the middle band put ~5.2 MiB in the accumulator per ceiling
interval and finish 200 MB in ~192 s (38 flushes), against the ~79 minutes the
single-producer shape needed — and, unlike that shape, the run actually spends
its whole length in the state criterion 5 is about.

**Makes hard.** Any future single-producer benchmark at the ceiling is now
known to be measuring the interval, not the code path — a benchmark written
without reading this ADR would report a 20x regression that is not one. The
`bench` skill's own rule about naming the operating point applies with force
here: a throughput number from this system is meaningless without the interval
it was taken at.

**Leaves an open obligation, not a closed one.** The ~44 KB/s per-connection
cap on a normally-loaded pod is accepted for M3 and **carried in
[roadmap.md](../roadmap.md)'s own "Deferred into a later milestone" table**,
not resolved here. The two rejected fixes above (a blocked-producer input to
the control loop; pipelined chunk appends) are both still open, and both are
cheaper to argue now that the number is measured rather than assumed. ⚠️ An
ADR that accepts a limitation without leaving a row to revisit it is how a
known cost becomes an unknown one.

**⚠️ Cost impact: none.** Request rate is unchanged — this is the same one PUT
plus one commit delta per flush, at the same flush cadence the interval already
dictated. What changed is how much *payload* a lone producer gets through per
flush, which affects that producer's wall-clock latency, not the bill.
Non-negotiable 6 is untouched: nothing here scales a request with records,
shards, partitions or indices.
