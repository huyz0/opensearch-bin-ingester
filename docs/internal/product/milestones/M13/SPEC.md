# M13 — Fast mode (FR-17), after M12's harvest

## Completion condition

The roadmap's M13 row, restated: **an index with `wal=true` is acked and its
records reach its consumers, on any pod and whichever pods wrote them, within
NFR-15's bound (ack and visibility p99 < 10 ms), before their segment is in the
object store; every acked fast record appears later in a
committed segment at the offset it was acked with (invariant I6), through the
death of the writing pod or of the sequencer leader while a quorum of its copies
survives; the default path carries no record through fast mode for every
index with `wal=false` (its takeover pays what criterion 19 states); and
every item of M12's review harvest is a closed M13 row or dropped with its reason
below.**

M13 collects the obligations M12 handed it:

| Obligation | Assigned by |
|---|---|
| Open rows M12.27, M12.28 | M12/VERIFIED.md § Rows carried open |
| Harvest R1, R2, R3, R4, R5, R6, R7, R8, R9, R10, R11, R12, R13, R14, R15, R16, R17, R18 (R19 dropped below) | M12/VERIFIED.md § Harvest for the specification of M13 |
| The research corpus proposals (five documents) | M12/VERIFIED.md § Research corpus |
| The re-plan: R1, M12.28, R2, M12.27, then R4, R5, then fast mode | M12/VERIFIED.md § Re-plan |
| Fast mode (FR-17) | roadmap; ADR-0013 |

## Requirements

- **FR-17** fast mode: opt-in per index; ack and visibility in ~1.5–6 ms;
  `flush_timer`, `wal`, `wal_quorum` set in OpenSearch and pushed by the plugin.
- **NFR-15** fast-mode ack-and-visible latency, p99 < 10 ms (ADR-0013).
- **NFR-14** RPO for acked fast writes: 0 while a quorum member survives the
  upload window (ADR-0013) -- the guarantee obligations 3, 4 and 9 state
  precisely.
- **NFR-8** becomes conditional (ADR-0013 Consequences): an acked fast record
  lives on `wal_quorum` pods until its segment is committed. **NFR-5** (< 0.1%
  cross-AZ bytes) and **NFR-10** (no data loss on one AZ's loss) cannot hold
  for every fast index -- its quorum and publication copies cross AZs, and at
  `q = 1` its exposed tail is voided with the leader's AZ -- so NFR-5 becomes
  conditional on `wal=false`, and NFR-10 on `wal=false or wal_quorum ≥ 2` --
  and, at `q = 2`, on no second holder of an entry being lost within the upload
  that the first loss triggers (NFR-14's window) -- with
  a `q = 3` index on three AZs not acking while one is lost. ⚠️ Moving a
  requirement is an ADR (cost.md rule 18, non-negotiable 2): the two conditions
  are decided in M13.22's protocol decision record, amending ADR-0013's
  Consequences, which make only NFR-8 conditional; M13.38 then edits the
  requirements table to match.
- **NFR-1, NFR-4, NFR-5** (cost, request rates, cross-AZ bytes) and
  non-negotiable 6: fast mode adds cross-AZ bytes, never a request rate that
  scales with records, shards, partitions or indices.
- **FR-6, FR-10** (consumers, catch-up), **FR-15, FR-19** (ingest seams),
  **FR-21** (cost attribution) for the harvest rows.

## Scope

1. M12's re-plan rows first: the write-path split (R1), the harness port race
   (M12.28), `PartitionVisibilityIT` as a distribution (R2), the reader
   connection on close (M12.27), ADR-0078's amendment (R4), the silent defaults
   (R5).
2. The rest of M12's harvest, R3 and R6–R18.
3. Fast mode (specified by M13.44, split from M13.43 at M13.43's third
   review round; M13.43 was split from M13.42, M13.42 from M13.41, and M13.41
   from M13.0, each at its third):
   - the three per-index settings, read by the plugin and carried by the index
     registration;
   - the protocol, designed and recorded before any code (M13.22), meeting the
     eleven obligations in § Design;
   - the fast write path: offsets assigned by the sequencer leader, the batch
     held on a quorum of pods (leader journal plus copies) before the ack, and
     the producer acked with its offsets;
   - publication: the leader pushes each fast batch to every pod with a
     subscriber on its streams;
   - the leader uploading the journal as ordinary segments committed at the
     assigned offsets, and releasing every copy;
   - catch-up of fast streams beyond the committed chain, from the leader's
     journal;
   - recovery: the writing pod's death (the leader holds the batch), the
     leader's death and an AZ's loss (a new leader recovers every surviving copy
     and uploads it before assigning again), and void ranges after quorum loss;
   - switching an index's `wal` on and off on a live index;
   - shutdown: a leader uploads its journal before it releases the lease;
   - metrics and the cost evidence.

### Not in scope

- **R19, the commit hooks.** Whether to install `.pre-commit-config.yaml`'s
  hooks is the owner's decision. Until then every M13 commit runs `./gradlew
  gates checkReviewed checkTdd checkTestIntegrity checkCommitMessage
  -PcommitMessageFile=<file>` by hand, and its body says so.
- **ADR-0080's compact per-(node, segment) event and the own-zone relay (H16),**
  weighed as M12's re-plan assigned and deferred: they cut the default path's
  per-stream event floor, while fast mode's cross-AZ bytes are the quorum and
  publication copies (§ Cost impact), which neither changes; a wire change to
  the subscription event is not needed for fast mode. They go back to the
  roadmap's deferred table, milestone not yet assigned, with H16 recorded as
  weighed here.
- **A per-lane ack mode** (research 12 §9's open question): needs per-lane offset
  spaces that OpenSearch does not provide.
- **Fast mode's latency on AWS.** Measured on M9's rig only (criterion 18).
- **AWS S3 latency, TTFB and billed dollars**, NOT-RUN since M9.

## Design

### The harvest

Each harvest row is decomposed below with the finding it closes, as M12 did;
the design of each is the finding's own fix. Two carry design weight:

- **R1, the split.** `DefaultIngest` has 8 lines of headroom and `Assembly` 6,
  and fast mode lands in both. The split extracts the durable flush path from
  `DefaultIngest` (the `FlushCoordinator` wiring and the settlements) and the
  sequencer and serving construction from `Assembly`, so that fast mode adds a
  sibling rather than growing either file. `ConsumerClient` (606) goes back under
  a named ceiling, and `LocalSequencer` (700, which fast mode touches) is split
  too. The ceiling gate holds `DefaultIngest` and `Assembly` at 500 and the other
  two at 600.
- **R5, the silent defaults.** The consumer's fetch policy stops having a
  clock-less default in production code: `SegmentFetchRetry.DEFAULT` is removed
  from main sources and every production caller passes a clock; the two
  `ConsumerDeliveryQueues` and `IndexQuotas` default constructors go.

**R16, the failure hold's latency against NFR-7 (M12.11 review P1).** Since
M12.11 the node's failure hold is its backoff plus up to half again, only ever
longer: at the 30 s ceiling a failed segment is held (30, 45] s where it was held
exactly 30 s before, so a consumer's recovery once the store returns can take up
to 15 s longer. (A lone client's own wait is jittered both ways, [15, 45) s at
the ceiling; the node's hold raises the shortest and the mean, not the longest.)
ADR-0063 fixes NFR-7's endpoints and divisor and names no exclusion, so this is
not claimed to be inside NFR-7: a record whose delivery waits out a failed
segment's hold during a store outage is outside its bound (p99 < 3x the flush
window) with or without the jitter, and the jitter lengthens that tail by up to
15 s. Stated here and in `holdFailures`' javadoc (M13.18) rather than left
implicit.

### Fast mode

⚠️ **This SPEC decides fast mode's architecture and states the protocol's
obligations; the protocol itself is designed, recorded and reviewed in M13.22,
before any fast-mode code.** Three review rounds of this SPEC found major
defects in successive drafts of the protocol (a deposed leader acking; voids
over surviving copies; switching collisions in both directions; an AZ loss
stalling fast mode; a leader outside its own roster; epochs left uncollected).
A distributed protocol is not settled by a SPEC paragraph; each finding is an
obligation below, and each has a criterion and a named test.

**Decided: the sequencer leader sequences, holds, publishes and uploads every
fast batch.** A departure from research 12 §5, which has each writer keep and
upload its own WAL. That cannot give the contiguous per-stream runs a segment
needs: any pod accepts writes for any partition, so two writers' batches for one
stream interleave in offset order, and `SegmentWriter` keeps one run per stream
with one `firstOffset` (checked against the code by M13.0's review), so neither
writer can upload a run covering the other's offsets. The leader, which assigns
every offset, holds every fast batch in offset order, publishes it to the pods
with subscribers on its streams, and uploads it as ordinary segments committed at
the offsets it assigned.

**Decided: the shape of a fast write.** The writer sends the batch to the
leader, which assigns offsets, journals the batch with them (fsynced) and
replicates the assigned entry until `q` copies in `q` distinct AZs exist; the
writer acks the producer with its offsets once the leader reports the quorum
complete. **A copy counts toward `q` only once it durably holds the entry WITH its
assigned offsets**: the writer's copy of the batch counts, when the writer is in
another AZ, only after it has journaled the offsets from the leader's answer and
confirmed that to the leader -- an offsetless copy cannot be recovered at its
offset (M13.41 review P1). So `q ≥ 2` costs `q − 1` cross-AZ copies per record,
research 12's figure, and `q = 1` costs the writer-to-leader hop (§ Cost impact).
Settings `flush_timer`, `wal`, `wal_quorum` are dynamic OpenSearch index settings
read by the plugin's registrar and carried in `IndexRegistration` v2.

**Obligations the protocol (M13.22) must meet**, each pinned by a criterion:

1. **No deposed leader acks** (criterion 10). A leader's own incarnation is on
   its roster; fencing covers a live leader that believes it still holds the
   lease -- a GC pause across an early takeover, a stopped monotonic clock --
   with the renewal instant defined as the successful renewal's SEND time on a
   named clock; a replica or writer refuses a lower epoch.
2. **Takeover is complete across every earlier term** (criteria 11, 12). A
   leader collects every unreleased entry of every earlier epoch, however many
   leaders died in between, from a durable record of which incarnations can hold
   copies -- not from a release that exists only pod to pod.
3. **An index that has not lost quorum is never voided, and its takeover
   stalls only while a surviving rostered incarnation is alive but
   unreachable** (criterion 13) -- the new leader cannot know which
   incarnations actually hold copies, only which can (obligation 2), so it
   waits for every one it has not heard from (M13.44 review R2-P1). "Lost quorum" is obligation 4's rule, judged over the incarnations
   ROSTERED for the term, never over which pods actually held copies (M13.43
   review P1): an AZ counts as lost as soon as ANY incarnation rostered for the
   term in it is gone, and the leader's AZ whenever the leader is gone. So at
   `q ≥ 2` the leader's loss alone is not quorum loss unless rostered
   incarnations of the term are already gone in `q − 1` other AZs, and at
   `q = 1` it always is (ADR-0013). A partitioned or unready incarnation is not
   gone (§ Alternatives): the stream waits for it, never voids over a copy it
   holds, and resumes once it answers or its UID is deleted. A pod departing gracefully (a
   scale-down, a rolling restart) first has its unreleased copies committed --
   it triggers an upload and waits for it, never releasing an uncommitted copy
   (obligation 11) -- and then leaves the term's roster before it stops (one write per departure, scaling with
   nodes), so routine churn is never counted as quorum loss (M13.43 review
   R2-T4). Completeness and the resumption of
   assignment are per stream, so an index waiting on lost pods stalls only its
   own streams. ⚠️ Acking at `q` needs `q` live AZs, so an index whose `q`
   exceeds the live AZs stops acking fast writes until an AZ returns -- a
   `q = 3` index on three AZs does so with one lost (NFR-10's condition,
   M13.38).
4. **No exposed offset is ever lost or given other content, except inside a
   void, and a void only after quorum loss** (criterion 13). The oracle is a
   ground-truth model of which pods hold which copies: a void may cover an
   exposed offset only if the model says every copy of an exposed entry of that
   stream is gone, and never covers an offset whose entry survives on a pod
   whose UID survives (obligation 3: such a pod is waited for, not voided over).
   Offsets never exposed may be truncated and assigned again (their writers told
   to retry), and may be covered by a void: after an index loses quorum the
   survivors cannot tell a stream that had an exposed uncommitted tail from one
   that had none, so a void may cover never-exposed offsets of any stream of
   that index, within the tail bound (M13.42 review R2-P1). A record of which
   streams have an uncommitted tail, made before the ack, is the alternative and
   is rejected: at `q = 1` it is a cross-AZ write per ack.
   **Quorum loss is judged from what the survivors can know** (M13.42 review
   R3-P1). The shape: an index has lost quorum for a term being recovered when
   incarnations rostered for that term are gone in at least `q` AZs (at `q = 1`,
   the leader), `q` being the smallest `wal_quorum` recorded in the term
   record for the index during the term. **The exact predicate, and the
   oracle's copy of it, are M13.22's decision** (M13.43 review R3-P1: each of
   three SPEC drafts of it was wrong in a case the next round named), under two
   constraints: it is a function only of events the survivors can know and the
   model records -- rostered joins, graceful departures, UID deletions, the
   `wal_quorum` values written to the term record, which a new leader reads,
   and (⚠️ added by M13.22i, M13.22f review round 1, T1) each term's leader and
   its closure, as written to the roster, for terms whose `LATEST` write
   landed only (an orphan roster is no term, ADR-0081 §1) --
   never of which pods held copies, under which `q` a write was assigned, or a
   setting the leader received but did not record; and it holds in every case where an exposed
   entry may have lost every copy. A void of never-exposed offsets is allowed
   exactly when it holds; a void over an exposed offset still needs
   every copy of an exposed entry of its stream to be gone. **Every void lies
   within `[c₀, c₀ + B)`** of its stream, `c₀` being the stream's committed
   next offset when the takeover began: a committed offset is never voided,
   though its copies are released and "every copy gone" is then trivially true
   (M13.43 review R2-P1).
   Each stream's commits and voids in a takeover are ONE chain entry (⚠️
   amended by M13.22g: a batch too large for one upload is split by stream,
   ADR-0081 §5), and a recovery upload is
   segments and deltas at the ordinary cadence, never one request per stream
   (M13.42 review R3-P3). **M13.22 decides, and records, how this is
   met**: the quorum-loss detection rule; the per-stream bound on uncommitted
   fast offsets that bounds a void's tail, since the dead leader's cursor is
   unknown and no durable per-stream cursor exists (rejected below); and a
   durable record of the `wal_quorum` in force for each term, raises and lowers
   both, written before the leader assigns under a changed value, every change
   since the last write coalesced into one write at most once per
   `min_upload_interval` (never one per index changed) -- after quorum
   loss the entries that would say so are the lost ones, and the catalog's
   current value may be neither. Three review rounds of M13.41 found an
   enumerated rule for each of these wrong in a case the next round named; the
   invariant, checked against the model over seeds, is what this SPEC fixes.
5. **One stream, one order, in both switching directions** (criterion 15): no
   fast assignment while a default-path commit for the stream is in flight or
   ambiguous, and no default-path assignment while a fast tail is uncommitted.
6. **Publication is in offset order per stream** (criterion 14), and a
   subscriber served proxy (ADR-0076) is not left with nothing to fetch before
   the upload.
7. **The leader's capacity is not the cadence**: an upload is triggered by the
   journal's fill, by any stream nearing its per-stream bound, and by the loss
   of a holder of an unreleased entry (shrinking the window in which a second
   loss is quorum loss; M13.43 review R3-P3), as well as by
   `flush_timer` -- but never more often than once per `min_upload_interval`
   (M13.22 fixes it), so the request rate scales with time and leaders, never
   with records (M13.43 review P2). A stream at its bound is backpressured
   (cost.md rule 14), not refused; its ceiling is `B / (min_upload_interval +
   commit latency)`, and M13.22 sizes `B` so that ceiling covers one partition's
   rate at the fleet's per-partition limit. The journal cap likewise throttles
   the fleet only above `cap / (min_upload_interval + commit latency)`, journal
   entries being held until their commit. A barrier wait does not
   hold unrelated `wal=false` records (criterion 17).
8. **A graceful handover costs no fast outage beyond the upload** (criterion 16).
9. **No offset is exposed before its quorum completes** (criterion 13): not by
   the producer's ack, not by publication, not by catch-up. An offset a consumer
   may have seen must be recoverable, or it could be reassigned to different
   content after an AZ loss that obligation 3 does not void (M13.0 review R3-P1).
10. **Exposure is in offset order per stream** (criterion 13): an offset is acked,
   published or served only once every lower uncommitted offset of its stream
   has completed its quorum, whatever order the replicas answer in; so recovered
   entries above a hole that is not quorum loss were never exposed (M13.41
   review P2).
11. **A copy is released only after the commit delta covering it is in the
   chain, and a recovery upload never commits an entry twice** (criteria 11,
   12): the recovering leader skips every recovered entry below the chain's
   committed next offset (M13.41 review R2-P3).

Candidates M13.22 starts from, not decisions: a per-epoch roster object written
by compare-and-swap, including the leader, with each epoch's roster pointing at
its predecessor's; lease-time fencing plus an epoch fence on every pod; pod-UID
liveness from the Kubernetes API; completeness by AZ coverage (obligation 3).

**Formats** (M13.22's formats record; the `wire-format-change` procedure):
`IndexRegistration` v2; the fast commit, answer, replica, publish, interest,
fence, collect and release frames; the roster object, carrying each term's
`wal_quorum` record (the value in force at the term's start and every change
made during it); the journal file's entry format; the chain's recovery entry
kind (a takeover's commits and voids, ADR-0082); and the default-path commit
answer naming every delta and the redirected runs (ADR-0082 §7, added by
M13.22i).

### Alternatives rejected

- **Each writer uploads its own WAL** (research 12 §5): no contiguous runs for a
  stream written through several pods (above).
- **Pin each fast stream to one writer pod**: a hop for every record not written
  through the owner, cross-AZ two times in three, and a routing layer.
- **Persist each fast assignment to the object store before the ack**: one store
  round trip per batch is the 42–708 ms fast mode exists to avoid.
- **Record, before each ack, which streams have an uncommitted fast tail**: at
  `q = 1` a cross-AZ write per ack; a void may instead cover never-exposed
  offsets of an index that lost quorum (obligation 4).
- **Persist a per-stream offset reservation to the store**: a request rate per
  stream, forbidden by non-negotiable 6; an in-memory per-stream bound on uncommitted fast offsets sizes the void instead (obligation 4).
- **Treat a pod out of the EndpointSlice as gone**: readiness is not disk loss
  (M13.0 review P6); only a deleted pod UID is.
- **Deliver fast batches only through the writer's hub**: a consumer on a
  partition written through several pods blocks on every other writer's batch
  until catch-up (M13.0 review P4).
- **Fence fast mode through the chain alone**: a fast ack never touches the
  chain, so a deposed leader would keep acking (M13.0 review P1).

## Cost impact

- **cost.md rules touched:** rule 1 (bundling: fast indices are bundled into
  the leader's segments, not each pod's), rule 6 (commit rate independent of
  index count: one delta per upload, at most one per `min_upload_interval`,
  plus at most one term-record write per `min_upload_interval`, coalescing
  every `wal_quorum` change since the last, however many indices changed), rule 9 (every fast PUT counted by `CountingBinStore`) and
  rule 13 (the governor's expected rate includes the leader's uploads); and
  non-negotiable 6.
- **The harvest rows:** none on the request path. R3 and R8 correct what the
  cost report says, not what it costs; R7 gates the test fixture's memory.
- **Object-store requests.** Fast indices are uploaded by the leader alone: one
  segment and one delta per upload, in place of each pod's own segments for
  those indices, uploads being at most one per `min_upload_interval` however
  they are triggered (obligation 7). Whatever durable record M13.22's protocol keeps
  of which incarnations can hold copies must cost a request rate that scales with
  nodes and terms only (the roster candidate: one write per pod joining a term),
  the per-term `wal_quorum` record at most one write per `min_upload_interval`
  (coalesced, cost.md rule 6 above), a graceful departure one roster write, each stream's commits and voids in
  a takeover one chain entry, and a recovery upload segments and deltas at the ordinary cadence.
  Replication,
  publication, fencing and collection are pod-to-pod HTTP. No request rate
  scales with records, shards, partitions or indices.
- **Cross-AZ bytes** (NFR-5), per fast record, with writers spread evenly over
  three AZs:
  - **the quorum:** `max(q − 1, [W ∉ AZ(L)])` copies. At `q ≥ 2` that is `q − 1`
    (the writer-to-leader hop IS a quorum copy, because the writer's copy counts
    once it holds the offsets): research 12 §1's $54/month per 1 MiB/s at
    `q = 2`. ⚠️ **At `q = 1` it is not zero**: a writer outside the leader's AZ
    ships its batch to the leader, 2/3 of a copy on average, $36/month per
    1 MiB/s where research 12's `q = 1` row has none (M13.41 review P6; its
    banner says so);
  - **publication:** one copy per interested pod in an AZ other than the
    leader's. With one consumer per partition (ADR-0009's default,
    `all_active=false`) that is at most one copy, 2/3 on average; with
    `all_active=true` one more per consuming replica's pod in another AZ.
  - So at `q = 2` and `all_active=false`: at most two copies, $108/month per
    1 MiB/s; at `q = 1`: at most two, 4/3 on average.
  Catch-up of fast streams from the leader's journal adds bytes on repair only.
- **The default path:** its own requests unchanged; each takeover adds the
  per-term requests criterion 19 lists, asserted by the existing cost suites
  with their pins moved by exactly those.

## Acceptance criteria

1. **The write path is split** (R1): `DefaultIngest` and `Assembly` are each
   **below 500 lines** -- the headroom fast mode needs, not merely the 600 they
   already meet -- and `ConsumerClient` and `LocalSequencer` below 600, all four
   pinned by the ceiling gate at those numbers, and `./gradlew test` is green
   after each split.
2. **The harness survives losing its probed port** (M12.28): when the child
   refuses to start because another process took the port between the probe
   and its bind, `NodeProcess` starts it again on a fresh port, at most three
   times, and retries no other failure; pinned by a test. ⚠️ Corrected by
   M13.2 from "passes port 0 to the child and reads back the port it bound":
   the lease advertises the endpoint, port included, when the graph is
   assembled -- before the front door binds -- so a child on port 0 would
   advertise a port nobody knows. M12.28 allowed the retry ("or retry the
   bind").
3. **`PartitionVisibilityIT` is measured as a distribution** (R2): at least ten
   runs on M9's rig, `trigger202` and `drainEnd` recorded per run, the RustFS
   memory plateau measured or OBSERVED-NOT, variant B completed, the ~1.03 s
   trigger write investigated and its cause named or OBSERVED-NOT, and what
   `trigger202` and `drainEnd` each include reported. ⚠️ Corrected by M13.3
   from "`trigger202 ≤ drainEnd` asserted by the IT": both clocks start at the
   heal and `drainEnd` is read after the trigger's `202`, so the assertion
   could not fail; and measured concurrently an M=10 inbox empties before its
   forwarded trigger's `202`, so it is not a property of the system either.
4. **A reader's connection closes with its subscription** (M12.27): decided and
   either fixed and pinned or refuted with its measurement.
5. **ADR-0078 matches M12.4** (R4) and **no production code carries a silent
   default** (R5): `SegmentFetchRetry.DEFAULT` and the default constructors of
   `ConsumerDeliveryQueues`, `IndexQuotas` and `ServerConfig` are gone from main
   sources, and the plugin's started client receives the clocked policy.
   ⚠️ `ServerConfig` added by M13.6a: R5 names it and this criterion had
   dropped it.
6. **Every other harvest row (R3, R6–R18) has a disposition**, and each of the
   five research-corpus proposals is made or dropped with its reason (M13.21),
   enumerated once each in VERIFIED.md at close, as M12's criterion 17 was;
   `checkMilestoneVerified` refuses an enumeration missing a harvest ID listed
   individually in this SPEC's obligations table (M13.40).
7. **The three settings reach the ingester** through the registrar and
   `IndexRegistration` v2 (golden files), and a change on the live index updates
   the catalog.
8. **A fast append is acked with its offsets before any object-store request is
   made for it**: with every object-store request blocked on the injected
   store, and no `wal_quorum` change pending and no roster join in progress,
   the append is acked; with one pending, it is acked once that one
   term-record or roster write alone is let through. Requests unrelated to the
   append (an upload of earlier batches, another index's flush) are not
   counted against it.
9. **An acked fast record is held on `q` pods in `q` distinct AZs**, for `q` in 1,
   2 and 3, with W in L's AZ and in another: the ack does not complete while any
   required copy is missing, including a `q = 3` write with one AZ lost. ⚠️
   Added by M13.22i (M13.22e review round 1, T1): the model drops a pod's
   unfsynced journal suffix at every crash it injects (a node reboot keeping
   the UID), and an EXPOSED, an ack by a writer that is the leader, and a
   CONFIRM are each delayed until the answering pod's group fsync -- a crash
   between the write and the fsync leaves no answer sent; ⚠️ added by
   M13.22i (M13.22e review round 2, T2): so is a holder's REPLICA_ACK -- at
   `q = 2` a holder rebooting (UID kept) between its journal write and its
   fsync, then the leader deleted, must leave the entry unexposed.
10. **No deposed leader acks** (obligation 1): a live leader that paused across
    an early takeover, with its renewal's response delayed, and one whose
    monotonic clock stopped, acks nothing its
    successor may reuse -- driven at `q = 1` with the writer other than the
    leader, deleted after the ack; a pod refuses an epoch below the highest it
    has seen; a pod not on the roster can neither write nor replicate. ⚠️ Added
    by M13.22i (M13.22 review; ADR-0081 §3): also a leader whose WALL clock stopped, and a
    successor that took the lease by the early challenge; the asserted property
    is that, on one global clock, the old leader exposes nothing after the
    successor's first fast assignment or first void; and with the two pods'
    wall clocks skewed by just under the margin, in each direction; and, ⚠️
    added by M13.22i (M13.22b review round 3, T3), the two MONOTONIC clocks
    running at rates just under ~18 % apart in each direction. ⚠️ Added by
    M13.22b (M13.22a review round 1, T1): every case above is driven ALSO with the
    writer being the leader (W = L), force-deleted while it keeps running, and
    exposing through an ack, a catch-up and a proxied `/seg` read -- the
    `q = 1`, W ≠ L shape alone is held by the epoch fence, so a broken lease-time
    fence would pass it; in the stopped-wall-clock case the successor must wait
    on its own monotonic clock (ADR-0081 §3). ⚠️ Added by M13.22i (M13.22a review
    round 2, T2 and T4; narrowed by M13.22c's review round 1, T4): the pause is
    injected at each point after assignment and BEFORE each validity check that
    guards an exposing send (the ack, a PUBLISH, a catch-up and a `/seg`
    response), not only before assignment -- and never between that check and
    its send toward a non-member, which ADR-0081 §3 states as the residual
    lease risk. ⚠️ Added by M13.22i (M13.22d review round 2, T1): the pause IS
    injected between the check and the send toward each fenced member -- an
    EXPOSED to a writer W ≠ L, a PUBLISH to an interested pod, a journal-served
    `/seg` response through a forwarding pod -- with the successor's FENCE
    delivered meanwhile; the writer must not ack, the interested pod must
    refuse the PUBLISH, and the forwarding pod must refuse the response by its
    `Binstore-Fast-Epoch`, each the only guard left; and the old leader renews several
    times while the successor watches, the successor's first read seeing an
    early version (it must not assign before the LAST renewal's send instant
    plus TTL minus the margin, on one global clock). ⚠️ Added by M13.22i
    (M13.22a review round 3, T1): a graceful release, then a pod that takes the
    lease, creates its roster and is force-deleted while running, then a
    successor by the early challenge that read the released lease, then the
    deposed pod's late `LATEST` write and its acks at `q = 1`, W = L (the
    successor's re-walk must restore both waits); and a term whose leader
    crashed with its wall clock stopped, its successor departing gracefully
    before its own monotonic wait ended, then the next successor (the
    exemption must not hold: an unclosed term's leader did not depart). ⚠️
    Added by M13.22i (M13.22b review round 1, T3): a holder's container restarted
    between seeing a higher epoch and receiving a deposed leader's frame of the
    lower one (refused: the seen epoch is in the fsynced epoch file, not only in
    memory).
11. **I6 under the writer's death**: kill W after the ack and before the upload;
    the record is committed at its acked offset; no copy is released before the
    commit delta covering it is in the chain (obligation 11).
12. **I6 under the leader's death, and across terms** (obligation 2): kill L
    after the ack and before the upload; the new leader collects and uploads, and
    the record is committed at its acked offset; and with a middle leader that
    died after partly fencing, the next leader still recovers the first term's
    entries. Killing L also between the segment PUT and the commit delta, and
    between the delta and the release, commits every entry exactly once
    (obligation 11); and a live subscriber across the takeover delivers each
    committed offset to the plugin exactly once.
13. **AZ loss and quorum loss** (obligations 3, 4, 9, 10): `FastRecoveryModelTest`
    drives, over seeds, fast writes from several pods on shared streams, indices
    at `q` 1, 2 and 3, `wal_quorum` raised and lowered on live indices, streams
    driven up to and past the per-stream bound, replicas answering out of order,
    and kills of any set of pods (the leader, writers, replicas, pods in partly
    alive AZs) and of whole AZs, each dead pod's UID deleted or not, a pod
    rescheduled under its old name, partitions and unreadiness of live pods,
    and graceful departures of non-leader pods.
    The model's rule: a pod whose UID survives (a container restart, a
    partition, unreadiness) keeps its journal; a deleted UID loses it. Against
    that ground truth of which pods hold which copies and which offsets were
    exposed (acked, published or served), **at every step** (safety): no
    exposed offset is committed with other content or assigned again; a void
    covers an exposed offset only where the model says every copy of an exposed
    entry of that stream is gone, and covers never-exposed offsets only of an
    index that lost quorum; no void covers an offset whose entry survives on a
    pod whose UID survives; every void lies within `[c₀, c₀ + B)`, where `c₀`
    is the stream's committed next offset when the takeover BEGAN and `B` the
    per-stream bound -- no committed offset is ever voided; no stream of an
    index is voided unless M13.22's quorum-loss predicate holds for it,
    evaluated by the oracle over the MODEL's events: its own joins,
    departures and deletions, never the protocol's roster object, and the
    `wal_quorum` values, each term's leader and each term's closure the model
    saw written to rosters in its store, checked for truth (a closure only
    after every stream of the term is decided and committed) -- every recorded value is one the harness set, and no
    write is assigned under a value before the model saw it recorded; no exposed offset precedes its quorum
    or a lower offset's. **At quiescence** (liveness), once every dead pod's UID is
    deleted and every live rostered incarnation is reachable, within a bounded
    number of model
    steps -- after quorum loss too: every stream's takeover completes; every
    exposed offset is committed with its content or lies in a void; assignment
    resumes on every stream whose `q` is at most the live AZs (a larger `q` stops
    acks, obligation 3, not the takeover); and for an index for which the
    predicate does not hold, no takeover waited except on a surviving,
    unreachable rostered incarnation. A
    consumer skips a void with a counted gap. Named seed-fixed cases, each
    asserted by the same oracle: at `q = 2`, the leader's node in AZ1 and a replica's node in AZ2 lost
    while both AZs keep other pods; `wal_quorum` 1→2 and 2→1 across a leader
    loss; a `q = 1` stream past the bound at the leader's loss; the writer in
    another AZ before its copy holds its offsets; 102 answered before 101 and the
    leader lost with 101 on no pod; a `q = 1` and a `q = 2` index on a leader
    whose AZ is lost; at `q = 2`, the leader lost and the only surviving holder
    partitioned, then healed; at `q = 2`, the leader lost and the only surviving
    holder unready (out of the EndpointSlice) but reachable; a `q = 1` index
    losing its leader's AZ with one stream holding an acked uncommitted tail and
    another fully committed; a `q = 3` index on three AZs with one lost; an idle,
    fully released `q = 1` index losing its leader (quorum loss: its streams may
    be voided within the bound); at `q = 2`, the leader in AZ1 and a pod in AZ2
    whose copies were all released both lost (quorum loss by the roster); a
    rolling restart reaching the leader last, then the new leader's crash at
    `q = 2`, voiding nothing; at `q = 2`, pods in AZ2 and AZ3 departing
    gracefully while holding the second copy of exposed, unreleased entries,
    then the leader in AZ1 crashing -- no void, every exposed offset committed; a takeover over many streams writing recovery chain
    entries one per committed batch, their number independent of the stream
    count, each stream's commits and voids in one of them. ⚠️ Added by M13.22i
    (M13.22 review round 1): the driver's events include a graceful leader
    shutdown and a paused leader (one that stops and resumes, its store writes
    delayed), and more named cases -- a never-exposed copy truncated by one
    takeover surviving on a holder, its offset then reassigned and exposed, then
    quorum loss with EVERY copy of the reassigned entry lost (the stale copy must
    be superseded by the recorded decision, not merely outranked by a higher
    epoch's copy, and not committed); a graceful
    leader shutdown while its own takeover still waits on a partitioned holder
    (the successor must still find the earlier term); a crash before, and one
    after, a quorum-loss takeover's recovery entry; and a deposed leader writing
    `LATEST` after its successor, then a later takeover (the newer term must
    not be hidden from it); a crash
    after EACH of a quorum-loss takeover's store writes, in turn (no state
    between its commits and its voids); an exposure in the new term after its
    recovery entry, then a crash before the decisions write, then every copy of
    the new entry lost while a stale copy of the earlier term at the same offset
    survives (assignment must not have resumed, and the stale copy must not be
    committed); a retry, after a
    takeover that discarded the retried batch's entry above a hole, of the same
    key (answered with new offsets, never the discarded ones); a default-path
    run for an undecided stream during a takeover, by a stale registration and
    by a switch to `wal=false` (committed only after the stream is decided, never
    over an exposed entry or inside a void); a graceful departure while two
    terms are unclosed, then losses in the earlier term one AZ short of quorum
    loss unless the departure is counted (it must not be counted in either). ⚠️
    Added by M13.22i (M13.22 review round 3): the driver's events include a
    `wal_quorum` change and a switch to and from `wal=false`; and named cases --
    a switch's discard, then a takeover finding a copy of a discarded entry
    (superseded within its own term, by decision number); a writer's CONFIRM
    arriving during a switch pause (the discarded entry is never exposed); a
    default-path run arriving before a takeover's walk completes (never
    committed over an exposed entry the walk has not yet found); at `q = 2` with
    W outside the leader's AZ, an entry assigned and exposed after a switch's
    decision, held by L and W, then L lost -- the takeover must find W's own copy
    live (W journaled the leader's `assignedAfter`, not 0) and commit it, and in
    the converse a discarded entry's surviving copy on W must be superseded (W
    did not journal a value above the decision's number); a leader that released early, then its
    successor's crash before the wait the first leader owed (the next successor
    still waits it); a gracefully replaced leader's later loss (never counted as
    quorum loss). ⚠️ Added by M13.22i (M13.22a review round 2, T3 and T5): `wal_quorum`
    raised from 1 to 2 within a term, then the leader PARTITIONED (not deleted)
    while holding entries exposed at `q = 1` -- the takeover must not decide
    early by AZ coverage at the latest value (it waits, by `q_min`); and a
    departing holder holding, on one stream and one epoch, a switch-discarded
    entry and a later live exposed one -- only the first is answered superseded;
    ⚠️ added by M13.22i (M13.22a review round 3, T3): a departure between a
    switch's decision write and the commit of its exposed tail, one
    `(epoch, assignedAfter)` group straddling the resume offset -- its exposed
    part is answered pending until committed, and only the part at or above the
    resume offset superseded. ⚠️ Added by M13.22i (M13.22b review round 1, T1 and
    T2): an early-challenge takeover in which the old leader, still running,
    answers COLLECTED and then is offered a `q = 1`, W = L batch and a catch-up
    read (it must assign and serve nothing; the offset is never committed with
    other content); and at `q = 2`, W outside the leader's AZ killed between
    ASSIGNED and CONFIRM with the leader alive, then a second writer on the same
    stream -- acked within the replica timeout plus one upload on the injected
    clock (the copy replaced, or the entry discarded). ⚠️ Added by M13.22i
    (M13.22b review round 2, T1 and T2): the same with NO spare pod in W's AZ or
    another uncovered one and W's pod DELETED (its UID gone), so the discard
    path is forced -- the second writer
    acked at the discarded offset only after the decision is durable, and the
    leader crashing before, and separately after, that decision write (W2's
    acked entry committed, never superseded); a late REPLICA_ACK and a late
    CONFIRM for the discarded entry arriving after its offset is reassigned in
    the same term (counted toward nothing); and a `q = 3` index on two live AZs
    for many flush intervals (its batches held unassigned, the roster's
    decisions unchanged). ⚠️ Added by M13.22i (M13.22c review round 1, T2 and T3):
    a holder that stays ready but refuses with backpressure at its journal cap,
    and one whose fsync stalls past the replica timeout, each the only pod of
    its AZ at `q = 3` -- no discard and no decision write while it stays ready,
    batches held unassigned, acks resuming when it drains; and a `q = 2`
    index whose writers and subscribers are all in the leader's AZ -- acked
    (pods of other AZs joined the term though they neither write nor
    subscribe), also after a takeover. ⚠️ Added by M13.22i (M13.22c review round 2,
    T1-T3): a holder's container killed mid-append, restarted, then an entry
    appended, acked and exposed, then a second restart (the entry recovered:
    the torn tail was truncated first), and kills during compaction and during
    the epoch file's write (the old or the new file whole, never a mix), and a
    restart after DROP records (dropped entries not reported held); a deposed
    leader's join, decision and term-record write each landing between the
    successor's walk and its fence write (the successor's members, decisions
    and held indices taken from the version it fenced); and, after a leader
    crash, a `wal=false` index and a fast index with no value in an earlier
    unclosed term committing default-path runs within the walk's store
    requests on the injected clock -- not waiting for `notBefore` or any
    stream's decision -- while an index with such a value waits. ⚠️ Added by
    M13.22i (M13.22c review round 3, T1 and T2): at `q = 2`, entry e0 waiting
    on a slow ready pod, entry e above it complete on L and a pod P, P deleted,
    the upload its loss triggers, e0 then confirmed, then L lost -- e is
    exposed only after a replacement copy holds it (never on L's copy alone);
    ⚠️ added by M13.22i (M13.22d review round 3, T1): the same with NO
    replacement pod, and the slow pod's REPLICA_ACK completing e0's quorum
    while the discard's roster write is in flight -- e0 is never exposed (the
    stream stopped exposing before the frontier was read), its writer is told
    `discarded`, and no exposed offset is reassigned;
    and, with the model's `B` set small, B offsets committed past a decision
    on a stream, the decision pruned, then a takeover and a departure that
    meet a copy it superseded -- the stale copy is skipped as committed-below,
    a new entry's `assignedAfter` is the next `seq` (not the list's size), and
    no `seq` is reused; ⚠️ added by M13.22i (M13.22d review round 2, T2): a
    pod's JOIN whose roster write fails against a successor's fence (the pod
    holds nothing and is told nothing joined until the write lands). ⚠️ Added
    by M13.22i (M13.22d review round 1, T1 and
    T3-T6): with the model's journal cap set small, a RELEASE to the only
    holder of AZ3 at `q = 3` dropped -- the holder's half-cap HELD answered by
    HELD_STATUS drains it and acks resume with no leader change; a holder
    leaving the EndpointSlice (not deleted) treated as a loss exactly as a
    deletion is; an entry whose offsets would reach `c₀ + B` held, never
    assigned (`c₀ + B − 1` the last assignable); the oracle's join counted
    when the roster write holding it lands, and its departure when the
    `DEPARTED` write lands; a crash takeover completing within one renewal
    interval plus TTL plus the margin of the old leader's last renewal, plus
    the collection and one upload paced by `min_upload_interval`, on the
    injected clock (an upper bound, as ADR-0081 §10 states it); ⚠️ added by
    M13.22i (M13.22d review round 3, T4): a commit request without the
    answer's `Accept` answered with the bare delta when every run is on a
    default-path stream, and refused whole when one run's stream is fast. ⚠️
    Added by M13.22i (M13.22e review round 1, T2-T4): a leader paused past a TTL
    before its walk, then reading `LATEST` and a roster above its epoch (it
    writes no fence, roster or `LATEST`; `LATEST` and `fencedBy` never fall;
    the live leader keeps assigning); a departure completing within one
    upload plus one `min_upload_interval` of its last pending entry's commit,
    with no grace-period timeout (the re-sent HELD drives it); and a departing
    pod, the only one of its AZ, never counted at admission. ⚠️ Added by
    M13.22i (M13.22e review round 2, T1, T3 and T4): at `q = 2` the only holder of
    AZ2 near its cap with stale entries from dropped RELEASEs while in-term
    writes continue on several streams -- acks never stall past one HELD
    round trip, and the holder never holds an entry above one it refused; at
    `q = 2`, entry 101 exposed on L and X (AZ2) and 102 on L and Y (AZ3), then
    L and X deleted (quorum loss: 102 committed, never discarded, only 101
    voided); and the pruning case's stale copy placed at `resume + B − 1`.
    ⚠️ Added by M13.22i (M13.22e review round 3, T1-T3): with the model's
    upload size set small, a quorum-loss recovery batch split by size and a
    crash between its entries (every stream's commits and voids in the same
    entry, so no committed run sits above an unvoided hole); a pod
    rescheduled under its predecessor's name and endpoint into another AZ,
    joined, then sent a REPLICA addressed to the predecessor (refused; the
    copy counts for no AZ); and the leader's segment PUTs and chain appends
    counted through a takeover while resumed streams upload (at most one of
    each per `min_upload_interval` in all); and a holder's lost HELD exchange
    above half its cap (re-sent; acks never stall); and a truncated
    COLLECTED (the member not answered until its `last` page). ⚠️ Added by
    M13.22i (M13.22f review round 1, T1-T3): a `q = 1` index whose term-3 leader
    was deleted, term 3 later closed, then a takeover after a later leader's
    partition (no quorum loss: a closed term's losses never count, and the
    oracle's closure input says so); at `q = 2`, a term-5 leader shutting down
    while holding an exposed term-4 entry whose other copy is on the
    partitioned term-4 leader (it stays undeparted in roster 4, and the
    successor waits for it rather than decide early or void); and `q` raised
    from 1 to 2 across terms, term 1's leader partitioned in the same AZ as
    term 2's, which crashes (AZ coverage evaluated over every unclosed term
    with its own `q_min`: no early decision). ⚠️ Added by M13.22i (M13.22f review
    round 2, T2-T4): a quorum-loss stream with no recovered entry receiving a
    default-path run during the takeover (held until decided like any
    other); a member answering COLLECTED only after its raised epoch is
    fsynced (a crash between leaves the old epoch and no answer); and one
    COMMIT spanning a `q = 1` and a `q = 2` index, the writer outside the
    leader's AZ (each run journaled with its own `walQuorum`, and the copy
    required for the `q = 2` run only). ⚠️ Added by M13.22i (M13.22f review
    round 3, T2 and T3): the switch-to-`wal=false` case run with a catalog
    holding no other fast index (the takeover still walks and holds); and a
    checkpoint and a backfill taken over a recovery entry with voids (the
    stream's next offset stays past each void); and (M13.22g review round 2,
    T3) a roster whose leader's `LATEST` write failed against a newer term
    (an orphan: no JOIN accepted, nothing assigned, never walked, ignored by
    the oracle); ⚠️ added by M13.22i (M13.22h review round 1, T1): with no fast
    index, a successor taking the lease by the early challenge from a live
    predecessor, then default-path commits on an index X, then X made fast
    while the predecessor is still valid on its clocks -- the successor's
    roster and `LATEST` were written at its takeover, so the predecessor's
    term-record write for X fails against the successor's `fencedBy` and it
    assigns nothing (⚠️ corrected by M13.22i review round 2, T3), and no
    offset the successor committed is acked again with other content; ⚠️
    added by M13.22i (M13.22h review round 2, T1 and T2): with no fast index in
    the successor's catalog, a default-path run on X arriving before the
    successor's step 3 while the predecessor's term-record write for X lands
    between the successor's walk and its fence write (the run held until step
    3, committed above anything the predecessor exposed, never at an offset
    it acked); and an interested pod that has never journaled, fenced, then
    restarted, then sent a deposed leader's lower-epoch PUBLISH (refused: its
    seen epoch was taken from the lease at start). ⚠️ Added by M13.22i (M13.22b review round 3, T1 and
    T2): at `q = 2`, entry A waiting on a CONFIRM, entry B above it complete,
    an upload triggered, then A discarded -- nothing above A is committed
    before A, and every offset is committed exactly once, voided or never
    exposed; and at `q = 2`, the leader and another pod in the leader's AZ both
    lost (not quorum loss: no void).
    And one liveness case for AZ coverage: at `q = 3`, one pod of AZ3 partitioned and
    holding nothing, a leader loss -- the index's streams decide without waiting.
14. **Millisecond visibility, whichever pods write** (obligation 6): two writers
    on one partition, replicas completing out of order; a consumer on a third pod
    receives each batch in offset order (obligation 10) from the leader's publication before its
    segment is committed and without catch-up, measured on an injected clock as
    zero added waits; a subscriber served proxy (ADR-0076) in another zone gets
    each fast batch before the upload.
15. **Switching under skew, both directions** (obligation 5): two pods receive
    the `wal` change at different times while both write one stream, in each
    direction, including a default-path delta PUT in flight and one ambiguous;
    every offset of the stream is committed exactly once, in order, and acked
    once. ⚠️ Added by M13.22i (M13.22 review): with a fast writer that never stops, the
    default-path run commits within one roster write and one upload on the
    injected clock; and with unexposed entries that cannot complete their
    quorum (a `q = 3` index on two live AZs), the switch still commits, the
    entries discarded and their writers unacked. ⚠️ Added by M13.22i (M13.22a review round 2, T1): a
    leader crash, and separately a writer retry, between the two deltas of one
    split segment -- the request is acked only after the last delta, and the
    retry is answered only once every run of the segment is committed (the
    paused run committed by the successor from the uploaded segment, never
    lost and never moved to a new segment; every RECORD committed exactly once,
    by record identity, not only every offset -- added by M13.22i, M13.22d review
    round 2, T3). ⚠️ Added by M13.22i (M13.22c review
    round 1, T1): a segment with runs on a default-path stream and on one the
    catalog has just made fast, its commit ambiguous and retried -- the fast
    stream's run redirected both times and never committed from the segment,
    and the request acked only after the redirected records' fast ack.
16. **Shutdown and handover** (obligation 8): a leader closing with an
    unreleased journal uploads and commits it before releasing its lease, and its
    successor assigns fast offsets without the crash-takeover wait. ⚠️ Added by
    M13.22i (M13.22 review): a writer's CONFIRM arriving after the leader began shutting down is
    not answered as complete, and on one global clock the old leader exposes
    nothing after its successor's first assignment. ⚠️ Added by M13.22i (its
    review round 2, T3): a leader shutting down at `q = 1` crashing after it
    marked itself `DEPARTED` would be quorum-safe only if every exposed entry
    was committed first -- so a crash between its last commit and its
    `DEPARTED` write, and the order itself (no `DEPARTED` write while an exposed
    entry is uncommitted), are asserted.
17. **Cost and capacity** (obligation 7): a fleet workload above `cap /
    flush_timer`, and one hot stream above `B / flush_timer`, each below its
    stated ceiling, are acked with no added wait on the injected clock -- not
    merely unrefused; above the ceiling the hot stream is backpressured, not
    refused; a stream waiting at its barrier delays no other `wal=false` index's
    acks; after a holder of an unreleased entry is lost, the leader's segment
    PUT follows within `min_upload_interval` on the injected clock; the
    leader's segment PUTs never exceed one per `min_upload_interval`,
    on the hot stream too; over a fast workload, commit and data PUTs per MiB
    are no higher than the default path's at the same cadence; roster writes
    are at most one per pod joining or departing a term, and join writes at
    most one per `min_upload_interval` however many JOIN frames arrive, and term-record writes
    at most one per `min_upload_interval` however many indices change
    `wal_quorum` at once; ⚠️ added by M13.22i (M13.22a review round 1, T2): with
    switches on 1,000 streams at once, switch-decision roster writes are at most
    one per `min_upload_interval`, under readiness flaps of every holder the
    roster holds at most one decision per stream per flap, each pruned once
    `B` more offsets of its stream commit (decisions never left unpruned), and so are discard decisions over 1,000
    streams losing a holder at once, and so are the tail uploads' segment PUTs
    and the deltas -- never one per switched stream -- and a takeover over 1,000
    streams writes at
    most one decisions write per committed batch, the batch count independent of
    the stream count (both counted by the store's request counter); per fast MiB, for `q` in 1 and
    2, the quorum frames' cross-AZ bytes equal `max(q − 1, [W ∉ AZ(L)])` copies
    within 5%, and the publication frames' are at most one copy per interested
    pod in another AZ, plus 5% -- an upper bound, since a pod that already holds
    the entry need not be sent it again,
    with each frame's overhead computed from its format, not measured into the
    bound; and the fast-mode metrics M13.36 adds -- ack and quorum-wait
    latency, journal bytes, upload triggers by cause, voids -- are exported
    under observability.md's names, without per-index labels.
18. **Latency on M9's rig** (NFR-15): over a sustained run at `q = 2`, a fast
    index's ack p99 and publish-to-consumer p99 are **each below 10 ms**, asserted
    by the IT, with p50s and the fast outage after a leader failover recorded
    (T3). A p99 at or above 10 ms is recorded OBSERVED-NOT with its number and
    the milestone does not claim NFR-15; the bound is never moved. NOT-RUN on
    AWS.
19. **The default path carries no record through fast mode**: with no index at `wal=true`, every
    existing suite is green and no fast frame carries a record. ⚠️ Amended by
    M13.22i (M13.22f review round 2, P1; the dormant mode that tried to keep
    "no fast endpoint is called" was withdrawn after M13.22g review round 3;
    restated after M13.22h review round 1, P1 and P3): in a fleet with no
    `wal=true` index every mechanism of ADR-0081 still runs except carrying
    records -- each takeover walks, creates its roster, writes `LATEST`,
    fences, sends FENCE and collects, and closes; every node JOINs; every
    departure runs DEPART and HELD_STATUS and its `DEPARTED` writes; and
    default-path commits are held after each lease change until the
    takeover's step 3 -- while no node journals an entry, writes an epoch
    file, or carries a record in a fast frame; the existing store-count pins
    are moved by exactly those store requests, each move stated in its
    commit.

## Test plan

| Criterion | Tier | First failing test | Mutation it must kill |
|---|---|---|---|
| 1 | T0 (gate) | `FileSizeCeilingTest` cases: `DefaultIngest` and `Assembly` refused at 500, `ConsumerClient` and `LocalSequencer` at 600 | M13.1 leaving `DefaultIngest` and `Assembly` untouched; a ceiling of 700 for the other two |
| 2 | T1, T3 | `NodeProcessPortTest`, `NodeProcessLostPortIT` (a real node whose first port is held) | no retry after a lost port, or `startProbing` given one attempt (⚠️ a public `start` that bypassed `startProbing` is NOT killed: the IT enters through the port-source seam); a retry on any other death -- a real dead child whose log is another refusal or another port's, classified as a lost port (M13.2a) -- or on any other runtime failure; a dead child for any reason waiting out 120 s; unbounded retries; a lost port read from another port's, a port prefix's or another refusal's message; a dead child's refusal not leaving the wait as a `PortLost`, or only after 120 s; the node not closed before a retry; `portLostIn` not walking the cause chain |
| 3 | T3 | `PartitionVisibilityIT` (measurement) | the evidence line cites the measurement file's per-run table, at least ten rows, or is NOT-RUN |
| 4 | T1 | `SubscriptionReaderConnectionTest`: 30 subscriptions closed mid-poll; an answer after the close; a frame after the listener's close; a handshake after the close; a closed subscription backing off | the reader interrupted on close; both stop checks removed; `deliver`'s per-frame check removed; the check before the reconnect block removed; the ladder kept until the reader exits |
| 5 | T0/T1 | `SilentDefaultsGoneTest` (reflection) and `FetchBackoffClockWiringTest`'s started-client case | any of the three defaults left in main; a client built on a clock-less policy |
| 6 | T0 (gate) | `MilestoneEvidenceTest` harvest-enumeration case (M13.40) | a harvest ID missing from the enumeration |
| 7 | T1 | `FastSettingsRegistrationTest` (registration and a live update), `IndexRegistrationV2GoldenTest` | a setting dropped in the registrar or the codec; the catalog ignoring a live settings change |
| 8 | T1 | `FastAckBeforeStoreTest` | the ack after the segment PUT; a roster or term-record write on the ack path with nothing pending |
| 9 | T0/T1 | `FastQuorumPlacementTest` | a replica set short of `q` AZs; the ack before a replica's acknowledgement; a `q = 3` ack with two live AZs; EXPOSED, a leader-writer's ack or a CONFIRM sent before the group fsync; REPLICA_ACK sent before the holder's group fsync |
| 10 | T1 | `FastDeposedLeaderTest` (a delayed response; a stopped monotonic clock; a stopped wall clock; an early-challenge takeover), `FastEpochFenceTest`, `FastRosterTest` | the renewal instant taken at the response; lease validity read from a clock that stopped (either clock alone); a successor assigning before the old lease's expiry plus the margin after an early challenge; the leader left off its roster; a replica accepting a lower epoch; an unrostered writer admitted; a margin of 0 (the skew cases); lease-time checks removed with W = L (only the epoch fence left); the successor's monotonic wait left out (the stopped-wall-clock case); validity checked only at assignment, not before each send; `seenMono` kept from a first read of an older version; the waits not recomputed on a re-walk; the graceful exemption judged only on the newest unclosed term; the lease read at container start skipped (a pod without a journal accepting a lower epoch after a restart); the margin left out of the successor's monotonic wait or the leader's validity (the rate-skew cases); a writer acking on a lower-epoch EXPOSED; an interested pod accepting a lower-epoch PUBLISH; a forwarding pod ignoring `Binstore-Fast-Epoch` |
| 11 | T1 | `FastWriterDeathTest` | L's copy released before the upload; copies released when the segment PUT succeeds, before the delta |
| 12 | T1 | `FastLeaderDeathTest` (kills before the upload, between PUT and delta, between delta and release), `FastMultiTermTakeoverTest`, `FastTakeoverSubscriberTest` | assigning before recovery; skipping a recovered entry; collecting only the last term; copies released before the delta; a recovered entry below the chain's next offset committed again; a re-published offset delivered twice |
| 13 | T1 | `FastRecoveryModelTest` (seeds and the named cases), `FastQuorumLossVoidTest`, `FastExposureBeforeQuorumTest`, `FastInOrderExposureTest` | an AZ counted lost only when all its pods are gone (the AZ1+AZ2 case); `wal_quorum` for recovery taken from the catalog or the recovered entries (the 1→2 case: the exposed `q = 1` tail left neither committed nor voided); a lowering assigned under before its term record is durable (the 2→1 case: the record-before-assignment check); a value recorded that the harness never set; one recovery chain entry per stream in a takeover (counted by the store's request counter in the many-streams case); the per-stream bound unchecked at assignment (the past-bound case); a void whenever any incarnation is gone; a void over a partitioned holder's surviving entry (the partition case); a pod out of the EndpointSlice treated as gone; a stall forever after quorum loss once the UIDs are deleted; a gracefully departed pod left on the protocol's roster, and a departing pod releasing its uncommitted copies without an upload (both killed by the AZ2+AZ3 departure case, the oracle reading departures from the model); a stream of an index that did not lose quorum voided (the mixed-`q` case); no void at `q = 1` leader loss; the tail voided to `Long.MAX_VALUE`; a void starting at the dead term's first assigned offset, over committed and released offsets (the committed-stream case); a stream with no recovered entry left unvoided after its quorum loss; catch-up or publication before the quorum; the writer's offsetless copy counted; 102 exposed before 101's quorum; a recovered 102 committed above an unrecovered 101; ⚠️ added by M13.22 -- the walk stopping at a roster closed while an earlier one is unclosed; `LATEST` written without `putIfMatch`; a takeover's voids and recovery commits in separate chain entries; a superseded stale copy collected as live; assignment resumed before the decisions write; no early decision by AZ coverage (the `q = 3` partition case); a superseded copy kept only because a higher epoch outranks it (the strict P1 case); a retry answered from a discarded entry; a default-path commit on an undecided stream; a departure marked only in the current roster; exposure during a switch pause; a default-path commit before the walk; an inherited wait shortened by an early release (the round-3 cases); a writer journaling `assignedAfter` as 0, or as its maximum; early decision by the latest `wal_quorum` rather than `q_min`; departure status answered per epoch rather than per `(epoch, assignedAfter)`; a group answered superseded when any of its offsets is at or above a resume offset; a leader assigning or serving after a higher epoch reached it; a missing required copy never replaced or discarded (the stream wedged while the leader lives); an answer matched by offset alone; assignment resumed before a discard decision is durable; the cursor not reset to the resume offset; batches assigned while `q` exceeds the reachable AZs; every quorum-complete entry uploaded rather than the exposed prefix; the leader's own AZ counted toward quorum loss; a backpressure refusal or a slow holder treated as a loss and discarded; admission counting a full or slow holder; only writing or subscribing pods joining; appending after a torn tail without truncating; compaction or the epoch file rewritten in place; DROP records ignored on recovery; the first read's members, decisions or held indices kept after a failed fence write; every default-path commit held until the takeover ends; a lost holder's copy still counted for an unexposed entry; `assignedAfter` taken as the decisions list's size; pruning at the resume offset rather than `B` past it; a pruned `seq` reused; HELD at half the cap never sent, or HELD_STATUS's release offset ignored (the dropped-RELEASE case); loss judged by UID deletion only; offset `c₀ + B` assignable; `seenMono` taken at the lease CAS rather than the first read (the upper-bound case); JOINED sent before the roster write holding the join lands; exposure continuing while a loss-triggered discard's decision write is in flight; BCAN sent to a writer that did not ask, or a no-Accept run on a fast stream committed; `LATEST` or `fencedBy` written downward by a leader that read a newer term; a departing pod counted at admission; HELD not re-sent after RELEASE (a departure that never ends); a holder's cap equal to the leader's (the stall behind a refused lower entry); the loss branch discarding entries above the first hole; pruning at `resume + B/2`; an answer counted from an incarnation other than the one addressed; a size-split batch with a stream's voids apart from its commits; recovery uploads on their own timer; HELD sent only once; a COLLECTED read complete without its last page; a closed term's losses counted; a shutting-down leader marked `DEPARTED` in an earlier roster while holding a pending entry there; AZ coverage evaluated over the newest unclosed term only; default-path runs held only for streams with collected entries; COLLECTED sent before the raised epoch is recorded; a batch-level `walQuorum` or `copyRequired`; a checkpoint or backfill ignoring a recovery entry's voids; an orphan roster walked, joined or counted by the oracle; a roster and `LATEST` written only when the first fast index appears; default-path commits admitted right after the lease write when the catalog holds no fast index |
| 14 | T1 | `FastVisibilityTest`, `FastProxySubscriberTest` | publication only through the writer's hub; out-of-order publication; catch-up blocking until the chain commits; a cross-zone subscriber's `/seg` GET failing until the upload |
| 15 | T1 | `FastModeSwitchTest` (two pods, skewed registrations, both directions) | a durable commit assigned before the fast tail commits; a fast cursor taken from `nextOffset` while a durable PUT is in flight or ambiguous; fast assignment continuing while a default-path run waits (the never-stopping writer); waiting for an unexposed entry's quorum at a switch (the `q = 3` case); ack after the first delta of a split segment; a retry answered from a partly committed segment; a retry committing a missing run whatever its stream's mode; a request acked before its redirected run's fast ack; a paused run re-sent by the writer and committed twice |
| 16 | T1 | `FastLeaderShutdownTest` | the lease released first; a TTL wait after a graceful handover; exposure after the shutdown began (the late CONFIRM); `DEPARTED` written before the exposed entries commit |
| 17 | T1 | `FastCostTest`, `FastCapacityTest`, `FastBarrierIsolationTest`, `FastMetricsTest` | a PUT per batch; a metric left unregistered or labelled per index; a replica in W's AZ; publication to an uninterested pod; uploads only at the cadence; no upload on a holder's loss (the entries left on the leader alone until `flush_timer`); a term-record write per index whose `wal_quorum` changed; a roster write per ack; no upload on a stream nearing its bound, assignment waiting at `B` instead (an added wait below the ceiling); an upload per `B` records of the hot stream, unthrottled (PUTs above one per `min_upload_interval`); the hot stream refused at its bound; the writer-to-leader hop left out of the meter at `q = 1`; publication sent twice to one pod; a barriered stream holding a batched delta that delays another `wal=false` index's acks; a switch-decision roster write per stream; a decisions write per decided stream; a tail upload or delta per switched stream; a discard decision write per stream rather than coalesced; decisions never pruned (roster bytes growing with flaps); a roster write per JOIN frame |
| 18 | T3 | `FastLatencyIT` (measurement, p99 < 10 ms asserted) | a 50 ms delay before the ack; publication deferred to the upload |
| 19 | T1 | the existing suites; `FastWalFalseFleetTest`, which drives a crash takeover, a graceful takeover and a rolling restart in a fleet with no `wal=true` index and asserts each request ADR-0081 §1 and §9 list is MADE (roster, `LATEST`, fences, close, `DEPARTED`) as well as that none beyond is | a record carried by a fast frame, an entry journaled or an epoch file written in a fleet with no `wal=true` index; a takeover or departure in such a fleet skipping its roster, `LATEST`, fence, close or `DEPARTED` write; a takeover store request beyond those listed; a join write per JOIN frame |

Suites extended: the commit-protocol simulation (fast commits, a deposed leader,
leader death and quorum loss over seeds), the store conformance suite (the
pre-assigned commit), the cost assertions. T1 tests use an in-process fleet over
`MemoryBinStore` with injected clocks, fsync, transport and pod liveness; the
journal's fsync and the Kubernetes pod lookup are injected seams.

## Risks

- **R2 may not name the trigger write's cause.** Then criterion 3 records it
  OBSERVED-NOT with the variants measured; the bound is never moved.
- **M12.27 may be a JDK behaviour, not ours.** Then criterion 4 is met by its
  refutation, with the measurement.
- **The recovery protocol is wrong in a case no test drives.** Mitigation: the
  simulation suite gets fast commits, a deposed leader and leader death over
  seeds, and M13.22's decision records are reviewed before their code.
- **The protocol M13.22 designs is wrong in a case no test drives.** Three SPEC
  review rounds found majors in every draft; M13.22 gets its own review rounds,
  and the simulation suite drives it over seeds. If M13.22 cannot land in three
  rounds it is split, and fast mode's tasks wait for it.
- **Availability after losing more than `q − 1` AZs** (NFR-10): fast commits for
  affected streams stall until the lost pods are confirmed gone, and a stream
  switched to `wal=false` waits behind its uncommitted fast tail. M13.22 states
  the bound, and whether an operator can release it.
- **Fast mode is unavailable for one lease TTL after a crash failover** (10 s by
  default), if M13.22 keeps lease-time fencing. Measured by criterion 18.
- **The leader carries the fleet's fast load** (research 12 §9): assignment,
  journal, replication and publication. Measured by criterion 18; bounded by the
  journal cap.
- **The per-stream bound `B` caps a stream's uncommitted fast offsets**; an
  upload triggered near it keeps a hot stream moving at the cost of smaller
  segments, and above its ceiling the stream is backpressured. M13.22 sizes `B`;
  criterion 17 drives one hot stream.
- **Crashes that accumulate turn a later leader crash into quorum loss.** A
  pod that crashes stays on the term's roster (only a graceful departure leaves
  it, obligation 3), so by the roster rule a later leader loss can count as
  quorum loss and void up to `B` never-exposed offsets per stream as counted
  gaps. A new term's roster starts from the live pods, which bounds it to one
  term.
- **fsync latency on Windows and in containers** may exceed research 12's
  0.05–1.0 ms. Measured by criterion 18.
- **The Kubernetes API is a new dependency of takeover.** Out of Kubernetes (the
  tests, a local run) the pod lookup is injected.

## Decisions

- ADR-0078 amended (R4).
- The fast-mode protocol decision (M13.22): leader-sequenced, -held, -published
  and -uploaded fast batches, and a protocol meeting the eleven obligations above;
  the scoped exceptions to ADR-0001 (a void is a committed hole, after quorum loss
  only), ADR-0012 (fast batches cross AZs to interested pods) and ADR-0013 (the
  writer's WAL is not the upload source).
- The fast-mode formats decision (M13.22): every format listed above.
- NFR-5 conditional on `wal=false`; NFR-10 on `wal=false or wal_quorum ≥ 2`,
  and at `q = 2` on no second holder of an entry being lost within the upload
  the first loss triggers, a `q = 3` index on three AZs not acking while one is
  lost (§ Requirements; M13.22's protocol decision, amending ADR-0013's
  Consequences).

## Tasks

| ID | Task | Serves |
|---|---|---|
| M13.0 | Specify M13 | — |
| M13.1 | R1: split `DefaultIngest` and `Assembly` below 500 along the write path, `ConsumerClient` and `LocalSequencer` below 600, the gate at those numbers | — (quality) |
| M13.1a | R1: `DefaultIngest` below 500: its flush path (active buffer, waiters, ceiling, flush loop) moved to `FlushPath`; its ceiling lowered to 500 | — (quality) |
| M13.1b | R1: `Assembly` below 500: the sequencer and serving construction moved out; its ceiling lowered to 500 | — (quality) |
| M13.1c | R1: `ConsumerClient` below 600, named by the ceiling gate at 600 | — (quality) |
| M13.1d | R1: `LocalSequencer` below 600, named by the ceiling gate at 600 | — (quality) |
| M13.2 | M12.28: `NodeProcess` starts a node that lost its probed port again on a fresh one | — (quality) |
| M13.2a | Split from M13.2 at its review budget: a real dead child that did not lose its port is not retried, and fails the wait at once; criterion 2's row claims only the kills it has | — (quality) |
| M13.3 | R2: `PartitionVisibilityIT` as a distribution | — (evidence) |
| M13.4 | M12.27: the reader connection on close | FR-6 |
| M13.45 | Opened by M13.3: `PartitionVisibilityIT`'s slow drains (2 of 10 green on a day-old RustFS container, 5 of 6 on each fresh one); separate container age and state, memory and the drain's phases (apply, delete), name what applies the trigger's own deferred intent, never moving the bound | — (evidence) |
| M13.5 | R4: ADR-0078 amended for M12.4 | — (docs) |
| M13.46 | Opened by M13.5: quota tickets for a name unknown at admission -- a window write through an alias binds a bucket keyed by the alias (its debt refuses nothing sent to the concrete name), gets a free ticket under an unlimited default (uncharged), and carries the default rather than the concrete override; compute the limit when the ticket binds from the concrete name, issue no free ticket for an unknown name, and pin by tests: the alias cases, the window limit, binding into a bucket already at its cap, and the idle sweep's once-per-expiry throttle | FR-21, NFR-6 |
| M13.47 | Opened by M13.12: a failing live segment starves a due catch-up; decide the order and pin it | FR-6 |
| M13.48 | Opened by M13.14's review round 2: a row's wording, a false comment, a bound deferred ticket's slot pinned | — (quality) |
| M13.49 | Opened by M13.19: the other `413` sites read the body they refuse on a kept-alive connection; measure and close | — (quality) |
| M13.6 | R5: no silent defaults; the started client's clock pinned | FR-6, FR-19 |
| M13.6a | R5: `IndexQuotas`' three-argument and `ConsumerDeliveryQueues`' two-argument constructors removed; `SilentDefaultsGoneTest` per module | — (quality) |
| M13.6b | R5: `ServerConfig`'s older constructors removed | — (quality) |
| M13.6c | R5: `SegmentFetchRetry`'s clock required and `DEFAULT` gone from main; a client started after `holdFailuresWith` pinned to receive the clocked policy (M12.26 T3) | FR-6, FR-19 |
| M13.7 | R3: M11.5 T2 (the top-K log's prices) and the catch-up GET's share | FR-21 |
| M13.8 | R6: quarantine the legacy buildSrc failures | — (harness) |
| M13.8a | Split from M13.8 at its review budget: the quarantine guard refuses a wildcard, a `nativeGateTest` member, a missing class and one whose code names no script under `scripts/`; the build script takes the list from the file alone, its only exclusion | — (harness) |
| M13.9 | R7: gate the compose `mem_limit`; build.md's script references | — (harness) |
| M13.10 | R8: the top-K line's residue | FR-21 |
| M13.11 | R9: the explicit-wait residue | FR-13 |
| M13.12 | R10: two nodes' jitter and the cross-lane attempts pinned | FR-6 |
| M13.13 | R11: two test doubles in production's buffered order | — (quality) |
| M13.14 | R12: admission and quota residue pins | — (quality) |
| M13.15 | R13: behavioural gate-wiring pins; the dump property in every task | — (harness) |
| M13.16 | R14: `PushQueue` abandonment exposed and pinned | FR-5 |
| M13.17 | R15: `ConsumerClient`'s gap report returned; `expiredOr` pinned | FR-6 |
| M13.18 | R16: two stale javadocs | — (docs) |
| M13.19 | R17: `checkCommitMessage` requires a `Cost:` line; `PeerCommitTest` | — (harness) |
| M13.20 | R18: a shared `Ingest` test base | — (quality) |
| M13.21 | The research corpus updates M12 proposed (research 12's banner landed with M13.44) | — (docs) |
| M13.21a | Split from M13.21 at its review budget: the five research-corpus banners, each claim held to its source | — (docs) |
| M13.22 | The fast-mode protocol and formats decision records, meeting the eleven obligations and amending ADR-0013's Consequences for NFR-5 and NFR-10 -- including the quorum-loss predicate and the oracle's copy of it (obligation 4), the per-stream uncommitted-offset bound and the per-term `wal_quorum` record (obligation 4) -- reviewed before any fast-mode code | FR-17 |
| M13.22a | Split from M13.22 at its review budget: the fast-mode protocol and formats decision records, with round 3's findings fixed | FR-17 |
| M13.22b | Split from M13.22a at its review budget: the fast-mode protocol and formats decision records, with M13.22a's round 3 findings fixed | FR-17 |
| M13.22c | Split from M13.22b at its review budget: the fast-mode protocol and formats decision records, with M13.22b's round 3 findings fixed | FR-17 |
| M13.22d | Split from M13.22c at its review budget: the fast-mode protocol and formats decision records, with M13.22c's round 3 findings fixed | FR-17 |
| M13.22e | Split from M13.22d at its review budget: the fast-mode protocol and formats decision records, with M13.22d's round 3 findings fixed | FR-17 |
| M13.22f | Split from M13.22e at its review budget: the fast-mode protocol and formats decision records, with M13.22e's round 3 findings fixed | FR-17 |
| M13.22g | Split from M13.22f at its review budget: the fast-mode protocol and formats decision records, with M13.22f's round 3 findings fixed | FR-17 |
| M13.22h | Split from M13.22g at its review budget: the fast-mode protocol and formats decision records, the dormant mode withdrawn and criterion 19 amended | FR-17 |
| M13.22i | Split from M13.22h at its review budget: the fast-mode protocol and formats decision records, with the deprecated term removed from M13.22g's row | FR-17 |
| M13.23 | The three settings: plugin index settings, `IndexRegistration` v2, the catalog | FR-17 |
| M13.24 | The fast journal: append, fsync seam, entries, the byte bound, release, recovery read | FR-17 |
| M13.25 | The fast frames, the roster object and the recovery chain entry (a takeover's commits and voids in one entry, ADR-0082), with golden files, and every reader of it in the same commit (non-negotiable 8): the chain-entry kinds made a sealed type decoded by exhaustive `switch`, so a reader that ignores the new kind fails to compile -- `DeltaReader`, `CommitChargingBinStore`, `ChainEnd`, `ChainReplay`, `Checkpoint`, `ChainBackfill` -- and the consumer's and the plugin's counted skip | FR-17 |
| M13.26 | The roster and lease-time fencing: join, admission, the epoch fence, the TTL wait, the per-term `wal_quorum` record, and a non-leader's graceful departure (upload, wait, then leave the roster) | FR-17 |
| M13.27 | The leader's fast sequencer: cursor, assignment and the per-stream bound, journal, replica set, answer | FR-17 |
| M13.28 | The replica endpoint: store, epoch fence, release | FR-17 |
| M13.29 | The writer's fast path: commit, offset confirmation, the epoch check, ack with offsets; no fast frame for a `wal=false` index (`FastWalFalseFleetTest`) | FR-17 |
| M13.30 | Publication: interest registration, the leader's push to interested pods, and a cross-zone proxied `/seg` read served from the leader's journal before the upload (`FastProxySubscriberTest`) | FR-17 |
| M13.31 | The upload at pre-assigned offsets, triggered by the journal's fill, a stream nearing its bound and a holder's loss as well as the timer (`FastCapacityTest`), release everywhere, and the per-stream barrier | FR-17 |
| M13.32 | Catch-up of fast streams from the leader's journal | FR-17, FR-10 |
| M13.33 | Takeover: fence, collect per stream, pod-UID liveness, recovery upload, truncation, writing void ranges, exactly-once delivery of a re-published offset across the takeover (`FastTakeoverSubscriberTest`), `FastRecoveryModelTest`, and the commit-protocol simulation extended over seeds | FR-17 |
| M13.34 | Switching `wal` on a live index under skew, including a segment's runs committed across two deltas: the ack after the last, and `IdempotencyWindow` answering a retry only when every run is committed (ADR-0081 §8) | FR-17 |
| M13.35 | Shutdown: the leader uploads its journal before releasing the lease | FR-17 |
| M13.36 | Fast-mode metrics and the cost evidence | FR-17, NFR-5 |
| M13.37 | The fast latency measurement on M9's rig | FR-17 |
| M13.38 | The requirements table matched to the fast-mode decision, ADR-0081 (NFR-8, NFR-5, NFR-9 and NFR-10 conditional); the architecture and operator docs | FR-17, NFR-8, NFR-5, NFR-9, NFR-10 |
| M13.40 | `checkMilestoneVerified` refuses a VERIFIED.md enumeration missing a harvest ID its SPEC lists | — (harness) |
| M13.41 | Specify fast mode: its design, the protocol's obligations, cost, criteria 7 onward, test plan, risks and tasks M13.22-M13.38 | FR-17 |
| M13.42 | Specify fast mode, split from M13.41 at its third round: recovery stated as invariants against a ground-truth model, its enumerated rules moved to M13.22 | FR-17 |
| M13.43 | Specify fast mode, split from M13.42 at its third round: quorum loss judged over rostered incarnations at the `q` in force, the per-stream bound triggering uploads, one void entry per takeover | FR-17 |
| M13.44 | Specify fast mode, split from M13.43 at its third round: the quorum-loss predicate and the oracle's copy of it delegated to M13.22 under stated constraints | FR-17 |
| M13.39 | Close M13 | — (evidence) |

