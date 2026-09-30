# M13 — Fast mode (FR-17), after M12's harvest

## Completion condition

The roadmap's M13 row, restated: **an index with `wal=true` is acked and its
records reach its consumers, on any pod and whichever pods wrote them, within
NFR-15's bound (ack and visibility p99 < 10 ms), before their segment is in the
object store; every acked fast record appears later in a
committed segment at the offset it was acked with (invariant I6), through the
death of the writing pod or of the sequencer leader while a quorum of its copies
survives; the default path is unchanged for every index with `wal=false`; and
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
   model records -- rostered joins, graceful departures, UID deletions, and the
   `wal_quorum` values written to the term record, which a new leader reads --
   never of which pods held copies, under which `q` a write was assigned, or a
   setting the leader received but did not record; and it holds in every case where an exposed
   entry may have lost every copy. A void of never-exposed offsets is allowed
   exactly when it holds; a void over an exposed offset still needs
   every copy of an exposed entry of its stream to be gone. **Every void lies
   within `[c₀, c₀ + B)`** of its stream, `c₀` being the stream's committed
   next offset when the takeover began: a committed offset is never voided,
   though its copies are released and "every copy gone" is then trivially true
   (M13.43 review R2-P1).
   All voids of one takeover are ONE chain entry, and a recovery upload is
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
made during it); the journal file's entry format; the chain's void-range entry
kind.

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
  (coalesced, cost.md rule 6 above), a graceful departure one roster write, all of one takeover's voids one chain
  entry, and a recovery upload segments and deltas at the ordinary cadence.
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
- **The default path:** unchanged; asserted by the existing cost suites.

## Acceptance criteria

1. **The write path is split** (R1): `DefaultIngest` and `Assembly` are each
   **below 500 lines** -- the headroom fast mode needs, not merely the 600 they
   already meet -- and `ConsumerClient` and `LocalSequencer` below 600, all four
   pinned by the ceiling gate at those numbers, and `./gradlew test` is green
   after each split.
2. **The harness binds its own port** (M12.28): `NodeProcess` passes port 0 to
   the child and reads back the port it bound; pinned by a test.
3. **`PartitionVisibilityIT` is measured as a distribution** (R2): at least ten
   runs on M9's rig, `trigger202` and `drainEnd` recorded per run, the RustFS
   memory plateau measured or OBSERVED-NOT, variant B completed, the ~1.03 s
   trigger write investigated and its cause named or OBSERVED-NOT, and `trigger202
   ≤ drainEnd` asserted by the IT.
4. **A reader's connection closes with its subscription** (M12.27): decided and
   either fixed and pinned or refuted with its measurement.
5. **ADR-0078 matches M12.4** (R4) and **no production code carries a silent
   default** (R5): `SegmentFetchRetry.DEFAULT` and the default constructors of
   `ConsumerDeliveryQueues` and `IndexQuotas` are gone from main sources, and the
   plugin's started client receives the clocked policy.
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
   required copy is missing, including a `q = 3` write with one AZ lost.
10. **No deposed leader acks** (obligation 1): a live leader that paused across
    an early takeover, with its renewal's response delayed, and one whose
    monotonic clock stopped, acks nothing its
    successor may reuse -- driven at `q = 1` with the writer other than the
    leader, deleted after the ack; a pod refuses an epoch below the highest it
    has seen; a pod not on the roster can neither write nor replicate.
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
    `wal_quorum` values the model saw written to the term record in its store,
    checked for truth -- every recorded value is one the harness set, and no
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
    then the leader in AZ1 crashing -- no void, every exposed offset committed; a takeover over many streams writing one void chain entry
    and a number of deltas independent of the stream count.
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
    once.
16. **Shutdown and handover** (obligation 8): a leader closing with an
    unreleased journal uploads and commits it before releasing its lease, and its
    successor assigns fast offsets without the crash-takeover wait.
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
    are at most one per pod joining or departing a term, and term-record writes
    at most one per `min_upload_interval` however many indices change
    `wal_quorum` at once; per fast MiB, for `q` in 1 and
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
19. **The default path is unchanged**: with no index at `wal=true`, every
    existing suite is green and no fast endpoint is called.

## Test plan

| Criterion | Tier | First failing test | Mutation it must kill |
|---|---|---|---|
| 1 | T0 (gate) | `FileSizeCeilingTest` cases: `DefaultIngest` and `Assembly` refused at 500, `ConsumerClient` and `LocalSequencer` at 600 | M13.1 leaving `DefaultIngest` and `Assembly` untouched; a ceiling of 700 for the other two |
| 2 | T1 | `NodeProcessPortTest` | the probe-then-hand-over port |
| 3 | T3 | `PartitionVisibilityIT` (measurement) and its `trigger202 ≤ drainEnd` assertion | `trigger202` timed after the drain; the evidence line cites the measurement file's per-run table, at least ten rows, or is NOT-RUN |
| 4 | T1 | `SubscriptionReaderConnectionTest` close case, or the refutation's measurement | the reader client not closed |
| 5 | T0/T1 | `SilentDefaultsGoneTest` (reflection) and `FetchBackoffClockWiringTest`'s started-client case | any of the three defaults left in main; a client built on a clock-less policy |
| 6 | T0 (gate) | `MilestoneEvidenceTest` harvest-enumeration case (M13.40) | a harvest ID missing from the enumeration |
| 7 | T1 | `FastSettingsRegistrationTest` (registration and a live update), `IndexRegistrationV2GoldenTest` | a setting dropped in the registrar or the codec; the catalog ignoring a live settings change |
| 8 | T1 | `FastAckBeforeStoreTest` | the ack after the segment PUT; a roster or term-record write on the ack path with nothing pending |
| 9 | T0/T1 | `FastQuorumPlacementTest` | a replica set short of `q` AZs; the ack before a replica's acknowledgement; a `q = 3` ack with two live AZs |
| 10 | T1 | `FastDeposedLeaderTest` (a delayed response; a stopped monotonic clock), `FastEpochFenceTest`, `FastRosterTest` | the renewal instant taken at the response; lease validity read from a clock that stopped; the leader left off its roster; a replica accepting a lower epoch; an unrostered writer admitted |
| 11 | T1 | `FastWriterDeathTest` | L's copy released before the upload; copies released when the segment PUT succeeds, before the delta |
| 12 | T1 | `FastLeaderDeathTest` (kills before the upload, between PUT and delta, between delta and release), `FastMultiTermTakeoverTest`, `FastTakeoverSubscriberTest` | assigning before recovery; skipping a recovered entry; collecting only the last term; copies released before the delta; a recovered entry below the chain's next offset committed again; a re-published offset delivered twice |
| 13 | T1 | `FastRecoveryModelTest` (seeds and the named cases), `FastQuorumLossVoidTest`, `FastExposureBeforeQuorumTest`, `FastInOrderExposureTest` | an AZ counted lost only when all its pods are gone (the AZ1+AZ2 case); `wal_quorum` for recovery taken from the catalog or the recovered entries (the 1→2 case: the exposed `q = 1` tail left neither committed nor voided); a lowering assigned under before its term record is durable (the 2→1 case: the record-before-assignment check); a value recorded that the harness never set; one void chain entry or delta per stream in a takeover (counted by the store's request counter in the many-streams case); the per-stream bound unchecked at assignment (the past-bound case); a void whenever any incarnation is gone; a void over a partitioned holder's surviving entry (the partition case); a pod out of the EndpointSlice treated as gone; a stall forever after quorum loss once the UIDs are deleted; a gracefully departed pod left on the protocol's roster, and a departing pod releasing its uncommitted copies without an upload (both killed by the AZ2+AZ3 departure case, the oracle reading departures from the model); a stream of an index that did not lose quorum voided (the mixed-`q` case); no void at `q = 1` leader loss; the tail voided to `Long.MAX_VALUE`; a void starting at the dead term's first assigned offset, over committed and released offsets (the committed-stream case); a stream with no recovered entry left unvoided after its quorum loss; catch-up or publication before the quorum; the writer's offsetless copy counted; 102 exposed before 101's quorum; a recovered 102 committed above an unrecovered 101 |
| 14 | T1 | `FastVisibilityTest`, `FastProxySubscriberTest` | publication only through the writer's hub; out-of-order publication; catch-up blocking until the chain commits; a cross-zone subscriber's `/seg` GET failing until the upload |
| 15 | T1 | `FastModeSwitchTest` (two pods, skewed registrations, both directions) | a durable commit assigned before the fast tail commits; a fast cursor taken from `nextOffset` while a durable PUT is in flight or ambiguous |
| 16 | T1 | `FastLeaderShutdownTest` | the lease released first; a TTL wait after a graceful handover |
| 17 | T1 | `FastCostTest`, `FastCapacityTest`, `FastBarrierIsolationTest`, `FastMetricsTest` | a PUT per batch; a metric left unregistered or labelled per index; a replica in W's AZ; publication to an uninterested pod; uploads only at the cadence; no upload on a holder's loss (the entries left on the leader alone until `flush_timer`); a term-record write per index whose `wal_quorum` changed; a roster write per ack; no upload on a stream nearing its bound, assignment waiting at `B` instead (an added wait below the ceiling); an upload per `B` records of the hot stream, unthrottled (PUTs above one per `min_upload_interval`); the hot stream refused at its bound; the writer-to-leader hop left out of the meter at `q = 1`; publication sent twice to one pod; a barriered stream holding a batched delta that delays another `wal=false` index's acks |
| 18 | T3 | `FastLatencyIT` (measurement, p99 < 10 ms asserted) | a 50 ms delay before the ack; publication deferred to the upload |
| 19 | T1 | the existing suites; `FastEndpointsUnusedTest` | a fast frame sent for a `wal=false` index |

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
| M13.2 | M12.28: `NodeProcess` binds port 0 and reports it | — (quality) |
| M13.3 | R2: `PartitionVisibilityIT` as a distribution | — (evidence) |
| M13.4 | M12.27: the reader connection on close | FR-6 |
| M13.5 | R4: ADR-0078 amended for M12.4 | — (docs) |
| M13.6 | R5: no silent defaults; the started client's clock pinned | FR-6, FR-19 |
| M13.7 | R3: M11.5 T2 (the top-K log's prices) and the catch-up GET's share | FR-21 |
| M13.8 | R6: quarantine the legacy buildSrc failures | — (harness) |
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
| M13.22 | The fast-mode protocol and formats decision records, meeting the eleven obligations and amending ADR-0013's Consequences for NFR-5 and NFR-10 -- including the quorum-loss predicate and the oracle's copy of it (obligation 4), the per-stream uncommitted-offset bound and the per-term `wal_quorum` record (obligation 4) -- reviewed before any fast-mode code | FR-17 |
| M13.23 | The three settings: plugin index settings, `IndexRegistration` v2, the catalog | FR-17 |
| M13.24 | The fast journal: append, fsync seam, entries, the byte bound, release, recovery read | FR-17 |
| M13.25 | The fast frames, the roster object and the void-range chain entry, with golden files, and every reader of the void entry in the same commit (non-negotiable 8): the chain-entry kinds made a sealed type decoded by exhaustive `switch`, so a reader that ignores the new kind fails to compile -- `DeltaReader`, `CommitChargingBinStore`, `ChainEnd`, `ChainReplay`, `Checkpoint`, `ChainBackfill` -- and the consumer's and the plugin's counted skip | FR-17 |
| M13.26 | The roster and lease-time fencing: join, admission, the epoch fence, the TTL wait, the per-term `wal_quorum` record, and a non-leader's graceful departure (upload, wait, then leave the roster) | FR-17 |
| M13.27 | The leader's fast sequencer: cursor, assignment and the per-stream bound, journal, replica set, answer | FR-17 |
| M13.28 | The replica endpoint: store, epoch fence, release | FR-17 |
| M13.29 | The writer's fast path: commit, offset confirmation, the epoch check, ack with offsets; no fast frame for a `wal=false` index (`FastEndpointsUnusedTest`) | FR-17 |
| M13.30 | Publication: interest registration, the leader's push to interested pods, and a cross-zone proxied `/seg` read served from the leader's journal before the upload (`FastProxySubscriberTest`) | FR-17 |
| M13.31 | The upload at pre-assigned offsets, triggered by the journal's fill, a stream nearing its bound and a holder's loss as well as the timer (`FastCapacityTest`), release everywhere, and the per-stream barrier | FR-17 |
| M13.32 | Catch-up of fast streams from the leader's journal | FR-17, FR-10 |
| M13.33 | Takeover: fence, collect per stream, pod-UID liveness, recovery upload, truncation, writing void ranges, exactly-once delivery of a re-published offset across the takeover (`FastTakeoverSubscriberTest`), `FastRecoveryModelTest`, and the commit-protocol simulation extended over seeds | FR-17 |
| M13.34 | Switching `wal` on a live index under skew | FR-17 |
| M13.35 | Shutdown: the leader uploads its journal before releasing the lease | FR-17 |
| M13.36 | Fast-mode metrics and the cost evidence | FR-17, NFR-5 |
| M13.37 | The fast latency measurement on M9's rig | FR-17 |
| M13.38 | The requirements table matched to M13.22's decision (NFR-8, NFR-5 and NFR-10 conditional); the architecture and operator docs | FR-17, NFR-8, NFR-5, NFR-10 |
| M13.40 | `checkMilestoneVerified` refuses a VERIFIED.md enumeration missing a harvest ID its SPEC lists | — (harness) |
| M13.41 | Specify fast mode: its design, the protocol's obligations, cost, criteria 7 onward, test plan, risks and tasks M13.22-M13.38 | FR-17 |
| M13.42 | Specify fast mode, split from M13.41 at its third round: recovery stated as invariants against a ground-truth model, its enumerated rules moved to M13.22 | FR-17 |
| M13.43 | Specify fast mode, split from M13.42 at its third round: quorum loss judged over rostered incarnations at the `q` in force, the per-stream bound triggering uploads, one void entry per takeover | FR-17 |
| M13.44 | Specify fast mode, split from M13.43 at its third round: the quorum-loss predicate and the oracle's copy of it delegated to M13.22 under stated constraints | FR-17 |
| M13.39 | Close M13 | — (evidence) |

