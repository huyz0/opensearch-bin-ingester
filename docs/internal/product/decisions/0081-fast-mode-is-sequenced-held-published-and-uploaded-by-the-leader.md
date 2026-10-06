# 0081. Fast mode is sequenced, held, published and uploaded by the leader

Status: accepted
Date: 2026-10-01
Requirements: FR-17, NFR-14, NFR-15, NFR-8, NFR-5, NFR-9, NFR-10, NFR-1, NFR-4
Research: docs/research/30-design-space/12-fast-mode-wal-and-quorum.md (§5 overturned; its 2026-09-30 banner)

## Context

[ADR-0013](0013-fast-mode-wal-and-quorum.md) decided an opt-in tier: a batch
replicated to `wal_quorum` AZs with its sequencer-assigned offset, acked and
visible in ~1.5–6 ms (NFR-15: p99 < 10 ms), uploaded to the store afterwards
(invariant I6). It left the protocol open. M13's SPEC (§ Fast mode) fixed the
architecture -- the sequencer LEADER sequences, holds, publishes and uploads
every fast batch, because `SegmentWriter` keeps one run per stream with one
`firstOffset` and two writers' batches for one stream interleave in offset
order -- and stated eleven obligations this record must meet. Five SPEC drafts
of the protocol were refused in review, each for a case the next draft broke,
and this record's drafts for more (M13.22 review round 1: a stale lower-epoch
copy committed over a reassigned offset; a graceful close hiding an unfinished
takeover; voids and recovery deltas with no safe order; round 2: a releasing
leader still exposing; a switch stalled behind an entry that cannot complete; a
default-path commit on an undecided stream; a retry answered from a discarded
entry; round 3: a switch exposing while it discards; a default-path commit
before the walk; a writer's copy missing its supersession field; an early
release shortening the successor's wait). So the
protocol is stated as rules with the invariant each keeps, and the
commit-protocol simulation (criterion 13, `FastRecoveryModelTest`) checks the
invariants over seeds.

What exists to build on (checked against the tree, 2026-10-01):

- **One leader per fleet.** ADR-0007's S = 1: one lease
  (`<prefix>/ctl/lease/0.json`), one chain (`ctl/log/0/<epoch>/<seq>.delta`).
  The lease is a CAS object whose `epoch` rises only on takeover; TTL 10 s,
  renewal every 3 s. A renewal stamps `expiresAtMillis = now + TTL` on the
  holder's WALL clock (the injected `java.time.Clock`) just before it sends the
  `putIfMatch`; a challenger takes over once ITS wall clock reaches that stamp
  (`Lease.isExpiredAt`), or earlier when the early challenge (ADR-0070,
  `EndpointSliceView.holderGone`) finds the holder's UID neither ready nor
  terminating in the EndpointSlice -- which a live but unready holder satisfies.
  ADR-0002: the lease is liveness, never safety, on the default path.
- **The chain** is a sealed `ChainEntry` (`CommitDelta`, `Seal`, `Continue`),
  every reader decoding by kind; a stream's next offset is the fold of its
  committed runs, taking the maximum (`ChainReplay.fold`).
- **Commits** are serialised by `LocalSequencer.commitAll` (`synchronized`);
  an ambiguous append is reconciled before the next commit.
- **Pod identity.** `pod.id`, `pod.uid` (the Kubernetes UID), `pod.az`.
- **Peers** speak binary frames over HTTP, `keepAlive(false)`, one lease TTL
  as timeout (`HttpSequencerTransport`).

Terms: the **term** of a leader is its lease epoch `E`; an **incarnation** is a
pod's `(pod.id, pod.uid)` -- a pod rescheduled under its old name is a new
incarnation; a pod is **gone** when the Kubernetes API holds no pod of that name
with that UID, and only then; a fast **entry** is one stream's run of one batch,
with its epoch, first offset and records; an offset is **exposed** once acked,
published or served; the **holders** of an entry are the pods that durably hold
it with its offsets.

## Decision

### 1. The roster: who can hold copies of a term

**Every term has a roster, whether or not any index is fast.** A leader
taking the lease runs its takeover walk (§5) eagerly, never lazily at a first
fast write (M13.22d review round 3, P3), and creates its roster and writes
`LATEST` in every term. A dormant mode for fleets with no fast index was
tried in review (M13.22f round 2 to M13.22g round 3) and withdrawn: each of
its four drafts let a deposed leader, or a race between a dormant write and
an activation, expose or hide an entry; its saving was a few store requests
per term. So every mechanism of this record except carrying records runs in
every fleet, fast index or not: the takeover (walk, roster, `LATEST`, fences,
FENCE and COLLECTED, close), JOIN and JOINED, departures (DEPART, HELD_STATUS
and the `DEPARTED` writes), and the hold of default-path commits until the
takeover's step 3 -- all scaling with terms and pods (criterion 19, amended;
M13.22h review round 1, P1). The default path's RTO (NFR-9) after a lease
change therefore includes the walk's sequential store requests, about five
plus two per unclosed earlier term: a measurement target for criterion 18's
rig, not a claim (M13.22h review round 2, P4).

**An orphan roster** -- one whose leader's `LATEST` write did not land (§5
step 3's "term above `E′`") -- holds nothing: its leader accepts no JOIN and
assigns nothing before that write lands. No walk reaches it, since every
walk starts from `LATEST`, and the oracle counts only terms whose `LATEST`
write landed (M13.22g review round 2, P3 and T3).

For every term `E`, the leader keeps a
**roster** object, `<prefix>/ctl/fast/0/<E>.roster` (ADR-0082), naming:

- its predecessor term (the newest earlier roster, or none);
- its members: incarnations with AZ and endpoint, each `ROSTERED` or
  `DEPARTED`; the leader is a member from the roster's creation;
- the **term record**: the `wal_quorum` of every fast index in force when the
  term began, then every change made during it, in order;
- the **decisions** of this term, in order, each numbered: a stream and its
  resume offset, written by the term's takeover (§5) or by a switch that
  discarded unexposed entries (§8);
- a decision is PRUNED on the leader's next roster write once its stream's
  committed next offset reaches its resume offset plus `B`: every entry it
  supersedes was assigned below that (§4) and is skipped as committed-below
  (obligation 11), so it supersedes nothing more; numbers are kept, never
  reused (M13.22c review round 2, P2);
- `notBefore`: the wall-clock instant before which this term's leader might
  not assign or decide (§3), inherited by later leaders;
- `fencedBy` (the successor term that fenced it, 0 while none) and `closed`.

Only the leader of `E` writes roster `E`, except that a successor writes
`fencedBy`, a departure, and `closed` (§5, §9). Every write is `putIfMatch`
against the version the writer read. `<prefix>/ctl/fast/0/LATEST` names the
newest roster and is itself written only by `putIfMatch` (§5).

**A pod joins before it writes or holds.** Every
ready ingester node JOINs each term as soon as it learns its leader -- at
startup and at every leader change -- whether or not it writes or subscribes, so holders exist in every AZ that has
a pod (M13.22c review round 1, P3: a `q ≥ 2` index whose writers are all in the
leader's AZ still finds holders elsewhere); the leader appends every pending
join in ONE roster write, at most one per `min_upload_interval`, and only then
answers. A pod holds no entry and sends no batch of a term
it has not been told it joined. **So the incarnations that can hold a term's
copies are the roster's members, fixed once the roster is fenced**: a deposed
leader's later roster write fails its `putIfMatch`.

⚠️ **How a pod learns its leader** (amended by M13.27n; the paragraph above
said when, not how). A pod that does not hold the lease READS IT once per
lease renew interval -- one GET, no `stat`, absent or unreadable meaning "look
again" -- and JOINs the term it names when that term is newer than the last it
joined and led by another incarnation. An IDLE pod forwards no commit, so
ADR-0039's forward-path lease read never reaches it, and holders in every AZ
depend on idle pods joining. The rate is one GET per pod per renew interval
(about 0.33/s per pod at the shipped 3 s), scaling with pods (non-negotiable
6) and costing about $0.35 a month per pod at S3's GET price. Rejected:
learning only from a successor's FENCE (§5 step 4) -- it reaches only members
of terms still unclosed after the term start, and M13.27g closes an empty
predecessor at the start, so an idle fleet would hear nothing; and a leader
announcing itself to every pod it can see -- a new frame kind, and a pod the
announcement missed would still need the read.
⚠️ This amends M8's idle-pod criterion (M8 SPEC R3, NFR-2: an idle pod owes
its lease renewals and nothing else) for a pod that does NOT lead: it now
owes this one lease GET per renew interval as well. A leader still owes its
renewals alone -- the watch reads nothing while its term serves.
`IdlePodCostSoakTest` builds the assembly and its front door, not a node, so
it runs no watch and does not see this read: the watch's rate is pinned by
`LeaderWatchRunTest` (one read per interval) and by a node test bounding a
follower's reads above (M13.27n review round 2, P5; round 3, P7).

Writes: one roster creation per term; one write per batch of joins (at most one
per pod per term); one per departure per unclosed term listing the pod; at most
one term-record write per `min_upload_interval` (§7); one fence and one close
per term; one `LATEST` per term; one decisions write per batch of streams a
takeover decides; at most one switch-decisions write per
`min_upload_interval` (§8) -- scaling with pods and terms, never with
records, streams or indices (non-negotiable 6).

### 2. The fast write

1. **Writer W** -- any pod rostered in the leader's term -- sends the batch to
   the leader `L` with its epoch and an idempotency key
   `(pod.id, incarnationId, fastSeq)`.
2. **L assigns** under §3 and §4: offsets from the stream's cursor; the entry,
   with its key and `assignedAfter` -- the `seq` the NEXT decision of its
   roster will take when it is assigned, never the list's size, since pruned
   decisions keep their numbers (§1; M13.22c review round 3, P2; §5 invariant
   b) -- is journaled with its offsets
   and fsynced (one `fsync` per group of batches); L answers W with the
   offsets, `assignedAfter`, the `wal_quorum` assigned under, and whether W's
   copy is required -- everything W's journal entry needs (round 3, R3-3).
3. **The quorum.** An entry needs holders in `q` distinct AZs, `q` the index's
   `wal_quorum` as recorded in the term record (§4). **L always holds every
   entry of its term** (its journal, in AZ(L)). **W's copy counts only once W
   has journaled the entry WITH its offsets and confirmed** -- an offsetless copy
   cannot be recovered at its offset -- and is required exactly when
   `W ∉ AZ(L)` and `q ≥ 2`. L sends the entry to further rostered holders, one
   per still-missing AZ, until `q` distinct AZs hold it. Cross-AZ copies per
   record: `max(q − 1, [W ∉ AZ(L)])`. A pod is **available** to L while it is
   ready in the EndpointSlice, has answered within the replica timeout, and has
   refused with neither backpressure (a holder at its journal cap, §5.7) nor
   `departing` (§9) since -- a departing pod is never counted (M13.22e review
   round 1, P2).
   L re-sends a waiting REPLICA to an unavailable ready pod every replica
   timeout, and the pod is available again on its first answer that is not a
   refusal (M13.22c review round 2, P6).
   **A copy that does not arrive** -- W silent before its CONFIRM, a holder past
   the replica timeout or refusing -- is replaced; and **a copy that arrived on
   a pod since LOST** (UID deleted, or out of the EndpointSlice) stops counting
   for every entry not yet exposed, which is then short of its quorum and
   handled the same way (M13.22c review round 3, P1: otherwise an entry
   complete but unexposed at the loss is later exposed on one live copy).
   Missing copies are replaced: L, which holds the entry's
   bytes, sends it to another available rostered pod in an AZ not yet covered
   (W itself, once it returns, included). If none is available the entry WAITS;
   it is **discarded only on a LOSS** -- the pod that was to hold it gone (UID
   deleted) or out of the EndpointSlice, the same signal that triggers an
   upload (§7) -- with every entry at or above the stream's frontier, by a
   decision whose resume offset is the frontier (coalesced with other
   decisions, §8; M13.22c review round 3, P4); their writers, never acked,
   are told by REFUSED (reason `discarded`, naming the batch's idempotency
   key) once the decision is durable, and
   retry at once rather than after a timeout (M13.22c review round 2, P5). So an entry that cannot complete never blocks its stream's frontier
   past a loss while L lives (M13.22b review round 1, P2; obligation 3), and a
   slow or full holder that stays ready causes waiting, never a discard
   (M13.22c review round 1, P2).

   **Every decision L makes in its own term** -- this discard or a switch's
   (§8) -- follows one rule (M13.22b review round 2, P1): L stops assigning
   AND exposing on the stream, reads its frontier, appends the decision, and
   until the roster write holding it is durable assigns nothing on the stream;
   then it sets the stream's cursor to the resume offset (the frontier) and
   resumes, every later entry carrying an `assignedAfter` above the decision's
   number -- so no entry assigned at a reused offset is superseded by the
   decision that freed it.

   **Admission before assignment** (M13.22b review round 2, P2): L assigns a
   batch only while AVAILABLE rostered pods (L included) lie in at least `q`
   distinct AZs; otherwise the batch is held -- backpressure, never a refusal
   (cost.md rule 14) -- unassigned. So a discard happens only for entries
   already assigned when a pod became unreachable: at most one decision per
   stream per pod loss, never one per retry while an AZ stays lost or a
   holder stays full.
4. **A holder** checks the frame's epoch against the highest it has seen (§3)
   and that it is rostered in that term, journals the entry and fsyncs, then
   answers. Every answer about an entry (CONFIRM, REPLICA_ACK, EXPOSED) names
   it by `(epoch, assignedAfter, RunKey, firstOffset)` -- for a multi-run batch,
   every run's RunKey and firstOffset -- so a late answer about a
   discarded entry never counts toward a later one at the same offset
   (M13.22b review round 1, P3).
5. **Exposure in order.** L keeps per stream a **quorum frontier**: the end of
   the longest prefix of uncommitted entries each of whose quorum is complete.
   An entry is exposed -- answered to W as complete, published, served to
   catch-up -- only once the frontier passes it, whatever order holders answer
   in (obligations 9, 10). W acks the producer only on that answer, and only if
   its epoch is not below the highest W has seen.

A batch that cannot complete -- L deposed, a holder unreachable past the
timeout, a refusal -- is NOT acked; W retries it, to whichever pod leads, with
the same key. A leader answers a retry with existing offsets only for an entry
it still holds that was exposed in its own term (round 2, N4: never from an
entry it discarded or that is superseded, whose offset it may reassign; the
chain does not carry idempotency keys, so a committed entry is not recognised
either); otherwise it assigns anew, so a retry of an entry already committed is
a duplicate, removed by `_id` as the default path's retries are (failure matrix
F2, F3).

### 3. No deposed leader exposes (obligation 1)

Fast mode makes the lease's TIMING part of safety for one case (§11, ADR-0002),
so its fence does not rest on the early challenge. Two fences:

- **Lease time.** On each successful renewal L keeps the stamped
  `expiresAtMillis` (wall clock) and the monotonic instant `sentMono` captured
  before the send. L assigns and exposes only while
  `now_wall < expiresAtMillis − margin` AND `now_mono < sentMono + TTL − margin`,
  `margin = TTL / 10` (1 s at 10 s), and only while it is not shutting down
  (§9). The check is made **immediately before each exposing send** -- the ack
  to the producer, a PUBLISH, a catch-up or `/seg` response, an EXPOSED -- not
  only at assignment (M13.22a review round 2, P2). **A successor assigns no fast offset and
  decides no stream (§5) before its wall clock passes its `notBefore`: the
  later of the replaced lease's `expiresAtMillis + margin` and the `notBefore`
  of every unclosed roster it walks**, **and before its own monotonic clock
  passes `seenMono + TTL + margin`**, `seenMono` being the instant it first read
  the version of the lease it replaced -- reset at every newer version it reads,
  since each renewal is a new version (M13.22a review, P1: the wall-clock
  expiry was stamped on the old leader's clock, so a stopped or backwards wall
  clock there would let the successor in early; that version's write followed
  the old leader's `sentMono`, so this wait outlasts its monotonic validity
  whatever either wall clock says, provided the two monotonic clocks' rates
  differ by less than `2 · margin / (TTL + margin)`, ~18 %) -- whether it took the lease at
  expiry or by the early challenge, and however soon its predecessor released (round 3,
  R3-4: an early release must not shorten the wait the predecessor itself
  owed). It records the wall-clock part as its roster's `notBefore`; the
  monotonic part cannot be inherited across pods and need not be: a later
  successor's own `seenMono` follows this leader's lease write, which
  followed every earlier leader's last renewal, so its own monotonic wait
  covers them all. ⚠️ So under the stated assumptions the inherited
  wall-clock `notBefore` never binds: it is a second fence kept against a
  monotonic clock that misbehaves, not a load-bearing one (M13.22d review
  round 3, P4). **After a
  graceful handover** -- the lease version it replaced names as holder (its pod
  UID, ADR-0002's lease carries it) a leader the walk finds `DEPARTED` (a
  release writes an expired lease and no marker, so safety rests on `DEPARTED`,
  M13.22c review round 2, P4), and the leader of EVERY unclosed term found by EVERY walk it makes (step 2 and each
  re-walk of step 3) marked itself `DEPARTED` before its release (§9) --
  neither the replaced lease's expiry nor
  the monotonic wait applies (only the inherited `notBefore` of the rosters
  walked does): that
  leader stopped exposing before it wrote the release, and the store orders
  the release before the successor's read, so no clock is involved (obligation
  8; round 3, R3-9 asked that this wait be stated, and it is: none beyond the
  inherited one).

  So a leader paused across the takeover, a renewal whose
  response arrived late (validity runs from the send), a leader whose monotonic
  clock stopped (the wall check holds), one whose wall clock stopped or ran
  backwards (the monotonic check holds), and a leader still running after its
  pod was force-deleted (§10) all stop exposing before the successor's first
  assignment or void. **Residual assumption:** the two pods' monotonic clocks
  run at rates within ~18 % (`2 · margin / (TTL + margin)`) of each other; and for the early-assignment bound
  inherited through `notBefore`, their wall clocks differ by less than
  `margin` (1 s) -- three orders of magnitude above NTP's offset. A crash
  takeover therefore costs at least one TTL plus `margin` after the successor
  first reads the lease's last version. **And a stated residual
  risk, as in every lease:** a process paused between its last check and the
  send that check admitted, for longer than `margin`, can expose after its
  successor's first assignment or void. The send path between the check and
  the socket write holds no lock, no allocation-heavy step and no I/O wait;
  the epoch fence (below) closes it for every receiver that is a member and
  has been fenced -- every holder, writer and interested pod is a member (§6),
  and a successor fences every member it collects from; a member it has NOT
  yet fenced, because a stream decided early by AZ coverage (§5), is not
  closed, though safety then rests on the coverage argument -- leaving those,
  the ack to the producer from a leader that is also the writer, and a read
  served by the leader to a consumer as the windows a pause there can open
  (M13.22d review round 2, P4a).
- **The epoch fence on every pod.** Every pod records the highest fast epoch it
  has seen, durably beside its journal once it has one (a container restart
  keeps it) -- and at every container start it raises that value to the
  lease object's epoch, read once from the store: every FENCE and frame of
  epoch `E` is sent by a holder of lease `E`, so the lease's epoch is at least
  any epoch a frame carried, and a pod that has never journaled an entry
  needs no file and no disk (M13.22h review round 1, P2: a fleet with no fast
  index stays diskless, as the glossary says `wal=false` is). A pod whose
  start-up lease read fails stays unready until it succeeds; the file, where
  kept, is a second copy for a pod whose store reads are failing as it
  restarts, not a load-bearing one (M13.22h review round 2, P3). It is raised by
  any frame carrying a higher one and by the successor's FENCE (§5). It refuses
  every frame of a lower epoch: a holder does not journal it, a writer does not
  ack on it. **The leader itself is fenced the same way:** once the highest
  epoch it has seen exceeds its own, it assigns nothing and exposes nothing
  more -- no ack, PUBLISH, catch-up or `/seg` response -- and it records that
  epoch (in its epoch file if it has a journal; a container restart recovers
  it from the lease in any case) BEFORE it answers a FENCE with COLLECTED. So a successor's
  collection is a cutoff: every entry the old leader exposed is in its
  COLLECTED, since it holds every entry of its term and assigns none after the
  fence -- a send admitted before the fence may complete after the COLLECTED,
  but its entry is already collected (M13.22b review round 1, P1; M13.22f
  review round 2, P3). Catch-up and `/seg`
  responses from a pod's journal carry the epoch they were served under, and
  a rostered pod forwarding one refuses a lower epoch than it has seen. Since a successor fences every member of every unclosed term
  before it assigns (§5), any exposure needing another pod -- every `q ≥ 2`
  entry, and every `q = 1` entry written through `W ≠ L` -- is refused once the
  fence reached that pod; the lease-time fence covers exposures that need no
  other pod.

The testable property (criterion 10): on one global clock, no exposure by the
old leader follows the successor's first fast assignment or first void.

### 4. Assignment rules at the leader

- **The term record before assignment.** L assigns for an index only under a
  `wal_quorum` the term record already holds: a new fast index's first value
  and every change, raise or lower, is written before any assignment under it.
  Changes are coalesced into one roster write at most once per
  `min_upload_interval`, however many indices changed (cost.md rule 6). Until
  its value is recorded, an index's batches WAIT (acked once that one write
  lands, criterion 8). So every entry of term `T` was assigned under some
  `q ≥ q_min(T, I)`, the smallest value recorded for its index in `T`.
- **The per-stream bound `B`.** L never lets a stream's cursor run more than `B`
  offsets past the stream's committed next offset as L knows it from the chain.
  A batch that would pass it waits -- backpressure, never a refusal (cost.md rule
  14) -- and triggers an upload (§7). A later leader's committed next offset can
  only be higher, so **every offset any leader of an unclosed term assigned lies
  below `c₀ + B`**, `c₀` being the stream's committed next offset when a
  takeover begins. `B = 65,536`: a stream's ceiling is
  `B / (min_upload_interval + commit latency)`, ~68,000 records/s at an assumed
  commit latency of ~708 ms -- the upper end of research 04's ESTIMATED
  42-708 ms range, not a measurement (M13.22d review round 1, P1). No per-partition limit is
  configured that it must cover; the ceiling is a stated measurement target
  (criterion 17 drives one hot stream). A takeover after quorum loss voids at most
  `B` offsets per stream (§5); a takeover that crashes after its recovery entry
  and before its decisions write leaves the stream undecided, and the next one
  may void up to `B` more, so the bound is `B` per takeover that writes a
  recovery entry, not per quorum loss.
- **One stream, one order** (obligation 5): §8.

### 5. Takeover (obligations 2, 3, 4, 11)

**Invariants kept:** (a) a term is `closed` only once every earlier term is
closed, so walking back from the newest roster to the first closed one finds
every unclosed term; (b) an entry of epoch `e`, assigned after `d` decisions
of its roster, at offset `o` of stream `s` -- identified by `(e, d, s, o)`, since
after a discard one term may hold two entries of `s` at one offset -- is
**superseded** -- never committed,
never collected as live, never answered to a retry -- once the roster of a term
`T` records a decision for `s` with resume offset `≤ o`, where `T > e`, or
`T = e` and the decision is numbered `d` or later: while such a term is unclosed
the walk reads its roster, and once it is closed every term before it is closed
and its entries are never collected; (c) every takeover's
commits and voids for a decided batch of streams land in ONE chain entry, so a
crash leaves all or none of them; (d) assignment on a stream resumes only after
both its chain entry and its decision are durable.

A new leader `L′` holding the lease at epoch `E′`, before it assigns any fast
offset, does steps 2-4 at once -- they are store reads and writes and frames,
needing no time bound -- and admits **no default-path commit until step 3 is
done** (round 3, R3-2: a commit admitted before the walk could take offsets of
exposed entries it has not found yet):

1. **Notes `seenMono`** (§3).
2. **Finds the unclosed terms**, then computes its `notBefore` from them (§3);
   steps 5-6 wait for it and for the monotonic wait (§3). It reads `LATEST` (keeping its version) and
   walks predecessor pointers back to the first `closed` roster or the first
   term. Burned epochs (ADR-0037) have no roster. **If `LATEST`, any walked
   roster's epoch, or any `fencedBy` it reads is above `E′`, `L′` is deposed**:
   it writes nothing more -- no fence, no roster, no `LATEST` -- and assigns
   nothing, until its lease renewal fails. `LATEST` and every `fencedBy` are
   only ever raised, never written lower (M13.22e review round 1, P1: a
   leader paused past a TTL before its walk must not fence a newer term).
3. **Fences them and starts its own.** For each unclosed term it writes
   `fencedBy = E′` (`putIfMatch`); a deposed leader's next roster write fails and,
   re-reading a `fencedBy` above its own epoch, it learns it is deposed. If a fence write fails -- the deposed
   leader wrote its roster after the walk read it -- `L′` re-reads that roster,
   recomputes from the version it fences the held-index set (below), its
   members and its decisions, and retries (M13.22b review round 2, P4). It creates roster `E′` (`putIfAbsent`)
   with itself as member, predecessor the newest found, and the term record's
   starting values -- the current `wal_quorum` of every fast index -- then writes
   `LATEST = E′` by `putIfMatch` against the version read in step 2. If that
   fails, `L′` re-reads `LATEST`: `E′` itself means its own write landed with
   its answer lost; a term above `E′` means a newer leader exists, and `L′`
   assigns no fast offset in `E′` (its lease is gone too, which its next renewal
   finds); a term below `E′` is a deposed leader's late write, and `L′` walks
   again from it, fences what it finds, rewrites roster `E′`'s predecessor to the
   newest roster found (`putIfMatch`), **recomputes `notBefore` and whether
   §3's graceful exemption still holds over every term found so far** (M13.22a
   review round 3, P1: a deposed leader's late `LATEST` names a term whose
   leader did not depart), and retries (round 3, R3-7). Then `L′` holds the
   default-path runs of every index with a term-record value in a walked term,
   each until its own stream is decided (§5 below, round 2, N3).
4. **Collects.** It sends FENCE `E′` to every `ROSTERED` member of every
   unclosed term, read AFTER the fence writes. `DEPARTED` members hold nothing
   exposed and uncommitted (§9; a departed leader may hold never-exposed
   entries, which no decision needs) and are in no wait set and no coverage count below. Each raises its epoch to `E′`,
   then answers with every entry it holds of those terms. A member that does not
   answer is retried until it answers or is GONE (the Kubernetes pod lookup, an
   injected seam); not ready, out of the EndpointSlice, or partitioned is not
   gone, and is waited for (obligation 3). Collected entries that a walked
   roster marks superseded (invariant b) are dropped. (Two live entries at one
   offset cannot occur: a reassignment follows a recorded decision that
   supersedes the earlier one.)
5. **Decides each stream**, once its wall clock passes `notBefore` AND its
   monotonic clock passes `seenMono + TTL + margin` (§3; both, unless §3's
   graceful exemption holds), of each index that has entries, or a term-record
   value, in an unclosed term, from the live entries at offsets `≥ c₀`:

   **The quorum-loss predicate.** Index `I` has **lost quorum in term `T`** iff
   the leader of `T` is GONE and not `DEPARTED`, and the members of roster `T`
   that are GONE and not `DEPARTED`, other than that leader, lie in at least `q_min(T, I) − 1` distinct
   AZs other than the leader's. `I` **has lost quorum** iff it has in some
   unclosed term. It reads only what survivors can know -- the rosters (members,
   departures), pod-UID deletion, and the recorded values -- never which pods held
   copies, under which `q` an entry was assigned, or a value received but not
   recorded. **Why it is enough:** an exposed entry of `I` assigned in `T` was
   held by `T`'s leader and by holders in `q − 1 ≥ q_min − 1` other distinct AZs,
   all undeparted members of `T` (§1, §2; a departure waits for its copies to
   commit, and a departing leader commits every exposed entry of its term
   first, §9). If every copy is lost, the leader is gone and the other holders
   are gone in `q − 1 ≥ q_min − 1` distinct AZs, so the predicate holds. At
   `q = 1` the leader's loss alone is quorum loss; at `q ≥ 2` it is not, unless
   gone members lie in `q − 1` other AZs -- the SPEC's rule (obligation 3).

   - **If `I` has not lost quorum**, every exposed entry of its streams has a
     copy on a member that answered or is still awaited. A stream may decide
     early, without waiting for an unanswered member, when for every unclosed
     term `T` the AZs of its members that are unanswered or gone number fewer
     than `q_min(T, I)` (AZ coverage: then every exposed entry has a copy on an
     answered member); otherwise it waits. It commits the live entries from
     `c₀` up to the first hole; its **resume offset** is the hole. Entries above
     the hole are discarded: by exposure in order (§2.5) none was exposed, and
     the decision supersedes them on every pod (invariant b).
   - **If `I` has lost quorum**, the stream waits until every member of every
     unclosed term has answered or is gone -- a void must not cover a copy on a pod
     that survives -- then commits every live entry at its offset and voids every
     hole between them and the tail up to `c₀ + B`; its resume offset is `c₀ + B`.
     A hole may be an exposed entry whose every copy is gone; an entry above it
     may be exposed and survive, so it is committed, never discarded. Every void
     lies within `[c₀, c₀ + B)`: a committed offset is never voided.

   Streams decide independently; an index waiting on an unreachable member
   stalls only its own streams. Decided streams are committed in batches.
6. **Commits each batch atomically, then records it.** The decided streams'
   entries are uploaded as ordinary segments (at most one per
   `min_upload_interval`), then ONE **recovery entry** (ADR-0082) is appended to
   the chain carrying their segment commits at their offsets and every void of
   those streams; then the batch's streams and resume offsets are appended to
   roster `E′`'s decisions (one write). An entry already below the chain's next
   offset when the batch is built is skipped (obligation 11). A crash before the
   decisions write leaves streams on which nothing was assigned in `E′`
   (invariant d), which the next takeover decides afresh. A stream's commits
   and voids are always in one recovery entry; voids are decided only after
   every member is heard or gone, so they usually share the takeover's first
   quorum-loss batch, but a batch too large for one upload is split by stream
   and its voids then span its entries (M13.22e review round 2, P3).
7. **Releases and closes.** Holders release an entry only on a RELEASE sent
   after the chain entry covering it. A holder that missed one learns it from
   a HELD_STATUS (ADR-0082): the leader answers every COLLECTED, JOIN and HELD
   report with each reported stream's committed next offset, and every FENCE
   and JOINED carries `closedThrough`, the highest closed epoch (closed terms
   are a prefix, invariant a). **A non-leader's journal cap is twice the
   leader's** (M13.22e review round 2, P1): the leader holds every entry it
   sends a holder and backpressures at its own cap, so a holder's unreleased
   entries of the current term never exceed one leader cap, and a holder
   refuses REPLICA with backpressure only when stale entries -- missed
   releases, entries of terms still being taken over -- fill the other half.
   It sends HELD to the leader once its journal passes half its cap, and
   again on every backpressure refusal it answers and every replica timeout
   while above half (M13.22e review round 3, P1: one lost exchange must not
   leave it full), and the
   committed, superseded and closed-term entries HELD_STATUS names are
   dropped, so a missed RELEASE costs at most one report, never a stall
   behind a holder that refuses a lower entry and accepts higher ones
   (M13.22a review, P2). A dropped entry is journaled as a DROP
   record (ADR-0082 §4), so a restart does not bring it back as held. A term is `closed` once every one of its streams is decided and
   committed and every earlier term is closed (invariant a).

Assignment resumes per stream once its recovery entry and its decision are
durable. **Until then the stream takes no default-path commit either** (round
2, N3; round 3, R3-2): no default-path commit is admitted before step 3, and
after it a default-path run of any index with a term-record value in an EARLIER
unclosed term -- one the takeover walks, not `E′` itself -- waits until its
stream is decided and committed (its request's other runs commit now, by §8's
split). So `c₀` stays the stream's next offset until the decision, and a void
never covers a default-path commit. Within the current term, a switch is §8's
pause. A stream
of an index whose `q` exceeds the live AZs is decided and then cannot ack (§2.3):
a `q = 3` index on three AZs with one lost stops acking until the AZ returns.

**The oracle's copy.** `FastRecoveryModelTest` evaluates the same predicate as an
independent function of the MODEL's own events -- joins, departures, UID
deletions -- and of three facts it intercepts in roster writes to its store,
each checked for truth: the `wal_quorum` values in the term record (each one
the harness set, recorded before any assignment under it); each term's
leader, from the roster's creation (checked to be the model's lease holder of
that epoch); and each term's closure, from its `closed` write (checked to come
only after the model saw every stream of the term decided and committed). It
reads no other field of the protocol's roster (M13.22f review round 1, P1 and
T1: leadership and closure are inputs of the predicate the earlier text did
not give the oracle).

### 6. Publication and catch-up (obligations 6, 9, 10)

Every pod with a subscriber on a fast stream registers INTEREST with the leader
(again on a leader change); only a rostered pod may, so a successor's FENCE
reaches every interested pod, which then refuses a lower epoch's PUBLISH
(M13.22a review round 2, P2). L pushes each exposed entry, in offset order per
stream, once to each interested pod, which hands it to its subscription hub at
its offsets; the pod drops a pushed copy, as a holder drops a released one,
once its stream's committed next offset passes it, read from the chain its
subscribers already follow (M13.22c review round 2, P6). A subscriber the pod
serves proxy (ADR-0076) fetches the batch by
its fast key (ADR-0082) from its own zone's pod, which serves it from the copy it
was pushed, or forwards to the leader's journal before the upload. Catch-up
beyond the committed chain reads exposed entries from the leader's journal,
never an unexposed one.

### 7. Uploads (obligation 7)

L uploads EXPOSED entries only -- those below each stream's quorum frontier,
never a quorum-complete entry above an incomplete one (M13.22b review round 3,
P1: committing it would leave an uncovered hole below the chain's next offset
and collide with the offsets a later discard resets the cursor to) -- one
segment holding every stream's exposed runs at their assigned offsets, and one
delta -- triggered by the tightest
`flush_timer` among fast indices, by the journal at half its cap, by any stream
at `B / 2` uncommitted offsets, by the loss of a holder of an unreleased entry
(its UID gone, or out of the EndpointSlice -- shrinking the window in which a
second loss is quorum loss), and by a departure or a switching barrier (§8, §9);
never more often than once per **`min_upload_interval` = 250 ms**, the default
path's interval floor. Recovery uploads and recovery entries (§5) and a
switch's tail uploads (§8) are uploads of this one cadence, sharing its
budget, never added to it (M13.22e review round 1, P3). A leader makes at most
4 segment PUTs and 4 deltas a second, however hot a stream: the rate scales with time and leaders, never with
records (NFR-1, NFR-4). The **leader's journal cap is 256 MiB** (a holder's,
512 MiB, §5.7): the fleet's fast
ceiling is `cap / (min_upload_interval + commit latency)`, ~267 MiB/s at the same
assumed ~708 ms; above it, batches are backpressured. The leader carries every
fast batch, measured by criterion 18 and bounded by the cap.

### 8. Switching `wal` on a live index (obligation 5)

The leader's catalog decides a stream's mode at assignment; a writer whose
registration disagrees is answered and uses the other path. So a stale
default-path run for a stream the catalog has in fast mode is redirected to the
fast path, never admitted; the pause below applies only to a stream the catalog
has on the default path that still has a fast tail -- at most one pause per
stream per switch to `wal=false` (M13.22b review round 3, P3).

- **No fast assignment while a default-path commit of the stream is in flight
  or ambiguous**: the fast cursor is read from the committed next offset only
  inside `LocalSequencer`'s commit monitor, with no unreconciled ambiguous append.
- **A default-path run of a stream the catalog has on the default path that
  still has a fast tail pauses that stream's fast assignment** until the run
  is committed: L stops assigning
  AND exposing on the stream (round 3, R3-1: a CONFIRM arriving now must not
  expose an entry the decision discards), then reads its quorum frontier;
  entries above it -- never exposed -- are discarded, by a decision appended to
  L's roster with the frontier as resume offset (which supersedes their copies
  everywhere, §5 invariant b), coalesced with every other stream's switch into
  one roster write at most once per `min_upload_interval` (round 3, R3-6); their
  writers, never acked, retry. Then L uploads the exposed tail with its next
  upload, brought forward to the earliest instant §7 allows -- every switched
  stream's tail in one segment and one delta, never one per stream (M13.22a
  review round 3, P5) -- and commits the default-path run at the next offset
  after it, in the next delta, again coalesced; fast assignment and
  exposure resume after. So the run waits for at most one roster write and one
  upload --
  never for a quorum that cannot complete (round 2, N2: a `q = 3` index on two
  live AZs) -- is never starved (fast writers on the stream wait for it), and
  never collides (it takes offsets above every exposed fast offset). The default-path request's runs of
  other streams are committed now, in their own delta; the paused run follows in
  a later delta of the SAME segment -- so a segment's runs may be committed across
  more than one delta, and every chain reader takes a segment's runs per delta.
  **Each run of a default-path commit is committed, possibly paused first, or
  redirected** (the catalog has its stream in fast mode; the writer re-sends
  those records by COMMIT, §2). The leader answers the commit only once every
  paused run is committed -- at most one roster write and one upload later --
  with the commit answer (ADR-0082 §7): every delta that committed one of the
  request's runs, and the redirected runs, so a paused run is never re-sent
  and a redirected one never awaited (M13.22d review round 2, P1). **The writer acks the producer's
  request only once every run is committed or fast-acked.** A retry is answered
  per run: a committed run from the idempotency window; a missing run is
  committed from the already-uploaded segment ONLY if the catalog still has its
  stream on the default path, and redirected again otherwise -- never committed
  over fast offsets (M13.22c review round 1, P1); then the leader answers (M13.22a review round 2, P1: a crash
  between the two deltas must not lose the paused run or answer the retry from
  the first delta). The idempotency window therefore keeps, per segment, the
  runs committed, seeded from the chain; M13.34 owns that change, with
  `IdempotencyWindow` and the `Sequencer` contract that a segment is found in
  one delta.
  No other stream's acks wait.

### 9. Departure and shutdown (obligations 3, 8)

- **A non-leader pod departing** stops accepting entries, asks L for an upload,
  and sends L the streams and offsets of the entries it holds (HELD); L answers
  each as committed, superseded, of a closed term (all of which the pod drops),
  or pending (HELD_STATUS), giving per entry group the offset from which a
  decision supersedes it, so a group a switch's decision splits is answered
  for both parts (M13.22a review round 3, P2). The pod re-sends HELD after
  every RELEASE it receives while any entry is pending, so a pending answer
  clears as soon as the upload commits. The pod waits until no entry is pending, then asks L to mark it
  `DEPARTED` in the current roster and in every unclosed roster listing it (one
  write per such term), and stops. It never drops an uncommitted live copy, so
  routine churn is never counted as quorum loss. A pending entry stays pending
  while its stream's takeover waits on an unreachable pod; if the pod's grace
  period ends first, it stops undeparted and counts as a crash would.
- **A leader shutting down** FIRST stops exposing as well as assigning (round 2,
  N1: a CONFIRM arriving after the release must not be answered as complete),
  then uploads and commits every EXPOSED entry -- the others were never exposed,
  and their writers, never acked, retry -- marks itself `DEPARTED` in its roster
  (so its later loss is not quorum loss, round 3, R3-5), and in each earlier
  unclosed roster listing it only once it holds no pending entry of that term,
  as a non-leader does; where it still holds one it stays undeparted and its
  deletion counts there as a crash would (M13.22e review round 3, P2) -- and
  the roster `closed`
  **only if every earlier term is closed** (invariant a; a takeover it left
  unfinished stays for its successor), then releases the lease. Since it exposes nothing
  after it stops, nothing it exposed follows its successor's first assignment.
  A successor that finds no unclosed term and for which §3's graceful
  exemption holds assigns at once, with no fence and no collection (M13.22e
  review round 1, P5).
- **A crashed pod stays rostered** until the term closes. So crashes that
  accumulate within one term can make a later leader loss quorum loss, voiding
  up to `B` offsets per stream as counted gaps, though every entry the lost
  pods held may already be committed; a new term's roster starts from the live
  pods, which bounds the exposure to one term. ⚠️ A stated residual
  (M13.22c review round 2, P3): settling a lost pod would need which entries
  named it a holder, an input obligation 4 excludes from the predicate and its
  oracle, so it is left to a later ADR if measured to matter.

### 10. Availability, stated

- After a leader CRASH, fast mode is unavailable until the successor holds the
  lease and its wall clock passes its `notBefore` -- one TTL plus 1 s after the
  last renewal, at most 14 s at the defaults when the challenger reads the
  lease at least once per renewal interval (3 s), as ADR-0002's challenger does -- plus the collection and one
  upload. After a GRACEFUL handover, only the upload (obligation 8) -- when
  every unclosed term's leader departed; with an earlier term still unclosed
  at the shutdown, the crash wait applies (§3; M13.22d review round 1, P2).
- The default path's NFR-9 holds for every index except one with a term-record
  value in an unclosed earlier term: its default-path commits wait for its
  streams' decisions, as long as the takeover waits (round 3, R3-2). Every other
  index's commits wait only for the walk's few store reads and writes.
- A stream waits while a member of an unclosed term is alive but unreachable
  (obligation 3), unless AZ coverage lets it decide early. There is no operator
  release: deleting the unreachable pod (its UID gone) is the release, and its
  copies then count as lost. A FORCE-deleted pod may still run: it can expose
  nothing after §3's wait as leader, and as a non-leader it exposes nothing
  without a leader; its copies are treated as lost.
- An index whose `wal_quorum` exceeds the live AZs stops acking fast writes.

### 11. Scoped exceptions to earlier decisions

- **ADR-0001**: a void is a committed hole, only after quorum loss, within `B`
  per stream, skipped by consumers as a counted gap.
- **ADR-0002** (the lease is liveness, never safety): for an exposure that needs
  no other pod (`q = 1` written through the leader), fast mode relies on lease
  timing, under §3's stated clock assumption.
- **ADR-0012**: fast batches cross AZs to interested pods and to holders.
- **ADR-0013**: the writer's WAL is not the upload source; the leader's journal
  is (research 12 §5 overturned).
- **ADR-0070**: the early challenge does not shorten a FAST takeover (§3).

### 12. Requirements moved (amending ADR-0013's Consequences)

- **NFR-8** is conditional on `wal=false` (ADR-0013 already).
- **NFR-14**, precisely: an acked fast record is lost only if every holder of its
  entry is gone before the upload commits it -- all `q`, in `q` distinct AZs.
- **NFR-5** holds only for `wal=false` indices: a fast record crosses AZs
  `max(q − 1, [W ∉ AZ(L)])` times for its quorum, plus once per interested pod in
  another AZ.
- **NFR-9** holds for every index except one with a term-record value in an
  unclosed earlier term, whose default-path commits wait for a takeover's
  decisions (§5, §10).
- **NFR-10** holds for indices with `wal=false`, or `wal_quorum ≥ 2`, and at
  `q = 2` only if no second holder of an entry is lost within the upload the
  first loss triggers (NFR-14's window); a `q = 3` index on three AZs does not ack
  while one AZ is lost.

M13.38 edits the requirements table to match.

## Alternatives considered

- **Each writer keeps and uploads its own WAL** (research 12 §5). Rejected: no
  contiguous run per stream when several pods write one stream.
- **Pin each fast stream to one writer pod.** Rejected: a forwarding hop for
  every record not written through the owner -- cross-AZ two times in three --
  and a routing layer.
- **Persist each assignment to the store before the ack.** Rejected: one store
  round trip per batch is the 42–708 ms fast mode exists to remove.
- **Record which streams have an uncommitted tail before each ack.** Rejected:
  at `q = 1` a cross-AZ write per ack.
- **Persist a per-stream offset reservation.** Rejected: a request rate per
  stream (non-negotiable 6); `B` and the committed offset bound the void.
- **Judge quorum loss by which pods held copies.** Rejected: after the loss the
  survivors cannot know. The cost is conservatism -- a gone pod whose copies were
  all released still counts -- bounded by `B` and by one term.
- **Count an AZ lost only when all its pods are gone.** Rejected: an exposed
  entry's copy in that AZ may have been on the one pod lost (M13.43 review P1).
- **Take recovery's `q` from the catalog or the recovered entries.** Rejected:
  after quorum loss the entries that would say so may be the lost ones.
- **Treat a pod out of the EndpointSlice as gone.** Rejected: readiness is not
  disk loss (M13.0 review P6).
- **Fence fast mode through the chain alone.** Rejected: a fast ack never
  touches the chain (M13.0 review P1).
- **Lease validity on one clock, or from the response's arrival.** Rejected: a
  single clock that stops or jumps extends validity; the response may arrive a
  TTL late. Two clocks from the send cost two comparisons.
- **Keep the early challenge for fast takeover.** Rejected: it fires on an
  unready but live holder, which can still expose `q = 1` writes for up to one
  renewal interval (M13.22 review round 1, P6); the TTL-plus-margin wait is
  the price.
- **Write voids as their own chain entry, beside the recovery deltas.** Rejected
  (round 1, P3): `fold` takes the maximum, so a crash between the two leaves
  either an exposed hole below the next offset that can never be voided, or a
  void past a surviving copy. One recovery entry is atomic.
- **Truncate by telling holders to drop entries.** Rejected (round 1, P1): an
  unreachable holder keeps the stale copy, and a later takeover would commit it
  over the reassigned offset; the roster's recorded resume offsets supersede
  it durably, wherever it is. (Not in the chain entry: a checkpoint would fold
  it away before the later takeover reads it.)
- **Overwrite `LATEST` blindly.** Rejected (round 1, P4): a paused deposed leader
  writing it late hides the newer term.
- **Hold a default-path run at a barrier while fast assignment continues.**
  Rejected (round 1, P5): continuous fast writes starve it, or it collides with
  fast offsets.
- **Stop only assigning at shutdown.** Rejected (round 2, N1): an entry
  assigned before the stop can complete its quorum after the release and be
  exposed at an offset the successor reassigns.
- **Wait at a switch for the pre-pause entries' quorum.** Rejected (round 2,
  N2): an entry whose `q` exceeds the live AZs never completes, and the switch
  would never commit.
- **Embedded Raft or a consensus group for the roster.** Rejected (ADR-0011,
  ADR-0013): election stays the CAS lease.
- **An operator release for a stall on an unreachable pod.** Rejected: any
  release that voids over a live pod's copies breaks obligation 4.

## Consequences

- The roster object's size grows with the streams a term decides: one
  `decisions` element (~60 bytes) per stream at a crash takeover, plus one per
  switched stream, plus at most one per stream per loss of reachability
  (admission, §2.3, keeps an AZ that stays lost from adding more). Its write COUNT does not scale with streams, but every roster
  write re-PUTs it, including the term-record write assignment waits on; at
  10,000 fast streams a crash takeover adds ~600 KB. That size, and the
  term-record write's latency at it, are measurement targets (criterion 17).
  Over a long-lived term a cold stream's decisions are never pruned (pruning
  waits for `B` more of its offsets to commit), so the roster also grows with
  streams times readiness flaps within a term; a new term starts empty
  (M13.22e review round 1, P4). The remedy if either bites
  -- decisions in immutable per-write objects the
  roster counts -- is an ADR, not a task.

- One CAS object family (`ctl/fast/0/`), written per term and per pod join, and
  new pod-to-pod frames (ADR-0082); no request rate scales with records, streams
  or indices. The commit-protocol simulation, the store conformance suite (a
  commit at pre-assigned offsets) and the cost assertions are extended (M13.33,
  M13.36).
- A fast takeover is slower than the default path's: at least one TTL plus 1 s.
- Pods hold state for the upload window: a departure waits for its copies to
  commit; `terminationGracePeriodSeconds` must cover one upload.
- Readers of the chain take a segment's runs per delta (§8) and read the
  recovery entry (ADR-0082); the sealed `ChainEntry` makes a reader that ignores
  it fail to compile.
- The Kubernetes API is a dependency of takeover; outside Kubernetes it is
  injected, and a pod is never gone unless the seam says so.
- Six constants are fixed here and are measurement targets: `B = 65,536`,
  `min_upload_interval = 250 ms`, the 256 MiB journal cap, the TTL/10 margin,
  and the triggers at half the cap and half of `B`. A seventh is named and
  left to M13.27 to fix and measure: the **replica timeout**, which bounds how
  long one slow holder stalls a stream's acks and must be far below the TTL
  (M13.22d review round 1, P4).
