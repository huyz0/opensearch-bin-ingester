# 0082. Fast mode's formats

Status: accepted
Date: 2026-10-01
Requirements: FR-17, NFR-12, NFR-14
Research: docs/research/30-design-space/12-fast-mode-wal-and-quorum.md

## Context

[ADR-0081](0081-fast-mode-is-sequenced-held-published-and-uploaded-by-the-leader.md)
decided fast mode's protocol. Every byte it adds that outlives a process or
crosses one is a format, and changing a format later is a reader, a writer, the
fakes, golden files and an ADR in one commit (non-negotiable 8,
`wire-format-change`). M13's SPEC lists them: `IndexRegistration` v2; the fast
commit, answer, replica, publish, interest, fence, collect and release frames;
the roster object with its term record; the journal's entry format; and the
chain's recovery entry kind (a takeover's commits and voids); and, added in
review, the default-path commit answer (§7). This record fixes each, in the conventions the
tree already uses: big-endian binary frames opening with a 4-byte ASCII magic
and a version (`CommitRequestFrame` "BPCR", the chain's "BDLT"), and canonical
JSON for CAS objects an operator reads (the lease, ADR-0002).

## Decision

### 1. `IndexRegistration` v2

`IndexRegistration` ("BIRG") gains `VERSION_2 = 2`, which appends to v1's
fields:

| Field | Encoding | Default when absent (v1) |
|---|---|---|
| `flushTimerMillis` | i64, `> 0` | 5,000 |
| `wal` | u8, 0 or 1 | 0 |
| `walQuorum` | u8, 1–3; read only when `wal = 1` | 2 |

The plugin's registrar reads `index.ingestion_source.param.flush_timer`,
`.wal` and `.wal_quorum`; the ingester decodes v1 and v2 (a v1 frame comes
from an older plugin, or from a newer one for an index at the defaults --
either way the index is `wal=false` at the default timer); an unknown
version stops the read, as today. ⚠️ Amended by M13.23: the registrar writes **v1 for an index
at all three defaults and v2 otherwise** -- not v2 always -- because an older
ingester refuses an unknown version, and "always v2" would make every index
unroutable on every older ingester the moment the plugin upgraded first,
falsifying this record's own rolling-upgrade consequence; only an index that
sets one of the three needs the newer ingester. ⚠️ And the newer plugin on
every node: an older plugin node pushes the same index as v1, and the
catalog takes the last push, so set the three only once every plugin node
runs a build that reads them (M13.23 review round 3, P2). With `wal = 0` the quorum is
absent from the wire, so the decoded value is the default 2 and the
registration normalises to it (equality means the same bytes; the registrar
diffs by equality). The params are free-form strings OpenSearch does not
validate, so the registrar reads them strictly (`wal` exactly `true` or
`false`, `wal_quorum` exactly 1, 2 or 3 whatever `wal` says, `flush_timer` a
positive whole number of `ms`, `s`, `m` or `h`) and, for a malformed value, keeps the index at its last
good settings -- the last this node pushed -- with its placement re-derived,
held through a reconnect, so an ingester restart re-registers it; logs an error and counts it once per
distinct value; never a lenient guess at a durability. The last good
settings live in the registering node's memory: an index never registered,
or one whose shards moved or whose node restarted while the value was
malformed, stays unregistered until it is fixed. Golden files:
v1, v2 per `walQuorum` at `wal = 1`, and v2 with only `flush_timer` set.

### 2. The fast frames

Every pod-to-pod fast frame shares one header -- magic `0x42465354` ("BFST"),
version `1` (u8), kind (u8), `epoch` (i64), the sender's pod UID and the
target's pod UID (each a length-prefixed string) -- so every receiver applies
the epoch fence (ADR-0081 §3) before reading anything else, refuses a frame
addressed to another incarnation, and the leader counts an answer only from
the incarnation it addressed: a reused pod name or address never makes a
copy in one AZ count for another (M13.22e review round 2, P2; ADR-0070).
Kinds:

| Kind | Name | Direction | Body after the header |
|---|---|---|---|
| 1 | `COMMIT` | writer → leader | idempotency key (podId, incarnationId UUID, fastSeq i64); runs: RunKey, recordCount, records |
| 2 | `ASSIGNED` | leader → writer | `assignedAfter` u32; per run: RunKey, firstOffset, `walQuorum` u8, `copyRequired` u8 -- per run, since one batch may span indices with different `wal_quorum` (M13.22f review round 2, P2); every field the writer's journal entry needs |
| 3 | `CONFIRM` | writer → leader | idempotency key; `assignedAfter` u32; per run: RunKey, firstOffset; copy journaled u8 |
| 4 | `EXPOSED` | leader → writer | `assignedAfter` u32; per run: RunKey, firstOffset |
| 5 | `REPLICA` | leader → holder | one batch's journal entries (§4), one per run -- a frame per batch per holder, never per stream |
| 6 | `REPLICA_ACK` | holder → leader | `assignedAfter` u32; per run: RunKey, firstOffset (with the header's epoch, the batch's full identity) -- one per batch |
| 7 | `PUBLISH` | leader → interested pod | the exposed entries the pod is interested in, of one or more batches (§4) -- a frame per exposure step per pod, never per stream |
| 8 | `INTEREST` | pod → leader | RunKeys added and removed |
| 9 | `FENCE` | successor → member | the unclosed terms' epochs; `closedThrough` i64, the highest closed epoch, every entry at or below which the member drops |
| 10 | `COLLECTED` | member → successor | page number u32, `last` u8, entry count u32, then that many entries it holds of those terms (§4); the successor treats a member as answered only on the page marked `last` with every earlier page received (M13.22e review round 3, P3) |
| 11 | `RELEASE` | leader → holder | per stream: RunKey, release below offset |
| 12 | `JOIN` | pod → leader | incarnation: podId, podUid, az, endpoint; the HELD body of what its journal still holds |
| 13 | `DEPART` | pod → leader | incarnation; `phase` u8 (1 upload and report, with the HELD body; 2 mark departed); phase 1 is answered by HELD_STATUS, phase 2 by an empty HELD_STATUS once every `DEPARTED` write has landed |
| 14 | `REFUSED` | any answer | reason u8 (lower epoch, not rostered, not fast, backpressure, departing, discarded); for `discarded`, the batch's idempotency key; text |
| 15 | `JOINED` | leader → pod | `closedThrough` i64; then a HELD_STATUS body for the streams JOIN reported |
| 16 | `HELD` | holder → leader | per stream held: RunKey, and per `(epoch, assignedAfter)` held: lowest and highest offset; sent at DEPART phase 1, in JOIN, when its journal passes half its cap, and after every RELEASE while a departing pod has an entry pending |
| 17 | `HELD_STATUS` | leader → holder | per reported stream: RunKey, release below offset (the committed next offset), and per reported `(epoch, assignedAfter)` a `supersededFrom` i64 (the lowest resume offset of a decision superseding the group, `Long.MAX_VALUE` if none: offsets at or above it are superseded) and, for the part below it, a `status` u8 (1 committed, 2 superseded, 3 closed term, 4 pending) |

A catch-up or `/seg` response served from a journal is not a frame but an HTTP
body; it carries the epoch it was served under in a `Binstore-Fast-Epoch`
response header, which a forwarding pod checks against its epoch fence.

Records use the segment's record encoding (`SegmentRecord`), so a published or
collected batch is decoded by the code that decodes segments. Strings are
length-prefixed UTF-8 with the limits the existing frames use; RunKey is the
index UUID (two i64) and the partition (i32). Each frame is capped -- `COMMIT`,
`REPLICA` and `PUBLISH` at the `_bulk` chunk size the front door already
bounds, `COLLECTED` streamed in pages of that size -- and refused `413` past
its cap, with the connection closed (M13.19).

### 3. The roster object

`<prefix>/ctl/fast/0/<epoch %016x>.roster`, canonical JSON in the lease's
style (fixed field order, strict decode refusing unknown or duplicate fields):

```json
{"epoch":7,"predecessor":5,
 "leader":{"podId":"ingester-1","podUid":"…","az":"az-a","endpoint":"http://…"},
 "members":[{"podId":"…","podUid":"…","az":"…","endpoint":"…","state":"ROSTERED"}],
 "termRecord":[{"seq":0,"walQuorum":{"<indexUuid>":2}},{"seq":1,"walQuorum":{"<indexUuid>":1}}],
 "decisions":[{"seq":0,"index":"<indexUuid>","partition":3,"resumeAt":4242}],
 "notBefore":1727740800000,"fencedBy":0,"closed":false}
```

- `predecessor` is `-1` for the first term; `members` includes the leader;
  `state` is `ROSTERED` or `DEPARTED`. `decisions` elements are pruned
  (ADR-0081 §1); `seq` keeps their numbers, and `assignedAfter` is a `seq`.
- `termRecord[0]` holds every fast index's value when the term began; each
  later element is one coalesced write's changes, in order. `q_min(T, I)` is
  the minimum over the elements naming `I`.
- `decisions` lists, in order and numbered from 0, every stream this term
  decided -- by its takeover or by a switch's discard -- with its resume offset,
  appended one batch per write (ADR-0081 §5, §8).
- `notBefore` is the wall-clock instant (ms) before which the term's leader
  assigned and decided nothing (ADR-0081 §3).
- `<prefix>/ctl/fast/0/LATEST` holds `{"epoch":7}`, written by the leader of
  each new term with `putIfMatch` against the version its walk read (never a
  blind overwrite).
- Key length is fixed and short (NFR-12).

⚠️ Stated by M13.26b, which landed the format (M13.25c's) with its first
reader and writer, the new leader's walk:
- a term record's `walQuorum` keys are ordered by the UUID's canonical text,
  never by a language's own UUID order (Java's compares signed halves), so
  another encoder can reproduce the form;
- `notBefore` is `0` when there is nothing to wait for, `fencedBy` `0` while
  unfenced; strings carry no escapes, and an encoder refuses a quote, a
  backslash, a control character or an unpaired surrogate, as the lease does;
- `predecessor` is `-1` or an earlier term, `fencedBy` `0` or a later one, the
  leader is among `members` and listed once by pod UID, and `termRecord` is
  numbered from 0 without gaps;
- ⚠️ the next decision's number is the last listed one's plus one, so pruning
  (ADR-0081 §1) must keep the newest decision of a roster, or a reused number
  would let an entry assigned after a pruned decision read as superseded by
  it. M13.33, which writes and prunes decisions, owns that.

### 4. The journal

One append-only file per pod under the pod's `emptyDir`, plus a small
`epoch` file holding the highest fast epoch the pod has seen (ADR-0081 §3),
written and fsynced before it is relied on -- by writing a temporary file,
fsyncing it, renaming it over the old one and fsyncing the directory, never in
place. ⚠️ Its bytes, stated by M13.26c, which landed it: magic `0x42464550`
("BFEP") u32, version 1 u8, the epoch i64, and the CRC32C of those thirteen
bytes u32 -- seventeen bytes, big-endian. Replaced whole, it has no torn form,
so a file that does not decode is refused and the pod stays unready, never
read as a lower epoch -- except an EMPTY file, which is no file yet (a crash
between its creation and its first write), and is written at once. An entry:

| Field | Encoding |
|---|---|
| magic `0x42464A45` ("BFJE"), version 1 | u32, u8 |
| length of the rest, CRC32C of the rest | u32, u32 |
| kind | u8: 1 = entry, 2 = release, 3 = drop |
| epoch | i64 |
| RunKey, firstOffset, recordCount | as in the frames |
| walQuorum assigned under | u8 |
| `assignedAfter`: the `seq` the roster's next decision would take when assigned | u32 |
| idempotency key | as in `COMMIT` |
| records | segment record encoding |

A release record carries RunKey and "release below offset". A drop record
carries RunKey, epoch, `assignedAfter` and "drop from offset": the pod no
longer holds that group at or above it (superseded, or of a closed term --
`closedThrough` is written as a drop of every group of those epochs). Recovery reads to
the first entry whose magic, length or CRC fails (a torn tail) and stops there, then
**truncates the file there and fsyncs it before the first append**. Entries
are answered only after their group's fsync, so every entry after the first
failure was in the unfsynced group or never written, and none was answered;
corruption of an entry already fsynced (media damage) is a stated residual
risk -- it loses that pod's copies while its UID lives, invisibly to the
predicate, as a lost disk would (M13.22d review round 3, P1, replacing round
2's rule, which could not tell a torn group from damage without an fsync
watermark). An entry appended after a tear would be lost to the next recovery (M13.22c review round
2, P1). ⚠️ Amended by M13.24: a record whose CRC VERIFIES but whose version, kind or
fields this build cannot read is not a tear -- some build fsynced it, and its
copies may have been answered -- so recovery STOPS without truncating and the
pod refuses to start (it stays unready) until a build that reads it runs.
Unknown means stop, never skip and never cut. So the 13-byte header --
magic, version, length, CRC32C, in that order and covering the bytes after
it -- is frozen across versions: a later version that moved the checksum
would be read as a tear on a rollback and cut. An entry is held while no later
release or drop covers it; recovery applies both. The file is compacted by writing the held entries to a new
file, fsyncing it, renaming it over the old one and fsyncing the directory,
once released bytes pass half of it -- never rewritten in place.

### 5. The recovery chain entry

`ChainEntry` gains `KIND_RECOVERY = 4`, a record `Recovery(long sequence,
List<SegmentCommit> segments, List<VoidRange> voids)` with `VoidRange(RunKey key,
long fromOffset, long toOffsetExclusive)`, under the kinded header
(`VERSION_KINDED`). Its segment commits are those of a batched delta, each run
at its pre-assigned offset; its voids are committed holes. It is one entry, so a
crash leaves all of it or none (ADR-0081 §5, invariant c). The sealed
`ChainEntry` permits it, and every reader decodes it by exhaustive `switch`:

- `ChainReplay.fold` folds its runs as a delta's and advances a stream's next
  offset over each void to `toOffsetExclusive`;
- `DeltaReader`, `ChainEnd`, `Checkpoint`, `ChainBackfill` treat it as a delta
  whose voids are part of the fold;
- `CommitChargingBinStore` charges its segment runs as a delta's, and its voids
  to no index (they move no bytes);
- the consumer and the plugin read its runs as a delta's and skip each void as
  a counted gap.

⚠️ Landed by M13.25a (M13.25 split at its review budget) for every CHAIN
reader, each now an exhaustive `switch` over the sealed `ChainEntry` -- the
plugin's tier-2 poller included, which read every slot as a delta and so
would have stopped for good at the first recovery. `ChainEnd` needs only a chain's last entry and
`Checkpoint` is built from the fold, so neither branches on the kind. The
last bullet -- a void reaching a consumer as a counted skip -- needs the
subscription and catch-up paths to carry it, which are formats of their own:
M13.25d. Until it lands no writer emits a recovery entry (the takeover is
M13.33), and a void would reach a consumer as an unexplained offset jump,
reported by `DeliveryGapException` as an upstream gap -- and in the plugin the
stream would be held and its repair retried at every progress interval
without ever completing, a permanent stall. So M13.33 must not land before
M13.25d; the backlog records the dependency.

The body after the kinded header and the kind: `sequence` (uvarint); the
segment count (uvarint, zero allowed) and each segment exactly as a batched
delta writes it -- key length and key, run count, and per run the index UUID
(two i64), the partition, the record count and the first offset (uvarints);
then the void count (uvarint) and each void as the index UUID (two i64), the
partition, `fromOffset` and `toOffsetExclusive` (uvarints). Segments carry no
attribution. (Layout stated by M13.25, which landed it.)

Voids are sorted by RunKey and never overlap a committed offset. A stream's
commits and voids are always in one recovery entry; a takeover's voids span
more than one only when its batch is split by size (ADR-0081 §5).

### 6. A fast batch's key, for a proxied read

A subscriber served proxy (ADR-0076) fetches a fast batch before its upload by
the key `<prefix>/fast/<epoch %016x>/<assignedAfter %08x>/<indexUuid>/<partition>/<firstOffset %016x>`
(`assignedAfter` because after a switch's discard one term may hold two entries
at one offset, ADR-0081 §5),
which names no object: `/seg` serves it from the copy pushed to the pod, or
from the leader's journal, and answers `404` once the entry is released
(the subscriber then reads the committed segment). It is under the 1,024-byte
limit for every input.

### 7. The default-path commit answer

ADR-0081 §8 lets one default-path commit request's runs land in more than one
delta, or be redirected to the fast path. The answer, today a bare
`CommitDelta`, becomes a versioned frame when the request asks for it (an
`Accept` of it on the commit request; a writer that does not ask is an older
binary, whose indices are `wal=false`, and gets the bare delta, which for them
is always the whole answer; such a request carrying a run on a stream the
catalog has in fast mode, or paused, is refused whole, never committed over
fast offsets -- M13.22d review round 3, P2):

| Field | Encoding |
|---|---|
| magic `0x4243414E` ("BCAN"), version 1 | u32, u8 |
| delta count | u16 |
| each delta | its `CommitDelta` encoding, length-prefixed (u32) |
| redirected run count | u32 |
| each redirected run | RunKey |

A run appears in exactly one delta or among the redirected runs. Golden files:
one delta, two deltas (a paused run), and a redirect; landing with M13.34.

## Alternatives considered

- **One magic per fast frame.** Rejected: a shared header lets every receiver
  check the epoch before parsing a body; seventeen magics would be seventeen
  places to forget the fence.
- **JSON for the frames.** Rejected: records are binary, and the hot frames
  (`COMMIT`, `REPLICA`, `PUBLISH`) carry them; JSON is kept for the roster, a
  CAS object an operator reads, as the lease is.
- **The term record as its own object.** Rejected: a second CAS object written
  with the roster would need ordering between them; in the roster it is fenced
  with it, and a successor reads one object per term.
- **Carry `walQuorum` in a new index setting object instead of the
  registration.** Rejected: ADR-0015 already pushes the registration; the
  catalog is where the leader reads modes.
- **Represent a void as a delta of empty segments.** Rejected: every reader
  would read a segment key that names nothing; a kind of its own is caught at
  compile time by the sealed type.
- **A void kind beside ordinary recovery deltas.** Rejected (M13.22 review
  round 1, P3): `fold` takes the maximum, so a crash between the two leaves an
  exposed hole below the next offset, or a void past a surviving copy.
- **A journal per stream.** Rejected: a file per stream is a resource that
  scales with streams; one file with entries per stream fsyncs once per group.

## Consequences

- Seventeen frame kinds, a CAS object, a journal and a chain kind -- each with
  golden files, landing with its first reader and writer in one commit (M13.23,
  M13.24, M13.25).
- `IndexRegistration` v2 is the plugin's first registration change since v1;
  mixed fleets work in both directions of a rolling upgrade.
- The recovery kind is a wire change to the chain: an older binary reading a
  chain that holds one stops at it -- a fleet must run a binary that reads it
  before any index uses `wal=true`.
