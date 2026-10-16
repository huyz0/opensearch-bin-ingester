// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.format.Checkpoint;
import io.github.huyz0.os.biningester.format.CommitDelta;
import java.io.IOException;
import java.util.List;
import io.github.huyz0.os.biningester.format.SegmentCommit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * What each pod incarnation has already had applied, so a replay is answered
 * rather than applied twice (M4.10d, ADR-0036).
 *
 * <p>⚠️ THE KEY IS THE TRIPLE'S FIRST TWO FIELDS, never {@code podId} alone.
 * {@code flushSeq} restarts at 0 when a pod restarts while {@code podId} stays
 * the same, so a window keyed on the pod refuses a restarted pod's genuinely
 * new records -- the defect ADR-0036 exists for, and the reason each
 * incarnation gets its own dense counter here.
 *
 * <p>⚠️ AN INCARNATION'S SLOT IS REPLACED, NOT MERGED, when a NEW incarnation
 * of the same pod arrives. Carrying {@code Math::max} ACROSS incarnations keeps
 * the dead one's higher watermark and refuses the restarted pod's second
 * commit -- depth one passes that mutation, which is why
 * {@code CheckpointWriter} states the same rule and why the fixtures go to
 * depth two.
 *
 * <p>⚠️ IT IS UNBOUNDED, AND THAT IS A KNOWN GAP RATHER THAN AN OVERSIGHT.
 * The map gains an entry per pod INCARNATION -- one per restart, not one per
 * pod -- and nothing evicts, so a long-lived leader's window grows with the
 * fleet's restart history. A bound was written and WITHDRAWN under review:
 * three mutations of its loop survived, including one that CLEARED the whole
 * window at the threshold, so it shipped weaker than no bound at all while
 * looking tested. M4.10g owns it, with the fixture properties review named.
 *
 * <p>⚠️ IT IS SEEDED FROM THE CHAIN (M5.1) -- see {@link #seed} -- so a
 * SUCCESSOR does NOT start empty and a replay crossing a takeover is caught,
 * WITH THE LIMITS {@link Sequencer} ENUMERATES -- SOME of which DUPLICATE
 * rather than refuse. ⚠️ "SOME", NOT A NUMBER: a count here would be a
 * count of a list living in another file, which is the drift this commit
 * argues against by name. An earlier draft wrote THREE and disagreed with
 * {@link Sequencer} in the same commit.
 *
 * <p>⚠️ STATED BY REFERENCE AND NOT RESTATED HERE. This sentence carried
 * M5.25's "one limit" long after the count was known to be wrong, and two
 * {@code src/main} statements of the same property disagreeing is what M5.34
 * exists to end. ⚠️ {@link #seed}'s javadoc is the ONE restatement left
 * standing, and the warning about it is repeated THERE rather than only here,
 * because a reader who jumps straight to {@code seed} never reads this
 * paragraph.
 *
 * <p>⚠️ WHAT IT STILL CANNOT KNOW BY ITSELF is an append this instance made and
 * never learned the outcome of: {@link LocalSequencer} records a commit as
 * applied only after the write RETURNS. That gap is closed IN THIS PROCESS by
 * reading the slot the append names and seeding this map from what is there
 * (M5.23), which is why {@link #applied} takes an attribution off a delta as
 * well as off a request.
 *
 * <p>⚠️ AND IT IS CLOSED ACROSS A TAKEOVER TOO (M5.25). A successor seeds from
 * {@code Checkpoint.pods} plus the uncheckpointed tail, and the reconciliation
 * now writes the ambiguous flush into that map as well -- so the fact survives
 * a later checkpoint bounding past its delta.
 *
 * <p>⚠️ PER SEGMENT, ACROSS DELTAS (M13.34b; ADR-0081 §8): a switch's
 * paused run is committed in a later delta than the rest of its segment, so
 * each recent segment's runs are kept with the delta carrying each -- the
 * last {@link #RETAINED_FLUSHES} flushes of an incarnation -- and a retry is
 * answered from every delta carrying one of its runs, and only once every run
 * it asks for is committed: else {@link IncompleteReplayException}, never the
 * first delta's runs alone. A flush older than that falls back to the one
 * pointer above. ⚠️ PROCESS-LOCAL: seeding these from the chain, so a retry
 * crossing a takeover finds a split segment's runs, is M13.34h's.
 */
final class IdempotencyWindow {

    /** How many of an incarnation's latest flushes keep their segments' runs (M13.34b). */
    static final int RETAINED_FLUSHES = 64;

    /** A delta: the chain it lives in and its slot. */
    record At(long epoch, long sequence) {
    }

    /** A retry asking for a run its segment has not committed yet (M13.34b). */
    static final class IncompleteReplayException extends IOException {
        private final java.util.Set<io.github.huyz0.os.biningester.format.RunKey> missing;

        IncompleteReplayException(CommitRequest request,
                java.util.Set<io.github.huyz0.os.biningester.format.RunKey> missing) {
            super("pod " + request.podId() + " replayed flushSeq " + request.flushSeq()
                    + " of segment " + request.segmentKey() + ", whose runs " + missing
                    + " are not committed yet");
            this.missing = java.util.Set.copyOf(missing);
        }

        java.util.Set<io.github.huyz0.os.biningester.format.RunKey> missing() {
            return missing;
        }
    }

    /**
     * The highest {@code flushSeq} applied for one incarnation, and WHERE.
     *
     * @param lastAppliedFlushSeq the highest applied, never a count
     * @param epoch the chain the answering delta lives in
     * @param sequence the slot in that chain
     */
    record Applied(long lastAppliedFlushSeq, long epoch, long sequence) {
    }

    private final Map<String, Applied> byIncarnation = new HashMap<>();
    /** Per incarnation, per recent flushSeq, per segment key: each run's delta (M13.34b). */
    private final Map<String, java.util.TreeMap<Long, Map<String,
            Map<io.github.huyz0.os.biningester.format.RunKey, At>>>> runsByFlush = new HashMap<>();
    /**
     * A checkpoint's split flushes (ADR-0086), by slot, then flushSeq and segment
     * key: the deltas to read once a retry asks, into {@link #runsByFlush}.
     */
    private final Map<String, Map<String, List<At>>> splitsBySlot = new HashMap<>();

    private final BinStore store;
    private final String prefix;

    IdempotencyWindow(BinStore store, String prefix) {
        this.store = store;
        this.prefix = prefix;
    }

    private static String key(String podId, String incarnationId) {
        // ⚠️ A NUL SEPARATOR, so ("a b", "c") and ("a", "b c") cannot collide
        // into one slot. Neither field can contain NUL and be legible: both are
        // refused blank by `CommitRequest` and carried as UTF-8 on the wire.
        return podId + "\0" + incarnationId;
    }

    /**
     * Rebuilds the window from what a chain replay found (M5.1).
     *
     * <p>⚠️ THIS IS THE HALF OF IDEMPOTENCY THAT DID NOT EXIST. Until now
     * nothing seeded this map, so a SUCCESSOR began with an empty window and a
     * replay crossing a takeover was applied twice -- {@link Sequencer}'s
     * javadoc named the fix M4.10f and nothing owned it. Forwarding is what
     * makes it reachable: a forwarded commit whose reply is lost, retried after
     * the lease moves, arrives at a pod that never saw the original.
     *
     * <p>⚠️ MERGES, KEEPING THE HIGHEST, rather than replacing. The seed is the
     * chain's view and anything already recorded is this process's; taking the
     * lower of the two would re-admit a replay the running window had already
     * answered.
     *
     * <p>⚠️ ONE INCARNATION PER POD, and that is a stated limit rather than an
     * oversight: {@code Checkpoint.pods} is keyed by {@code podId}, so it
     * remembers a pod's LATEST incarnation only. A replay from an incarnation
     * that has since been superseded is therefore not answered, and lands
     * twice. That window is narrow -- it needs a pod to restart between the
     * original and its retry -- and it is the direction that duplicates rather
     * than suppresses, which ADR-0036 records as the less damaging of the two.
     *
     * <p>⚠️ THIS RESTATES A LIMIT {@link Sequencer} OWNS, and is the one
     * restatement M5.34 left standing: the mechanism ({@code Checkpoint.pods}
     * is keyed by {@code podId}) belongs beside the code that implements it.
     * ⚠️ SO IT CAN DRIFT, and the drift to watch is M5.52b updating
     * {@link Sequencer}'s list alone. The warning is here rather than only in
     * the class javadoc because a reader who jumps straight to this method
     * never reads that one.
     */
    void seed(Map<String, io.github.huyz0.os.biningester.format.Checkpoint.PodState> fromChain) {
        // ⚠️ THE KEYS ARRIVE ALREADY SLOTTED, `podId\0incarnationId`, which is
        // this map's own key shape -- so they are used AS-IS. Re-keying them
        // here produced `podb\0i1\0i1`, which nothing can ever match: the seed
        // landed, the lookup missed, and every replay was treated as fresh
        // while the window looked populated.
        // ⚠️ `ChainReplay` builds them that way so two incarnations of one pod
        // never compete on flushSeq -- a dead incarnation with a higher
        // watermark would otherwise evict the live one and leave it unprotected.
        fromChain.forEach((slot, state) -> byIncarnation.merge(slot,
                new Applied(state.lastAppliedFlushSeq(), state.epoch(), state.sequence()),
                (prior, seeded) ->
                        seeded.lastAppliedFlushSeq() > prior.lastAppliedFlushSeq()
                                ? seeded : prior));
        // ⚠️ AND ITS SPLIT FLUSHES (ADR-0086): a retry of one is answered from
        // every delta its segment's runs lie in, though the replay read none.
        fromChain.forEach((slot, state) -> state.splits().forEach(f -> splitsBySlot
                .computeIfAbsent(slot, k -> new HashMap<>())
                .put(f.flushSeq() + "\0" + f.segmentKey(), f.deltas().stream()
                        .map(at -> new At(at.epoch(), at.sequence())).toList())));
    }

    /**
     * {@code request}'s split flush, if a checkpoint seeded one and nothing
     * tracks it yet, read into {@link #runsByFlush} (ADR-0086): one GET per
     * delta it lists, once -- then answered as a split this process committed.
     */
    void resolveSplit(CommitRequest request) throws IOException {
        Map<String, List<At>> splits = splitsBySlot.get(key(request.podId(),
                request.incarnationId()));
        String flush = request.flushSeq() + "\0" + request.segmentKey();
        List<At> deltas = splits == null ? null : splits.get(flush);
        if (deltas == null) {
            return;
        }
        for (At at : deltas) {
            CommitDelta held = DeltaReader.at(store, prefix, at.epoch(), at.sequence())
                    .orElseThrow(() -> new IOException("a split flush of pod "
                            + request.podId() + " points at sequence " + at.sequence()
                            + " of epoch " + at.epoch() + ", which holds no delta"));
            for (SegmentCommit segment : held.segments()) {
                if (answers(segment, request)) {
                    runsCommitted(segment.attribution(), segment.segmentKey(),
                            segment.runs().stream()
                                    .map(io.github.huyz0.os.biningester.format.RunCommit::key)
                                    .toList(), at.epoch(), at.sequence());
                }
            }
        }
        // ⚠️ FORGOTTEN ONLY ONCE READ: a GET that failed leaves it for the retry.
        splits.remove(flush);
    }

    /**
     * Where {@code request} has already been applied, or empty if it is fresh.
     *
     * <p>⚠️ {@code <=}, NOT {@code <}: the EXACT-EQUAL case is the retry this
     * row exists for, and a fixture that only replays a strictly lower
     * {@code flushSeq} leaves that mutation alive.
     */
    Optional<Applied> replayOf(CommitRequest request) {
        Applied applied = byIncarnation.get(key(request.podId(), request.incarnationId()));
        if (applied == null || request.flushSeq() > applied.lastAppliedFlushSeq()) {
            return Optional.empty();
        }
        return Optional.of(applied);
    }

    /** How one batch divides: what to commit, and what to answer. */
    record Split(List<CommitRequest> fresh, List<CommitRequest> replays) {
    }

    /**
     * Divides {@code requests} into fresh work and replays.
     *
     * <p>⚠️ CLASSIFIED AGAINST THE WINDOW AS IT STANDS, before any of this
     * batch is recorded, so two requests in one batch sharing a triple are BOTH
     * fresh. That is reachable and is why {@link #applied} keeps the highest
     * rather than the last.
     */
    Split split(List<CommitRequest> requests) {
        List<CommitRequest> fresh = new ArrayList<>(requests.size());
        List<CommitRequest> replays = new ArrayList<>();
        for (CommitRequest request : requests) {
            if (replayOf(request).isPresent()) {
                replays.add(request);
            } else {
                fresh.add(request);
            }
        }
        return new Split(fresh, replays);
    }

    /** Records that {@code request} -- every run of it -- is now durable at {@code (epoch, sequence)}. */
    void applied(CommitRequest request, long epoch, long sequence) {
        Objects.requireNonNull(request, "request");
        SegmentCommit.Attribution attribution = new SegmentCommit.Attribution(
                request.podId(), request.incarnationId(), request.flushSeq());
        applied(attribution, epoch, sequence);
        runsCommitted(attribution, request.segmentKey(), request.recordCounts().keySet(),
                epoch, sequence);
    }

    /**
     * Records that {@code segment}'s runs -- possibly only some of its flush's
     * -- are durable in the delta at {@code (epoch, sequence)} (M13.34b).
     */
    void applied(SegmentCommit segment, long epoch, long sequence) {
        Objects.requireNonNull(segment, "segment");
        if (segment.attribution() == null) {
            return;
        }
        applied(segment.attribution(), epoch, sequence);
        runsCommitted(segment.attribution(), segment.segmentKey(),
                segment.runs().stream().map(io.github.huyz0.os.biningester.format.RunCommit::key)
                        .toList(), epoch, sequence);
    }

    private void runsCommitted(SegmentCommit.Attribution a, String segmentKey,
            java.util.Collection<io.github.huyz0.os.biningester.format.RunKey> runs, long epoch,
            long sequence) {
        var flushes = runsByFlush.computeIfAbsent(key(a.podId(), a.incarnationId()),
                k -> new java.util.TreeMap<>());
        var bySegment = flushes.computeIfAbsent(a.flushSeq(), k -> new HashMap<>())
                .computeIfAbsent(segmentKey, k -> new HashMap<>());
        runs.forEach(run -> bySegment.putIfAbsent(run, new At(epoch, sequence)));
        // ⚠️ BOUNDED: only the incarnation's latest flushes keep their runs.
        flushes.headMap(flushes.lastKey() - RETAINED_FLUSHES + 1).clear();
    }

    /**
     * Each attributed segment's runs in {@code replayed}, in chain order, with
     * the delta carrying each (M13.34v): what a successor's replay read, so a
     * retry crossing the takeover finds a split segment's runs. A flush the
     * replay did not read -- behind a checkpoint -- keeps its one pointer.
     */
    void seedRuns(List<EpochDelta> replayed) {
        for (EpochDelta held : replayed) {
            for (SegmentCommit segment : held.delta().segments()) {
                if (segment.attribution() != null) {
                    runsCommitted(segment.attribution(), segment.segmentKey(),
                            segment.runs().stream()
                                    .map(io.github.huyz0.os.biningester.format.RunCommit::key)
                                    .toList(), held.epoch(), held.delta().sequence());
                }
            }
        }
    }

    /** Each run of {@code request}'s segment committed so far, and its delta; empty if untracked. */
    Optional<Map<io.github.huyz0.os.biningester.format.RunKey, At>> runsOf(
            CommitRequest request) {
        var flushes = runsByFlush.get(key(request.podId(), request.incarnationId()));
        if (flushes == null || !flushes.containsKey(request.flushSeq())) {
            return Optional.empty();
        }
        return Optional.ofNullable(flushes.get(request.flushSeq()).get(request.segmentKey()))
                .map(Map::copyOf);
    }

    /**
     * Records that {@code attribution}'s flush is durable at
     * {@code (epoch, sequence)}.
     *
     * <p>⚠️ TAKEN FROM A DELTA RATHER THAN FROM A REQUEST (M5.23). After an
     * AMBIGUOUS append the two differ: the delta that landed carries every
     * flush its batch held, while the retry that reconciles it may resubmit a
     * subset, a superset, or a mix. Recording only what was resubmitted leaves
     * the rest of that batch unrecorded, and each of THEM then commits a second
     * time -- the duplicate this window exists to prevent, moved one flush
     * sideways.
     */
    void applied(SegmentCommit.Attribution attribution, long epoch, long sequence) {
        Objects.requireNonNull(attribution, "attribution");
        byIncarnation.compute(key(attribution.podId(), attribution.incarnationId()),
                (ignored, prior) -> {
                    if (prior == null || attribution.flushSeq() > prior.lastAppliedFlushSeq()) {
                        return new Applied(attribution.flushSeq(), epoch, sequence);
                    }
                    return prior;
                });
    }

    /**
     * Whether {@code segment} is the one {@code request} already committed.
     *
     * <p>⚠️ THE EXACT TRIPLE, not the segment key: the key alone would answer a
     * retry from a DIFFERENT flush that happened to reuse it, and an
     * unattributed segment answers nothing at all.
     */
    static boolean answers(SegmentCommit segment, CommitRequest request) {
        SegmentCommit.Attribution a = segment.attribution();
        return a != null
                && a.podId().equals(request.podId())
                && a.incarnationId().equals(request.incarnationId())
                && a.flushSeq() == request.flushSeq()
                // ⚠️ AND THE SEGMENT KEY. The triple alone is not unique: one
                // flush may submit SEVERAL segments under one flushSeq, and
                // matching on the triple then resolves both replays to the same
                // `SegmentCommit` -- so the returned delta names one segment
                // twice and `CommitDelta` refuses it, turning a landed flush
                // into a producer-visible failure. Measured by review with
                // (podx,i1,5,seg/a) and (podx,i1,5,seg/b).
                // ⚠️ THE KEY IS NOT SUFFICIENT ON ITS OWN either, which is why
                // this is a conjunction: a key reused by a DIFFERENT flush
                // would answer a retry with another flush's offsets.
                && segment.segmentKey().equals(request.segmentKey());
    }

    /**
     * The already-durable segments answering {@code replays}.
     *
     * <p>⚠️ AT MOST ONE GET PER DISTINCT {@code (epoch, sequence)}, which is the
     * cost bound ADR-0036 states. A batch of retries from one pod points at one
     * delta and reads it once; the rate scales with retrying pods, never with
     * records, shards, partitions or indices.
     *
     * <p>⚠️ A REPLAY THE POINTED DELTA DOES NOT CARRY IS REFUSED, never
     * answered with whatever else that delta holds. TWO histories reach this
     * branch and the sequencer cannot tell them apart: a {@code flushSeq}
     * consumed by a commit that then threw, and a genuine duplicate whose delta
     * has been superseded. Answering either with the wrong offsets is worse
     * than failing, because a producer would acknowledge records that were
     * never committed at those offsets.
     */
    List<SegmentCommit> answer(List<CommitRequest> replays) throws IOException {
        Map<String, CommitDelta> read = new HashMap<>();
        List<SegmentCommit> answered = new ArrayList<>(replays.size());
        for (CommitRequest request : replays) {
            resolveSplit(request);
            Optional<Map<io.github.huyz0.os.biningester.format.RunKey, At>> tracked =
                    runsOf(request);
            if (tracked.isPresent()) {
                answered.add(acrossDeltas(request, tracked.get(), read));
                continue;
            }
            Applied at = replayOf(request).orElseThrow();
            // ⚠️ KEYED ON THE PAIR, not the sequence alone: a successor
            // inherits pointers into the PREDECESSOR's chain, where the same
            // sequence number names a different object.
            String slot = at.epoch() + "/" + at.sequence();
            CommitDelta pointed = read.get(slot);
            if (pointed == null) {
                // ⚠️ ONE GET, whichever chain it lands in: the key is
                // computed, so reading a PREDECESSOR's delta costs exactly what
                // reading this chain's does and issues no LIST.
                // ⚠️ UNREACHABLE UNTIL THE WINDOW IS INHERITED (M4.10f), and
                // said here rather than left to look tested: every pointer this
                // build records was written by `applied` alongside the delta it
                // names, so the slot always holds one. A pointer INHERITED from
                // a predecessor's checkpoint can outlive its delta, which is
                // where this refusal starts to matter and where its test lives.
                pointed = DeltaReader.at(store, prefix, at.epoch(), at.sequence())
                        .orElseThrow(() -> new IOException(
                        "a replay by pod " + request.podId() + " points at sequence "
                        + at.sequence() + " of epoch " + at.epoch() + ", which holds no delta"));
                read.put(slot, pointed);
            }
            SegmentCommit match = null;
            for (SegmentCommit segment : pointed.segments()) {
                if (answers(segment, request)) {
                    match = segment;
                    break;
                }
            }
            if (match == null) {
                throw new IOException("pod " + request.podId() + " replayed flushSeq "
                        + request.flushSeq() + " of incarnation " + request.incarnationId()
                        + ", which the delta at sequence " + at.sequence()
                        + " does not carry; it was consumed by a commit that did not land"
                        + " and its records were never assigned offsets");
            }
            // ⚠️ NEVER IN PART (M13.34v): a split segment's other runs may lie
            // in another delta this one pointer cannot name -- refused, never
            // answered with offsets for only some of the runs asked.
            java.util.Set<io.github.huyz0.os.biningester.format.RunKey> absent =
                    new java.util.TreeSet<>(request.recordCounts().keySet());
            match.runs().forEach(run -> absent.remove(run.key()));
            if (!absent.isEmpty()) {
                throw new IncompleteReplayException(request, absent);
            }
            answered.add(match);
        }
        return answered;
    }

    /**
     * {@code request}'s segment, its runs gathered from every delta carrying one
     * (M13.34b) -- each delta read once -- or refused while a run it asks for is
     * not committed.
     */
    private SegmentCommit acrossDeltas(CommitRequest request,
            Map<io.github.huyz0.os.biningester.format.RunKey, At> tracked,
            Map<String, CommitDelta> read) throws IOException {
        java.util.Set<io.github.huyz0.os.biningester.format.RunKey> missing =
                new java.util.TreeSet<>(request.recordCounts().keySet());
        missing.removeAll(tracked.keySet());
        if (!missing.isEmpty()) {
            throw new IncompleteReplayException(request, missing);
        }
        List<io.github.huyz0.os.biningester.format.RunCommit> runs = new ArrayList<>();
        SegmentCommit.Attribution attribution = null;
        for (var run : new java.util.TreeSet<>(request.recordCounts().keySet())) {
            At at = tracked.get(run);
            String slot = at.epoch() + "/" + at.sequence();
            CommitDelta pointed = read.get(slot);
            if (pointed == null) {
                pointed = DeltaReader.at(store, prefix, at.epoch(), at.sequence())
                        .orElseThrow(() -> new IOException("a replay by pod " + request.podId()
                                + " points at sequence " + at.sequence() + " of epoch "
                                + at.epoch() + ", which holds no delta"));
                read.put(slot, pointed);
            }
            io.github.huyz0.os.biningester.format.RunCommit found = null;
            for (SegmentCommit segment : pointed.segments()) {
                if (answers(segment, request)) {
                    attribution = segment.attribution();
                    for (var committed : segment.runs()) {
                        if (committed.key().equals(run)) {
                            found = committed;
                        }
                    }
                }
            }
            if (found == null) {
                throw new IOException("the delta at sequence " + at.sequence()
                        + " does not carry run " + run + " of pod " + request.podId()
                        + "'s flushSeq " + request.flushSeq());
            }
            runs.add(found);
        }
        return new SegmentCommit(request.segmentKey(), runs, attribution);
    }

    /**
     * {@code request}'s segment as the deltas that committed it (M13.34c;
     * ADR-0082 §7): one per delta carrying any of its runs, in chain order,
     * each holding only this segment's runs there -- what a commit answer
     * lists. Every run it asks for must be tracked and committed.
     */
    List<CommitDelta> answerByDelta(CommitRequest request) throws IOException {
        resolveSplit(request);
        Map<io.github.huyz0.os.biningester.format.RunKey, At> tracked =
                runsOf(request).orElseThrow(() -> new IOException("pod " + request.podId()
                        + "'s flushSeq " + request.flushSeq() + " of segment "
                        + request.segmentKey() + " is not tracked"));
        java.util.Set<io.github.huyz0.os.biningester.format.RunKey> missing =
                new java.util.TreeSet<>(request.recordCounts().keySet());
        missing.removeAll(tracked.keySet());
        if (!missing.isEmpty()) {
            throw new IncompleteReplayException(request, missing);
        }
        java.util.TreeMap<At, List<io.github.huyz0.os.biningester.format.RunCommit>> byDelta =
                new java.util.TreeMap<>(java.util.Comparator.comparingLong(At::epoch)
                        .thenComparingLong(At::sequence));
        SegmentCommit.Attribution attribution = null;
        Map<At, CommitDelta> read = new HashMap<>();
        for (var run : new java.util.TreeSet<>(request.recordCounts().keySet())) {
            At at = tracked.get(run);
            CommitDelta pointed = read.get(at);
            if (pointed == null) {
                pointed = DeltaReader.at(store, prefix, at.epoch(), at.sequence())
                        .orElseThrow(() -> new IOException("a commit of pod " + request.podId()
                                + " points at sequence " + at.sequence() + " of epoch "
                                + at.epoch() + ", which holds no delta"));
                read.put(at, pointed);
            }
            for (SegmentCommit segment : pointed.segments()) {
                if (answers(segment, request)) {
                    attribution = segment.attribution();
                    for (var committed : segment.runs()) {
                        if (committed.key().equals(run)) {
                            byDelta.computeIfAbsent(at, k -> new ArrayList<>()).add(committed);
                        }
                    }
                }
            }
            if (!byDelta.containsKey(at) || byDelta.get(at).stream()
                    .noneMatch(c -> c.key().equals(run))) {
                throw new IOException("the delta at sequence " + at.sequence()
                        + " does not carry run " + run + " of pod " + request.podId()
                        + "'s flushSeq " + request.flushSeq());
            }
        }
        // ⚠️ ACROSS A TAKEOVER, ONE DELTA (M13.34ae): two chains number their
        // deltas each from its own start, and an answer lists them by sequence
        // alone (ADR-0082 §7) -- so the runs are answered in one delta at the
        // newest's sequence, as a bare replay's answer is.
        if (byDelta.keySet().stream().map(At::epoch).distinct().count() > 1) {
            List<io.github.huyz0.os.biningester.format.RunCommit> all = new ArrayList<>();
            byDelta.values().forEach(all::addAll);
            all.sort(java.util.Comparator.comparing(
                    io.github.huyz0.os.biningester.format.RunCommit::key));
            return List.of(new CommitDelta(byDelta.lastKey().sequence(), List.of(
                    new SegmentCommit(request.segmentKey(), all, attribution))));
        }
        List<CommitDelta> deltas = new ArrayList<>();
        for (var e : byDelta.entrySet()) {
            deltas.add(new CommitDelta(e.getKey().sequence(), List.of(new SegmentCommit(
                    request.segmentKey(), e.getValue(), attribution))));
        }
        return deltas;
    }

    /** The sequence of the delta answering an all-replay batch. */
    long sequenceOf(List<CommitRequest> replays) {
        long sequence = -1;
        for (CommitRequest request : replays) {
            sequence = Math.max(sequence, replayOf(request).orElseThrow().sequence());
            for (At at : runsOf(request).orElse(Map.of()).values()) {
                sequence = Math.max(sequence, at.sequence());
            }
        }
        return sequence;
    }
}
