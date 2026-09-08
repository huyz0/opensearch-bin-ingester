// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import binjava.binstore.BinStore;
import binjava.format.Checkpoint;
import binjava.format.CommitDelta;
import java.io.IOException;
import java.util.List;
import binjava.format.SegmentCommit;
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
 * <p>⚠️ THIS IS PROCESS-LOCAL STATE. Nothing seeds it from the chain or from a
 * checkpoint, so a SUCCESSOR starts with an empty window and a replay that
 * crosses a takeover is NOT caught. That is the remaining half of M4.10d's row
 * and it is stated here rather than implied by silence.
 */
final class IdempotencyWindow {

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
     */
    void seed(Map<String, binjava.format.Checkpoint.PodState> fromChain) {
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

    /** Records that {@code request} is now durable at {@code (epoch, sequence)}. */
    void applied(CommitRequest request, long epoch, long sequence) {
        Objects.requireNonNull(request, "request");
        byIncarnation.compute(key(request.podId(), request.incarnationId()), (ignored, prior) -> {
            if (prior == null || request.flushSeq() > prior.lastAppliedFlushSeq()) {
                return new Applied(request.flushSeq(), epoch, sequence);
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
            answered.add(match);
        }
        return answered;
    }

    /** The sequence of the delta answering an all-replay batch. */
    long sequenceOf(List<CommitRequest> replays) {
        long sequence = -1;
        for (CommitRequest request : replays) {
            sequence = Math.max(sequence, replayOf(request).orElseThrow().sequence());
        }
        return sequence;
    }
}
