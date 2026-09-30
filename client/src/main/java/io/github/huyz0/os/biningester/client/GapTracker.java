// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.client;

import io.github.huyz0.os.biningester.format.RunKey;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;

/**
 * One stream's offset contiguity for {@link ConsumerClient}: the offset the
 * next delivery must start at, the gaps found against it, and the drops those
 * gaps are blamed on (M6.1). Moved out of {@code ConsumerClient} unchanged by
 * M13.1c, which keeps the decoding and the queues.
 */
final class GapTracker {

    private static final System.Logger LOG =
            System.getLogger(ConsumerClient.class.getName());

    private final RunKey key;

    /**
     * The offset the NEXT delivery must start at, or {@code -1} before the
     * first one (M6.1).
     *
     * <p>⚠️ THE CONTIGUITY IS THE COMMIT LOG'S, not this class's invention:
     * offsets are assigned per run at commit, so a delivery of {@code n}
     * records starting at {@code f} is followed by one starting at
     * {@code f + n}. Anything else means records this consumer will never see.
     *
     * <p>⚠️ AND {@code -1} IS "NOTHING YET", NEVER "ZERO". A subscriber may
     * legitimately start mid-stream -- a resumed session answers at the next
     * record, not at the beginning -- so the FIRST delivery can carry any
     * offset and comparing it against 0 would report a gap for every resume.
     */
    private long expectedNextOffset = -1;

    /**
     * Guards {@link #expectedNextOffset} across the two lanes.
     *
     * <p>⚠️ ONE LOCK FOR BOTH LANES (M12.17, M11.13 R4). The live lane decodes
     * under {@code liveDecodeLock} and the catch-up lane under
     * {@code catchUpDecodeLock}, so neither excluded the other: the race
     * guard's undo in {@link #commit} wrote the offset in a window where a
     * catch-up commit could land between the commit it undoes and the undo,
     * and be overwritten. The commit and its undo are now one critical
     * section. ⚠️ The gap handler is called AFTER it is released: it belongs
     * to the node's coordinator, which takes its own monitor.
     */
    private final Object offsetLock = new Object();

    /** A gap {@link #reportAnyGap} found, reported once {@link #offsetLock} is released. */
    record GapReport(Consumer<DeliveryGapException> handler, DeliveryGapException gap) {
    }

    /**
     * What {@link #commit} decided: whether to hand the decoded records on,
     * the gap to report once the lock is released, and whether the commit was
     * undone because a race made it one the check did not foresee.
     */
    record Commit(boolean decode, GapReport report, boolean undone) {
    }

    /**
     * What {@link #reportAnyGap} decided, and the gap it found if a handler must
     * be told. ⚠️ RETURNED, NOT LEFT IN A FIELD (M13.17, M12.17 review P1): a
     * field the caller had to clear could be read stale by the next commit.
     */
    private record Reported(boolean decode, GapReport report) {
        static final Reported DECODE = new Reported(true, null);
        static final Reported SKIP = new Reported(false, null);
    }

    /**
     * Deliveries this consumer's queue was too full to take.
     *
     * <p>⚠️ IT IS WHAT TELLS THE TWO GAPS APART. A gap with a local drop behind
     * it means this node fell behind; a gap with none means the records never
     * arrived, which is the other end of the channel and a different problem.
     */
    private final LongAdder dropped = new LongAdder();

    /**
     * ⚠️ HOW MANY DROPS HAVE ALREADY BEEN BLAMED ON A GAP. A drop belongs to
     * the first gap reported after it and to no other, so a lifetime
     * comparison -- which is what an earlier draft did -- makes one early drop
     * label every later gap "local", the misdiagnosis
     * {@link DeliveryGapException} exists to prevent, and the two have
     * opposite remedies.
     */
    private long droppedAttributed;

    /** Gaps reported on this stream (M6.1). */
    private final LongAdder gaps = new LongAdder();

    /** The most recent gap, or {@code null} if there has been none. */
    private volatile DeliveryGapException lastGap;

    private volatile Consumer<DeliveryGapException> gapHandler;
    private volatile boolean gapRepairPending;

    GapTracker(RunKey key) {
        this.key = key;
    }

    void recordDrop() {
        dropped.increment();
    }

    long dropped() {
        return dropped.sum();
    }

    /**
     * Whether {@link #reportAnyGap} will decode this delivery, read without
     * committing anything: first, in order, replayed, or a gap nobody repairs.
     */
    boolean wouldDecode(Delivery delivery, boolean replay) {
        long expected;
        synchronized (offsetLock) {
            expected = expectedNextOffset;
        }
        if (replay || expected < 0 || delivery.firstOffset() == expected) {
            return true;
        }
        return delivery.firstOffset() > expected && gapHandler == null;
    }

    /**
     * Commits {@code delivery} against the expected offset; {@code decoded}
     * says whether its records were decoded before the commit.
     */
    Commit commit(Delivery delivery, boolean replay, boolean decoded) {
        synchronized (offsetLock) {
            Reported reported = reportAnyGap(delivery, replay);
            boolean decode = reported.decode();
            GapReport report = reported.report();
            if (decode && !decoded) {
                // ⚠️ NEVER A DECODE AFTER THE COMMIT: one that threw here would
                // leave its retry reading as a duplicate -- the drop above.
                // ⚠️ REACHED ONLY BY A RACE (M11.13, H8; M10.23 review R4): the
                // catch-up lane committed up to this delivery between
                // `wouldDecode` and `reportAnyGap`, which then took its one
                // committing branch the check did not foresee --
                // `firstOffset == expected`. So the commit is undone to
                // `firstOffset` -- not to a value sampled before it, which the
                // racing commit may have moved (review R1) -- and the delivery
                // stays at the head, NOT CONSUMED: the next poll decodes it in
                // order. Throwing would pause the shard for a state that is not
                // unrecoverable (research 02 §6; review R2).
                // ⚠️ AND UNDER THE LOCK THE COMMIT TOOK (M12.17, M11.13 R4).
                expectedNextOffset = delivery.firstOffset();
                return new Commit(false, null, true);
            }
            return new Commit(decode, report, false);
        }
    }

    /**
     * Reports records this consumer will never be handed (M6.1).
     *
     * <p>⚠️ IT DOES NOT THROW, AND THIS PARAGRAPH SAID THE OPPOSITE UNTIL
     * ROUND-2 REVIEW. Research doc 02 § 6: "Any exception thrown from
     * {@code readNext} pauses ingestion for that shard ... and requires
     * operator intervention to resume ... only throw for genuinely
     * unrecoverable states." A gap is the opposite of unrecoverable -- the
     * records still exist upstream and the ladder can re-read them -- so a
     * throw converts a queue that overflowed for a second into a shard an
     * operator must restart by hand, and {@code BinStoreShardConsumer.drain}
     * would discard the records it had already polled, losing MORE than the
     * gap did. ⚠️ Whoever wires the ladder in M8 reads this method first: the
     * contract is a counter and a log, and reinstating the throw reinstates
     * the defect.
     *
     * <p>⚠️ SO {@link DeliveryGapException} IS CONSTRUCTED AND NEVER THROWN.
     * It stays an exception type because it carries a message an operator
     * reads and because a caller that CAN fail a batch -- one outside the
     * ingestion poller's contract -- may still want to throw it.
     *
     * <p>⚠️ AND EVERY GAP IS COUNTED, NOT ONLY THE FIRST. Reporting once and
     * falling silent is the same loss one step along, and a two-gap case pins
     * it.
     */
    private Reported reportAnyGap(Delivery delivery, boolean replay) {
        long expected = expectedNextOffset;
        long end = delivery.firstOffset() + delivery.recordCount();
        if (replay) {
            expectedNextOffset = Math.max(expectedNextOffset, end);
            return Reported.DECODE;
        }
        if (expected < 0) {
            expectedNextOffset = end;
            return Reported.DECODE;
        }
        if (delivery.firstOffset() < expected && end <= expected) {
            return Reported.SKIP;
        }
        if (delivery.firstOffset() < expected) {
            throw new IllegalStateException("delivery overlaps the next expected offset");
        }
        if (delivery.firstOffset() == expected) {
            expectedNextOffset = end;
            return Reported.DECODE;
        }
        // ⚠️ ATTRIBUTED, NOT COMPARED TO ZERO. A drop belongs to the FIRST gap
        // reported after it, and only to that one: the baseline moves when a
        // gap consumes it. A lifetime comparison labels every later gap
        // "local", which is the misdiagnosis this type exists to prevent.
        // ⚠️ AND THE BASELINE MOVES ONLY HERE, not on every delivery read -- a
        // drop can be counted while an EARLIER delivery is still queued, so
        // advancing it on that delivery would hand the drop to a gap that had
        // not happened yet and leave the real one looking upstream. Measured,
        // as the first draft of this method.
        long droppedNow = dropped.sum();
        boolean locally = droppedNow > droppedAttributed;
        droppedAttributed = droppedNow;
        DeliveryGapException gap =
                new DeliveryGapException(key, expected, delivery.firstOffset(), locally,
                        delivery.sequencerEpoch(), delivery.chainSequence());
        gaps.increment();
        lastGap = gap;
        // ⚠️ LOGGED AND COUNTED, NEVER THROWN, and the corpus is what decides
        // that: "Any exception thrown from `readNext` pauses ingestion for that
        // shard ... and requires operator intervention to resume ... only throw
        // for genuinely unrecoverable states" (research doc 02 § 6). A gap is
        // the opposite of unrecoverable -- the records still exist upstream and
        // the ladder can re-read them -- so throwing would convert a queue that
        // overflowed for a second into a shard an operator must restart by
        // hand. An earlier draft of this class DID throw, and the review that
        // caught it is the reason this paragraph exists.
        LOG.log(System.Logger.Level.WARNING, gap.getMessage());
        Consumer<DeliveryGapException> handler = gapHandler;
        if (handler == null) {
            expectedNextOffset = end;
            return Reported.DECODE;
        }
        gapRepairPending = true;
        // Keep the delivery that exposed the gap queued until replay reaches
        // this offset; decoding it now could make it impossible to suppress an
        // overlap if replay includes the same records.
        return new Reported(false, new GapReport(handler, gap));
    }

    void onGap(Consumer<DeliveryGapException> handler) {
        gapHandler = Objects.requireNonNull(handler, "handler");
    }

    boolean repairPending() {
        return gapRepairPending;
    }

    void clearRepair() {
        gapRepairPending = false;
    }

    long gapsDetected() {
        return gaps.sum();
    }

    Optional<DeliveryGapException> lastGap() {
        return Optional.ofNullable(lastGap);
    }
}
