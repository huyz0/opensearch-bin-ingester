// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.client;

import io.github.huyz0.os.biningester.format.RunKey;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;

/**
 * One stream's retained floor as a {@link ConsumerClient} was told it, and the
 * resume checks made against it (M7.16, M8.6, M8.44, ADR-0056).
 *
 * <p>⚠️ **EXTRACTED FROM {@code ConsumerClient} WITH NO CHANGE OF BEHAVIOUR**
 * (M11.1): the client's public floor methods delegate here, so the rules and
 * their reasons are stated once, beside the code they explain.
 */
final class RetainedFloorTracker {

    /**
     * Nobody has said where this stream now starts.
     *
     * <p>⚠️ {@code MAX_VALUE} RATHER THAN A NEGATIVE, so the unknown case is
     * carried by the explicit check and not by arithmetic: with {@code -1} the
     * comparison {@code fromOffset >= floor} is true for every real offset, so
     * deleting the check changes nothing and the sentinel's VALUE silently
     * becomes the rule. Here, deleting it refuses everything.
     */
    private static final long FLOOR_UNKNOWN = Long.MAX_VALUE;

    private final RunKey key;

    /**
     * The oldest offset still readable, as last reported.
     *
     * <p>⚠️ IT STARTS UNKNOWN AND ONLY RISES, so a client nobody has told
     * refuses nothing and a stale lower report cannot walk it back.
     */
    private final AtomicLong retainedFloor = new AtomicLong(FLOOR_UNKNOWN);

    private final LongAdder refusals = new LongAdder();

    /** Reports this client refused to believe — a defect upstream, counted here. */
    private final LongAdder floorReportsIgnored = new LongAdder();

    RetainedFloorTracker(RunKey key) {
        this.key = Objects.requireNonNull(key, "key");
    }

    /**
     * The oldest offset of this stream that is still readable (M7.16).
     *
     * <p>⚠️ IT ONLY EVER RISES — with one in-domain exception, stated below:
     * reporting {@code Long.MAX_VALUE} stores the SENTINEL, after which the
     * next report is treated as the first and can be lower. Retention does not
     * give records back, so a LOWER report is stale rather than a correction — and a floor that walked
     * backwards would let a consumer resume into records that are already gone
     * and meet a 404 with no name on it.
     *
     * <p>⚠️ A NEGATIVE REPORT IS IGNORED AND COUNTED. It is a defect upstream,
     * and the consumer path is not where it should be discovered — but a guard
     * that silently returned would make it undiscoverable anywhere.
     *
     * <p>⚠️ {@code Long.MAX_VALUE} IS THE SENTINEL, so reporting it literally
     * leaves the floor unknown and a later report is treated as the first. No
     * real stream reaches that offset; it is stated because the value is in
     * this parameter's domain rather than outside it.
     */
    void retainedFrom(long oldestRetainedOffset) {
        if (oldestRetainedOffset < 0) {
            floorReportsIgnored.increment();
            return;
        }
        // ⚠️ A REPORT IS AN ANSWER, WHATEVER ITS VALUE. A resume waits for a
        // floor that arrived AFTER it asked (see `requestFreshFloor`), and a
        // report lower than what is held is still news that the ingester was
        // asked and answered -- the held floor, not the report, is what is
        // checked against.
        floorAsksLeft.set(0);
        // ⚠️ THE FLOOR RISES BEFORE THE COUNT DOES (M8.44), so a reader that
        // sees the count move sees a floor at least that new -- see
        // `checkResume`, which reads them in the opposite order.
        raise(oldestRetainedOffset);
        floorReports.incrementAndGet();
    }

    private void raise(long oldestRetainedOffset) {
        long floor = retainedFloor.get();
        if (floor == FLOOR_UNKNOWN
                && retainedFloor.compareAndSet(FLOOR_UNKNOWN, oldestRetainedOffset)) {
            // ⚠️ THE FIRST REPORT WINS OUTRIGHT, because the sentinel is
            // MAX_VALUE and `max` would keep it forever.
            return;
        }
        // ⚠️ AND A LOSER OF THAT RACE FALLS THROUGH RATHER THAN RETURNING.
        // Two reports arriving TOGETHER into a fresh client would otherwise
        // discard the higher one -- the floor stays low, the client
        // under-refuses, and that is the SILENT direction.
        // ⚠️ THE RACING LOSER IS UNPINNED, AND SAID SO: both reports have to be
        // in flight at once to reach that arm, and a case that raced them would
        // assert a schedule rather than the rule. The ORDINARY path through
        // this line -- every non-first report -- is deterministic and is pinned
        // by `aHIGHERReportADVANCESTheFloor`.
        retainedFloor.accumulateAndGet(oldestRetainedOffset, Math::max);
    }

    /**
     * ⚠️ **HOW MANY POLLS A RESUME MAY SPEND ASKING FOR A FLOOR** (ADR-0056).
     * Bounded, because asking for one the ingester does not have -- a stream
     * missing from the newest checkpoint, a term that has not checkpointed yet
     * -- would otherwise ask for ever, and each ask can cost the serving pod a
     * store read once per refresh interval: a timer on an idle pod by another
     * name, which review found in the round that asked until a floor arrived.
     * At the default poll wait this is a few minutes; past it the shard keeps
     * checking against whatever it holds, and refuses nothing it cannot prove.
     */
    static final int MAX_FLOOR_ASKS = 8;

    private final AtomicInteger floorAsksLeft = new AtomicInteger();

    private final AtomicLong floorReports = new AtomicLong();

    /**
     * Asks the subscription for a fresh floor, and returns the report count to
     * wait past (M8.6, ADR-0056).
     *
     * <p>⚠️ **CALLED ON A RESUME, AND ONLY THERE.** A floor matters when a
     * shard asks for a stored position; a shard tailing the stream sits at the
     * head, far above any floor, and asks for nothing. So a node whose shards
     * are all tailing asks for no floor and costs no read.
     *
     * <p>⚠️ **FRESH, BECAUSE A HELD FLOOR CAN BE HOURS OLD.** One learned at
     * 09:00 and checked against a reset at 15:00 passes positions GC has since
     * collected -- and the shard then meets the unnamed 404 this whole change
     * exists to replace. A held floor may still REFUSE (floors only rise, so it
     * is a true lower bound), but only one reported after this call may CLEAR
     * a resume.
     */
    long requestFreshFloor() {
        long seen = floorReports.get();
        floorAsksLeft.set(MAX_FLOOR_ASKS);
        return seen;
    }

    /** How many floor reports this client has received. */
    long floorReports() {
        return floorReports.get();
    }

    /**
     * Whether the next poll should ask for the floor, spending one ask if so.
     *
     * <p>⚠️ **SPENT HERE, BY THE POLL THAT ASKS**, so the bound is counted in
     * polls the ingester actually receives rather than in time this module
     * cannot read without a clock.
     */
    boolean takeFloorAsk() {
        return floorAsksLeft.getAndUpdate(n -> n > 0 ? n - 1 : 0) > 0;
    }

    /**
     * The retained floor this client has been told, or empty if none yet.
     *
     * <p>⚠️ **SO A CALLER CAN TELL "NOT TOLD YET" FROM "FINE".** The floor
     * arrives on the subscription, asynchronously, and may land after the
     * first read of a resumed shard. {@code BinStoreShardConsumer} does not
     * wait for it: it re-checks a resume against whatever is held until a
     * report newer than the resume arrives ({@link #checkResume}), and the
     * asks for one are bounded by {@link #MAX_FLOOR_ASKS}.
     */
    java.util.OptionalLong retainedFloor() {
        long floor = retainedFloor.get();
        return floor == FLOOR_UNKNOWN ? java.util.OptionalLong.empty()
                : java.util.OptionalLong.of(floor);
    }

    /**
     * Refuses a position whose records have been collected.
     *
     * <p>⚠️ AN UNKNOWN FLOOR REFUSES NOTHING, and is not a floor of zero nor
     * one of {@code MAX_VALUE}: a deployment whose ingester never reports the
     * boundary must keep working exactly as it did before this existed.
     *
     * <p>⚠️ AND THE FLOOR ITSELF IS FINE. It is the oldest offset still
     * READABLE, so a consumer sitting exactly on it has lost nothing; refusing
     * there would refuse every consumer of a stream the moment anything was
     * collected.
     *
     * <p>⚠️ WHERE IT IS THROWN DECIDES WHETHER AN OPERATOR EVER SEES IT, and
     * M6.8 measured the wrong site: {@code DefaultStreamPoller} CATCHES
     * whatever {@code createShardConsumer} throws, logs at WARN and retries
     * forever — index GREEN, shard started, nothing indexed, which is ADR-0020's
     * failure mode. This refusal belongs on the {@code readNext} path, where
     * research doc 02 §6 says an exception PAUSES that shard and the state is
     * visible through {@code _ingestion/_state}. Thrown from consumer
     * construction it would be swallowed, and this class would reproduce the
     * failure it exists to replace.
     *
     * @throws IllegalArgumentException if {@code fromOffset} is negative — a
     *     caller defect, and reporting it as a loss of {@code floor + 1}
     *     records would be an incident report with a fabricated size
     * @throws PositionCollectedException if {@code fromOffset} is below the
     *     floor — ⚠️ thrown rather than counted-and-continued, unlike a gap:
     *     research doc 02 §6 says an exception out of the poll path pauses that
     *     shard until an operator intervenes, which is EXACTLY right here and
     *     exactly wrong for a gap, because the records below the floor will
     *     never arrive however long the shard waits
     */
    void refuseIfCollected(long fromOffset) throws PositionCollectedException {
        if (fromOffset < 0) {
            throw new IllegalArgumentException("a position is never negative: " + fromOffset);
        }
        long floor = retainedFloor.get();
        if (floor == FLOOR_UNKNOWN || fromOffset >= floor) {
            return;
        }
        refusals.increment();
        throw new PositionCollectedException(key, fromOffset, floor);
    }

    /**
     * Checks a resume, and says whether a floor reported after
     * {@code freshAfter} has now cleared it (M8.44).
     *
     * <p>⚠️ **THE COUNT IS READ BEFORE THE CHECK**, and {@link #retainedFrom}
     * raises the floor before it moves the count: a count that says a fresh
     * report arrived therefore comes with a floor at least that fresh. Read the
     * other way round, a report landing between the two cleared a resume its
     * floor was never checked against. The fallback was the old 404, not data
     * loss, but it is the defect this check exists to replace.
     *
     * @return true if a report newer than {@code freshAfter} was checked and passed
     * @throws PositionCollectedException if {@code fromOffset} is below the floor
     */
    boolean checkResume(long fromOffset, long freshAfter)
            throws PositionCollectedException {
        return checkResume(fromOffset, freshAfter, floorReports::get);
    }

    /**
     * Package-private count seam so the read/check ordering can be tested under
     * a forced report interleaving without changing the public resume API.
     */
    boolean checkResume(long fromOffset, long freshAfter, LongSupplier reportCount)
            throws PositionCollectedException {
        long reports = Objects.requireNonNull(reportCount, "reportCount").getAsLong();
        refuseIfCollected(fromOffset);
        return reports > freshAfter;
    }

    /** How many positions this client has refused — one per attempt, not one per stream. */
    long positionsRefused() {
        return refusals.sum();
    }

    /** How many floor reports were ignored as impossible. */
    long floorReportsIgnored() {
        return floorReportsIgnored.sum();
    }
}
