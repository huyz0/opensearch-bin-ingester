// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.client;

import io.github.huyz0.os.biningester.format.RunEntry;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentReader;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.Deque;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;

/**
 * Subscribes to one stream and hands records to the plugin in order.
 *
 * <p>WARNING: {@code readNext} BLOCKS. That is what makes the zero-idle-cost
 * property reachable: a consumer with nothing to do is parked on a queue, not
 * spinning on a timer, so it issues no request and burns no CPU. A version that
 * returned empty immediately would turn every caller into a polling loop and
 * NFR-2 would be unreachable through the SPI (the M1 risk table calls this out
 * as a stop-and-re-plan).
 *
 * <p>WARNING: it also blocks for the FULL pollTimeout when nothing arrives.
 * Returning early is the same defect wearing a different mask: OpenSearch's
 * ingestion loop calls readNext again immediately, so an early return is a poll
 * at whatever rate the loop runs.
 *
 * <p>WARNING: the queue is BOUNDED. An unbounded one turns a slow consumer into
 * an OOM on the node, and constraint C8 budgets what may be in flight.
 */
public final class ConsumerClient implements AutoCloseable {

    private static final System.Logger LOG =
            System.getLogger(ConsumerClient.class.getName());

    private final ConsumerDeliveryQueues deliveryQueues;
    private final AutoCloseable subscription;
    private final RunKey key;
    private final SegmentFetcher fetcher;

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

    /** This stream's retained floor and the resume checks against it (M11.1). */
    private final RetainedFloorTracker floor;

    /** The most recent gap, or {@code null} if there has been none. */
    private volatile DeliveryGapException lastGap;

    private volatile java.util.function.Consumer<DeliveryGapException> gapHandler;
    private volatile boolean gapRepairPending;

    public ConsumerClient(SubscriptionTransport transport, RunKey key, int queueCapacity) {
        this(transport, key, queueCapacity, null);
    }

    public ConsumerClient(SubscriptionTransport transport, RunKey key, int queueCapacity,
            SegmentSource segmentSource) {
        this(transport, key, queueCapacity, segmentSource, SegmentFetchRetry.DEFAULT);
    }

    /** A subscribing client whose failed segment fetches are retried under {@code retry}. */
    public ConsumerClient(SubscriptionTransport transport, RunKey key, int queueCapacity,
            SegmentSource segmentSource, SegmentFetchRetry retry) {
        this(key, queueCapacity, segmentSource, retry,
                client -> Objects.requireNonNull(transport, "transport")
                        .subscribe(key, client.listener()));
    }

    /**
     * A client whose deliveries are FED to it, holding no subscription of its
     * own (M5.62).
     *
     * <p>⚠️ IT EXISTS SO ONE SUBSCRIPTION CAN SERVE MANY RUNS. A node holding
     * K runs of a segment used to build K clients and therefore K
     * subscriptions, which is K consumers to {@code SubscriptionHub} and K
     * copies of the bytes. The node-scoped subscriber registers ONCE for every
     * key it holds and routes each delivery here by its {@link Delivery#key()}.
     *
     * <p>⚠️ CLOSING ONE IS NOT UNSUBSCRIBING. The subscription belongs to
     * whoever fed it -- {@code NodeSubscriptions} -- and a client that tore it
     * down on close would unsubscribe every OTHER run sharing it. So
     * {@link #close()} here does NOTHING AT ALL: its subscription handle is a
     * no-op, the queue is dropped with the client itself, and unsubscribing is
     * the owner's to do.
     */
    public ConsumerClient(RunKey key, int queueCapacity, SegmentSource segmentSource) {
        this(key, queueCapacity, segmentSource, SegmentFetchRetry.DEFAULT);
    }

    /** A fed client whose failed segment fetches are retried under {@code retry} (M10.23). */
    public ConsumerClient(RunKey key, int queueCapacity, SegmentSource segmentSource,
            SegmentFetchRetry retry) {
        this(key, queueCapacity, segmentSource, retry, client -> () -> { });
    }

    private ConsumerClient(RunKey key, int queueCapacity, SegmentSource segmentSource,
            SegmentFetchRetry retry,
            java.util.function.Function<ConsumerClient, AutoCloseable> subscribe) {
        this.fetcher = new SegmentFetcher(segmentSource, retry);
        this.key = Objects.requireNonNull(key, "key");
        this.floor = new RetainedFloorTracker(key);
        if (queueCapacity <= 0) {
            throw new IllegalArgumentException("queue capacity must be positive");
        }
        this.deliveryQueues = new ConsumerDeliveryQueues(queueCapacity, this::decodeAndReportGap);
        this.subscription = subscribe.apply(this);
    }

    /**
     * This client as a subscription listener: deliveries queued, and the
     * retained floor applied (ADR-0056).
     *
     * <p>⚠️ **NOT {@code this::deliver}**, which is what subscribed before the
     * floor could travel -- a method reference implements only the one
     * abstract method, so the floor would be dropped by the default and this
     * client would never refuse a collected position.
     */
    private SubscriptionTransport.Listener listener() {
        return new SubscriptionTransport.Listener() {
            @Override
            public void onDelivery(Delivery delivery) {
                deliver(delivery);
            }

            @Override
            public void onRetainedFloor(RunKey floorKey, long oldestRetainedOffset) {
                // ⚠️ A FLOOR FOR ANOTHER STREAM IS IGNORED, not applied: this
                // client's refusal is about ITS stream, and applying another's
                // floor would refuse positions that are fine.
                if (key.equals(floorKey)) {
                    retainedFrom(oldestRetainedOffset);
                }
            }

            @Override
            public boolean wantsRetainedFloor(RunKey floorKey) {
                return key.equals(floorKey) && takeFloorAsk();
            }
        };
    }

    /**
     * Queues one delivery for this stream.
     *
     * <p>⚠️ OFFER, NOT PUT. A full queue must not block the ingester's commit
     * path; the consumer falls behind and recovers from the log, which is what
     * the log is for. That was true when this client held its own subscription
     * and is more so now that one subscription feeds many: a blocking client
     * would stall every other run on the node as well as the ingester.
     *
     * <p>⚠️ IT DOES NOT CHECK THE KEY. The router that calls this is what knows
     * which client a delivery belongs to, and a check here would be a second
     * copy of that decision -- but a mis-routed delivery is not silent either:
     * {@code decodeInto} looks up THIS client's key in the segment and throws
     * if the segment carries no run for it.
     */

    public void deliver(Delivery delivery) {
        if (!deliveryQueues.deliverLive(Objects.requireNonNull(delivery, "delivery"))) {
            // ⚠️ COUNTED, NOT LOGGED AND NOT THROWN. This runs on the
            // ingester's push path, where throwing would be indistinguishable
            // from a dead subscriber and would cost this node every OTHER
            // stream's deliveries too. The count is read at the moment a gap
            // is reported, which is where it answers a question.
            dropped.increment();
        }
    }

    /** Opens a node-scoped catch-up delivery lane for one exchange. */
    public void beginCatchUp(java.util.UUID requestId) {
        deliveryQueues.beginCatchUp(requestId, gapRepairPending);
    }

    /** Adds a replay delivery, applying backpressure when the bounded lane is full. */
    public void deliverCatchUp(java.util.UUID requestId, Delivery delivery)
            throws InterruptedException {
        if (!key.equals(Objects.requireNonNull(delivery, "delivery").key())) {
            throw new IllegalArgumentException("catch-up delivery belongs to another stream");
        }
        deliveryQueues.deliverCatchUp(requestId, delivery);
    }

    /** Adds replay to the bounded lane without holding the caller on consumer progress. */
    public boolean tryDeliverCatchUp(java.util.UUID requestId, Delivery delivery) {
        if (!key.equals(Objects.requireNonNull(delivery, "delivery").key())) {
            throw new IllegalArgumentException("catch-up delivery belongs to another stream");
        }
        return deliveryQueues.tryDeliverCatchUp(requestId, delivery);
    }

    /** Marks the matching exchange end; true when all replay records are handed out. */
    public boolean completeCatchUp(java.util.UUID requestId) {
        return deliveryQueues.completeCatchUp(requestId);
    }

    /** Whether the matching exchange ended and all its records were handed out. */
    public boolean catchUpComplete(java.util.UUID requestId) {
        return deliveryQueues.catchUpComplete(requestId);
    }

    /** Bounded number of live records served while catch-up is also pending. */
    public static final int LIVE_RECORD_QUANTUM = 8;

    /**
     * The oldest offset of this stream that is still readable (M7.16): only
     * ever rises, a negative report is ignored and counted, and
     * {@code Long.MAX_VALUE} is the unknown sentinel. See
     * {@link RetainedFloorTracker} for the rules and their reasons.
     */
    public void retainedFrom(long oldestRetainedOffset) {
        floor.retainedFrom(oldestRetainedOffset);
    }

    /**
     * How many polls a resume may spend asking for a floor (ADR-0056); see
     * {@link RetainedFloorTracker}.
     */
    public static final int MAX_FLOOR_ASKS = RetainedFloorTracker.MAX_FLOOR_ASKS;

    /**
     * Asks the subscription for a fresh floor, and returns the report count to
     * wait past (M8.6, ADR-0056). Called on a resume, and only there.
     */
    public long requestFreshFloor() {
        return floor.requestFreshFloor();
    }

    /** How many floor reports this client has received. */
    public long floorReports() {
        return floor.floorReports();
    }

    /** Whether the next poll should ask for the floor, spending one ask if so. */
    public boolean takeFloorAsk() {
        return floor.takeFloorAsk();
    }

    /** The retained floor this client has been told, or empty if none yet. */
    public java.util.OptionalLong retainedFloor() {
        return floor.retainedFloor();
    }

    /**
     * Refuses a position whose records have been collected; an unknown floor
     * refuses nothing, and the floor itself is fine.
     *
     * @throws IllegalArgumentException if {@code fromOffset} is negative
     * @throws PositionCollectedException if {@code fromOffset} is below the floor
     */
    public void refuseIfCollected(long fromOffset) throws PositionCollectedException {
        floor.refuseIfCollected(fromOffset);
    }

    /**
     * Checks a resume, and says whether a floor reported after
     * {@code freshAfter} has now cleared it (M8.44).
     *
     * @return true if a report newer than {@code freshAfter} was checked and passed
     * @throws PositionCollectedException if {@code fromOffset} is below the floor
     */
    public boolean checkResume(long fromOffset, long freshAfter)
            throws PositionCollectedException {
        return floor.checkResume(fromOffset, freshAfter);
    }

    /** The count seam of {@link #checkResume(long, long)}, for an interleaving test. */
    boolean checkResume(long fromOffset, long freshAfter, LongSupplier reportCount)
            throws PositionCollectedException {
        return floor.checkResume(fromOffset, freshAfter, reportCount);
    }

    /** How many positions this client has refused — one per attempt, not one per stream. */
    public long positionsRefused() {
        return floor.positionsRefused();
    }

    /** How many floor reports were ignored as impossible. */
    public long floorReportsIgnored() {
        return floor.floorReportsIgnored();
    }

    /** Deliveries dropped because this consumer's queue was full (M6.1). */
    public long droppedDeliveries() {
        return dropped.sum();
    }

    /**
     * The next record, or empty if none arrived within {@code pollTimeout}.
     *
     * <p>WARNING: blocks for the WHOLE timeout when nothing arrives.
     *
     * <p>⚠️ A FAILED SEGMENT FETCH IS AN EMPTY POLL, NOT A THROW (M10.23,
     * research 02 §6): the delivery stays at the head and a later call retries
     * it once its backoff is served -- see {@link SegmentFetcher} for why the
     * retry is not made within this call's deadline. It throws only once the
     * policy's attempts are spent, or for a segment that will not decode.
     */
    public Optional<ConsumerRecord> readNext(Duration pollTimeout) throws InterruptedException {
        try {
            return deliveryQueues.readNext(pollTimeout);
        } catch (SegmentFetcher.Deferred deferred) {
            // ⚠️ A CALL THAT FETCHED WAITS NO FURTHER: it may already have
            // blocked on the queue and on the fetch, and waiting out the new
            // backoff on top could hold the caller past its timeout.
            if (!deferred.attempted()) {
                fetcher.awaitBackoff(pollTimeout);
            }
            return Optional.empty();
        }
    }

    private boolean decodeAndReportGap(Delivery delivery, Deque<ConsumerRecord> out,
            boolean replay) {
        // ⚠️ DECODED BEFORE ANY OFFSET IS COMMITTED (M10.23). `reportAnyGap`
        // advances the expected offset, so a fetch failing after it would
        // leave its retry looking like a duplicate of itself -- dropped, and
        // the window with it. Decoding first leaves a throw touching nothing.
        Deque<ConsumerRecord> decoded = null;
        if (wouldDecode(delivery, replay)) {
            decoded = new java.util.ArrayDeque<>();
            decodeInto(delivery, decoded);
        }
        boolean decode = reportAnyGap(delivery, replay);
        if (decode) {
            if (decoded == null) {
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
                expectedNextOffset = delivery.firstOffset();
                return false;
            }
            out.addAll(decoded);
        }
        return decode || !gapRepairPending;
    }

    /**
     * Whether {@link #reportAnyGap} will decode this delivery, read without
     * committing anything: first, in order, replayed, or a gap nobody repairs.
     */
    private boolean wouldDecode(Delivery delivery, boolean replay) {
        long expected = expectedNextOffset;
        if (replay || expected < 0 || delivery.firstOffset() == expected) {
            return true;
        }
        return delivery.firstOffset() > expected && gapHandler == null;
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
    private boolean reportAnyGap(Delivery delivery, boolean replay) {
        long expected = expectedNextOffset;
        long end = delivery.firstOffset() + delivery.recordCount();
        if (replay) {
            expectedNextOffset = Math.max(expectedNextOffset, end);
            return true;
        }
        if (expected < 0) {
            expectedNextOffset = end;
            return true;
        }
        if (delivery.firstOffset() < expected && end <= expected) {
            return false;
        }
        if (delivery.firstOffset() < expected) {
            throw new IllegalStateException("delivery overlaps the next expected offset");
        }
        if (delivery.firstOffset() == expected) {
            expectedNextOffset = end;
            return true;
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
        java.util.function.Consumer<DeliveryGapException> handler = gapHandler;
        if (handler == null) {
            expectedNextOffset = end;
            return true;
        }
        gapRepairPending = true;
        deliveryQueues.pauseLiveForGap();
        handler.accept(gap);
        // Keep the delivery that exposed the gap queued until replay reaches
        // this offset; decoding it now could make it impossible to suppress an
        // overlap if replay includes the same records.
        return false;
    }

    /** Installs the node-wide replay owner for delivery gaps. */
    public void onGap(java.util.function.Consumer<DeliveryGapException> handler) {
        gapHandler = Objects.requireNonNull(handler, "handler");
    }

    /** Releases this client's read hold after its gap replay completes. */
    public void completeGapRepair() {
        if (gapRepairPending) {
            deliveryQueues.resumeLiveAfterGap();
            gapRepairPending = false;
        }
    }

    /** How many gaps this consumer has reported (M6.1). */
    public long gapsDetected() {
        return gaps.sum();
    }

    /**
     * The most recent gap, or empty if there has been none.
     *
     * <p>⚠️ IT IS STATE RATHER THAN AN EXCEPTION because of what the SPI does
     * with a throw; see {@link #readNext}.
     */
    public Optional<DeliveryGapException> lastGap() {
        return Optional.ofNullable(lastGap);
    }

    private void decodeInto(Delivery delivery, Deque<ConsumerRecord> out) {
        try {
            SegmentReader reader = SegmentReader.open(fetcher.bytesOf(delivery));
            RunEntry entry = reader.find(key).orElseThrow(
                    () -> new IOException("segment " + delivery.segmentKey()
                            + " carries no run for " + key));
            List<SegmentRecord> records = reader.read(entry);
            long createdAt = reader.createdAtMillis();
            long offset = delivery.firstOffset();
            for (SegmentRecord r : records) {
                // WARNING: the offset comes from the COMMIT LOG's assignment,
                // advanced per record. Deriving it from the segment position
                // instead coincides within one flush and diverges across them.
                out.add(new ConsumerRecord(offset++, r, createdAt));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** How many deliveries are queued but not yet decoded. */
    public int queuedDeliveries() {
        return deliveryQueues.queuedLiveDeliveries();
    }

    /**
     * WARNING: declares no checked exception. AutoCloseable's default would let
     * close() throw InterruptedException, and a caller using try-with-resources
     * would then have to handle an interrupt on a path that cannot be
     * interrupted -- javac warns about exactly this.
     */
    @Override
    public void close() {
        try {
            subscription.close();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("failed to unsubscribe", e);
        }
    }
}
