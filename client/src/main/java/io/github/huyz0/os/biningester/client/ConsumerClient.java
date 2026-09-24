// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.client;

import io.github.huyz0.os.biningester.format.FetchMode;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

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
    private final SegmentSource segmentSource;

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

    /** The most recent gap, or {@code null} if there has been none. */
    private volatile DeliveryGapException lastGap;

    public ConsumerClient(SubscriptionTransport transport, RunKey key, int queueCapacity) {
        this(transport, key, queueCapacity, null);
    }

    public ConsumerClient(SubscriptionTransport transport, RunKey key, int queueCapacity,
            SegmentSource segmentSource) {
        this(key, queueCapacity, segmentSource,
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
        this(key, queueCapacity, segmentSource, client -> () -> { });
    }

    private ConsumerClient(RunKey key, int queueCapacity, SegmentSource segmentSource,
            java.util.function.Function<ConsumerClient, AutoCloseable> subscribe) {
        this.segmentSource = segmentSource;
        this.key = Objects.requireNonNull(key, "key");
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
        deliveryQueues.beginCatchUp(requestId);
    }

    /** Adds a replay delivery, applying backpressure when the bounded lane is full. */
    public void deliverCatchUp(java.util.UUID requestId, Delivery delivery)
            throws InterruptedException {
        if (!key.equals(Objects.requireNonNull(delivery, "delivery").key())) {
            throw new IllegalArgumentException("catch-up delivery belongs to another stream");
        }
        deliveryQueues.deliverCatchUp(requestId, delivery);
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
    public void retainedFrom(long oldestRetainedOffset) {
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
    public static final int MAX_FLOOR_ASKS = 8;

    private final java.util.concurrent.atomic.AtomicInteger floorAsksLeft =
            new java.util.concurrent.atomic.AtomicInteger();

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
    public long requestFreshFloor() {
        long seen = floorReports.get();
        floorAsksLeft.set(MAX_FLOOR_ASKS);
        return seen;
    }

    /** How many floor reports this client has received. */
    public long floorReports() {
        return floorReports.get();
    }

    /**
     * Whether the next poll should ask for the floor, spending one ask if so.
     *
     * <p>⚠️ **SPENT HERE, BY THE POLL THAT ASKS**, so the bound is counted in
     * polls the ingester actually receives rather than in time this module
     * cannot read without a clock.
     */
    public boolean takeFloorAsk() {
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
    public java.util.OptionalLong retainedFloor() {
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
    public void refuseIfCollected(long fromOffset) throws PositionCollectedException {
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
    public boolean checkResume(long fromOffset, long freshAfter)
            throws PositionCollectedException {
        long reports = floorReports.get();
        refuseIfCollected(fromOffset);
        return reports > freshAfter;
    }

    /** How many positions this client has refused — one per attempt, not one per stream. */
    public long positionsRefused() {
        return refusals.sum();
    }

    /** How many floor reports were ignored as impossible. */
    public long floorReportsIgnored() {
        return floorReportsIgnored.sum();
    }

    /** Deliveries dropped because this consumer's queue was full (M6.1). */
    public long droppedDeliveries() {
        return dropped.sum();
    }

    /**
     * The next record, or empty if none arrived within {@code pollTimeout}.
     *
     * <p>WARNING: blocks for the WHOLE timeout when nothing arrives.
     */
    public Optional<ConsumerRecord> readNext(Duration pollTimeout) throws InterruptedException {
        return deliveryQueues.readNext(pollTimeout);
    }

    private void decodeAndReportGap(Delivery delivery, Deque<ConsumerRecord> out) {
        reportAnyGap(delivery);
        decodeInto(delivery, out);
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
    private void reportAnyGap(Delivery delivery) {
        long expected = expectedNextOffset;
        long end = delivery.firstOffset() + delivery.recordCount();
        // ⚠️ NEVER REWOUND. A delivery starting BEFORE what was expected is a
        // REPEAT, not a gap -- the hub re-sending a window a resubscribe
        // already covered -- and moving the tracker backwards would then
        // report a gap on the next ordinary delivery, manufacturing the defect
        // this method exists to find.
        expectedNextOffset = Math.max(expectedNextOffset, end);
        if (expected < 0 || delivery.firstOffset() <= expected) {
            return;
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
                new DeliveryGapException(key, expected, delivery.firstOffset(), locally);
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
            SegmentReader reader = SegmentReader.open(bytesOf(delivery));
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

    /**
     * The segment's bytes: carried on the delivery, or fetched under a grant.
     *
     * <p>⚠️ A FAILED FETCH PROPAGATES. Catching it and returning an empty array
     * would have {@code readNext} report an ordinary empty poll while a whole
     * window went missing -- the caller is told the stream is healthy and the
     * records are simply gone. An expired grant and a 403 are the NORMAL
     * failures on this path rather than the corrupt-segment case, so the quiet
     * handling is the likely one and is what this method refuses.
     *
     * <p>⚠️ AND NO SOURCE IS AN {@code IllegalStateException}, not a silent
     * empty stream: it is the mirror of {@code SubscriptionHub}'s null-issuer
     * guard, and a pod that elects {@code direct} for a consumer built without
     * a source is a misconfiguration an operator must see.
     */
    private byte[] bytesOf(Delivery delivery) throws IOException {
        if (delivery.via() != FetchMode.DIRECT) {
            return delivery.segment();
        }
        if (segmentSource == null) {
            throw new IllegalStateException("segment " + delivery.segmentKey()
                    + " was served `direct` to a consumer with no segment source; a deployment "
                    + "enabling `direct` builds one (M5.45g)");
        }
        return segmentSource.fetch(delivery.grant());
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
