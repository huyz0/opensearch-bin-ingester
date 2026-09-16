// SPDX-License-Identifier: Apache-2.0
package binjava.client;

import binjava.format.FetchMode;
import binjava.format.RunEntry;
import binjava.format.RunKey;
import binjava.format.SegmentReader;
import binjava.format.SegmentRecord;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.TimeUnit;

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

    private final BlockingQueue<Delivery> deliveries;
    private final Deque<ConsumerRecord> ready = new ArrayDeque<>();
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

    /** The most recent gap, or {@code null} if there has been none. */
    private volatile DeliveryGapException lastGap;

    public ConsumerClient(SubscriptionTransport transport, RunKey key, int queueCapacity) {
        this(transport, key, queueCapacity, null);
    }

    public ConsumerClient(SubscriptionTransport transport, RunKey key, int queueCapacity,
            SegmentSource segmentSource) {
        this(key, queueCapacity, segmentSource,
                client -> Objects.requireNonNull(transport, "transport")
                        .subscribe(key, client::deliver));
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
        this.deliveries = new ArrayBlockingQueue<>(queueCapacity);
        this.subscription = subscribe.apply(this);
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
        if (!deliveries.offer(Objects.requireNonNull(delivery, "delivery"))) {
            // ⚠️ COUNTED, NOT LOGGED AND NOT THROWN. This runs on the
            // ingester's push path, where throwing would be indistinguishable
            // from a dead subscriber and would cost this node every OTHER
            // stream's deliveries too. The count is read at the moment a gap
            // is reported, which is where it answers a question.
            dropped.increment();
        }
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
        if (!ready.isEmpty()) {
            return Optional.of(ready.poll());
        }
        Delivery delivery = deliveries.poll(pollTimeout.toMillis(), TimeUnit.MILLISECONDS);
        if (delivery == null) {
            return Optional.empty();
        }
        reportAnyGap(delivery);
        decodeInto(delivery, ready);
        return Optional.ofNullable(ready.poll());
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
        return deliveries.size();
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
