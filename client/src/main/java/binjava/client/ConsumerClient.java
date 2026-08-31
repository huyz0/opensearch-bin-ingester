// SPDX-License-Identifier: Apache-2.0
package binjava.client;

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

    private final BlockingQueue<Delivery> deliveries;
    private final Deque<ConsumerRecord> ready = new ArrayDeque<>();
    private final AutoCloseable subscription;
    private final RunKey key;

    public ConsumerClient(SubscriptionTransport transport, RunKey key, int queueCapacity) {
        this.key = Objects.requireNonNull(key, "key");
        if (queueCapacity <= 0) {
            throw new IllegalArgumentException("queue capacity must be positive");
        }
        this.deliveries = new ArrayBlockingQueue<>(queueCapacity);
        this.subscription = Objects.requireNonNull(transport, "transport").subscribe(key, d -> {
            // WARNING: OFFER, not put. A full queue must not block the ingester's
            // commit path; the consumer falls behind and recovers from the log,
            // which is what the log is for.
            deliveries.offer(d);
        });
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
        decodeInto(delivery, ready);
        return Optional.ofNullable(ready.poll());
    }

    private void decodeInto(Delivery delivery, Deque<ConsumerRecord> out) {
        try {
            SegmentReader reader = SegmentReader.open(delivery.segment());
            RunEntry entry = reader.find(key).orElseThrow(
                    () -> new IOException("segment " + delivery.segmentKey()
                            + " carries no run for " + key));
            List<SegmentRecord> records = reader.read(entry);
            long offset = delivery.firstOffset();
            for (SegmentRecord r : records) {
                // WARNING: the offset comes from the COMMIT LOG's assignment,
                // advanced per record. Deriving it from the segment position
                // instead coincides within one flush and diverges across them.
                out.add(new ConsumerRecord(offset++, r));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
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
