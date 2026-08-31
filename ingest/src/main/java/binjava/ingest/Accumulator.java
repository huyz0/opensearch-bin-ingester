// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import binjava.format.RunKey;
import binjava.format.SegmentRecord;
import binjava.format.SegmentWriter;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;

/**
 * Buffers records across streams until a flush is due (M1.8).
 *
 * <p>⚠️ ONE ACCUMULATOR BUNDLES MANY STREAMS INTO ONE SEGMENT. That is the whole
 * economic argument: a PUT costs the same whether it carries one index's records
 * or a thousand, so request rate scales with FLUSHES, not with indices,
 * partitions or records (non-negotiable 6). An accumulator per partition would
 * restore exactly the cost Kafka has.
 *
 * <p>⚠️ THE TIMER STARTS AT THE FIRST APPEND OF A SEGMENT, not at the last. If
 * it restarted on every append, a steady trickle would never flush at all and
 * latency would be unbounded while the segment slowly filled.
 *
 * <p>⚠️ The clock is INJECTED. Business logic touches no clock directly
 * (non-negotiable 7), and criterion 3's 1,600 idle consumers advance an injected
 * clock across 3,000 poll intervals rather than sleeping — a test that slept
 * could not fit L0's 90-second budget.
 */
public final class Accumulator {

    private final IngestConfig config;
    private final Clock clock;

    private SegmentWriter writer = new SegmentWriter();
    private long bufferedBytes;
    private long firstAppendMillis = -1;

    public Accumulator(IngestConfig config, Clock clock) {
        this.config = Objects.requireNonNull(config, "config");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /**
     * Buffers one record.
     *
     * @return true when a flush is now due
     */
    public boolean add(RunKey key, SegmentRecord record) {
        long now = clock.millis();
        if (firstAppendMillis < 0) {
            firstAppendMillis = now;
        }
        writer.add(key, record, now);
        // ⚠️ An ESTIMATE of the framed size, not the payload length: the id, the
        // version and the length prefixes are bytes in the object too, and a
        // trigger that counted only payloads would overshoot the segment size by
        // however much framing costs.
        bufferedBytes += estimatedFramedBytes(record);
        return isFlushDue();
    }

    static long estimatedFramedBytes(SegmentRecord record) {
        return 1                                              // flags
                + 5 + record.id().getBytes(java.nio.charset.StandardCharsets.UTF_8).length
                + (record.version().isPresent() ? 10 : 0)
                + 5 + record.payload().length;
    }

    /** Whether the buffered records should be written now. */
    public boolean isFlushDue() {
        if (isEmpty()) {
            // ⚠️ An empty accumulator is NEVER due. A timer that fired on an
            // idle stream would issue a PUT per interval forever — the exact
            // shape criterion 3 asserts must not happen.
            return false;
        }
        if (bufferedBytes >= config.maxSegmentBytes()) {
            return true;
        }
        Duration waited = Duration.ofMillis(clock.millis() - firstAppendMillis);
        // ⚠️ `>= 0` on the comparison: at EXACTLY the interval the flush is due.
        // Strictly-greater delays every flush by one clock tick, which at 250 ms
        // is a latency floor nobody would find by reading the code.
        return waited.compareTo(config.flushInterval()) >= 0;
    }

    public boolean isEmpty() {
        return writer.isEmpty();
    }

    public long bufferedBytes() {
        return bufferedBytes;
    }

    /**
     * Takes the buffered segment and starts a new one.
     *
     * @return the segment bytes, or empty if nothing was buffered
     */
    public java.util.Optional<byte[]> drain() throws IOException {
        if (isEmpty()) {
            return java.util.Optional.empty();
        }
        byte[] segment = writer.toByteArray(firstAppendMillis);
        writer = new SegmentWriter();
        bufferedBytes = 0;
        firstAppendMillis = -1;
        return java.util.Optional.of(segment);
    }
}
