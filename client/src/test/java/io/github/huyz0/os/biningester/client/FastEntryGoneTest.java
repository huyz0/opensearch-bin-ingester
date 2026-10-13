// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.client;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.FastKey;
import io.github.huyz0.os.biningester.format.FetchMode;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.format.SegmentWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * A pushed fast entry its pod no longer holds (M13.30n; ADR-0082 §6): its
 * {@code 404} is its release, so the delivery is dropped -- not retried, and
 * nothing committed -- and the next delivery's gap reads its offsets from the
 * chain. A data segment's {@code 404} is still a failure, retried by policy.
 */
class FastEntryGoneTest {

    private static final RunKey KEY = RunKey.ofIndexUuid("nVzgup36TLqWp7VBBREj1w", 0);

    private static byte[] segmentOf(String id) throws Exception {
        SegmentWriter w = new SegmentWriter();
        w.add(KEY, new SegmentRecord(id, OpType.INDEX, OptionalLong.of(1),
                ("{\"id\":\"" + id + "\"}").getBytes(StandardCharsets.UTF_8)), 1L);
        return w.toByteArray(1L);
    }

    /** 404 for every key. */
    private static final class Gone implements SegmentSource {
        final AtomicInteger asked = new AtomicInteger();

        @Override
        public byte[] fetch(io.github.huyz0.os.biningester.format.Grant grant) {
            throw new AssertionError("proxy only");
        }

        @Override
        public byte[] fetchSegment(String segmentKey) throws IOException {
            asked.incrementAndGet();
            throw new SegmentNotFoundException(segmentKey + " answered 404");
        }
    }

    @Test
    void aGONEFastEntryIsDroppedUncommittedAndTheNextDeliveryIsAGap() throws Exception {
        Gone source = new Gone();
        try (ConsumerClient client = new ConsumerClient(KEY, 16, source,
                TestRetries.noFailedFetch())) {
            client.deliver(new Delivery(KEY, "seg-0", 1, 0L, FetchMode.INLINE, segmentOf("a")));
            assertThat(client.readNext(Duration.ofMillis(50))).as("PREMISE: offset 0").isPresent();
            client.deliver(new Delivery(KEY, FastKey.of("bins/c", 3, 0, KEY, 1), 1, 1L,
                    FetchMode.PROXY, new byte[0]));
            client.deliver(new Delivery(KEY, "seg-2", 1, 2L, FetchMode.INLINE, segmentOf("c")));

            Optional<ConsumerRecord> next = client.readNext(Duration.ofMillis(50));
            Optional<ConsumerRecord> after = next.isPresent() ? next
                    : client.readNext(Duration.ofMillis(50));

            assertThat(after).map(ConsumerRecord::offset).hasValue(2L);
            assertThat(source.asked).as("asked once, never retried").hasValue(1);
            assertThat(client.fastEntriesGone()).isEqualTo(1);
            assertThat(client.gapsDetected()).as("offset 1, read from the chain").isEqualTo(1);
        }
    }

    @Test
    void aNODEsHoldOfThe404IsTheReleaseToo() throws Exception {
        // ⚠️ NodeSegmentSource holds a failed key, its 404 kept as the cause.
        AtomicInteger asked = new AtomicInteger();
        SegmentSource held = new SegmentSource() {
            @Override
            public byte[] fetch(io.github.huyz0.os.biningester.format.Grant grant) {
                throw new AssertionError("proxy only");
            }

            @Override
            public byte[] fetchSegment(String segmentKey) throws IOException {
                asked.incrementAndGet();
                throw new SegmentFetchHeldException("held",
                        new SegmentNotFoundException("404"), Duration.ofSeconds(5), 1);
            }
        };
        try (ConsumerClient client = new ConsumerClient(KEY, 16, held,
                TestRetries.noFailedFetch())) {
            client.deliver(new Delivery(KEY, FastKey.of("bins/c", 3, 0, KEY, 0), 1, 0L,
                    FetchMode.PROXY, new byte[0]));

            assertThat(client.readNext(Duration.ofMillis(50))).isEmpty();

            assertThat(client.fastEntriesGone()).isEqualTo(1);
            assertThat(client.queuedDeliveries()).as("dropped").isZero();
        }
    }

    @Test
    void aDATASegments404IsStillAFailureRetriedByPolicy() throws Exception {
        Gone source = new Gone();
        try (ConsumerClient client = new ConsumerClient(KEY, 16, source,
                SegmentFetchRetry.standard(() -> 0L))) {
            client.deliver(new Delivery(KEY, "bins/c/data/seg", 1, 0L, FetchMode.PROXY,
                    new byte[0]));

            assertThat(client.readNext(Duration.ofMillis(50))).isEmpty();

            assertThat(client.queuedDeliveries()).as("still at the head, to be retried")
                    .isEqualTo(1);
            assertThat(client.fastEntriesGone()).isZero();
        }
    }
}
