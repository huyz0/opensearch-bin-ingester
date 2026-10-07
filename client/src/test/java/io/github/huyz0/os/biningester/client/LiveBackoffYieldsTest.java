// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.client;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.FetchMode;
import io.github.huyz0.os.biningester.format.Grant;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.format.SegmentWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * M13.47: a LIVE head that is backing off gives the catch-up its turn -- the
 * mirror of M12.26. The catch-up's turn came only once a quantum of live
 * records was served, and a failing live head serves none while it stays
 * pending, so a due catch-up was never loaded while live failed.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class LiveBackoffYieldsTest {

    private static final RunKey KEY = new RunKey(
            UUID.fromString("00000000-0000-0000-0000-0000000000cf"), 0);

    private static byte[] segmentOf(String id, long offset) throws Exception {
        SegmentWriter w = new SegmentWriter();
        w.add(KEY, new SegmentRecord(id, OpType.INDEX, OptionalLong.of(1),
                ("{\"id\":\"" + id + "\"}").getBytes(StandardCharsets.UTF_8)), offset);
        return w.toByteArray(offset);
    }

    private static Delivery proxied(String segmentKey, long firstOffset) {
        return new Delivery(KEY, segmentKey, 1, firstOffset, FetchMode.PROXY, new byte[0]);
    }

    private static String id(Optional<ConsumerRecord> record) {
        assertThat(record).as("a record").isPresent();
        return record.get().record().id();
    }

    @Test
    void aBACKINGOFFLiveHeadYieldsItsTurnToADueCatchUp() throws Exception {
        Map<String, byte[]> segments = new ConcurrentHashMap<>();
        segments.put("L0", segmentOf("L0", 100));
        segments.put("C0", segmentOf("C0", 0));
        AtomicInteger liveFetches = new AtomicInteger();
        SegmentSource source = new SegmentSource() {
            @Override public byte[] fetch(Grant grant) {
                throw new AssertionError("fetched by key");
            }

            @Override public byte[] fetchSegment(String segmentKey) throws IOException {
                if (segmentKey.equals("L0") && liveFetches.incrementAndGet() == 1) {
                    throw new IOException("502 from the ingester");
                }
                return segments.get(segmentKey);
            }
        };
        AtomicLong millis = new AtomicLong(1_000_000);
        List<Duration> waited = new CopyOnWriteArrayList<>();
        SegmentFetchRetry retry = new SegmentFetchRetry(Duration.ofSeconds(1),
                Duration.ofSeconds(30), 8, waited::add, millis::get);

        try (ConsumerClient c = new ConsumerClient(KEY, 64, source, retry)) {
            c.deliver(proxied("L0", 100));
            UUID request = UUID.randomUUID();
            c.beginCatchUp(request);
            c.deliverCatchUp(request, proxied("C0", 0));

            assertThat(c.readNext(Duration.ofSeconds(5)))
                    .as("the premise: live's turn, and its fetch failed").isEmpty();
            assertThat(liveFetches).hasValue(1);

            assertThat(id(c.readNext(Duration.ofSeconds(5))))
                    .as("⚠️ THE CATCH-UP IS SERVED while live backs off, no quantum served")
                    .isEqualTo("C0");
            assertThat(liveFetches).as("live was not fetched before it was due").hasValue(1);
            assertThat(waited).as("and nobody waited out live's backoff").isEmpty();

            millis.addAndGet(Duration.ofSeconds(2).toMillis());

            assertThat(id(c.readNext(Duration.ofSeconds(5))))
                    .as("live, due again, retried and served").isEqualTo("L0");
            assertThat(liveFetches).hasValue(2);
        }
    }

    /**
     * ⚠️ ONLY WHILE LIVE BACKS OFF, AND LIVE GETS ITS FULL QUANTUM BACK (review
     * round 1, T1, T2): a yield that outlived live's backoff, or a quantum
     * that kept the records served before the failure, would serve the second
     * catch-up before live had its eight.
     */
    @Test
    void aRECOVEREDLiveHeadTakesBackAFullQuantumBeforeTheNextCatchUp() throws Exception {
        Map<String, byte[]> segments = new ConcurrentHashMap<>();
        for (int i = 0; i < 12; i++) {
            segments.put("L" + i, segmentOf("L" + i, 100 + i));
        }
        segments.put("C0", segmentOf("C0", 0));
        segments.put("C1", segmentOf("C1", 1));
        AtomicInteger l3Fetches = new AtomicInteger();
        SegmentSource source = new SegmentSource() {
            @Override public byte[] fetch(Grant grant) {
                throw new AssertionError("fetched by key");
            }

            @Override public byte[] fetchSegment(String segmentKey) throws IOException {
                if (segmentKey.equals("L3") && l3Fetches.incrementAndGet() == 1) {
                    throw new IOException("502 from the ingester");
                }
                return segments.get(segmentKey);
            }
        };
        AtomicLong millis = new AtomicLong(1_000_000);
        SegmentFetchRetry retry = new SegmentFetchRetry(Duration.ofSeconds(1),
                Duration.ofSeconds(30), 8, waited -> { }, millis::get);

        try (ConsumerClient c = new ConsumerClient(KEY, 64, source, retry)) {
            for (int i = 0; i < 12; i++) {
                c.deliver(proxied("L" + i, 100 + i));
            }
            UUID request = UUID.randomUUID();
            c.beginCatchUp(request);
            c.deliverCatchUp(request, proxied("C0", 0));
            c.deliverCatchUp(request, proxied("C1", 1));

            for (int i = 0; i < 3; i++) {
                assertThat(id(c.readNext(Duration.ofSeconds(5)))).isEqualTo("L" + i);
            }
            assertThat(c.readNext(Duration.ofSeconds(5)))
                    .as("the premise: L3's fetch failed").isEmpty();
            assertThat(id(c.readNext(Duration.ofSeconds(5))))
                    .as("the catch-up takes live's turn while live backs off").isEqualTo("C0");

            millis.addAndGet(Duration.ofSeconds(2).toMillis());

            for (int i = 3; i < 3 + ConsumerClient.LIVE_RECORD_QUANTUM; i++) {
                assertThat(id(c.readNext(Duration.ofSeconds(5))))
                        .as("⚠️ live, due again, gets a FULL quantum: record %d", i)
                        .isEqualTo("L" + i);
            }
            assertThat(id(c.readNext(Duration.ofSeconds(5))))
                    .as("and only then the next catch-up").isEqualTo("C1");
        }
    }
}
