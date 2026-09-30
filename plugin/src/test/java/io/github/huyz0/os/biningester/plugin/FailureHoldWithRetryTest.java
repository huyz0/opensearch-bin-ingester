// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.client.ConsumerClient;
import io.github.huyz0.os.biningester.client.Delivery;
import io.github.huyz0.os.biningester.client.SegmentFetchRetry;
import io.github.huyz0.os.biningester.client.SegmentSource;
import io.github.huyz0.os.biningester.format.FetchMode;
import io.github.huyz0.os.biningester.format.Grant;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.format.SegmentWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * The node hold and the consumer's retry TOGETHER (M10.28 review T2): 16 runs
 * of one segment on a node, a store down for 75 s, the default policy of
 * eight attempts over ~90 s. Every run reads its record once the store is
 * back -- no shard pauses for an outage the policy covers -- and the store is
 * fetched about as often as ONE run would fetch it, not sixteen times that.
 */
class FailureHoldWithRetryTest {

    private static final int RUNS = 16;
    private static final long OUTAGE_MILLIS = 75_000;

    /**
     * ⚠️ WITH THE HOLD PRODUCTION BUILDS, jittered, across seeds (M12.11 review
     * T1): a jitter that shortened holds failed this for six seeds in eight.
     */
    @Test
    void sixteenRunsSurviveAnOutageThePolicyCoversAndCostOneRunsFetches() throws Exception {
        survives(java.util.function.LongUnaryOperator.identity());
        for (long seed = 1; seed <= 8; seed++) {
            survives(NodeSegmentSource.upJitter(new java.util.SplittableRandom(seed)));
        }
    }

    private static void survives(java.util.function.LongUnaryOperator jitter) throws Exception {
        UUID index = UUID.fromString("00000000-0000-0000-0000-0000000000aa");
        SegmentWriter writer = new SegmentWriter();
        for (int run = 0; run < RUNS; run++) {
            writer.add(new RunKey(index, run), new SegmentRecord("doc-" + run, OpType.INDEX,
                    OptionalLong.of(1), "{}".getBytes(StandardCharsets.UTF_8)), 1L);
        }
        byte[] segment = writer.toByteArray(1L);
        AtomicLong millis = new AtomicLong(0);
        AtomicInteger fetches = new AtomicInteger();
        SegmentSource store = new SegmentSource() {
            @Override
            public byte[] fetch(Grant grant) {
                throw new AssertionError("route only");
            }

            @Override
            public byte[] fetchSegment(String segmentKey) throws IOException {
                fetches.incrementAndGet();
                if (millis.get() < OUTAGE_MILLIS) {
                    throw new IOException("502 from the ingester");
                }
                return segment;
            }
        };
        NodeSegmentSource hold = new NodeSegmentSource(store, 1L << 20);
        hold.holdFailures(millis::get, jitter);
        // ⚠️ ONE TIMELINE, AS A SCHEDULE: each run's backoff sets when THAT run
        // next wakes, and the loop always runs the earliest -- the node's runs
        // waiting in parallel on one wall clock. A run's sleep must not move
        // the clock for the others, or sixteen runs' waits would add up and
        // the outage would pass in a few attempts each.
        long[] wakeAt = new long[RUNS];
        List<ConsumerClient> runs = new ArrayList<>();
        boolean[] read = new boolean[RUNS];
        try {
            for (int run = 0; run < RUNS; run++) {
                int me = run;
                // ⚠️ ON THE TIMELINE's CLOCK (M13.6c: a policy is always clocked):
                // a run's backoff is due when the loop's clock reaches the wake
                // its sleeper recorded, as the clock-less debt was paid there.
                SegmentFetchRetry retry = new SegmentFetchRetry(
                        io.github.huyz0.os.biningester.client.HttpSubscriptionTransport
                                .DEFAULT_RETRY_FLOOR,
                        io.github.huyz0.os.biningester.client.HttpSubscriptionTransport
                                .DEFAULT_RETRY_CEILING,
                        SegmentFetchRetry.DEFAULT_MAX_ATTEMPTS,
                        wait -> wakeAt[me] = millis.get() + wait.toMillis(), millis::get);
                RunKey key = new RunKey(index, run);
                ConsumerClient client = new ConsumerClient(key, 16, hold, retry);
                client.deliver(new Delivery(key, "seg", 1, 0L, FetchMode.PROXY, new byte[0]));
                runs.add(client);
            }
            int remaining = RUNS;
            for (int step = 0; remaining > 0 && step < 100_000; step++) {
                int next = -1;
                for (int run = 0; run < RUNS; run++) {
                    if (!read[run] && (next < 0 || wakeAt[run] < wakeAt[next])) {
                        next = run;
                    }
                }
                millis.set(Math.max(millis.get(), wakeAt[next]));
                // ⚠️ A SURFACED FAILURE THROWS OUT OF HERE and fails the case:
                // that is the shard an operator would have to resume.
                if (runs.get(next).readNext(Duration.ofDays(1)).isPresent()) {
                    read[next] = true;
                    remaining--;
                }
            }
            assertThat(remaining).as("every run read its record").isZero();
        } finally {
            for (ConsumerClient client : runs) {
                client.close();
            }
        }
        assertThat(millis.get()).as("the premise: the store came back first")
                .isGreaterThanOrEqualTo(OUTAGE_MILLIS);
        assertThat(fetches.get())
                .as("⚠️ ABOUT ONE RUN's FETCHES (eight over ~90 s), NOT %d x 8", RUNS)
                .isLessThanOrEqualTo(SegmentFetchRetry.DEFAULT_MAX_ATTEMPTS + 1);
    }
}
