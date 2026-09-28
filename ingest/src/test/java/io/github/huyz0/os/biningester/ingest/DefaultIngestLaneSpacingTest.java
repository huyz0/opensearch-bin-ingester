// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import io.github.huyz0.os.biningester.binstore.IndexCostLedger;
import static io.github.huyz0.os.biningester.ingest.IngestTestSupport.appendOnce;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.CostGovernor;
import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.GoverningBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The spacing the cost governor reads follows the buffered lanes (M10.7,
 * ADR-0075 §3): {@code max(floor, adaptiveInterval ÷ 2^L)}, L the highest
 * positive lane in the ACTIVE buffer.
 *
 * <p>⚠️ WITHOUT IT a +2 trickle spends four flushes per ceiling against a
 * governor that expects one, and reads as a 4x regression it is not.
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class DefaultIngestLaneSpacingTest {

    /** ⚠️ ADVANCED by the test, never slept on; shared by the ingest and the governor. */
    private static final class TestClock extends Clock {
        private final AtomicLong millis = new AtomicLong();

        @Override
        public long millis() {
            return millis.get();
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(millis());
        }

        @Override
        public ZoneOffset getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(java.time.ZoneId zone) {
            return this;
        }
    }

    private static IngestConfig lengtheningConfig(Duration ceiling) {
        // ⚠️ A ZERO lengthen delay, so one near-empty flush lengthens to the ceiling.
        return new IngestConfig(Duration.ofMillis(250), 8L << 20, "cluster-a",
                IngestConfig.DEFAULT_MAX_QUEUED_PUSH_BYTES, ceiling,
                IngestConfig.DEFAULT_FILL_RATIO_LOW_THRESHOLD,
                IngestConfig.DEFAULT_FILL_RATIO_HIGH_THRESHOLD, Duration.ZERO,
                IngestConfig.DEFAULT_INTERVAL_SHORTEN_DELAY);
    }

    private static void awaitSpacing(DefaultIngest ingest, long expected) {
        // ⚠️ A DEADLINE, NOT A SLEEP: the adapted interval is carried back to
        // the active buffer after the append is answered.
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (ingest.flushSpacingMillis() != expected && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        assertThat(ingest.flushSpacingMillis()).as("PREMISE: spacing settled").isEqualTo(expected);
    }

    private static CompletableFuture<AppendResult> appendLane(DefaultIngest ingest,
            int partition, byte lane) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return ingest.append(IngestTestSupport.PRINCIPAL, "logs", partition, lane,
                        IngestTestSupport.docs(1)::forEach);
            } catch (IOException e) {
                throw new CompletionException(e);
            }
        });
    }

    @Test
    void theGOVERNORSamplesTheLaneShortenedSpacingATThePlus2DataPUT() throws Exception {
        // ⚠️ THE SAMPLE IS TAKEN INSIDE THE DATA PUT, when the batch being
        // written has been detached and drained and the active buffer is empty:
        // a spacing read off either buffer there says the 2 s ceiling, and a +2
        // trickle at its own 500 ms spacing reads as 4x expected -- over the
        // 3x alarm, for traffic ADR-0074 says an operator bought knowingly.
        TestClock clock = new TestClock();
        AtomicReference<DefaultIngest> pod = new AtomicReference<>();
        CostGovernor governor = new CostGovernor(
                new CostGovernor.Settings(1.0, 300, Duration.ofSeconds(10), 8L << 20), clock,
                () -> pod.get() == null ? 250 : pod.get().flushSpacingMillis());
        CountingBinStore counted = new CountingBinStore(new MemoryBinStore());
        GoverningBinStore store = new GoverningBinStore(counted, governor);
        try (DefaultIngest ingest = new DefaultIngest(lengtheningConfig(Duration.ofSeconds(2)),
                store, IngestTestSupport.PREFIX, "pod1",
                IngestTestSupport.sequencer(counted, "pod1"), new SubscriptionHub(), clock,
                index -> IngestTestSupport.LOGS, ignored -> { }, new IndexCostLedger())) {
            pod.set(ingest);
            appendOnce(ingest, "logs", 0, 1);
            awaitSpacing(ingest, 2_000);

            // ⚠️ THE NEXT WINDOW, so the lengthening flush's floor-spacing
            // sample is not in the one evaluated below.
            clock.millis.set(10_000);
            long base = counted.putPurposeCounts().dataPuts();
            for (int i = 0; i < 20; i++) {
                CompletableFuture<AppendResult> plus2 = appendLane(ingest, 1, (byte) 2);
                IngestTestSupport.awaitPending(ingest, 1);
                ingest.flushNow();
                plus2.get(10, TimeUnit.SECONDS);
                clock.millis.addAndGet(500);
            }
            assertThat(counted.putPurposeCounts().dataPuts() - base)
                    .as("PREMISE: twenty +2 flushes in one 10 s window").isEqualTo(20);
            clock.millis.set(20_000);
            assertThat(governor.lastRatio())
                    .as("20 PUTs against 10 s ÷ 500 ms = 20 expected: 1x, not the 4x a "
                            + "ceiling sample reads")
                    .isLessThan(CostGovernor.ALARM)
                    .isEqualTo(1.0);
        }
    }

    @Test
    void aBUFFEREDPlus2RecordQUARTERSTheLengthenedSpacingUntilItIsFlushed() throws Exception {
        // ⚠️ A ZERO lengthen delay, so one near-empty flush lengthens to the
        // ceiling -- an HOUR, so the +2 record's own 15-minute deadline cannot
        // flush it between the two reads on a loaded machine.
        IngestConfig config = new IngestConfig(Duration.ofMillis(250), 8L << 20, "cluster-a",
                IngestConfig.DEFAULT_MAX_QUEUED_PUSH_BYTES, Duration.ofHours(1),
                IngestConfig.DEFAULT_FILL_RATIO_LOW_THRESHOLD,
                IngestConfig.DEFAULT_FILL_RATIO_HIGH_THRESHOLD, Duration.ZERO,
                IngestConfig.DEFAULT_INTERVAL_SHORTEN_DELAY);
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        try (DefaultIngest ingest = new DefaultIngest(config, store, IngestTestSupport.PREFIX,
                "pod1", IngestTestSupport.sequencer(store, "pod1"), new SubscriptionHub(),
                Clock.systemUTC(), index -> IngestTestSupport.LOGS, ignored -> { }, new IndexCostLedger())) {
            appendOnce(ingest, "logs", 0, 1);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (ingest.flushSpacingMillis() != 3_600_000 && System.nanoTime() < deadline) {
                Thread.onSpinWait();
            }
            assertThat(ingest.flushSpacingMillis())
                    .as("PREMISE: lengthened to the ceiling").isEqualTo(3_600_000);

            CompletableFuture<AppendResult> plus2 = CompletableFuture.supplyAsync(() -> {
                try {
                    return ingest.append(IngestTestSupport.PRINCIPAL, "logs", 1, (byte) 2,
                            IngestTestSupport.docs(1)::forEach);
                } catch (IOException e) {
                    throw new CompletionException(e);
                }
            });
            IngestTestSupport.awaitPending(ingest, 1);
            assertThat(ingest.flushSpacingMillis())
                    .as("a buffered +2 record: the ceiling ÷ 2^2").isEqualTo(900_000);

            ingest.flushNow();
            plus2.get(10, TimeUnit.SECONDS);
            assertThat(ingest.flushSpacingMillis())
                    .as("flushed: the active buffer holds no +2 record").isEqualTo(3_600_000);
        }
    }
}
