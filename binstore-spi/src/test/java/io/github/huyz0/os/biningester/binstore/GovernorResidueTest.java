// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.binstore;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * The governor's residue from M10.10's review (M11.11, H6): each surviving
 * mutation named there, killed here.
 */
class GovernorResidueTest {

    private static final String PREFIX = "bins/cluster-a";
    private static final String SEGMENT = PREFIX + "/data/2026/09/27/10/x.bseg";
    private static final long MIB = 1L << 20;

    /** A clock the test moves. */
    private static final class TestClock extends Clock {
        private Instant now = Instant.ofEpochMilli(1_790_000_000_000L);

        @Override public ZoneOffset getZone() {
            return ZoneOffset.UTC;
        }

        @Override public Clock withZone(java.time.ZoneId zone) {
            return this;
        }

        @Override public Instant instant() {
            return now;
        }

        void advance(Duration d) {
            now = now.plus(d);
        }
    }

    private final TestClock clock = new TestClock();

    /** Expected = max(bytes / 1,000, 1) per 60 s window. */
    private CostGovernor bytesDecide() {
        return new CostGovernor(new CostGovernor.Settings(1.0, 300, Duration.ofSeconds(60),
                1_000), clock, () -> 60_000);
    }

    private static BinStore conditionalDelegate() {
        return (BinStore) Proxy.newProxyInstance(BinStore.class.getClassLoader(),
                new Class<?>[] {BinStore.class}, (self, method, args) -> switch (method.getName()) {
                    case "putIfAbsent", "putIfMatch" -> Optional.empty();
                    default -> null;
                });
    }

    @Test
    void aPutIfAbsentOfADataSegmentFeedsTheRatioOnItsOwn() throws Exception {
        CostGovernor governor = bytesDecide();
        new GoverningBinStore(conditionalDelegate(), governor)
                .putIfAbsent(SEGMENT, Body.ofBytes(new byte[1_000]));
        clock.advance(Duration.ofSeconds(60));
        assertThat(governor.lastRatio()).as("⚠️ ONE PUT, NOT NONE: 1 / 1").isEqualTo(1.0);
    }

    @Test
    void aPutIfMatchOfADataSegmentFeedsTheRatioOnItsOwn() throws Exception {
        CostGovernor governor = bytesDecide();
        new GoverningBinStore(conditionalDelegate(), governor)
                .putIfMatch(SEGMENT, Body.ofBytes(new byte[1_000]), new Version("v"));
        clock.advance(Duration.ofSeconds(60));
        assertThat(governor.lastRatio()).as("⚠️ ONE PUT, NOT NONE: 1 / 1").isEqualTo(1.0);
    }

    /** 12 PUTs a window are expected at a 5 s spacing; 150 halts it. */
    private CostGovernor spacedAtFiveSeconds() {
        return new CostGovernor(new CostGovernor.Settings(1.0, 5, Duration.ofSeconds(60),
                8 * MIB), clock, () -> 5_000);
    }

    @Test
    void anIdleGapOfSeveralWindowsLiftsAHaltAtTheFirstCallAfterIt() {
        CostGovernor premise = spacedAtFiveSeconds();
        CostGovernor governor = spacedAtFiveSeconds();
        for (int i = 0; i < 150; i++) {
            premise.recordDataPut(1_000);
            governor.recordDataPut(1_000);
        }
        clock.advance(Duration.ofSeconds(60));
        assertThat(premise.discretionaryAllowed()).as("the premise: that window halts")
                .isFalse();
        assertThat(premise.alarmed()).as("the premise: and alarms").isTrue();

        // ⚠️ THE SHORTEST GAP (M11.11 review T1): two windows on, ONE of them
        // empty, is already "several" -- `windows > 1`, not `> 2`.
        clock.advance(Duration.ofSeconds(60));
        assertThat(governor.discretionaryAllowed())
                .as("⚠️ TWO WINDOWS ON, ONE OF THEM EMPTY: the halt is lifted now, not at "
                        + "the next call a window later")
                .isTrue();
        assertThat(governor.alarmed()).as("an idle pod exports no alarm").isFalse();
        assertThat(governor.lastRatio()).as("an empty window is a ratio of 0").isZero();
    }

    @Test
    void aDeclaredRecoveryListSpendsNoToken() throws Exception {
        CostGovernor governor = spacedAtFiveSeconds();
        GovernorScope.recovery(() -> {
            for (int i = 0; i < 20; i++) {
                governor.admitList();
            }
            return null;
        });
        for (int i = 0; i < 5; i++) {
            assertThat(governor.admitList())
                    .as("⚠️ THE BURST OF 5 IS WHOLE AFTER 20 RECOVERY LISTS: list %d", i + 1)
                    .isTrue();
        }
        assertThat(governor.admitList()).as("and the sixth is refused").isFalse();
    }

    @Test
    void theWindowRollsOnItsOwnBoundariesNotFromTheCallThatRolledIt() {
        CostGovernor governor = spacedAtFiveSeconds();
        clock.advance(Duration.ofSeconds(90));
        for (int i = 0; i < 150; i++) {
            governor.recordDataPut(1_000); // at 90 s: rolls, into the window [60, 120)
        }
        clock.advance(Duration.ofSeconds(31)); // 121 s: past 120, not past 90 + 60
        assertThat(governor.discretionaryAllowed())
                .as("⚠️ THE WINDOW [60 s, 120 s) HAS ENDED AND HALTS: re-anchored at the "
                        + "rolling call, it would run to 150 s and still be open")
                .isFalse();
    }

    @Test
    void theSpacingIsNotSampledAtConstructionOnlyByAFlush() {
        AtomicLong spacing = new AtomicLong(250);
        CostGovernor governor = new CostGovernor(new CostGovernor.Settings(1.0, 5,
                Duration.ofSeconds(60), 8 * MIB), clock, spacing::get);
        spacing.set(60_000);
        for (int i = 0; i < 12; i++) {
            governor.recordDataPut(1_000);
        }
        clock.advance(Duration.ofSeconds(60));
        assertThat(governor.alarmed())
                .as("⚠️ 12 PUTS AGAINST 1 EXPECTED at the flushes' 60 s spacing: a 250 ms "
                        + "sample from construction would expect 240 and call it quiet")
                .isTrue();
    }

    @Test
    void partsUploadedInParallelAreEachCounted() throws Exception {
        CostGovernor governor = bytesDecide();
        MultipartWriter discard = new MultipartWriter() {
            @Override public void uploadPart(int partNumber, Body body) {
            }

            @Override public Version complete() {
                return new Version("v");
            }

            @Override public void abort() {
            }

            @Override public void close() {
            }
        };
        BinStore delegate = (BinStore) Proxy.newProxyInstance(BinStore.class.getClassLoader(),
                new Class<?>[] {BinStore.class}, (self, method, args) ->
                        "multipart".equals(method.getName()) ? discard : null);
        int threads = 16;
        int partsEach = 2_000;
        try (MultipartWriter writer = new GoverningBinStore(delegate, governor)
                .multipart(SEGMENT);
                var pool = Executors.newFixedThreadPool(threads)) {
            CountDownLatch start = new CountDownLatch(1);
            List<Future<?>> uploads = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                int base = t * partsEach;
                uploads.add(pool.submit(() -> {
                    start.await();
                    for (int p = 1; p <= partsEach; p++) {
                        writer.uploadPart(base + p, Body.ofBytes(new byte[1]));
                    }
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> upload : uploads) {
                upload.get();
            }
            writer.complete();
        }
        clock.advance(Duration.ofSeconds(60));
        long total = (long) threads * partsEach;
        assertThat(governor.lastRatio())
                .as("⚠️ ONE PUT OF %d BYTES, every part counted: 1 / (%d / 1,000)", total,
                        total)
                .isEqualTo(1.0 / (total / 1_000.0));
    }
}
