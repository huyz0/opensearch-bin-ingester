// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.Recovery;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentCommit;
import io.github.huyz0.os.biningester.ingest.AppendResult;
import io.github.huyz0.os.biningester.ingest.FastPath;
import io.helidon.metrics.api.Gauge;
import io.helidon.metrics.api.Meter;
import io.helidon.metrics.api.Metrics;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * Fast mode's metrics (M13.36; SPEC criterion 17, test-plan row 17's
 * {@code FastMetricsTest}): each registered under its observability.md name
 * with no label, an ack timed only when the fast path answers, uploads
 * counted by cause, voids in offsets, and the journal's held bytes.
 *
 * <p>⚠️ The registry is process-wide, so every count here is a delta.
 */
class FastMetricsTest {

    private static final RunKey S = new RunKey(new UUID(1, 1), 0);

    @Test
    void everyFASTMetricIsRegisteredUnderItsNameWithNoLabel() {
        FastMetrics.ack();
        List<String> names = List.of(FastMetrics.ACK, FastMetrics.QUORUM_WAIT,
                FastMetrics.JOURNAL_BYTES, FastMetrics.UPLOADS_TIMER, FastMetrics.UPLOADS_LEAVE,
                FastMetrics.VOIDED);

        for (String name : names) {
            List<Meter> found = List.copyOf(Metrics.globalRegistry()
                    .meters(m -> m.id().name().equals(name)));
            assertThat(found).as(name).isNotEmpty()
                    .allSatisfy(m -> assertThat(m.id().tagsMap().keySet())
                            .as(name + " has no label but Helidon's own scope")
                            .isSubsetOf(java.util.Set.of("scope")));
        }
    }

    @Test
    void anACKIsTimedOnlyWhenTheFastPathAnswers() throws Exception {
        AtomicLong clock = new AtomicLong(1_000);
        FastPath answering = (stream, records) -> {
            clock.addAndGet(3_000_000);
            return Optional.of(new AppendResult(1, 0, 0));
        };
        FastPath notFast = (stream, records) -> Optional.empty();
        FastPath failing = (stream, records) -> {
            throw new IOException("the leader did not answer");
        };
        long before = FastMetrics.ack().count();
        double totalBefore = FastMetrics.ack().totalTime(java.util.concurrent.TimeUnit.NANOSECONDS);

        FastMetrics.timed(answering, clock::get).append(S, List.of());
        FastMetrics.timed(notFast, clock::get).append(S, List.of());
        assertThatThrownBy(() -> FastMetrics.timed(failing, clock::get).append(S, List.of()))
                .isInstanceOf(IOException.class);

        assertThat(FastMetrics.ack().count() - before).as("only the answered one").isEqualTo(1);
        assertThat(FastMetrics.ack().totalTime(java.util.concurrent.TimeUnit.NANOSECONDS)
                - totalBefore).as("its 3 ms").isEqualTo(3_000_000d);
    }

    @Test
    void anUPLOADIsCountedByItsCause() {
        long timer = FastMetrics.uploads(true);
        long leave = FastMetrics.uploads(false);

        FastMetrics.uploaded(true);
        FastMetrics.uploaded(true);
        FastMetrics.uploaded(false);

        assertThat(FastMetrics.uploads(true) - timer).isEqualTo(2);
        assertThat(FastMetrics.uploads(false) - leave).isEqualTo(1);
    }

    @Test
    void aRECOVERYsVoidsAreCountedInOffsets() {
        long before = FastMetrics.voided();

        FastMetrics.voided(new Recovery(1, List.<SegmentCommit>of(), List.of(
                new Recovery.VoidRange(S, 0, 5), new Recovery.VoidRange(S, 10, 12))));

        assertThat(FastMetrics.voided() - before).isEqualTo(7);
    }

    @Test
    void theJOURNALGaugeReadsTheBoundPodsHeldBytes() throws Exception {
        MemoryBinStore store = FastDepartureTest.leaseHeldBy("uid-l");
        try (FastDisk disk = FastDepartureTest.journaledDisk(store)) {
            FastMetrics.bindJournal(disk);
            Gauge<?> gauge = Metrics.globalRegistry().meters(
                    m -> m.id().name().equals(FastMetrics.JOURNAL_BYTES)).stream()
                    .map(m -> (Gauge<?>) m).findFirst().orElseThrow();

            assertThat(disk.journal().orElseThrow().heldBytes()).as("the premise").isPositive();
            assertThat(gauge.value().longValue())
                    .isEqualTo(disk.journal().orElseThrow().heldBytes());
        }
    }
}
