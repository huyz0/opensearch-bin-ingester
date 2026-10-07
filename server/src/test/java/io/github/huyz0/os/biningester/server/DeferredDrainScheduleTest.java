// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A deferring pod's drain is asked again every renew interval, on the node's
 * own schedule, and stops with the node (M13.78).
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class DeferredDrainScheduleTest {

    @Test
    void theRetryRunsEVERYIntervalUntilClosed() throws Exception {
        AtomicInteger asked = new AtomicInteger();
        AutoCloseable schedule = SequencerAssembly.retryDeferredDrains(
                asked::incrementAndGet, Duration.ofMillis(20));
        // ⚠️ WELL UNDER 3 x 1 s: an interval ignored for a fixed second fails.
        await().atMost(Duration.ofSeconds(2)).until(() -> asked.get() >= 3);
        schedule.close();
        int atClose = asked.get();
        await().during(Duration.ofMillis(200)).atMost(Duration.ofSeconds(2))
                .until(() -> asked.get() == atClose);
    }

    @Test
    void theRetryRunsEveryRENEWIntervalNotEveryTtl() {
        ServerConfig config = new ServerConfig("pod1", "az-a", "cluster-a", "bins/cluster-a",
                new StoreConfig("memory", java.util.Optional.empty()),
                Duration.ofSeconds(10), Duration.ofSeconds(3), "http://pod1:8080",
                io.github.huyz0.os.biningester.ingest.IngestConfig.defaults("cluster-a"), 0,
                "producer-1", java.util.Set.of("logs"),
                RetentionConfig.defaults(), java.util.Optional.empty(), "uid-pod1",
                io.github.huyz0.os.biningester.ingest.CostTopKReporter.DEFAULT_INTERVAL,
                io.github.huyz0.os.biningester.ingest.IndexQuotas.Config.none(), false,
                java.util.Optional.empty(), PeerConfig.off(0));
        assertThat(SequencerAssembly.retryInterval(config)).isEqualTo(Duration.ofSeconds(3));
    }

    @Test
    void aTHROWINGRetryDoesNotCancelTheNext() throws Exception {
        // ⚠️ A THROW OUT OF A SCHEDULED TASK CANCELS EVERY LATER RUN OF IT:
        // the pod would stop asking for good, and its deferred flushes wait for
        // its next write again.
        AtomicInteger asked = new AtomicInteger();
        try (AutoCloseable schedule = SequencerAssembly.retryDeferredDrains(() -> {
            if (asked.incrementAndGet() == 1) {
                throw new IllegalStateException("a retry failed");
            }
        }, Duration.ofMillis(20))) {
            await().atMost(Duration.ofSeconds(10)).until(() -> asked.get() >= 2);
        }
    }
}
