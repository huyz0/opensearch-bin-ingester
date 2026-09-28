// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static io.github.huyz0.os.biningester.ingest.IngestTestSupport.LOGS;
import static io.github.huyz0.os.biningester.ingest.IngestTestSupport.PREFIX;
import static io.github.huyz0.os.biningester.ingest.IngestTestSupport.appendAsync;
import static io.github.huyz0.os.biningester.ingest.IngestTestSupport.awaitPending;
import static io.github.huyz0.os.biningester.ingest.IngestTestSupport.pinnedIntervalConfig;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.sequencer.CommitRequest;
import io.github.huyz0.os.biningester.sequencer.Sequencer;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * An {@link Error} in {@link DefaultIngest}'s own flush fails its producers
 * and the pod flushes on (M11.10 review R1 and T1): failing only the batch's
 * future told a {@code flushNow()} caller, while every producer in the batch
 * waited for ever on a future nothing would complete.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class DefaultIngestErrorTest {

    @Test
    void aFlushThatThrowsAnErrorFailsItsProducersAndTheNextFlushGoesOn() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        Sequencer real = IngestTestSupport.sequencer(store, "pod1");
        AtomicBoolean thrown = new AtomicBoolean();
        Sequencer failingOnce = new Sequencer() {
            @Override
            public CommitDelta commitAll(List<CommitRequest> requests) throws IOException {
                if (thrown.compareAndSet(false, true)) {
                    throw new AssertionError("injected: the commit path died");
                }
                return real.commitAll(requests);
            }

            @Override
            public void close() throws IOException {
                real.close();
            }
        };
        try (DefaultIngest ingest = new DefaultIngest(
                pinnedIntervalConfig(Duration.ofDays(1), 8L << 20), store, PREFIX, "pod1",
                failingOnce, new SubscriptionHub(), Clock.systemUTC(), index -> LOGS)) {
            CompletableFuture<AppendResult> first = appendAsync(ingest, "logs", 0, 2);
            awaitPending(ingest, 1);
            try {
                ingest.flushNow();
            } catch (IOException | RuntimeException | Error expected) {
                // the flush's own outcome; the producer's is what this case reads
            }
            Throwable failure = first.handle((result, thrownBy) -> thrownBy)
                    .get(10, TimeUnit.SECONDS);
            assertThat(failure)
                    .as("⚠️ THE PRODUCER IS TOLD, not left waiting for ever")
                    .isNotNull()
                    .hasRootCauseMessage("injected: the commit path died");

            CompletableFuture<AppendResult> second = appendAsync(ingest, "logs", 0, 3);
            awaitPending(ingest, 1);
            ingest.flushNow();
            assertThat(second.get(10, TimeUnit.SECONDS).recordCount())
                    .as("and the pod flushes on").isEqualTo(3);
        }
    }
}
