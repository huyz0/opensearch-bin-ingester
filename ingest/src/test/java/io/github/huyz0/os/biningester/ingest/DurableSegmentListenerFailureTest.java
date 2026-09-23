// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.RunKey;
import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class DurableSegmentListenerFailureTest {
    @Test
    void aThrowingDurableHintDoesNotSuppressTheAlreadyCommittedPush() throws Exception {
        try (var raw = new MemoryBinStore()) {
            var store = new CountingBinStore(raw);
            var hub = new SubscriptionHub();
            var pushed = new AtomicInteger();
            var delivered = new CountDownLatch(1);
            var sequencer = IngestTestSupport.sequencer(store, "pod1");
            try (var subscription = hub.subscribe(new RunKey(IngestTestSupport.LOGS, 0),
                    SubscriptionHub.assembling(push -> {
                        pushed.incrementAndGet();
                        delivered.countDown();
                    }));
                    var ingest = new DefaultIngest(IngestTestSupport.pinnedIntervalConfig(
                            Duration.ofDays(1), 8L << 20), store, IngestTestSupport.PREFIX,
                            "pod1", sequencer, hub, Clock.systemUTC(), index -> IngestTestSupport.LOGS,
                            key -> { throw new IllegalStateException("hint unavailable"); })) {
                var append = IngestTestSupport.appendAsync(ingest, "logs", 0, 1);
                IngestTestSupport.awaitPending(ingest, 1);

                ingest.flushNow();

                assertThat(append.join().recordCount()).isEqualTo(1);
                assertThat(delivered.await(10, TimeUnit.SECONDS))
                        .as("the committed push is delivered after the optional hint fails")
                        .isTrue();
                assertThat(pushed).hasValue(1);
            }
        }
    }
}
