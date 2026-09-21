// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static io.github.huyz0.os.biningester.ingest.IngestTestSupport.LOGS;
import static io.github.huyz0.os.biningester.ingest.IngestTestSupport.PREFIX;
import static io.github.huyz0.os.biningester.ingest.IngestTestSupport.PRINCIPAL;
import static io.github.huyz0.os.biningester.ingest.IngestTestSupport.docs;
import static io.github.huyz0.os.biningester.ingest.IngestTestSupport.pinnedIntervalConfig;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.sequencer.CommitDeferredException;
import io.github.huyz0.os.biningester.sequencer.CommitRequest;
import io.github.huyz0.os.biningester.sequencer.Sequencer;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * An append whose commit was DEFERRED to the inbox completes, and says so
 * (M8.14a, ADR-0058, FR-4 as amended).
 *
 * <p>⚠️ **THE SEGMENT AND THE INTENT ARE BOTH DURABLE**, so the producer is
 * owed a 202 -- ADR-0058's reading of FR-4 during a partition. What it is NOT
 * owed is an offset, because none is assigned until the leaseholder drains
 * the inbox, and a result that invented one would be a lie a test could read.
 */
@Timeout(30)
class DeferredAckTest {

    /** A sequencer that is never reachable: every commit defers. */
    private static final class Partitioned implements Sequencer {
        final AtomicInteger asked = new AtomicInteger();

        @Override
        public CommitDelta commitAll(List<CommitRequest> requests) throws IOException {
            asked.incrementAndGet();
            throw new CommitDeferredException(PREFIX + "/ctl/inbox/0/pod1/i/0.intent",
                    new IOException("connect timed out"));
        }

        @Override
        public void close() {
        }
    }

    @Test
    void aDEFERREDCommitCompletesTheAppendWithNOOffsets() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        Partitioned sequencer = new Partitioned();
        SubscriptionHub hub = new SubscriptionHub();
        AtomicInteger pushed = new AtomicInteger();
        try (var subscription = hub.subscribe(new io.github.huyz0.os.biningester.format.RunKey(LOGS, 0), pushes -> {
                    pushed.addAndGet(pushes.size());
                    return null;
                });
                DefaultIngest ingest = new DefaultIngest(
                        pinnedIntervalConfig(Duration.ofMillis(20), 8L << 20), store, PREFIX,
                        "pod1", sequencer, hub, Clock.systemUTC(), index -> LOGS)) {
            AppendResult result = ingest.append(PRINCIPAL, "logs", 0,
                    sink -> docs(5).forEach(sink));

            assertThat(result.deferred())
                    .as("⚠️ A 202 ON A DURABLE INTENT, not an exception the producer retries "
                            + "into a partition")
                    .isTrue();
            assertThat(result.recordCount()).isEqualTo(5);
            Thread.sleep(200);
            assertThat(pushed.get())
                    .as("⚠️ NOTHING IS PUSHED: a consumer told about records with no offsets "
                            + "would index them nowhere")
                    .isZero();
            assertThat(sequencer.asked.get())
                    .as("and the flush is not retried: the drain owns it now").isEqualTo(1);
        }
    }

    @Test
    void anORDINARYResultIsNotDeferred() {
        assertThat(new AppendResult(3, 10, 12).deferred()).isFalse();
        assertThat(AppendResult.deferred(3).recordCount()).isEqualTo(3);
    }
}
