// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.PREFIX;
import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.counts;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;
import org.junit.jupiter.api.Test;

class InboxDrainMetricTest {

    @Test
    void failedBatchSizeIsCountedOnEveryFailedApplicationAttempt() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        Inbox.write(store, PREFIX, intent(0));
        Inbox.write(store, PREFIX, intent(1));
        Sequencer fails = new Sequencer() {
            @Override
            public io.github.huyz0.os.biningester.format.CommitDelta commitAll(
                    List<CommitRequest> requests) throws IOException {
                throw new IOException("injected failed intent application");
            }

            @Override
            public void close() {
            }
        };
        AtomicLong failedAttempts = new AtomicLong();
        LongConsumer record = failedAttempts::addAndGet;
        Method drain = InboxDrain.class.getMethod("drain", BinStore.class, String.class,
                Sequencer.class, String.class, LongConsumer.class);

        invokeFailedDrain(drain, store, fails, record);
        assertThat(failedAttempts.get()).isEqualTo(2);
        invokeFailedDrain(drain, store, fails, record);

        assertThat(failedAttempts.get())
                .as("the same two pending intents count again on a later failed retry")
                .isEqualTo(4);
    }

    private static CommitRequest intent(long sequence) {
        return new CommitRequest("poda", "incarnation-a", sequence,
                "seg/poda-" + sequence, counts(2));
    }

    private static void invokeFailedDrain(Method drain, BinStore store, Sequencer fails,
            LongConsumer record) throws Exception {
        try {
            drain.invoke(null, store, PREFIX, fails, null, record);
        } catch (InvocationTargetException wrapped) {
            if (wrapped.getCause() instanceof IOException) {
                return;
            }
            throw wrapped;
        }
        throw new AssertionError("the failed batch must keep the drain unsuccessful");
    }
}
