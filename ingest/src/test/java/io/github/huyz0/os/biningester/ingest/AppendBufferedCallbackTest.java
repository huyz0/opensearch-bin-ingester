// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import io.github.huyz0.os.biningester.binstore.IndexCostLedger;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.security.Principal;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Duration;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * An append runs its {@code buffered} callback once its records are held and
 * before the durable wait -- and an implementation that cannot tell the two
 * apart runs it after (M11.7).
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class AppendBufferedCallbackTest {

    private static final SegmentRecord DOC = new SegmentRecord("a", OpType.INDEX,
            OptionalLong.empty(), new byte[] {1});

    @Test
    void defaultIngestRunsItBeforeTheDurableWaitAndNeverForARefusedAppend() throws Exception {
        MemoryBinStore memory = new MemoryBinStore();
        CountDownLatch release = new CountDownLatch(1);
        BinStore held = (BinStore) Proxy.newProxyInstance(BinStore.class.getClassLoader(),
                new Class<?>[] {BinStore.class}, (self, method, args) -> {
                    if (method.getName().equals("put") && ((String) args[0]).endsWith(".bseg")) {
                        release.await();
                    }
                    try {
                        return method.invoke(memory, args);
                    } catch (java.lang.reflect.InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
        CountingBinStore store = new CountingBinStore(held);
        AtomicInteger buffered = new AtomicInteger();
        try (DefaultIngest ingest = new DefaultIngest(
                IngestTestSupport.pinnedIntervalConfig(Duration.ofMillis(20), 8L << 20), store,
                IngestTestSupport.PREFIX, "pod1", IngestTestSupport.sequencer(store, "pod1"),
                new SubscriptionHub(), Clock.systemUTC(), index -> IngestTestSupport.LOGS, ignored -> { }, new IndexCostLedger())) {
            CompletableFuture<AppendResult> append = CompletableFuture.supplyAsync(() -> {
                try {
                    return ingest.append(IngestTestSupport.PRINCIPAL, "logs", 0, (byte) 0,
                            sink -> sink.accept(DOC), buffered::incrementAndGet);
                } catch (java.io.IOException e) {
                    throw new java.io.UncheckedIOException(e);
                }
            });
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (buffered.get() == 0 && System.nanoTime() < deadline) {
                Thread.onSpinWait();
            }

            assertThat(buffered).as("⚠️ RUN ONCE THE RECORDS ARE HELD").hasValue(1);
            assertThat(append).as("while the append still waits on the held PUT").isNotDone();
            release.countDown();
            append.get(30, TimeUnit.SECONDS);
            assertThat(buffered).as("once, not again at durability").hasValue(1);

            Principal elsewhere = new Principal("cluster-b", "producer-1", java.util.Set.of("logs"));
            assertThatThrownBy(() -> ingest.append(elsewhere, "logs", 0, (byte) 0,
                    sink -> sink.accept(DOC), buffered::incrementAndGet))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThat(buffered).as("⚠️ A REFUSED APPEND BUFFERED NOTHING AND IS NOT TOLD SO")
                    .hasValue(1);
        }
    }

    @Test
    void anIngestThatCannotTellBufferedFromDurableRunsItAfterItsAppendHoweverItEnds() {
        AtomicInteger order = new AtomicInteger();
        int[] appendedAt = {-1};
        int[] bufferedAt = {-1};
        Ingest blocking = new Ingest() {
            @Override
            public AppendResult append(Principal principal, String index, int partition,
                    RecordSource records) throws java.io.IOException {
                appendedAt[0] = order.incrementAndGet();
                throw new java.io.IOException("the store failed");
            }

            @Override
            public void close() {
            }
        };

        assertThatThrownBy(() -> blocking.append(IngestTestSupport.PRINCIPAL, "logs", 0,
                (byte) 0, sink -> { }, () -> bufferedAt[0] = order.incrementAndGet()))
                .isInstanceOf(java.io.IOException.class);
        assertThatThrownBy(() -> blocking.appendRouted(IngestTestSupport.PRINCIPAL, "logs", "r",
                (byte) 0, sink -> { }, () -> bufferedAt[0] = order.incrementAndGet()))
                .isInstanceOf(PlacementRefusedException.class);

        assertThat(appendedAt[0]).isEqualTo(1);
        assertThat(bufferedAt[0])
                .as("⚠️ AFTER THE APPEND, EVEN A FAILED ONE, AND FOR THE ROUTED FORM TOO: the "
                        + "caller's resource is always given back")
                .isEqualTo(3);
    }
}
