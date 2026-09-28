// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.IndexCostLedger;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.security.Principal;
import java.io.IOException;
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
 * before the durable wait (M11.7); a routed append {@code DefaultIngest}
 * refuses still runs it, once (M12.2).
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

    /**
     * ⚠️ DEFAULT INGEST'S OWN ROUTED BUFFERED FORM (M12.2), replacing a case
     * that pinned the interface default M12.2 removed: with the default gone,
     * that case's double carried the default's body itself and constrained no
     * production code (M12.2 review T1). This ingester has no catalog, so a
     * routed append is refused as a placement -- and the caller's resource is
     * still given back, exactly once.
     */
    @Test
    void defaultIngestRefusesARoutedBufferedAppendAndStillRunsBufferedOnce() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        AtomicInteger buffered = new AtomicInteger();
        try (DefaultIngest ingest = new DefaultIngest(
                IngestTestSupport.pinnedIntervalConfig(Duration.ofMillis(20), 8L << 20), store,
                IngestTestSupport.PREFIX, "pod1", IngestTestSupport.sequencer(store, "pod1"),
                new SubscriptionHub(), Clock.systemUTC(), index -> IngestTestSupport.LOGS,
                segmentKey -> { }, new IndexCostLedger())) {
            assertThatThrownBy(() -> ingest.appendRouted(IngestTestSupport.PRINCIPAL, "logs",
                    "tenant-42", (byte) 0, sink -> sink.accept(DOC), buffered::incrementAndGet))
                    .isInstanceOf(PlacementRefusedException.class);

            assertThat(buffered).as("⚠️ GIVEN BACK EVEN WHEN REFUSED, AND ONCE").hasValue(1);
        }
    }

    /** ⚠️ NO CATALOG, NO ALIAS (M12.2): every name is its own index, so a quota is charged to it. */
    @Test
    void defaultIngestNamesEveryIndexItself() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        try (DefaultIngest ingest = new DefaultIngest(
                IngestTestSupport.pinnedIntervalConfig(Duration.ofMillis(20), 8L << 20), store,
                IngestTestSupport.PREFIX, "pod1", IngestTestSupport.sequencer(store, "pod1"),
                new SubscriptionHub(), Clock.systemUTC(), index -> IngestTestSupport.LOGS,
                segmentKey -> { }, new IndexCostLedger())) {
            assertThat(ingest.concreteIndex("logs-write")).isEqualTo("logs-write");
            assertThat(ingest.concreteIndex("logs-000002")).isEqualTo("logs-000002");
        }
    }
}
