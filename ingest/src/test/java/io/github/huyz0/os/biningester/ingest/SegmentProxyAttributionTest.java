// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.IndexCostLedger;
import io.github.huyz0.os.biningester.binstore.IndexCostLedger.Charge;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.format.SegmentWriter;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Every store GET {@link SegmentProxy} issues is charged once to the indices
 * of the segment it read (M11.3, ADR-0077 decisions 3, 4 and 4a, M11
 * criterion 5): split by the directory when the whole segment was held,
 * charged to {@code unattributed} when it was not, and never once per caller.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class SegmentProxyAttributionTest {

    private static final String KEY = "bins/c/data/attributed.bseg";
    private static final UUID HEAVY = UUID.fromString("00000000-0000-4000-8000-0000000000aa");
    private static final UUID LIGHT = UUID.fromString("00000000-0000-4000-8000-0000000000bb");

    private static byte[] twoIndexSegment() throws IOException {
        SegmentWriter writer = new SegmentWriter();
        for (int i = 0; i < 4; i++) {
            writer.add(new RunKey(HEAVY, 0), new SegmentRecord("h" + i, OpType.INDEX,
                    OptionalLong.empty(), new byte[2_000]), 0);
        }
        writer.add(new RunKey(LIGHT, 0), new SegmentRecord("l", OpType.INDEX,
                OptionalLong.empty(), new byte[20]), 0);
        return writer.toByteArray(0);
    }

    /** What the ledger should hold for ONE held GET of {@code segment}. */
    private static long heavyShareOfOneGet(byte[] segment) throws IOException {
        IndexCostLedger expected = new IndexCostLedger();
        expected.apportion(Charge.DATA_GET, SegmentCharges.runBytes(
                io.github.huyz0.os.biningester.format.SegmentReader.open(segment).directory()));
        return micros(expected.snapshot(), HEAVY);
    }

    private static long micros(IndexCostLedger.Snapshot snapshot, UUID index) {
        return snapshot.indices().stream().filter(i -> i.index().equals(index))
                .mapToLong(i -> i.micros().get(Charge.DATA_GET)).sum();
    }

    private static long unattributed(IndexCostLedger ledger) {
        return ledger.snapshot().unattributed().get(Charge.DATA_GET);
    }

    private static void assertExact(IndexCostLedger ledger, CountingBinStore counting) {
        assertThat(ledger.snapshot().totalMicros(Charge.DATA_GET))
                .as("⚠️ EXACT: Σ index GET shares + unattributed = counted data-segment GETs")
                .isEqualTo(counting.dataSegmentGets() * IndexCostLedger.MICROS_PER_REQUEST);
    }

    @Test
    void aHeldGetIsSplitByTheSegmentsOwnDirectory() throws Exception {
        MemoryBinStore memory = new MemoryBinStore();
        byte[] segment = twoIndexSegment();
        memory.put(KEY, Body.ofBytes(segment));
        CountingBinStore counting = new CountingBinStore(memory);
        IndexCostLedger ledger = new IndexCostLedger();
        SegmentProxy proxy = new SegmentProxy(counting, 1024,
                SegmentCache.forSegmentsOf(1 << 20), ledger);

        proxy.streamTo(KEY, List.of((b, o, l) -> { }));
        proxy.streamTo(KEY, List.of((b, o, l) -> { }));

        assertThat(counting.dataSegmentGets()).as("the premise: the second is a cache hit")
                .isEqualTo(1);
        assertThat(micros(ledger.snapshot(), HEAVY))
                .as("⚠️ BY RUN BYTES, not equally: the heavy index carried ~99%")
                .isEqualTo(heavyShareOfOneGet(segment))
                .isGreaterThan(900_000);
        assertThat(unattributed(ledger)).isZero();
        assertExact(ledger, counting);
    }

    @Test
    void aGetStreamedWithoutBeingHeldIsChargedWholeToUnattributed() throws Exception {
        MemoryBinStore memory = new MemoryBinStore();
        byte[] segment = twoIndexSegment();
        memory.put(KEY, Body.ofBytes(segment));
        CountingBinStore counting = new CountingBinStore(memory);
        IndexCostLedger ledger = new IndexCostLedger();
        // ⚠️ A HOLD SMALLER THAN THE SEGMENT: the bytes stream past and are gone.
        SegmentProxy proxy = new SegmentProxy(counting, 1024,
                new SegmentCache(segment.length / 2), ledger);

        proxy.streamTo(KEY, List.of((b, o, l) -> { }));
        proxy.streamTo(KEY, List.of((b, o, l) -> { }), Duration.ofSeconds(5));

        assertThat(counting.dataSegmentGets()).as("two GETs, neither held").isEqualTo(2);
        assertThat(unattributed(ledger))
                .as("⚠️ NEVER DROPPED: both are billed, neither can be split")
                .isEqualTo(2 * IndexCostLedger.MICROS_PER_REQUEST);
        assertExact(ledger, counting);
    }

    @Test
    void aGetThatFailsIsStillChargedOnce() throws Exception {
        CountingBinStore counting = new CountingBinStore(new MemoryBinStore());
        IndexCostLedger ledger = new IndexCostLedger();
        SegmentProxy proxy = new SegmentProxy(counting, 1024,
                SegmentCache.forSegmentsOf(1 << 20), ledger);

        assertThatThrownBy(() -> proxy.streamTo(KEY, List.of((b, o, l) -> { })))
                .isInstanceOf(IOException.class);

        assertThat(counting.dataSegmentGets()).as("the premise: the failed GET was counted")
                .isEqualTo(1);
        assertThat(unattributed(ledger)).isEqualTo(IndexCostLedger.MICROS_PER_REQUEST);
        assertExact(ledger, counting);
    }

    @Test
    void concurrentJoinedCallersShareOneGetAndOneCharge() throws Exception {
        MemoryBinStore memory = new MemoryBinStore();
        byte[] segment = twoIndexSegment();
        memory.put(KEY, Body.ofBytes(segment));
        CountDownLatch release = new CountDownLatch(1);
        BinStore gated = (BinStore) Proxy.newProxyInstance(BinStore.class.getClassLoader(),
                new Class<?>[] {BinStore.class}, (self, method, args) -> {
                    if (method.getName().equals("get")) {
                        release.await();
                    }
                    try {
                        return method.invoke(memory, args);
                    } catch (java.lang.reflect.InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
        CountingBinStore counting = new CountingBinStore(gated);
        IndexCostLedger ledger = new IndexCostLedger();
        SegmentProxy proxy = new SegmentProxy(counting, 1024,
                SegmentCache.forSegmentsOf(1 << 20), ledger);
        int callers = 8;

        List<CompletableFuture<Void>> calls = new ArrayList<>();
        var executor = Executors.newVirtualThreadPerTaskExecutor();
        for (int i = 0; i < callers; i++) {
            calls.add(CompletableFuture.runAsync(() -> {
                try {
                    proxy.streamTo(KEY, List.of((b, o, l) -> { }));
                } catch (IOException e) {
                    throw new java.io.UncheckedIOException(e);
                }
            }, executor));
        }
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (proxy.joinersOf(KEY) < callers - 1 && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        assertThat(proxy.joinersOf(KEY)).as("the premise: every other caller joined")
                .isEqualTo(callers - 1);
        release.countDown();
        for (CompletableFuture<Void> call : calls) {
            call.get(30, TimeUnit.SECONDS);
        }

        assertThat(counting.dataSegmentGets()).as("the premise: one GET").isEqualTo(1);
        assertThat(ledger.snapshot().totalMicros(Charge.DATA_GET))
                .as("⚠️ ONE CHARGE, NOT ONE PER CALLER: the joiners issued no request")
                .isEqualTo(IndexCostLedger.MICROS_PER_REQUEST);
        assertThat(micros(ledger.snapshot(), HEAVY)).isEqualTo(heavyShareOfOneGet(segment));
        assertExact(ledger, counting);
    }

    @Test
    void aHeldGetThatDoesNotDecodeIsChargedWholeToUnattributed() throws Exception {
        MemoryBinStore memory = new MemoryBinStore();
        memory.put(KEY, Body.ofBytes("not a segment".getBytes()));
        CountingBinStore counting = new CountingBinStore(memory);
        IndexCostLedger ledger = new IndexCostLedger();
        SegmentProxy proxy = new SegmentProxy(counting, 1024,
                SegmentCache.forSegmentsOf(1 << 20), ledger);

        proxy.streamTo(KEY, List.of((b, o, l) -> { }));

        assertThat(unattributed(ledger))
                .as("⚠️ HELD BUT UNDECODABLE: no directory to split by, and still billed")
                .isEqualTo(IndexCostLedger.MICROS_PER_REQUEST);
        assertExact(ledger, counting);
    }
}
