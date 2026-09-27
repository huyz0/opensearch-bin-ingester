// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.IndexCostLedger;
import io.github.huyz0.os.biningester.binstore.IndexCostLedger.Charge;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Duration;
import java.util.OptionalLong;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * A data PUT that FAILS is apportioned like one that succeeds (M11.2,
 * ADR-0077 decision 2): the counting store counts the attempt, so a ledger
 * charged only on success would sum to less than the requests it apportions.
 */
class SegmentPublisherAttributionTest {

    private static final UUID LOGS = UUID.fromString("00000000-0000-4000-8000-0000000000aa");

    @Test
    void aFailedDataPutIsStillChargedSoTheSharesSumToTheCountedPuts() throws Exception {
        MemoryBinStore memory = new MemoryBinStore();
        // ⚠️ UNDER THE COUNTER, as a real backend fails: the attempt is counted.
        BinStore failingDataPuts = (BinStore) Proxy.newProxyInstance(
                BinStore.class.getClassLoader(), new Class<?>[] {BinStore.class},
                (self, method, args) -> {
                    if (method.getName().equals("put") && ((String) args[0]).endsWith(".bseg")) {
                        throw new IOException("the store refused the data PUT");
                    }
                    try {
                        return method.invoke(memory, args);
                    } catch (java.lang.reflect.InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
        CountingBinStore counting = new CountingBinStore(failingDataPuts);
        IndexCostLedger ledger = new IndexCostLedger();
        SegmentPublisher publisher = new SegmentPublisher(counting, "bins/c", "pod7", ledger);
        Accumulator accumulator = new Accumulator(
                new IngestConfig(Duration.ofMillis(250), 1 << 20, "c"), Clock.systemUTC());
        accumulator.add(new RunKey(LOGS, 0),
                new SegmentRecord("a", OpType.INDEX, OptionalLong.empty(), new byte[10]));

        assertThatThrownBy(() -> publisher.publish(accumulator)).isInstanceOf(IOException.class);

        assertThat(counting.putPurposeCounts().dataPuts())
                .as("the premise: the failed PUT was counted").isEqualTo(1);
        assertThat(ledger.snapshot().totalMicros(Charge.DATA_PUT))
                .as("⚠️ AND APPORTIONED: charging only a successful PUT loses this one")
                .isEqualTo(IndexCostLedger.MICROS_PER_REQUEST);
    }
}
