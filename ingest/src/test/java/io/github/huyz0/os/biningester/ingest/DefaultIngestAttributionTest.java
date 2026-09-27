// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.IndexCostLedger;
import io.github.huyz0.os.biningester.binstore.IndexCostLedger.Charge;
import io.github.huyz0.os.biningester.binstore.StoreCounts;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.RunEntry;
import io.github.huyz0.os.biningester.format.SegmentReader;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Each flush's data PUT is apportioned across its indices by run bytes, and
 * the per-index shares sum exactly to the data PUTs the store counted
 * (M11.2, ADR-0077, M11 criteria 4 and 6).
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class DefaultIngestAttributionTest {

    private static final UUID LOGS = UUID.fromString("00000000-0000-4000-8000-0000000000aa");
    private static final UUID METRICS = UUID.fromString("00000000-0000-4000-8000-0000000000bb");

    private static List<SegmentRecord> docs(String prefix, int count, int payloadBytes) {
        List<SegmentRecord> docs = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            docs.add(new SegmentRecord(prefix + i, OpType.INDEX, OptionalLong.empty(),
                    new byte[payloadBytes]));
        }
        return docs;
    }

    private static CompletableFuture<AppendResult> append(DefaultIngest ingest, String index,
            List<SegmentRecord> docs) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return ingest.append(IngestTestSupport.PRINCIPAL, index, 0, docs::forEach);
            } catch (java.io.IOException e) {
                throw new CompletionException(e);
            }
        });
    }

    private static long micros(IndexCostLedger.Snapshot snapshot, UUID index, Charge charge) {
        return snapshot.indices().stream().filter(i -> i.index().equals(index))
                .mapToLong(i -> i.micros().get(charge)).sum();
    }

    private static long bytes(IndexCostLedger.Snapshot snapshot, UUID index) {
        return snapshot.indices().stream().filter(i -> i.index().equals(index))
                .mapToLong(IndexCostLedger.IndexCost::bytes).sum();
    }

    @Test
    void everyDataPutIsSplitByRunBytesAndTheSharesSumExactlyToTheCountedPuts()
            throws Exception {
        MemoryBinStore backing = new MemoryBinStore();
        CountingBinStore store = new CountingBinStore(backing);
        List<String> written = new ArrayList<>();
        try (DefaultIngest ingest = new DefaultIngest(
                IngestTestSupport.pinnedIntervalConfig(Duration.ofDays(1), 8L << 20), store,
                IngestTestSupport.PREFIX, "pod1", IngestTestSupport.sequencer(store, "pod1"),
                new SubscriptionHub(), Clock.systemUTC(),
                index -> index.equals("logs") ? LOGS : METRICS,
                written::add)) {
            long dataPutsBefore = store.putPurposeCounts().dataPuts();
            StoreCounts before = null;
            for (int flush = 0; flush < 4; flush++) {
                if (flush == 1) {
                    // ⚠️ AFTER THE FIRST FLUSH: an index first seen costs the pod
                    // its ordinal registry's requests (ADR-0008), which are not
                    // the ledger's; from here on every index is known.
                    before = store.counts();
                }
                // ⚠️ UNEQUAL BYTES, EQUAL RECORD COUNTS: a split by records or an
                // equal split gives each index half, a split by bytes does not.
                var big = append(ingest, "logs", docs("l" + flush + "-", 5, 900));
                var small = append(ingest, "audit", docs("m" + flush + "-", 5, 10));
                IngestTestSupport.awaitPending(ingest, 2);
                ingest.flushNow();
                big.get(10, TimeUnit.SECONDS);
                small.get(10, TimeUnit.SECONDS);
            }
            IndexCostLedger.Snapshot snapshot = ingest.costLedger().snapshot();
            long dataPuts = store.putPurposeCounts().dataPuts() - dataPutsBefore;

            assertThat(dataPuts).as("the premise: one data PUT per flush").isEqualTo(4);
            assertThat(snapshot.totalMicros(Charge.DATA_PUT))
                    .as("⚠️ EXACT: Σ index shares + unattributed = counted data PUTs")
                    .isEqualTo(dataPuts * IndexCostLedger.MICROS_PER_REQUEST);
            assertThat(snapshot.unattributed().get(Charge.DATA_PUT)).isZero();

            Map<UUID, Long> runBytes = new HashMap<>();
            long expectedLogsMicros = 0;
            for (String key : written) {
                Map<UUID, Long> thisSegment = new HashMap<>();
                for (RunEntry entry : SegmentReader.open(IngestTestSupport.read(store, key))
                        .directory()) {
                    thisSegment.merge(entry.key().indexId(), (long) entry.byteLen(), Long::sum);
                }
                thisSegment.forEach((id, b) -> runBytes.merge(id, b, Long::sum));
                IndexCostLedger one = new IndexCostLedger();
                one.apportion(Charge.DATA_PUT, thisSegment);
                expectedLogsMicros += micros(one.snapshot(), LOGS, Charge.DATA_PUT);
            }
            assertThat(written).as("the premise: every flushed segment was seen").hasSize(4);
            assertThat(micros(snapshot, LOGS, Charge.DATA_PUT))
                    .as("⚠️ BY RUN BYTES, from the directory the PUT carried")
                    .isEqualTo(expectedLogsMicros)
                    .isGreaterThan(3 * IndexCostLedger.MICROS_PER_REQUEST);
            assertThat(bytes(snapshot, LOGS)).as("bytes are the run byte lengths")
                    .isEqualTo(runBytes.get(LOGS));
            assertThat(bytes(snapshot, METRICS)).isEqualTo(runBytes.get(METRICS));

            StoreCounts after = store.counts();
            assertThat(after.gets() - before.gets())
                    .as("⚠️ THE LEDGER ISSUES NO REQUEST: the reads above are the test's own, "
                            + "one per segment")
                    .isEqualTo(written.size());
            assertThat(after.puts() - before.puts())
                    .as("three flushes: a data and a commit PUT each, nothing more")
                    .isEqualTo(6);
            assertThat(after.stats() - before.stats()).isZero();
            assertThat(after.lists() - before.lists()).isZero();
        }
    }
}
