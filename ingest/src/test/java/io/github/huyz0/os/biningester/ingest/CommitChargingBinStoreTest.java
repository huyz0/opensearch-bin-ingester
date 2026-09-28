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
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.Continue;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.Seal;
import io.github.huyz0.os.biningester.format.SegmentCommit;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Every commit-log PUT is charged to the indices of the delta it carries, by
 * record count, where it is ISSUED (M11.22, ADR-0077 as amended, M11
 * criterion 4): Σ index commit shares + unattributed = counted commit PUTs.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class CommitChargingBinStoreTest {

    private static final String LOG = "bins/c/ctl/log/1/";
    private static final UUID HEAVY = UUID.fromString("00000000-0000-4000-8000-0000000000aa");
    private static final UUID LIGHT = UUID.fromString("00000000-0000-4000-8000-0000000000bb");

    private static Body delta(long sequence) {
        CommitDelta delta = new CommitDelta(sequence, List.of(
                new SegmentCommit("bins/c/data/a.bseg", List.of(
                        new RunCommit(new RunKey(HEAVY, 0), 3, 0),
                        new RunCommit(new RunKey(LIGHT, 0), 1, 0))),
                new SegmentCommit("bins/c/data/b.bseg", List.of(
                        new RunCommit(new RunKey(HEAVY, 1), 4, 0)))));
        return Body.ofBytes(delta.encode());
    }

    private static long micros(IndexCostLedger ledger, UUID index) {
        return ledger.snapshot().indices().stream().filter(i -> i.index().equals(index))
                .mapToLong(i -> i.micros().get(Charge.COMMIT_PUT)).sum();
    }

    private static void assertExact(IndexCostLedger ledger, CountingBinStore counting) {
        assertThat(ledger.snapshot().totalMicros(Charge.COMMIT_PUT))
                .as("⚠️ EXACT: Σ index commit shares + unattributed = counted commit PUTs")
                .isEqualTo(counting.putPurposeCounts().commitPuts()
                        * IndexCostLedger.MICROS_PER_REQUEST);
    }

    @Test
    void aDeltaIsSplitByTheRecordCountsOfEveryRunItCarries() throws Exception {
        CountingBinStore counting = new CountingBinStore(new MemoryBinStore());
        IndexCostLedger ledger = new IndexCostLedger();
        BinStore store = new CommitChargingBinStore(counting, ledger);

        store.putIfAbsent(LOG + "00000000000000000001.delta", delta(1));

        assertThat(micros(ledger, HEAVY))
                .as("⚠️ ACROSS EVERY SEGMENT IN THE DELTA: 3 + 4 of 8 records")
                .isEqualTo(875_000);
        assertThat(micros(ledger, LIGHT)).isEqualTo(125_000);
        assertExact(ledger, counting);
    }

    @Test
    void aContinueASealAndAnUndecodableEntryAreUnattributedNeverDropped() throws Exception {
        CountingBinStore counting = new CountingBinStore(new MemoryBinStore());
        IndexCostLedger ledger = new IndexCostLedger();
        BinStore store = new CommitChargingBinStore(counting, ledger);

        store.putIfAbsent(LOG + "00000000000000000000.delta",
                Body.ofBytes(new Continue(0, 0, 0).encode()));
        store.putIfAbsent(LOG + "00000000000000000009.delta",
                Body.ofBytes(new Seal(9, 1).encode()));
        store.putIfAbsent(LOG + "00000000000000000010.delta", Body.ofBytes("junk".getBytes()));

        assertThat(ledger.snapshot().unattributed().get(Charge.COMMIT_PUT))
                .isEqualTo(3 * IndexCostLedger.MICROS_PER_REQUEST);
        assertExact(ledger, counting);
    }

    @Test
    void aLostRaceAndAFailedPutAreChargedAsTheCounterCountsThem() throws Exception {
        MemoryBinStore memory = new MemoryBinStore();
        BinStore failing = (BinStore) Proxy.newProxyInstance(BinStore.class.getClassLoader(),
                new Class<?>[] {BinStore.class}, (self, method, args) -> {
                    if (method.getName().equals("putIfAbsent")
                            && ((String) args[0]).endsWith("7.delta")) {
                        throw new IOException("the store refused the commit");
                    }
                    try {
                        return method.invoke(memory, args);
                    } catch (java.lang.reflect.InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
        CountingBinStore counting = new CountingBinStore(failing);
        IndexCostLedger ledger = new IndexCostLedger();
        BinStore store = new CommitChargingBinStore(counting, ledger);

        store.putIfAbsent(LOG + "00000000000000000002.delta", delta(2));
        assertThat(store.putIfAbsent(LOG + "00000000000000000002.delta", delta(2)))
                .as("the premise: the race is lost").isEmpty();
        assertThatThrownBy(() -> store.putIfAbsent(LOG + "00000000000000000007.delta",
                delta(7))).isInstanceOf(IOException.class);

        assertThat(counting.putPurposeCounts().commitPuts()).as("the premise").isEqualTo(3);
        assertThat(micros(ledger, HEAVY)).as("⚠️ THREE ATTEMPTS, THREE CHARGES")
                .isEqualTo(3 * 875_000);
        assertExact(ledger, counting);
    }

    @Test
    void aPutThatIsNotACommitIsNotCharged() throws Exception {
        CountingBinStore counting = new CountingBinStore(new MemoryBinStore());
        IndexCostLedger ledger = new IndexCostLedger();
        BinStore store = new CommitChargingBinStore(counting, ledger);

        store.put("bins/c/data/a.bseg", delta(3));
        store.put("bins/c/ctl/log/1/ckpt/00000000000000000003.ckpt", delta(3));
        store.putIfMatch("bins/c/ctl/lease/0.json", delta(3),
                store.put("bins/c/ctl/lease/0.json", Body.ofBytes(new byte[1])));

        assertThat(ledger.snapshot().totalMicros(Charge.COMMIT_PUT)).isZero();
        assertExact(ledger, counting);
    }

    @Test
    void everyCommitPutAPodsIngestIssuesIsCharged() throws Exception {
        CountingBinStore counting = new CountingBinStore(new MemoryBinStore());
        IndexCostLedger ledger = new IndexCostLedger();
        BinStore store = new CommitChargingBinStore(counting, ledger);
        try (DefaultIngest ingest = new DefaultIngest(
                IngestTestSupport.pinnedIntervalConfig(Duration.ofDays(1), 8L << 20), store,
                IngestTestSupport.PREFIX, "pod1", IngestTestSupport.sequencer(store, "pod1"),
                new SubscriptionHub(), Clock.systemUTC(),
                index -> index.equals("logs") ? HEAVY : LIGHT, key -> { }, ledger)) {
            for (int flush = 0; flush < 3; flush++) {
                var logs = append(ingest, "logs", 6);
                var audit = append(ingest, "audit", 2);
                IngestTestSupport.awaitPending(ingest, 2);
                ingest.flushNow();
                logs.get(10, TimeUnit.SECONDS);
                audit.get(10, TimeUnit.SECONDS);
            }

            assertThat(micros(ledger, HEAVY))
                    .as("⚠️ THREE DELTAS, EACH 6 OF 8 RECORDS: the CONTINUE that opened the "
                            + "chain carries none")
                    .isEqualTo(3 * 750_000);
            assertThat(ledger.snapshot().totalMicros(Charge.DATA_PUT))
                    .as("and the same ledger holds the pod's data PUTs")
                    .isEqualTo(counting.putPurposeCounts().dataPuts()
                            * IndexCostLedger.MICROS_PER_REQUEST);
            assertExact(ledger, counting);
        }
    }

    private static CompletableFuture<AppendResult> append(DefaultIngest ingest, String index,
            int count) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                List<io.github.huyz0.os.biningester.format.SegmentRecord> docs =
                        new java.util.ArrayList<>();
                for (int i = 0; i < count; i++) {
                    docs.add(new io.github.huyz0.os.biningester.format.SegmentRecord(
                            index + i + "-" + System.nanoTime(),
                            io.github.huyz0.os.biningester.format.OpType.INDEX,
                            OptionalLong.empty(), new byte[8]));
                }
                return ingest.append(IngestTestSupport.PRINCIPAL, index, 0, docs::forEach);
            } catch (IOException e) {
                throw new java.util.concurrent.CompletionException(e);
            }
        });
    }
}
