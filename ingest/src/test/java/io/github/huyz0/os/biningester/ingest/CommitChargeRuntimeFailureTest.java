// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.IndexCostLedger;
import io.github.huyz0.os.biningester.binstore.IndexCostLedger.Charge;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentCommit;
import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * M12.19a (M11.22 T1): reading a commit-log body to charge it can fail with an
 * UNCHECKED exception as well as an IOException -- a body supplier that
 * throws, a stream that does. Either way the PUT is charged to unattributed
 * and still issued: the charge is bookkeeping and must never fail the commit.
 * Only the IOException arm had a case.
 */
class CommitChargeRuntimeFailureTest {

    private static final String KEY = "bins/c/ctl/log/1/00000000000000000001.delta";

    @Test
    void aBodyThatThrowsUncheckedWhenReadForTheChargeIsChargedUnattributedAndStillPut()
            throws Exception {
        byte[] bytes = new CommitDelta(1, List.of(new SegmentCommit("bins/c/data/a.bseg",
                List.of(new RunCommit(new RunKey(UUID.randomUUID(), 0), 3, 0))))).encode();
        AtomicInteger opens = new AtomicInteger();
        // ⚠️ THE FIRST OPEN IS THE CHARGE's; the store's own read is the second.
        Body body = new Body(bytes.length, () -> {
            if (opens.getAndIncrement() == 0) {
                throw new IllegalStateException("the body's source is gone");
            }
            return new ByteArrayInputStream(bytes);
        });
        MemoryBinStore memory = new MemoryBinStore();
        CountingBinStore counting = new CountingBinStore(memory);
        IndexCostLedger ledger = new IndexCostLedger();
        BinStore store = new CommitChargingBinStore(counting, ledger);

        store.putIfAbsent(KEY, body);

        assertThat(memory.stat(KEY)).as("⚠️ THE COMMIT WAS STILL PUT: the charge never fails it")
                .isPresent();
        assertThat(ledger.snapshot().unattributed().get(Charge.COMMIT_PUT))
                .as("and charged, whole, to unattributed")
                .isEqualTo(IndexCostLedger.MICROS_PER_REQUEST);
        assertThat(ledger.snapshot().totalMicros(Charge.COMMIT_PUT))
                .as("EXACT: Σ shares + unattributed = counted commit PUTs")
                .isEqualTo(counting.putPurposeCounts().commitPuts()
                        * IndexCostLedger.MICROS_PER_REQUEST);
    }
}
