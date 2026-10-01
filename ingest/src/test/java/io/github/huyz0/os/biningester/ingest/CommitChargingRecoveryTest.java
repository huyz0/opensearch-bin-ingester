// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.IndexCostLedger;
import io.github.huyz0.os.biningester.binstore.IndexCostLedger.Charge;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.Recovery;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentCommit;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * A recovery entry's commit PUT is charged by its segment runs, as a delta's
 * is, and its voids charge no index (M13.25, ADR-0082 §5).
 */
class CommitChargingRecoveryTest {

    private static final String LOG = "bins/c/ctl/log/1/";
    private static final UUID HEAVY = UUID.fromString("00000000-0000-4000-8000-0000000000aa");
    private static final UUID LIGHT = UUID.fromString("00000000-0000-4000-8000-0000000000bb");

    private static long micros(IndexCostLedger ledger, UUID index) {
        return ledger.snapshot().indices().stream().filter(i -> i.index().equals(index))
                .mapToLong(i -> i.micros().get(Charge.COMMIT_PUT)).sum();
    }

    @Test
    void aRECOVERYIsChargedByItsRunsAndNotByItsVoids() throws Exception {
        CountingBinStore counting = new CountingBinStore(new MemoryBinStore());
        IndexCostLedger ledger = new IndexCostLedger();
        CommitChargingBinStore store = new CommitChargingBinStore(counting, ledger);
        Recovery recovery = new Recovery(1,
                List.of(new SegmentCommit("bins/c/data/r.bseg",
                        List.of(new RunCommit(new RunKey(HEAVY, 0), 3, 0)))),
                List.of(new Recovery.VoidRange(new RunKey(LIGHT, 0), 0, 1_000_000)));

        store.putIfAbsent(LOG + "00000000000000000001.delta", Body.ofBytes(recovery.encode()));

        assertThat(micros(ledger, HEAVY))
                .as("the whole PUT to the index whose records it commits")
                .isEqualTo(IndexCostLedger.MICROS_PER_REQUEST);
        assertThat(micros(ledger, LIGHT))
                .as("a void moves no bytes: a million voided offsets cost nothing")
                .isZero();
    }
}
