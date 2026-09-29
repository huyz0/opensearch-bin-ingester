// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.CostTable;
import io.github.huyz0.os.biningester.binstore.IndexCostLedger;
import io.github.huyz0.os.biningester.binstore.IndexCostLedger.Charge;
import io.github.huyz0.os.biningester.binstore.PutPurposeCounts;
import io.github.huyz0.os.biningester.binstore.StoreCounts;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * M12.19a (M11.4 T1): two indices that cost the same are ranked by id, so
 * {@code top} names the same one every time -- NOT by the order the ledger's
 * snapshot happens to list them, which follows a hash map and may differ
 * between two reports of the same numbers.
 */
class IndexCostReportTieBreakTest {

    private static final UUID LOWER = UUID.fromString("00000000-0000-4000-8000-000000000001");
    private static final UUID HIGHER = UUID.fromString("00000000-0000-4000-8000-000000000002");

    private static String topOne(List<IndexCostLedger.IndexCost> order) {
        Map<Charge, Long> same = Map.of(Charge.DATA_PUT, 500_000L, Charge.COMMIT_PUT, 0L,
                Charge.DATA_GET, 0L);
        List<IndexCostLedger.IndexCost> indices = order.stream()
                .map(cost -> new IndexCostLedger.IndexCost(cost.index(), same, 10)).toList();
        return IndexCostReport.json("pod1",
                new IndexCostLedger.Snapshot(indices,
                        Map.of(Charge.DATA_PUT, 0L, Charge.COMMIT_PUT, 0L, Charge.DATA_GET, 0L)),
                new IndexCostReport.PodTotals(new StoreCounts(1, 0, 0, 0, 0),
                        new PutPurposeCounts(1, 0, 0, 0, 0), 0),
                CostTable.awsS3Standard(), id -> null, 1);
    }

    @Test
    void aTieIsBrokenByIdWhateverOrderTheSnapshotListsThem() {
        IndexCostLedger.IndexCost lower = new IndexCostLedger.IndexCost(LOWER, Map.of(), 0);
        IndexCostLedger.IndexCost higher = new IndexCostLedger.IndexCost(HIGHER, Map.of(), 0);

        String higherFirst = topOne(List.of(higher, lower));
        String lowerFirst = topOne(List.of(lower, higher));

        assertThat(higherFirst)
                .as("⚠️ THE LOWER ID, though the snapshot listed the higher first")
                .contains("\"id\":\"" + LOWER + "\"")
                .doesNotContain("\"id\":\"" + HIGHER + "\"");
        assertThat(lowerFirst).contains("\"id\":\"" + LOWER + "\"");
    }
}
