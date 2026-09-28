// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.CostTable;
import io.github.huyz0.os.biningester.binstore.IndexCostLedger;
import io.github.huyz0.os.biningester.binstore.IndexCostLedger.Charge;
import io.github.huyz0.os.biningester.binstore.PutPurposeCounts;
import io.github.huyz0.os.biningester.binstore.StoreCounts;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The {@code /admin/cost} document (M11.4, ADR-0077, M11 criterion 7): the top
 * N indices by ESTIMATED COST, not by bytes or requests, their exact
 * apportioned requests, and the pod totals the shares sum to.
 */
class IndexCostReportTest {

    private static final UUID PUTS = UUID.fromString("00000000-0000-4000-8000-000000000009");
    private static final UUID GETS = UUID.fromString("00000000-0000-4000-8000-000000000005");
    private static final UUID NAMELESS = UUID.fromString("00000000-0000-4000-8000-000000000001");

    private static final IndexCostReport.PodTotals TOTALS = new IndexCostReport.PodTotals(
            new StoreCounts(7, 11, 2, 3, 4), new PutPurposeCounts(2, 1, 1, 2, 1), 9);

    /**
     * PUTS: one data PUT, few bytes. GETS: ten GETs, many bytes -- ⚠️ MORE
     * requests and MORE bytes, and still cheaper, because a GET costs 1/12.5 of
     * a PUT. NAMELESS: a quarter of a commit PUT.
     */
    private static IndexCostLedger ledger() {
        IndexCostLedger ledger = new IndexCostLedger();
        ledger.apportion(Charge.DATA_PUT, Map.of(PUTS, 1L));
        ledger.bytesWritten(PUTS, 10);
        for (int i = 0; i < 10; i++) {
            ledger.apportion(Charge.DATA_GET, Map.of(GETS, 1L));
        }
        ledger.bytesWritten(GETS, 1_000_000);
        ledger.apportion(Charge.COMMIT_PUT, Map.of(NAMELESS, 1L, PUTS, 3L));
        ledger.unattributed(Charge.DATA_GET);
        return ledger;
    }

    private static String report(int top) {
        return IndexCostReport.json("pod\"1", ledger().snapshot(), TOTALS,
                CostTable.awsS3Standard(), Map.of(PUTS, "logs", GETS, "audit")::get, top);
    }

    @Test
    void indicesAreRankedByEstimatedCostNotByRequestsOrBytes() {
        String json = report(50);

        int logs = json.indexOf("\"index\":\"logs\"");
        int audit = json.indexOf("\"index\":\"audit\"");
        int nameless = json.indexOf("\"index\":\"" + NAMELESS + "\"");
        assertThat(logs).as("⚠️ 1.75 PUTs outrank ten GETs and a million bytes")
                .isNotNegative().isLessThan(audit);
        assertThat(audit).isLessThan(nameless);
    }

    @Test
    void rowsAreOneJsonArrayInRankOrder() {
        String json = report(50);

        assertThat(json).as("⚠️ THE IDS SORT AGAINST THE COST, so an order by id fails here")
                .contains("\"indices\":[{\"index\":\"logs\"")
                .contains("},{\"index\":\"audit\"")
                .contains("},{\"index\":\"" + NAMELESS + "\"")
                .endsWith("}]}");
    }

    @Test
    void equalCostsAreOrderedByIdSoTheReportIsDeterministic() {
        IndexCostLedger ledger = new IndexCostLedger();
        ledger.apportion(Charge.DATA_PUT, Map.of(PUTS, 1L, GETS, 1L));

        String json = IndexCostReport.json("p", ledger.snapshot(), TOTALS,
                CostTable.awsS3Standard(), id -> null, 50);

        assertThat(json.indexOf("\"id\":\"" + GETS)).as("the lower id first")
                .isLessThan(json.indexOf("\"id\":\"" + PUTS));
    }

    @Test
    void aControlCharacterInANameIsEscaped() {
        IndexCostLedger ledger = new IndexCostLedger();
        ledger.apportion(Charge.DATA_PUT, Map.of(PUTS, 1L));

        String json = IndexCostReport.json("p", ledger.snapshot(), TOTALS,
                CostTable.awsS3Standard(), id -> "a\nb\u0001c\\d\te\rf", 50);

        assertThat(json).contains("\"index\":\"a\\nb\\u0001c\\\\d\\te\\rf\"");
    }

    @Test
    void sharesAreExactRequestsAndDollarsAreTheirEstimate() {
        String json = report(50);

        assertThat(json).contains("\"index\":\"logs\",\"id\":\"" + PUTS + "\",\"bytes\":10,"
                + "\"requests\":{\"dataPut\":1.000000,\"commitPut\":0.750000,\"dataGet\":0.000000},"
                // 1.75 PUTs x 5,000 micro-dollars per 1,000 = 8.75 micro-dollars
                + "\"estimatedUsd\":0.000008750}");
        assertThat(json).contains("\"index\":\"audit\",\"id\":\"" + GETS + "\","
                + "\"bytes\":1000000,\"requests\":{\"dataPut\":0.000000,\"commitPut\":0.000000,"
                + "\"dataGet\":10.000000},\"estimatedUsd\":0.000004000}");
        assertThat(json).contains("\"unattributed\":{\"dataPut\":0.000000,"
                + "\"commitPut\":0.000000,\"dataGet\":1.000000}");
        assertThat(json).contains("\"estimate\":");
    }

    @Test
    void anIndexTheCatalogCannotNameIsPrintedByItsId() {
        assertThat(report(50)).contains("{\"index\":\"" + NAMELESS + "\",\"id\":\""
                + NAMELESS + "\"");
    }

    @Test
    void thePodTotalsThePricesAndTheCountAreThere() {
        String json = report(50);

        assertThat(json).startsWith("{\"pod\":\"pod\\\"1\",\"by\":\"index\"");
        assertThat(json).contains("\"prices\":{\"putMicroUsdPerThousand\":5000,"
                + "\"getMicroUsdPerThousand\":400,\"listMicroUsdPerThousand\":5000}");
        assertThat(json).contains("\"totals\":{\"puts\":7,\"dataPuts\":2,\"commitPuts\":1,"
                + "\"checkpointPuts\":1,\"leasePuts\":2,\"otherPuts\":1,\"gets\":11,"
                + "\"dataSegmentGets\":9,\"lists\":2,\"stats\":3,\"deletes\":4}");
        assertThat(json).contains("\"indexCount\":3");
    }

    @Test
    void topBoundsTheRowsButNotTheCount() {
        String json = report(1);

        assertThat(json).contains("\"index\":\"logs\"").doesNotContain("\"index\":\"audit\"");
        assertThat(json).contains("\"indexCount\":3");
    }

    @Test
    void theLargestTopIsAccepted() {
        assertThat(report(IndexCostReport.MAX_TOP)).contains("\"indexCount\":3");
    }

    @Test
    void aTopOutsideItsBoundIsRefused() {
        assertThatThrownBy(() -> report(0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> report(IndexCostReport.MAX_TOP + 1))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
