// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.binstore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.IndexCostLedger.Charge;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * An index's share of a request is its weight over the request's, in
 * micro-requests that sum exactly (M11.2, ADR-0077 decisions 1 and 2).
 */
class IndexCostLedgerTest {

    private static final UUID A = UUID.fromString("00000000-0000-0000-0000-00000000000a");
    private static final UUID B = UUID.fromString("00000000-0000-0000-0000-00000000000b");
    private static final UUID C = UUID.fromString("00000000-0000-0000-0000-00000000000c");

    private static long micros(IndexCostLedger ledger, UUID index, Charge charge) {
        return ledger.snapshot().indices().stream().filter(i -> i.index().equals(index))
                .mapToLong(i -> i.micros().get(charge)).sum();
    }

    @Test
    void aRequestIsSplitByWeightNotEqually() {
        IndexCostLedger ledger = new IndexCostLedger();

        ledger.apportion(Charge.DATA_PUT, Map.of(A, 900L, B, 100L));

        assertThat(micros(ledger, A, Charge.DATA_PUT))
                .as("⚠️ BY BYTES: an equal split would give the trickle half the bill")
                .isEqualTo(900_000);
        assertThat(micros(ledger, B, Charge.DATA_PUT)).isEqualTo(100_000);
        assertThat(micros(ledger, A, Charge.DATA_GET)).as("another charge is untouched")
                .isZero();
    }

    @Test
    void theRemainderGoesToTheLargestFractionSoTheSharesSumToExactlyOneRequest() {
        IndexCostLedger ledger = new IndexCostLedger();
        Map<UUID, Long> thirds = new LinkedHashMap<>();
        thirds.put(C, 1L);
        thirds.put(B, 1L);
        thirds.put(A, 1L);

        ledger.apportion(Charge.DATA_PUT, thirds);

        assertThat(micros(ledger, A, Charge.DATA_PUT))
                .as("⚠️ A TIE GOES TO THE LOWEST INDEX ID, whatever order the map iterates")
                .isEqualTo(333_334);
        assertThat(micros(ledger, B, Charge.DATA_PUT)).isEqualTo(333_333);
        assertThat(micros(ledger, C, Charge.DATA_PUT)).isEqualTo(333_333);
        assertThat(ledger.snapshot().totalMicros(Charge.DATA_PUT))
                .as("⚠️ FLOOR ROUNDING WOULD LOSE ONE MICRO-REQUEST HERE")
                .isEqualTo(IndexCostLedger.MICROS_PER_REQUEST);
    }

    @Test
    void theLargestRemainderWinsOverTheLowestId() {
        // 1/7 = 142857.142..., 6/7 = 857142.857...: the second remainder is larger.
        long[] shares = IndexCostLedger.shares(new long[] {1, 6}, 7);

        assertThat(shares).containsExactly(142_857, 857_143);
    }

    @Test
    void everyShareIsWithinOneMicroOfProportionalAndEveryRequestSumsExactly() {
        IndexCostLedger ledger = new IndexCostLedger();
        Random random = new Random(11);
        int requests = 500;
        for (int r = 0; r < requests; r++) {
            int n = 1 + random.nextInt(40);
            long[] weights = new long[n];
            long total = 0;
            for (int i = 0; i < n; i++) {
                weights[i] = random.nextInt(8 << 20);
                total += weights[i];
            }
            if (total == 0) {
                continue;
            }
            long[] shares = IndexCostLedger.shares(weights, total);
            long sum = 0;
            for (int i = 0; i < n; i++) {
                double exact = (double) weights[i] * IndexCostLedger.MICROS_PER_REQUEST / total;
                assertThat((double) shares[i]).isCloseTo(exact,
                        org.assertj.core.data.Offset.offset(1.0));
                sum += shares[i];
            }
            assertThat(sum).isEqualTo(IndexCostLedger.MICROS_PER_REQUEST);
            Map<UUID, Long> byIndex = new LinkedHashMap<>();
            for (int i = 0; i < n; i++) {
                byIndex.put(UUID.randomUUID(), weights[i]);
            }
            ledger.apportion(Charge.DATA_GET, byIndex);
        }
        assertThat(ledger.snapshot().totalMicros(Charge.DATA_GET) % IndexCostLedger.MICROS_PER_REQUEST)
                .as("the ledger's total is a whole number of requests").isZero();
    }

    @Test
    void weightsBeyondALongProductAreSplitExactly() {
        long huge = Long.MAX_VALUE / 4;

        // ⚠️ UNEQUAL, so a product that wrapped cannot split evenly by symmetry.
        long[] shares = IndexCostLedger.shares(new long[] {huge, 2 * huge}, 3 * huge);

        assertThat(shares).containsExactly(333_333, 666_667);
    }

    @Test
    void aRequestWithNoWeightIsChargedWholeToUnattributedNeverDropped() {
        IndexCostLedger ledger = new IndexCostLedger();

        ledger.apportion(Charge.DATA_GET, Map.of());
        ledger.apportion(Charge.DATA_GET, Map.of(A, 0L));
        ledger.unattributed(Charge.DATA_GET);

        IndexCostLedger.Snapshot snapshot = ledger.snapshot();
        assertThat(snapshot.unattributed().get(Charge.DATA_GET))
                .isEqualTo(3 * IndexCostLedger.MICROS_PER_REQUEST);
        assertThat(snapshot.unattributed().get(Charge.DATA_PUT)).isZero();
        assertThat(micros(ledger, A, Charge.DATA_GET)).isZero();
        assertThat(snapshot.totalMicros(Charge.DATA_GET))
                .isEqualTo(3 * IndexCostLedger.MICROS_PER_REQUEST);
    }

    @Test
    void aNegativeWeightOrByteCountIsRefused() {
        IndexCostLedger ledger = new IndexCostLedger();

        assertThatThrownBy(() -> ledger.apportion(Charge.DATA_PUT, Map.of(A, -1L)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ledger.bytesWritten(A, -1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(ledger.snapshot().totalMicros(Charge.DATA_PUT)).isZero();
    }

    @Test
    void bytesWrittenAccumulatePerIndex() {
        IndexCostLedger ledger = new IndexCostLedger();

        ledger.bytesWritten(A, 10);
        ledger.bytesWritten(A, 0);
        ledger.bytesWritten(A, 5);
        ledger.bytesWritten(B, 7);

        assertThat(ledger.snapshot().indices())
                .anySatisfy(i -> {
                    assertThat(i.index()).isEqualTo(A);
                    assertThat(i.bytes()).isEqualTo(15);
                })
                .anySatisfy(i -> {
                    assertThat(i.index()).isEqualTo(B);
                    assertThat(i.bytes()).isEqualTo(7);
                });
    }
}
