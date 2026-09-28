// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.IndexCostLedger;
import io.github.huyz0.os.biningester.binstore.IndexCostLedger.Charge;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.CatchUpRequestFrame;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentReader;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.format.SegmentWriter;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * A catch-up responder's segment GET is charged to the indices of the
 * segment it read, whole bytes in hand (M11.3, ADR-0077 decision 3): it GETs
 * data segments outside {@code SegmentProxy}, and left out it would break the
 * read-side sum.
 */
class CatchUpAttributionTest {

    private static final String KEY = "bins/c/data/catch-up.bseg";
    private static final UUID REQUEST = UUID.fromString("11111111-2222-3333-4444-555555555555");
    private static final RunKey HEAVY = new RunKey(
            UUID.fromString("00000000-0000-4000-8000-0000000000aa"), 0);
    private static final RunKey LIGHT = new RunKey(
            UUID.fromString("00000000-0000-4000-8000-0000000000bb"), 0);

    private static byte[] segment() throws Exception {
        SegmentWriter writer = new SegmentWriter();
        writer.add(HEAVY, new SegmentRecord("h", OpType.INDEX, OptionalLong.empty(),
                new byte[3_000]), 0);
        writer.add(LIGHT, new SegmentRecord("l", OpType.INDEX, OptionalLong.empty(),
                new byte[30]), 0);
        return writer.toByteArray(0);
    }

    private static CommittedDeltaSource oneSegment() {
        List<CommittedDeltaSource.CommittedRun> runs = List.of(
                new CommittedDeltaSource.CommittedRun(HEAVY, KEY, 1, 41),
                new CommittedDeltaSource.CommittedRun(LIGHT, KEY, 1, 52));
        return new CommittedDeltaSource() {
            @Override
            public List<CommittedRun> replay(RunKey key, long offset, int limit) {
                return runs.stream().filter(r -> r.key().equals(key)).toList();
            }

            @Override
            public SegmentReplayCursor openSegments(List<ReplayRequest> requests) {
                return new SegmentReplayCursor() {
                    private boolean emitted;

                    @Override
                    public Optional<ReplaySegment> next() {
                        if (emitted) {
                            return Optional.empty();
                        }
                        emitted = true;
                        return Optional.of(new ReplaySegment(KEY, runs));
                    }
                };
            }
        };
    }

    private static CatchUpRequestFrame request() {
        return new CatchUpRequestFrame(REQUEST, List.of(
                new CatchUpRequestFrame.Stream(HEAVY, 0),
                new CatchUpRequestFrame.Stream(LIGHT, 0)));
    }

    private static long micros(IndexCostLedger ledger, UUID index) {
        return ledger.snapshot().indices().stream().filter(i -> i.index().equals(index))
                .mapToLong(i -> i.micros().get(Charge.DATA_GET)).sum();
    }

    @Test
    void aCatchUpReadIsSplitByTheDirectoryOfTheBytesItRead() throws Exception {
        byte[] segment = segment();
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        store.put(KEY, Body.ofBytes(segment));
        IndexCostLedger ledger = new IndexCostLedger();

        new DurableCatchUpResponder(store, oneSegment(), () -> 9, ledger).respond(request());

        IndexCostLedger expected = new IndexCostLedger();
        expected.apportion(Charge.DATA_GET,
                SegmentCharges.runBytes(SegmentReader.open(segment).directory()));
        assertThat(store.dataSegmentGets()).as("the premise: one GET for two streams")
                .isEqualTo(1);
        assertThat(micros(ledger, HEAVY.indexId()))
                .as("⚠️ SPLIT BY RUN BYTES, charged once for the two streams it served")
                .isEqualTo(micros(expected, HEAVY.indexId()))
                .isGreaterThan(900_000);
        assertThat(ledger.snapshot().totalMicros(Charge.DATA_GET))
                .isEqualTo(store.dataSegmentGets() * IndexCostLedger.MICROS_PER_REQUEST);
    }

    @Test
    void aReadOverItsBudgetWasStillBilledAndIsChargedToUnattributed() throws Exception {
        byte[] segment = segment();
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        store.put(KEY, Body.ofBytes(segment));
        IndexCostLedger ledger = new IndexCostLedger();

        assertThatThrownBy(() -> new DurableCatchUpResponder(store, oneSegment(), () -> 9,
                segment.length / 2, ledger).respond(request()))
                .isInstanceOf(DurableCatchUpResponder.ResponseTooLargeException.class);

        assertThat(store.dataSegmentGets()).as("the premise: the GET was issued").isEqualTo(1);
        assertThat(ledger.snapshot().unattributed().get(Charge.DATA_GET))
                .as("⚠️ NEVER DROPPED: a refused read was still a request")
                .isEqualTo(IndexCostLedger.MICROS_PER_REQUEST);
        assertThat(ledger.snapshot().totalMicros(Charge.DATA_GET))
                .isEqualTo(store.dataSegmentGets() * IndexCostLedger.MICROS_PER_REQUEST);
    }
}
