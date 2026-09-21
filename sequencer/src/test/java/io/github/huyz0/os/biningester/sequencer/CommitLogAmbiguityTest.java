// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.RunKey;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * An append whose outcome was lost says WHERE it was attempted (M5.23).
 *
 * <p>⚠️ THE LOG DOES NOT DECIDE WHAT THE AMBIGUITY MEANS, and that separation
 * is the row. Reconciling needs the idempotency window, which belongs to the
 * SEQUENCER; the log's job is to stop throwing away the one fact that makes
 * reconciliation possible at all -- the {@code (epoch, sequence)} of the slot
 * it wrote to. Before this row a lost response arrived as a bare
 * {@code IOException} naming no slot, so nothing downstream could ask the chain
 * what had happened.
 *
 * <p>⚠️ BOTH OUTCOMES REPORT THE SAME WAY, deliberately: a write that LANDED
 * and one the store never saw are indistinguishable to the writer, and a type
 * that claimed either would be inventing the fact the caller needs.
 */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class CommitLogAmbiguityTest {

    private static final UUID A = UUID.fromString("00000000-0000-0000-0000-0000000000aa");
    private static final RunKey RA = new RunKey(A, 0);
    private static final String PREFIX = "p";

    private static Map<RunKey, Integer> counts(int n) {
        Map<RunKey, Integer> m = new LinkedHashMap<>();
        m.put(RA, n);
        return m;
    }

    private static CommitRequest flush(long flushSeq, String segmentKey, int n) {
        return new CommitRequest("poda", "i1", flushSeq, segmentKey, counts(n));
    }

    /** How many objects the chain holds, whatever they decode to. */
    private static int objectsIn(BinStore store, CommitLog log) throws Exception {
        return store.list(log.logPrefix(), null, 1000).objects().size();
    }

    @Test
    void anAppendThatLANDEDBeforeItsResponseWasLostNamesTheSlotItWroteTo() throws Exception {
        MemoryBinStore backing = new MemoryBinStore();
        // ⚠️ LANDED is the dangerous half: the write took effect and only the
        // response was lost, so the records ARE committed and a caller that
        // treats this as a clean failure commits them again.
        BinStore store = new AmbiguousPutStore(backing, AmbiguousPutStore.Mode.LANDED,
                AmbiguousPutStore.Target.PUT_IF_ABSENT);
        CommitLog log = new CommitLog(store, PREFIX, 7);

        assertThatThrownBy(() -> log.commitAll(List.of(flush(0, "seg/0", 3))))
                .isInstanceOfSatisfying(AmbiguousAppendException.class, ambiguous -> {
                    assertThat(ambiguous.epoch())
                            .as("the chain it was attempted in")
                            .isEqualTo(7);
                    assertThat(ambiguous.sequence())
                            .as("the ONE slot that either holds this append or does not")
                            .isZero();
                    // ⚠️ THE STORE FAILURE IS KEPT. Without it an operator sees
                    // "may or may not have landed" and nothing saying whether
                    // the store timed out, reset the connection or returned
                    // 503 -- three different operational stories.
                    assertThat(ambiguous.getCause())
                            .as("why the outcome was lost, not merely that it was")
                            .hasMessageContaining("response lost");
                });

        assertThat(objectsIn(store, log))
                .as("it DID land -- which is what makes an ambiguous append dangerous")
                .isEqualTo(1);
    }

    @Test
    void anAppendTheStoreNEVERSAWReportsTheSameWay() throws Exception {
        MemoryBinStore backing = new MemoryBinStore();
        BinStore store = new AmbiguousPutStore(backing, AmbiguousPutStore.Mode.LOST,
                AmbiguousPutStore.Target.PUT_IF_ABSENT);
        CommitLog log = new CommitLog(store, PREFIX, 7);

        assertThatThrownBy(() -> log.commitAll(List.of(flush(0, "seg/0", 3))))
                .isInstanceOfSatisfying(AmbiguousAppendException.class, ambiguous ->
                        assertThat(ambiguous.sequence()).isZero());

        // ⚠️ THE SAME TYPE AND THE SAME SLOT AS THE LANDED CASE, and that is the
        // point rather than a duplicate test: the writer cannot tell these two
        // apart, so a type that reported them differently would be inventing
        // the fact its caller has to go to the chain for.
        assertThat(objectsIn(store, log))
                .as("nothing landed -- and the exception said nothing either way")
                .isZero();
    }
}
