// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.PREFIX;
import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.counts;
import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.manager;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.RunKey;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * No fast assignment over an ambiguous default-path commit (ADR-0081 §8;
 * M13.34c): a fast cursor opens at the chain's committed next offset, read
 * only once an append whose answer was lost is reconciled -- else it opens
 * under records that landed, and assigns their offsets again.
 */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class FastCursorAfterReconcileTest {

    private static final RunKey S = counts(3).keySet().iterator().next();

    private static LocalSequencer leaderOver(BinStore store) throws Exception {
        return LocalSequencer.start(store, PREFIX, manager(store, "poda"), 8,
                BoundedRecoveryFixture.noRenew()).orElseThrow();
    }

    @Test
    void aDEFAULTCommitThatLandedUnansweredMovesTheOffsetAFastCursorOpensAt()
            throws Exception {
        AtomicBoolean armed = new AtomicBoolean();
        BinStore store = new AmbiguousPutStore(new MemoryBinStore(),
                AmbiguousPutStore.Mode.LANDED, AmbiguousPutStore.Target.PUT_IF_ABSENT, false,
                key -> armed.get());
        LocalSequencer sequencer = leaderOver(store);
        try {
            armed.set(true);
            assertThatThrownBy(() -> sequencer.commitAll(List.of(
                    new CommitRequest("poda", "i1", 0, "seg/0", counts(3)))))
                    .isInstanceOf(AmbiguousAppendException.class);

            assertThat(sequencer.committedNext(S)).as("past the 3 records that landed")
                    .isEqualTo(3);
        } finally {
            sequencer.close();
        }
    }

    @Test
    void anAPPENDThatCannotYetBeReconciledOpensNoCursorUntilItIs() throws Exception {
        AtomicBoolean armed = new AtomicBoolean();
        BinStore store = new AmbiguousPutStore(new MemoryBinStore(),
                AmbiguousPutStore.Mode.LANDED, AmbiguousPutStore.Target.PUT_IF_ABSENT, true,
                key -> armed.get());
        LocalSequencer sequencer = leaderOver(store);
        try {
            armed.set(true);
            assertThatThrownBy(() -> sequencer.commitAll(List.of(
                    new CommitRequest("poda", "i1", 0, "seg/0", counts(3)))))
                    .isInstanceOf(AmbiguousAppendException.class);

            assertThatThrownBy(() -> sequencer.committedNext(S)).as("the read refused")
                    .isInstanceOf(java.io.UncheckedIOException.class)
                    .hasCauseInstanceOf(java.io.IOException.class);
            assertThat(sequencer.committedNext(S)).as("the mark kept, reconciled next time")
                    .isEqualTo(3);
        } finally {
            sequencer.close();
        }
    }
}
