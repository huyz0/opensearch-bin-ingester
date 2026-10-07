// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.PREFIX;
import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.counts;
import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.deltasCarrying;
import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.manager;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The drain's two remaining shapes (M13.73 review round 1, T1, T2): a batch
 * the term has applied WHOLE -- the common case, a leaseholder dead between
 * its commit and its deletes, the pod's next flush asking before it writes a
 * new intent -- and a batch whose earlier drain's append was ambiguous.
 */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class InboxDrainAllAppliedTest {

    private static CommitRequest flush(long seq) {
        return new CommitRequest("podx", "i1", seq, "seg/podx-" + seq, counts(2));
    }

    private static List<CommitRequest> flushes(long from, long through) {
        List<CommitRequest> out = new ArrayList<>();
        for (long seq = from; seq <= through; seq++) {
            out.add(flush(seq));
        }
        return out;
    }

    private static LocalSequencer leader(BinStore store) throws Exception {
        return LocalSequencer.start(store, PREFIX, manager(store, "poda"), 8,
                BoundedRecoveryFixture.noRenew()).orElseThrow();
    }

    @Test
    void aBATCHAppliedWholeIsDeletedAndNotCommittedAgain() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        try (LocalSequencer term = leader(store)) {
            for (CommitRequest intent : flushes(0, 4)) {
                Inbox.write(store, PREFIX, intent);
            }
            term.commitAll(flushes(0, 4));

            InboxDrain.drain(store, PREFIX, term);
        }
        assertThat(Inbox.pending(store, PREFIX)).as("every intent deleted").isEmpty();
        for (long seq = 0; seq <= 4; seq++) {
            assertThat(deltasCarrying(store, "seg/podx-" + seq))
                    .as("flush %d committed exactly once", seq).isEqualTo(1);
        }
    }

    @Test
    void aBATCHAppliedWholeIsDeletedThroughABatchingTerm() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        LocalSequencer local = leader(store);
        for (CommitRequest intent : flushes(0, 4)) {
            Inbox.write(store, PREFIX, intent);
        }
        local.commitAll(flushes(0, 4));
        try (BatchingSequencer term = new BatchingSequencer(local, Duration.ofMillis(5))) {
            InboxDrain.drain(store, PREFIX, term);
        }
        assertThat(Inbox.pending(store, PREFIX)).as("every intent deleted").isEmpty();
    }

    @Test
    void aBATCHWhoseEarlierAppendWasAmbiguousDrainsOnce() throws Exception {
        MemoryBinStore backing = new MemoryBinStore();
        AtomicBoolean armed = new AtomicBoolean();
        BinStore store = new AmbiguousPutStore(backing, AmbiguousPutStore.Mode.LANDED,
                AmbiguousPutStore.Target.PUT_IF_ABSENT, false, key -> armed.get());
        try (LocalSequencer term = leader(store)) {
            for (CommitRequest intent : flushes(0, 5)) {
                Inbox.write(backing, PREFIX, intent);
            }
            armed.set(true);
            assertThatThrownBy(() -> term.commitAll(flushes(0, 4)))
                    .as("the premise: an earlier drain's append landed, its answer lost")
                    .isInstanceOf(AmbiguousAppendException.class);

            InboxDrain.drain(store, PREFIX, term);
        }
        assertThat(Inbox.pending(backing, PREFIX)).as("every intent deleted").isEmpty();
        for (long seq = 0; seq <= 5; seq++) {
            assertThat(deltasCarrying(backing, "seg/podx-" + seq))
                    .as("flush %d committed exactly once", seq).isEqualTo(1);
        }
    }
}
