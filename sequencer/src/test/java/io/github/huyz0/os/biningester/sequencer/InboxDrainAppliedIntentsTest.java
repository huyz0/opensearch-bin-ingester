// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.PREFIX;
import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.counts;
import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.deltasCarrying;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Intents whose flushes already landed are deleted by the next drain, not
 * stranded (M13.73): a drain that commits a pod's batch and dies before its
 * deletes -- a leaseholder killed mid-drain -- leaves intents the window has
 * applied, and the window can answer a replay only from the delta of the
 * incarnation's LAST applied flush. Asked to answer the older ones, it refused
 * the whole batch for good: acked writes behind it never drained, and every
 * deferring flush re-read every intent.
 */
class InboxDrainAppliedIntentsTest {

    private static CommitRequest flush(long seq) {
        return new CommitRequest("podx", "i1", seq, "seg/podx-" + seq, counts(2));
    }

    private static LocalSequencer term(MemoryBinStore store, String pod) throws Exception {
        LeaseManager manager = new LeaseManager(store, new LeaseConfig(PREFIX, pod,
                pod + ":9000", Duration.ofSeconds(10), Duration.ofSeconds(3)),
                Clock.systemUTC());
        return LocalSequencer.start(store, PREFIX, manager, 8,
                BoundedRecoveryFixture.noRenew()).orElseThrow();
    }

    /** Intents 0-10 in the inbox; 0-4 and 5-9 committed in two deltas, as a dead drain left them. */
    private static void appliedButNotDeleted(MemoryBinStore store, LocalSequencer term)
            throws Exception {
        for (long seq = 0; seq <= 10; seq++) {
            Inbox.write(store, PREFIX, flush(seq));
        }
        List<CommitRequest> first = new ArrayList<>();
        List<CommitRequest> second = new ArrayList<>();
        for (long seq = 0; seq < 10; seq++) {
            (seq < 5 ? first : second).add(flush(seq));
        }
        term.commitAll(first);
        term.commitAll(second);
    }

    @Test
    void aDRAINDeletesIntentsItsTermAlreadyAppliedAndCommitsTheRestOnce() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        try (LocalSequencer term = term(store, "poda")) {
            appliedButNotDeleted(store, term);

            InboxDrain.drain(store, PREFIX, term);
        }

        assertThat(Inbox.pending(store, PREFIX)).as("every intent deleted").isEmpty();
        for (long seq = 0; seq <= 10; seq++) {
            assertThat(deltasCarrying(store, "seg/podx-" + seq))
                    .as("flush %d committed exactly once", seq).isEqualTo(1);
        }
    }

    @Test
    void aSUCCESSORsDrainDeletesThemToo() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        try (LocalSequencer dying = term(store, "poda")) {
            appliedButNotDeleted(store, dying);
        }

        try (LocalSequencer successor = term(store, "podb")) {
            InboxDrain.drain(store, PREFIX, successor);
        }

        assertThat(Inbox.pending(store, PREFIX)).as("every intent deleted").isEmpty();
        for (long seq = 0; seq <= 10; seq++) {
            assertThat(deltasCarrying(store, "seg/podx-" + seq))
                    .as("flush %d committed exactly once", seq).isEqualTo(1);
        }
    }
}
