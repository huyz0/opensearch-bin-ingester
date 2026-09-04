// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import static binjava.ingest.IngestTestSupport.appendOnce;
import static binjava.ingest.IngestTestSupport.docs;
import static binjava.ingest.IngestTestSupport.ingest;
import static org.assertj.core.api.Assertions.assertThat;

import binjava.binstore.CountingBinStore;
import binjava.binstore.ListPage;
import binjava.binstore.backend.MemoryBinStore;
import binjava.format.CommitDelta;
import binjava.sequencer.CommitRequest;
import binjava.sequencer.Sequencer;
import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * WHERE a commit lands, and WHAT identity it carries — the two things M4.6d
 * changes that nothing else in the suite can see.
 *
 * <p>⚠️ THESE EXIST BECAUSE THE HEADLINE CRITERION WAS UNPINNED. M4.6d's row
 * says the unleased epoch-0 chain "must GO, not merely be passed an epoch",
 * because a reader at epoch 1 never lists the epoch-0 prefix and so every record
 * committed there is acked and never becomes visible. Round-1 test review
 * MEASURED that nothing held it: it restored the fork in production — a
 * {@code CommitLog} at epoch 0 committed to instead of the sequencer, with the
 * {@code Sequencer} still constructed, held and closed — and the whole of
 * {@code :ingest:test} passed. The per-flush request count is identical either
 * way (one delta object), so no counting assertion could ever have caught it.
 *
 * <p>⚠️ THE KEY GRAMMAR IS SPELLED OUT HERE rather than obtained from
 * {@code CommitLog.logPrefix()}, which is package-private in another module
 * anyway. Asking the writer where it writes and then checking it wrote there
 * agrees with itself by construction; ADR-0022 makes key order the only ordering
 * a reader has, so the padded epoch in the path is a wire value and a test may
 * state it literally.
 */
class IngestCommitPathTest {

    /** ⚠️ Slot 0 per ADR-0007 (S = 1); the epoch is padded to 16 hex digits. */
    private static String chainPrefix(long epoch) {
        return String.format(java.util.Locale.ROOT, "%s/ctl/log/0/%016x/",
                IngestTestSupport.PREFIX, epoch);
    }

    @Test
    void everyCommitLandsUnderTheLeasedEpochAndNothingUnderTheReservedUnleasedChain()
            throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        try (DefaultIngest ingest = ingest(store)) {
            appendOnce(ingest, "logs", 0, 2);

            // ⚠️ THE RESERVED CHAIN IS EMPTY, and this half is the one that fails
            // against the fork. Epoch 0 is reserved for "no lease" (M4.4b); a pod
            // that commits there has forked the log invisibly, because the
            // records are acked and a reader of any leased epoch never looks.
            assertThat(keysUnder(store, chainPrefix(0)))
                    .as("nothing is written to the RESERVED unleased chain at epoch 0")
                    .isEmpty();

            // ⚠️ And the positive half, so the test cannot pass by the pod simply
            // never committing at all: the delta is under a LEASED epoch, which
            // `LeaseManager` starts at 1.
            List<String> leased = new ArrayList<>();
            for (long epoch = 1; epoch <= 4; epoch++) {
                leased.addAll(keysUnder(store, chainPrefix(epoch)));
            }
            assertThat(leased)
                    .as("and the commit IS there, under a leased epoch of at least 1")
                    .isNotEmpty();
        }
    }

    @Test
    void theCommitRequestCarriesThisPodAndAFlushSeqThatAdvancesOncePerFlush()
            throws Exception {
        // ⚠️ `(podId, flushSeq)` IS M4.10's idempotency key, and today nothing
        // downstream reads it: `CommitDelta` and `RunCommit` carry neither field,
        // and `FakeSequencer` documents that it ignores both until M4.10. So a
        // swapped argument, a constant, or a dropped increment is invisible
        // everywhere else -- round-1 review confirmed `new CommitRequest("mutant",
        // 0L, ...)` leaves the whole module green. A recording seam is the only
        // thing that can hold it, and holding it NOW is what stops M4.10
        // inheriting a key that was never right.
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        Recording recorder = new Recording(IngestTestSupport.sequencer(store, "pod7"));
        try (DefaultIngest ingest = new DefaultIngest(
                IngestTestSupport.pinnedIntervalConfig(IngestTestSupport.NEVER, 8L << 20),
                store, IngestTestSupport.PREFIX, "pod7", recorder, new SubscriptionHub(),
                Clock.systemUTC(), index -> IngestTestSupport.LOGS)) {
            appendOnce(ingest, "logs", 0, 2);
            appendOnce(ingest, "logs", 0, 3);
        }

        assertThat(recorder.seen).hasSizeGreaterThanOrEqualTo(2);
        assertThat(recorder.seen).allSatisfy(r -> assertThat(r.podId())
                .as("the request names THIS pod, not the fixture default")
                .isEqualTo("pod7"));
        // ⚠️ FROM 0, AND BY EXACTLY ONE. "Monotonic" alone would survive a field
        // that jumped, and "positive" would survive a constant.
        List<Long> seqs = recorder.seen.stream().map(CommitRequest::flushSeq).toList();
        assertThat(seqs.subList(0, 2))
                .as("flushSeq starts at 0 and advances by exactly one per flush")
                .containsExactly(0L, 1L);
    }

    private static List<String> keysUnder(CountingBinStore store, String prefix)
            throws IOException {
        List<String> keys = new ArrayList<>();
        String after = null;
        while (true) {
            ListPage page = store.list(prefix, after, 1000);
            page.objects().forEach(o -> keys.add(o.key()));
            if (page.nextStartAfter().isEmpty()) {
                return keys;
            }
            after = page.nextStartAfter().get();
        }
    }

    /**
     * ⚠️ A RECORDING DECORATOR over the real thing, not a hand-written stub: the
     * offsets a stub returned would be its own invention, and every other
     * assertion in this class depends on the commit actually happening.
     */
    private static final class Recording implements Sequencer {
        private final List<CommitRequest> seen = new ArrayList<>();
        private final Sequencer delegate;

        Recording(Sequencer delegate) {
            this.delegate = delegate;
        }

        @Override
        public CommitDelta commit(CommitRequest request) throws IOException {
            seen.add(request);
            return delegate.commit(request);
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
}
