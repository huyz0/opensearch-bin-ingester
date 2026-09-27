// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.PREFIX;
import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.counts;
import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.deltasCarrying;
import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.manager;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.CostGovernor;
import io.github.huyz0.os.biningester.binstore.GovernorRefusedException;
import io.github.huyz0.os.biningester.binstore.GoverningBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The four recovery LIST callers declare their scope, so a takeover recovers
 * with the governor's LIST bucket EMPTY (M10.11, ADR-0075, criterion 14).
 *
 * <p>⚠️ **THE BUCKET IS DRAINED AND ITS CLOCK IS FROZEN**, so no token refills
 * while a case runs: every LIST that reaches the governor undeclared is
 * refused. Each case first proves that with an undeclared LIST of its own --
 * a governor that admitted everything would make every case here green.
 *
 * <p>⚠️ **TWO OF THE FOUR RUN ON THEIR OWN VIRTUAL THREAD**, and a
 * {@code ScopedValue} does not cross a thread boundary: a scope bound around
 * {@code Thread.start} rather than inside the thread's body leaves the LIST
 * undeclared. So the backfill and the drain are driven through their
 * {@code inBackground} entry points and joined, never called inline.
 */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class GovernedRecoveryTest {

    private final MemoryBinStore raw = new MemoryBinStore();
    private final CostGovernor governor = drained();
    private final GoverningBinStore governed = new GoverningBinStore(raw, governor);

    private static CostGovernor drained() {
        CostGovernor governor = new CostGovernor(
                new CostGovernor.Settings(1.0, 1, Duration.ofMinutes(1), 1 << 20),
                Clock.fixed(Instant.parse("2026-09-27T00:00:00Z"), ZoneOffset.UTC), () -> 250);
        // ⚠️ THE ONE BURST TOKEN, spent as an undeclared LIST would spend it --
        // and no further ask, so the only refusal counted is the premise's.
        if (!governor.admitList()) {
            throw new AssertionError("a fresh bucket holds its burst");
        }
        return governor;
    }

    private void assertTheBucketIsEmpty() {
        assertThatThrownBy(() -> governed.list(PREFIX + "/ctl/log/", null, 1))
                .as("the premise: an UNDECLARED LIST, whatever its prefix, is refused")
                .isInstanceOf(GovernorRefusedException.class);
    }

    private static LocalSequencer term(MemoryBinStore store, String pod) throws Exception {
        return LocalSequencer.start(store, PREFIX, manager(store, pod), 8).orElseThrow();
    }

    @Test
    void aTAKEOVERsChainEndAndReplayRECOVERWithTheLISTBucketEMPTY() throws Exception {
        LocalSequencer first = term(raw, "poda");
        try {
            first.commit(new CommitRequest("poda", "i1", 0, "seg/first", counts(3)));
        } finally {
            first.close();
        }
        assertTheBucketIsEmpty();

        // ⚠️ THE SUCCESSOR RECOVERS THROUGH THE GOVERNED STORE: it finds the
        // predecessor's chain end (ChainEnd.of) to seal it, and replays what it
        // inherits (ChainReplay) -- a LIST each, and neither may be refused.
        LocalSequencer successor = LocalSequencer.start(governed, PREFIX,
                manager(raw, "podb"), 8).orElseThrow();
        try {
            successor.commit(new CommitRequest("podb", "i1", 0, "seg/second", counts(1)));
        } finally {
            successor.close();
        }

        assertThat(governor.counts().listRefusals())
                .as("only the premise's own undeclared LIST was refused")
                .isEqualTo(1);
        assertThat(governor.counts().recoveryLists())
                .as("⚠️ THE RECOVERY LISTS WERE ADMITTED AS DECLARED, not merely unrefused")
                .isPositive();
        assertThat(deltasCarrying(raw, "seg/second"))
                .as("and the recovered term commits").isEqualTo(1);
    }

    @Test
    void theTAKEOVERsInboxDrainOnITSOWNThreadDrainsWithTheBucketEMPTY() throws Exception {
        LocalSequencer term = term(raw, "poda");
        try {
            Inbox.write(raw, PREFIX, new CommitRequest("podz", "i9", 0, "seg/deferred",
                    counts(2)));
            assertTheBucketIsEmpty();

            Thread drain = InboxDrain.inBackground(governed, PREFIX, term);
            assertThat(drain.join(Duration.ofSeconds(30))).as("the drain finishes").isTrue();
        } finally {
            term.close();
        }

        assertThat(deltasCarrying(raw, "seg/deferred"))
                .as("⚠️ THE ACKED INTENT IS COMMITTED: a refused drain LIST strands it")
                .isEqualTo(1);
        assertThat(raw.list(Inbox.prefixFor(PREFIX), null, 10).objects())
                .as("and deleted from the inbox").isEmpty();
        assertThat(governor.counts().listRefusals())
                .as("only the premise's own undeclared LIST was refused").isEqualTo(1);
    }

    @Test
    void theTAKEOVERsBackfillOnITSOWNThreadReachesTheFLOORWithTheBucketEMPTY()
            throws Exception {
        LocalSequencer first = term(raw, "poda");
        try {
            for (long i = 0; i < LocalSequencer.CHECKPOINT_EVERY_DELTAS + 5; i++) {
                first.commit(new CommitRequest("poda", "i1", i, "seg/" + i, counts(1)));
            }
        } finally {
            first.close();
        }
        LocalSequencer successor = term(raw, "podb");
        try {
            assertThat(successor.chain().snapshot().fromFloor())
                    .as("the premise: the successor's replay began at a checkpoint")
                    .isFalse();
            assertTheBucketIsEmpty();

            Thread backfill = ChainBackfill.inBackground(governed, PREFIX, successor.chain(),
                    successor::serving);
            assertThat(backfill.join(Duration.ofSeconds(30))).as("the backfill finishes")
                    .isTrue();

            assertThat(successor.chain().snapshot().fromFloor())
                    .as("⚠️ THE CHAIN REACHES THE FLOOR: a refused backfill LIST is logged "
                            + "and leaves it short, the orphan sweep kept out for the term")
                    .isTrue();
        } finally {
            successor.close();
        }
        assertThat(governor.counts().listRefusals())
                .as("only the premise's own undeclared LIST was refused").isEqualTo(1);
    }
}
