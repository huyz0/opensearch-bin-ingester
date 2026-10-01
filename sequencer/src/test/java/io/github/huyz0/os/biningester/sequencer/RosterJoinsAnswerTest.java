// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.sequencer.FastLeaseFenceHolderTest.Mono;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * What a join writes and what an answered join leaves behind (M13.26d review
 * round 1, T1-T4, T6).
 */
class RosterJoinsAnswerTest {

    private static final Duration INTERVAL = Duration.ofMillis(250);

    private static MemoryBinStore startedAt7() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        new FastTermStart(store, "p").start(7, FastTermStartTest.SELF, Map.of(), 0,
                Optional.empty());
        return store;
    }

    @Test
    void aJOINERIsWrittenRosteredAndTheAnswerIsTheWrittenRoster() throws Exception {
        MemoryBinStore backing = startedAt7();
        RosterJoins joins = new RosterJoins(backing, "p", 7, new Mono(), INTERVAL);
        joins.request(FastTermStartTest.incarnation("a"));

        RosterJoins.Answered answered = (RosterJoins.Answered) joins.flush();

        Roster stored = FastTermStartTest.read(backing, 7);
        assertThat(stored.member("uid-a").orElseThrow().state())
                .as("admission counts only rostered members").isEqualTo(Roster.State.ROSTERED);
        assertThat(answered.roster()).isEqualTo(stored);
    }

    @Test
    void anANSWERWithoutAWriteLeavesNothingPendingAndAnIdleFlushCostsNothing()
            throws Exception {
        CountingBinStore store = new CountingBinStore(startedAt7());
        RosterJoins joins = new RosterJoins(store, "p", 7, new Mono(), INTERVAL);
        joins.request(FastTermStartTest.SELF);
        joins.flush();
        long requests = store.counts().total();

        for (int i = 0; i < 3; i++) {
            assertThat(joins.flush()).isEqualTo(new RosterJoins.Idle());
        }

        assertThat(store.counts().total()).as("a leader flushing on a timer reads nothing")
                .isEqualTo(requests);
    }

    @Test
    void aRETRANSMITTEDJoinedNeverReopensAClosedTerm() {
        JoinedTerms terms = new JoinedTerms();
        terms.joined(9, 7);

        terms.joined(9, 3);

        assertThat(terms.mayHold(7)).isFalse();
        terms.joined(7, 3);
        assertThat(terms.mayHold(7)).as("a closed term is never joined again").isFalse();
    }

    @Test
    void aZEROQuorumOrEpochIsRefused() {
        Roster r = FastTermStartTest.roster(7, -1, "x", false, 0, 0, false);

        assertThatThrownBy(() -> FastAdmission.admits(r, Set.of(), 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new JoinedTerms().joined(0, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
