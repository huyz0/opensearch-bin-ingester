// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.Version;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.format.RunKey;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * A takeover's decisions written to the successor's roster (M13.33h; ADR-0081
 * §5 step 6): each decided stream's resume offset under the next decision
 * number, by a conditional write -- a newer fence deposes it, a change in
 * between is re-read, and a decision already written is not written twice.
 */
class RosterDecisionsTest {

    private static final RunKey S = new RunKey(new UUID(1, 1), 0);
    private static final RunKey T = new RunKey(new UUID(2, 2), 0);

    private static MemoryBinStore withRoster(List<Roster.Decision> decisions) throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        Roster r = FastTermStartTest.roster(3, 2, "self", false, 0, 0, false);
        FastTermStartTest.put(store, new Roster(r.epoch(), r.predecessor(), r.leader(),
                r.members(), r.termRecord(), decisions, r.notBefore(), r.fencedBy(), r.closed()));
        return store;
    }

    @Test
    void eachSTREAMsResumeOffsetTakesTheNextDecisionNumber() throws Exception {
        // pruned: a list of one, numbered 9 -- the next is 10, never the size
        MemoryBinStore store = withRoster(List.of(new Roster.Decision(9, S, 0)));

        RosterDecisions.Outcome got = new RosterDecisions(store, "p", 3)
                .record(Map.of(S, 20L, T, 5L));

        assertThat(got).isInstanceOf(RosterDecisions.Recorded.class);
        assertThat(FastTermStartTest.read(store, 3).decisions()).containsExactly(
                new Roster.Decision(9, S, 0), new Roster.Decision(10, S, 20),
                new Roster.Decision(11, T, 5));
    }

    @Test
    void aDECISIONAlreadyWrittenIsNotWrittenTwice() throws Exception {
        // a lost response retried: the first write landed
        MemoryBinStore store = withRoster(List.of());
        RosterDecisions decisions = new RosterDecisions(store, "p", 3);
        decisions.record(Map.of(S, 20L));

        decisions.record(Map.of(S, 20L));

        assertThat(FastTermStartTest.read(store, 3).decisions())
                .containsExactly(new Roster.Decision(0, S, 20));
    }

    @Test
    void aROSTERFencedByANewerTermDeposesAndNothingIsWritten() throws Exception {
        MemoryBinStore store = withRoster(List.of());
        FastTermStartTest.put(store, FastTermStartTest.read(store, 3).fencedBy(5));

        RosterDecisions.Outcome got = new RosterDecisions(store, "p", 3)
                .record(Map.of(S, 20L));

        assertThat(got).isEqualTo(new RosterDecisions.Deposed(5));
        assertThat(FastTermStartTest.read(store, 3).decisions()).isEmpty();
    }

    @Test
    void aCHANGEBetweenTheReadAndTheWriteIsReadAgainAndKept() throws Exception {
        MemoryBinStore store = withRoster(List.of());
        boolean[] raced = {false};
        io.github.huyz0.os.biningester.binstore.BinStore racing = new ForwardingBinStore(store) {
            @Override
            public Optional<Version> putIfMatch(String key, Body body, Version expected)
                    throws IOException {
                if (!raced[0]) {
                    raced[0] = true;
                    // a join lands in between: this write's version is stale
                    Roster r;
                    try (var in = store.get(key)) {
                        r = Roster.decode(in.readAllBytes());
                    }
                    List<Roster.Member> members = new java.util.ArrayList<>(r.members());
                    members.add(new Roster.Member(FastTermStartTest.incarnation("j"),
                            Roster.State.ROSTERED));
                    store.put(key, Body.ofBytes(new Roster(r.epoch(), r.predecessor(),
                            r.leader(), members, r.termRecord(), r.decisions(), r.notBefore(),
                            r.fencedBy(), r.closed()).encode()));
                }
                return store.putIfMatch(key, body, expected);
            }
        };

        new RosterDecisions(racing, "p", 3).record(Map.of(S, 20L));

        Roster after = FastTermStartTest.read(store, 3);
        assertThat(after.decisions()).containsExactly(new Roster.Decision(0, S, 20));
        assertThat(after.member("uid-j")).as("the join is kept").isPresent();
    }

    @Test
    void aMISSINGRosterIsAnError() {
        assertThatThrownBy(() -> new RosterDecisions(new MemoryBinStore(), "p", 3)
                .record(Map.of(S, 20L))).isInstanceOf(IOException.class);
    }
}
