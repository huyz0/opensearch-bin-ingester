// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.sequencer.FastLeaseFenceHolderTest.Mono;
import io.github.huyz0.os.biningester.sequencer.FastTermStartRaceTest.HookedStore;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * The leader's JOINs (ADR-0081 §1; M13.26d): appended in one roster write per
 * batch, at most one per {@code min_upload_interval}, answered once durable.
 */
class RosterJoinsTest {

    private static final Duration INTERVAL = Duration.ofMillis(250);

    private static MemoryBinStore startedAt7() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        new FastTermStart(store, "p").start(7, FastTermStartTest.SELF, Map.of(), 0,
                Optional.empty());
        return store;
    }

    private static Roster.Incarnation pod(String name) {
        return FastTermStartTest.incarnation(name);
    }

    @Test
    void PENDINGJoinsAreAppendedInOneWrite() throws Exception {
        MemoryBinStore backing = startedAt7();
        CountingBinStore store = new CountingBinStore(backing);
        RosterJoins joins = new RosterJoins(store, "p", 7, new Mono(), INTERVAL);
        joins.request(pod("a"));
        joins.request(pod("b"));
        joins.request(pod("c"));

        RosterJoins.Answered answered = (RosterJoins.Answered) joins.flush();

        assertThat(answered.joined()).containsExactly(pod("a"), pod("b"), pod("c"));
        assertThat(store.counts().puts()).as("one roster write for three joins").isEqualTo(1);
        assertThat(FastTermStartTest.read(backing, 7).members()).hasSize(4);
        assertThat(joins.flush()).as("nothing pending").isEqualTo(new RosterJoins.Idle());
    }

    @Test
    void aSECONDBatchWaitsOutTheIntervalWithoutARequest() throws Exception {
        CountingBinStore store = new CountingBinStore(startedAt7());
        Mono mono = new Mono();
        RosterJoins joins = new RosterJoins(store, "p", 7, mono, INTERVAL);
        joins.request(pod("a"));
        joins.flush();
        long requests = store.counts().total();
        joins.request(pod("b"));
        mono.advance(Duration.ofMillis(249));

        assertThat(joins.flush()).isInstanceOf(RosterJoins.NotDue.class);
        assertThat(store.counts().total()).as("a flush not yet due costs no request")
                .isEqualTo(requests);
        mono.advance(Duration.ofMillis(1));

        assertThat(((RosterJoins.Answered) joins.flush()).joined()).containsExactly(pod("b"));
    }

    @Test
    void anALREADYRosteredPodIsAnsweredWithoutAWrite() throws Exception {
        CountingBinStore store = new CountingBinStore(startedAt7());
        RosterJoins joins = new RosterJoins(store, "p", 7, new Mono(), INTERVAL);
        joins.request(FastTermStartTest.SELF);

        RosterJoins.Answered answered = (RosterJoins.Answered) joins.flush();

        assertThat(answered.joined()).containsExactly(FastTermStartTest.SELF);
        assertThat(store.counts().puts()).isZero();
    }

    @Test
    void aDEPARTEDIncarnationIsRefused() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        Roster.Incarnation gone = pod("gone");
        FastTermStartTest.put(store, new Roster(7, -1, FastTermStartTest.SELF,
                List.of(new Roster.Member(FastTermStartTest.SELF, Roster.State.ROSTERED),
                        new Roster.Member(gone, Roster.State.DEPARTED)),
                FastTermStartTest.roster(7, -1, "self", false, 0, 0, false).termRecord(),
                List.of(), 0, 0, false));
        RosterJoins joins = new RosterJoins(store, "p", 7, new Mono(), INTERVAL);
        joins.request(gone);

        RosterJoins.Answered answered = (RosterJoins.Answered) joins.flush();

        assertThat(answered.refused()).containsExactly(gone);
        assertThat(answered.joined()).isEmpty();
    }

    @Test
    void aFENCEDRosterDeposesTheLeaderWithoutAWrite() throws Exception {
        MemoryBinStore backing = startedAt7();
        FastTermStartTest.put(backing, FastTermStartTest.read(backing, 7).fencedBy(9));
        CountingBinStore store = new CountingBinStore(backing);
        RosterJoins joins = new RosterJoins(store, "p", 7, new Mono(), INTERVAL);
        joins.request(pod("a"));

        assertThat(joins.flush()).isEqualTo(new RosterJoins.Deposed(9));
        assertThat(store.counts().puts()).isZero();
    }

    @Test
    void aWRITERefusedIsReReadAndKeepsWhatLandedFirst() throws Exception {
        MemoryBinStore backing = startedAt7();
        Roster.Incarnation earlier = pod("earlier");
        HookedStore store = new HookedStore(backing, Roster.key("p", 7), b -> {
            Roster r = FastTermStartTest.read(b, 7);
            List<Roster.Member> members = new ArrayList<>(r.members());
            members.add(new Roster.Member(earlier, Roster.State.ROSTERED));
            FastTermStartTest.put(b, new Roster(7, r.predecessor(), r.leader(), members,
                    r.termRecord(), r.decisions(), r.notBefore(), r.fencedBy(), r.closed()));
        });
        RosterJoins joins = new RosterJoins(store, "p", 7, new Mono(), INTERVAL);
        joins.request(pod("a"));

        joins.flush();

        assertThat(FastTermStartTest.read(backing, 7).member("uid-earlier")).isPresent();
        assertThat(FastTermStartTest.read(backing, 7).member("uid-a")).isPresent();
    }
}
