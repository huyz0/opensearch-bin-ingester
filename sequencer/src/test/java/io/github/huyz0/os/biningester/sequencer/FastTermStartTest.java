// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.Roster;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * A new leader's walk, fences, roster and {@code LATEST} (ADR-0081 §5 steps
 * 2-3; M13.26b).
 */
class FastTermStartTest {

    static final UUID INDEX = new UUID(1, 1);
    static final Roster.Incarnation SELF = incarnation("self");

    static Roster.Incarnation incarnation(String name) {
        return new Roster.Incarnation(name, "uid-" + name, "az-" + name, "");
    }

    static Roster roster(long epoch, long predecessor, String leader, boolean departed,
            long notBefore, long fencedBy, boolean closed) {
        Roster.Incarnation l = incarnation(leader);
        return new Roster(epoch, predecessor, l,
                List.of(new Roster.Member(l, departed ? Roster.State.DEPARTED
                        : Roster.State.ROSTERED)),
                List.of(new Roster.TermRecord(0, new TreeMap<>())), List.of(), notBefore, fencedBy,
                closed);
    }

    static void put(MemoryBinStore store, Roster r) throws Exception {
        store.put(Roster.key("p", r.epoch()), Body.ofBytes(r.encode()));
    }

    static void latest(MemoryBinStore store, long epoch) throws Exception {
        store.put(Roster.latestKey("p"), Body.ofBytes(Roster.encodeLatest(epoch)));
    }

    static Roster read(MemoryBinStore store, long epoch) throws Exception {
        try (InputStream in = store.get(Roster.key("p", epoch))) {
            return Roster.decode(in.readAllBytes());
        }
    }

    static long readLatest(MemoryBinStore store) throws Exception {
        try (InputStream in = store.get(Roster.latestKey("p"))) {
            return Roster.decodeLatest(in.readAllBytes());
        }
    }

    static FastTermStart.Outcome start(MemoryBinStore store, long epoch, long leaseNotBefore,
            Optional<String> replaced) throws Exception {
        return new FastTermStart(store, "p").start(epoch, SELF, Map.of(INDEX, 2), leaseNotBefore,
                replaced);
    }

    @Test
    void theFIRSTTermCreatesItsRosterAndLatest() throws Exception {
        MemoryBinStore store = new MemoryBinStore();

        FastTermStart.Outcome outcome = start(store, 1, Long.MIN_VALUE, Optional.empty());

        assertThat(outcome).isInstanceOf(FastTermStart.Started.class);
        Roster own = read(store, 1);
        assertThat(own.predecessor()).isEqualTo(-1);
        assertThat(own.leader()).isEqualTo(SELF);
        assertThat(own.members()).containsExactly(new Roster.Member(SELF, Roster.State.ROSTERED));
        assertThat(own.termRecord().get(0).walQuorum()).isEqualTo(Map.of(INDEX, 2));
        assertThat(own.notBefore()).isZero();
        assertThat(readLatest(store)).isEqualTo(1);
    }

    @Test
    void everyUNCLOSEDTermBackToTheFirstClosedIsFenced() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        put(store, roster(2, -1, "a", false, 100, 0, true));
        put(store, roster(3, 2, "b", false, 300, 0, false));
        put(store, roster(5, 3, "c", false, 200, 6, false));
        latest(store, 5);

        FastTermStart.Started started = (FastTermStart.Started) start(store, 7, 250,
                Optional.of("uid-c"));

        assertThat(read(store, 5).fencedBy()).isEqualTo(7);
        assertThat(read(store, 3).fencedBy()).isEqualTo(7);
        assertThat(read(store, 2).fencedBy()).as("a closed term is not walked past").isZero();
        assertThat(started.unclosed()).extracting(Roster::epoch).containsExactly(5L, 3L);
        assertThat(read(store, 7).predecessor()).isEqualTo(5);
        assertThat(started.notBefore()).as("the latest of the lease's and every walked roster's")
                .isEqualTo(300);
        assertThat(read(store, 7).notBefore()).isEqualTo(300);
        assertThat(started.handedOver()).as("c did not depart").isFalse();
        assertThat(readLatest(store)).isEqualTo(7);
    }

    @Test
    void aNEWERLatestDeposesWithoutAWrite() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        put(store, roster(9, -1, "a", false, 0, 0, false));
        latest(store, 9);

        FastTermStart.Outcome outcome = start(store, 7, 0, Optional.empty());

        assertThat(outcome).isEqualTo(new FastTermStart.Deposed(9));
        assertThat(store.stat(Roster.key("p", 7))).isEmpty();
        assertThat(readLatest(store)).isEqualTo(9);
    }

    @Test
    void aWALKEDRosterFencedByANewerTermDeposes() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        put(store, roster(3, -1, "a", false, 0, 0, false));
        put(store, roster(5, 3, "b", false, 0, 8, false));
        latest(store, 5);

        FastTermStart.Outcome outcome = start(store, 7, 0, Optional.empty());

        assertThat(outcome).isEqualTo(new FastTermStart.Deposed(8));
        assertThat(read(store, 3).fencedBy()).as("nothing is written once deposed").isZero();
        assertThat(store.stat(Roster.key("p", 7))).isEmpty();
    }

    @Test
    void aGRACEFULHandoverIsExemptFromTheLeasePart() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        put(store, roster(5, -1, "c", true, 120, 0, false));
        latest(store, 5);

        FastTermStart.Started started = (FastTermStart.Started) start(store, 7, 500,
                Optional.of("uid-c"));

        assertThat(started.handedOver()).isTrue();
        assertThat(started.notBefore()).as("the inherited part only").isEqualTo(120);
        assertThat(started.mayAssign(120, false)).isTrue();
        assertThat(started.mayAssign(119, true)).isFalse();
    }

    @Test
    void anUNDEPARTEDEarlierLeaderVoidsTheExemption() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        put(store, roster(3, -1, "b", false, 0, 0, false));
        put(store, roster(5, 3, "c", true, 120, 0, false));
        latest(store, 5);

        FastTermStart.Started started = (FastTermStart.Started) start(store, 7, 500,
                Optional.of("uid-c"));

        assertThat(started.handedOver()).as("term 3's leader b never departed").isFalse();
        assertThat(started.notBefore()).isEqualTo(500);
        assertThat(started.mayAssign(600, false)).isFalse();
        assertThat(started.mayAssign(600, true)).isTrue();
    }

    @Test
    void aSTARTWhoseLatestLandedEarlierIsFinishedNotRepeated() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        put(store, roster(5, -1, "c", false, 0, 0, false));
        latest(store, 5);
        start(store, 7, 0, Optional.empty());
        var before = store.stat(Roster.key("p", 7)).orElseThrow().version();

        FastTermStart.Outcome again = start(store, 7, 0, Optional.empty());

        assertThat(again).isInstanceOf(FastTermStart.Started.class);
        assertThat(((FastTermStart.Started) again).unclosed()).extracting(Roster::epoch)
                .containsExactly(5L);
        assertThat(store.stat(Roster.key("p", 7)).orElseThrow().version())
                .as("an unchanged roster is not rewritten").isEqualTo(before);
        assertThat(readLatest(store)).isEqualTo(7);
    }
}
