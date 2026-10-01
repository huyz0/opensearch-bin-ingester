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
import java.util.OptionalInt;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The term record (ADR-0081 §4; M13.26e): a {@code wal_quorum} recorded before
 * any assignment under it, changes coalesced into one write per interval.
 */
class TermRecordWriterTest {

    private static final Duration INTERVAL = Duration.ofMillis(250);
    private static final UUID I = new UUID(1, 1);
    private static final UUID J = new UUID(1, 2);

    private static TermRecordWriter writer(io.github.huyz0.os.biningester.binstore.BinStore s,
            MemoryBinStore backing, Mono mono) throws Exception {
        return new TermRecordWriter(s, "p", FastTermStartTest.read(backing, 7), mono, INTERVAL);
    }

    private static MemoryBinStore startedAt7(Map<UUID, Integer> quorums) throws Exception {
        MemoryBinStore s = new MemoryBinStore();
        new FastTermStart(s, "p").start(7, FastTermStartTest.SELF, quorums, 0, Optional.empty());
        return s;
    }

    @Test
    void theSTARTINGValuesAreAssignableAtOnce() throws Exception {
        MemoryBinStore backing = startedAt7(Map.of(I, 2));
        TermRecordWriter record = writer(backing, backing, new Mono());

        assertThat(record.assignable(I)).isEqualTo(OptionalInt.of(2));
        assertThat(record.assignable(J)).as("not fast, or not yet recorded").isEmpty();
    }

    @Test
    void aNEWIndexWaitsUntilItsValueIsRecorded() throws Exception {
        MemoryBinStore backing = startedAt7(Map.of());
        TermRecordWriter record = writer(backing, backing, new Mono());
        record.want(J, 3);
        assertThat(record.assignable(J)).isEmpty();

        assertThat(record.flush()).isInstanceOf(TermRecordWriter.Written.class);

        assertThat(record.assignable(J)).isEqualTo(OptionalInt.of(3));
        Roster stored = FastTermStartTest.read(backing, 7);
        assertThat(stored.termRecord()).hasSize(2);
        assertThat(stored.termRecord().get(1).walQuorum()).isEqualTo(Map.of(J, 3));
    }

    @Test
    void aCHANGEWaitsAndTheSmallestRecordedValueIsQMin() throws Exception {
        MemoryBinStore backing = startedAt7(Map.of(I, 2));
        TermRecordWriter record = writer(backing, backing, new Mono());
        record.want(I, 1);
        assertThat(record.assignable(I)).as("lowered: waits for the record").isEmpty();
        record.flush();
        assertThat(record.assignable(I)).isEqualTo(OptionalInt.of(1));

        Roster stored = FastTermStartTest.read(backing, 7);

        assertThat(TermRecordWriter.qMin(stored, I)).isEqualTo(OptionalInt.of(1));
        assertThat(TermRecordWriter.qMin(stored, J)).isEmpty();
    }

    @Test
    void CHANGESAcrossIndicesAreOneWriteAndAnUndoneOneIsDropped() throws Exception {
        MemoryBinStore backing = startedAt7(Map.of(I, 2));
        CountingBinStore counting = new CountingBinStore(backing);
        TermRecordWriter record = writer(counting, backing, new Mono());
        record.want(I, 3);
        record.want(J, 1);
        record.want(I, 2);

        record.flush();

        assertThat(counting.counts().puts()).isEqualTo(1);
        assertThat(FastTermStartTest.read(backing, 7).termRecord().get(1).walQuorum())
                .as("I went back to its recorded value before the write")
                .isEqualTo(Map.of(J, 1));
    }

    @Test
    void aSECONDWriteWaitsOutTheIntervalWithoutARequest() throws Exception {
        MemoryBinStore backing = startedAt7(Map.of(I, 2));
        CountingBinStore counting = new CountingBinStore(backing);
        Mono mono = new Mono();
        TermRecordWriter record = writer(counting, backing, mono);
        record.want(I, 3);
        record.flush();
        long requests = counting.counts().total();
        record.want(I, 1);
        mono.advance(Duration.ofMillis(249));

        assertThat(record.flush()).isInstanceOf(TermRecordWriter.NotDue.class);
        assertThat(counting.counts().total()).isEqualTo(requests);
        assertThat(record.assignable(I)).isEmpty();
        mono.advance(Duration.ofMillis(1));
        assertThat(record.flush()).isInstanceOf(TermRecordWriter.Written.class);

        assertThat(record.assignable(I)).isEqualTo(OptionalInt.of(1));
    }

    @Test
    void aFENCEDRosterDeposesWithoutAWrite() throws Exception {
        MemoryBinStore backing = startedAt7(Map.of(I, 2));
        TermRecordWriter record = writer(backing, backing, new Mono());
        FastTermStartTest.put(backing, FastTermStartTest.read(backing, 7).fencedBy(9));
        record.want(I, 3);

        assertThat(record.flush()).isEqualTo(new TermRecordWriter.Deposed(9));
        assertThat(FastTermStartTest.read(backing, 7).termRecord()).hasSize(1);
        assertThat(record.assignable(I)).isEmpty();
    }

    @Test
    void aWRITERefusedIsReReadAndKeepsAJoinThatLandedFirst() throws Exception {
        MemoryBinStore backing = startedAt7(Map.of(I, 2));
        Roster.Incarnation joiner = FastTermStartTest.incarnation("joiner");
        HookedStore hooked = new HookedStore(backing, Roster.key("p", 7), b -> {
            Roster r = FastTermStartTest.read(b, 7);
            List<Roster.Member> members = new ArrayList<>(r.members());
            members.add(new Roster.Member(joiner, Roster.State.ROSTERED));
            FastTermStartTest.put(b, new Roster(7, r.predecessor(), r.leader(), members,
                    r.termRecord(), r.decisions(), r.notBefore(), r.fencedBy(), r.closed()));
        });
        TermRecordWriter record = writer(hooked, backing, new Mono());
        record.want(I, 3);

        record.flush();

        Roster stored = FastTermStartTest.read(backing, 7);
        assertThat(stored.member("uid-joiner")).isPresent();
        assertThat(stored.termRecord()).hasSize(2);
    }
}
