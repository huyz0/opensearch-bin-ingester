// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.sequencer.FastLeaseFenceHolderTest.Mono;
import io.github.huyz0.os.biningester.sequencer.FastTermStartRaceTest.HookedStore;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * The term record's writer under a slow or failing store, and the values it
 * keeps (M13.26e review round 1, P1, P2, T1-T6).
 */
class TermRecordWriterIsolationTest {

    private static final Duration INTERVAL = Duration.ofMillis(250);
    private static final UUID I = new UUID(1, 1);
    private static final UUID J = new UUID(1, 2);

    private static MemoryBinStore startedAt7(Map<UUID, Integer> quorums) throws Exception {
        MemoryBinStore s = new MemoryBinStore();
        new FastTermStart(s, "p").start(7, FastTermStartTest.SELF, quorums, 0, Optional.empty());
        return s;
    }

    private static TermRecordWriter writer(io.github.huyz0.os.biningester.binstore.BinStore s,
            MemoryBinStore backing, Mono mono) throws Exception {
        return new TermRecordWriter(s, "p", FastTermStartTest.read(backing, 7), mono, INTERVAL);
    }

    @Test
    void anINDEXWithNothingPendingIsAssignableWhileAnotherIsWritten() throws Exception {
        MemoryBinStore backing = startedAt7(Map.of(I, 2));
        CountDownLatch writing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        HookedStore slow = new HookedStore(backing, Roster.key("p", 7), b -> {
            writing.countDown();
            release.await(10, TimeUnit.SECONDS);
        });
        TermRecordWriter record = writer(slow, backing, new Mono());
        record.want(J, 3);
        Thread flusher = Thread.ofVirtual().start(() -> {
            try {
                record.flush();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        assertThat(writing.await(5, TimeUnit.SECONDS)).isTrue();

        OptionalInt[] seen = new OptionalInt[1];
        Thread reader = Thread.ofVirtual().start(() -> seen[0] = record.assignable(I));
        reader.join(2_000);

        assertThat(reader.isAlive()).as("SPEC criterion 8: I's append is acked while J's "
                + "change is being written").isFalse();
        assertThat(seen[0]).isEqualTo(OptionalInt.of(2));
        release.countDown();
        flusher.join(5_000);
        assertThat(record.assignable(J)).isEqualTo(OptionalInt.of(3));
    }

    @Test
    void aDEPOSEDFlushStillWaitsOutTheInterval() throws Exception {
        MemoryBinStore backing = startedAt7(Map.of(I, 2));
        CountingBinStore counting = new CountingBinStore(backing);
        Mono mono = new Mono();
        TermRecordWriter record = writer(counting, backing, mono);
        FastTermStartTest.put(backing, FastTermStartTest.read(backing, 7).fencedBy(9));
        record.want(I, 3);
        assertThat(record.flush()).isEqualTo(new TermRecordWriter.Deposed(9));
        long requests = counting.counts().total();

        TermRecordWriter.Flush again = record.flush();

        assertThat(again).as("a batch waiting on a deposed leader costs no request per call")
                .isEqualTo(new TermRecordWriter.NotDue(INTERVAL.toNanos()));
        assertThat(counting.counts().total()).isEqualTo(requests);
    }

    @Test
    void qMINIsTheSmallestNotTheLatestAfterARaise() throws Exception {
        MemoryBinStore backing = startedAt7(Map.of(I, 2));
        TermRecordWriter record = writer(backing, backing, new Mono());
        record.want(I, 3);
        record.flush();

        Roster stored = FastTermStartTest.read(backing, 7);

        assertThat(TermRecordWriter.qMin(stored, I)).isEqualTo(OptionalInt.of(2));
        assertThat(record.assignable(I)).as("assignment uses the latest").isEqualTo(OptionalInt.of(3));
    }

    @Test
    void anIDLEFlushCostsNoRequest() throws Exception {
        MemoryBinStore backing = startedAt7(Map.of(I, 2));
        CountingBinStore counting = new CountingBinStore(backing);
        TermRecordWriter record = writer(counting, backing, new Mono());

        assertThat(record.flush()).isEqualTo(new TermRecordWriter.Idle());
        record.want(I, 2);
        assertThat(record.flush()).as("the recorded value: nothing pending")
                .isEqualTo(new TermRecordWriter.Idle());

        assertThat(counting.counts().total()).isZero();
    }

    @Test
    void anOUTOfRangeQuorumIsRefusedAndWrittenIsTheGrownRoster() throws Exception {
        MemoryBinStore backing = startedAt7(Map.of(I, 2));
        TermRecordWriter record = writer(backing, backing, new Mono());
        assertThatThrownBy(() -> record.want(I, 4)).isInstanceOf(IllegalArgumentException.class);
        record.want(I, 1);

        TermRecordWriter.Written written = (TermRecordWriter.Written) record.flush();

        assertThat(written.roster()).isEqualTo(FastTermStartTest.read(backing, 7));
        assertThat(written.roster().termRecord()).hasSize(2);
    }

    @Test
    void aMISSINGRosterIsAnIOException() throws Exception {
        MemoryBinStore backing = startedAt7(Map.of(I, 2));
        TermRecordWriter record = writer(backing, backing, new Mono());
        backing.delete(java.util.List.of(Roster.key("p", 7)));
        record.want(I, 3);

        assertThatThrownBy(record::flush).isInstanceOf(IOException.class);
        assertThat(record.assignable(I)).as("the change stays pending").isEmpty();
    }
}
