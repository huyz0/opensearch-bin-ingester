// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
import org.junit.jupiter.api.Test;

/**
 * A change made while the term record's write is in flight (M13.26e review
 * round 2, P1, T1-T3): it stays pending, and the commit clears only the value
 * it wrote.
 *
 * <p>⚠️ THE DANGEROUS DIRECTION IS AN UNDO OF A LOWERING: 2, lowered to 1 and
 * set back to 2 during the write, kept 1 -- the leader assigning at a quorum
 * below the operator's for the rest of the term.
 */
class TermRecordWriterInFlightTest {

    private static final Duration INTERVAL = Duration.ofMillis(250);
    private static final UUID I = new UUID(1, 1);

    private static MemoryBinStore startedAt7() throws Exception {
        MemoryBinStore s = new MemoryBinStore();
        new FastTermStart(s, "p").start(7, FastTermStartTest.SELF, Map.of(I, 2), 0,
                Optional.empty());
        return s;
    }

    /** A writer whose first roster write runs {@code during} first. */
    private static TermRecordWriter writer(MemoryBinStore backing, Mono mono,
            java.util.function.Consumer<TermRecordWriter> during) throws Exception {
        TermRecordWriter[] self = new TermRecordWriter[1];
        HookedStore hooked = new HookedStore(backing, Roster.key("p", 7),
                b -> during.accept(self[0]));
        self[0] = new TermRecordWriter(hooked, "p", FastTermStartTest.read(backing, 7), mono,
                INTERVAL);
        return self[0];
    }

    @Test
    void anUNDODuringTheWriteIsKeptAndWrittenNext() throws Exception {
        Mono mono = new Mono();
        TermRecordWriter record = writer(startedAt7(), mono, r -> r.want(I, 2));
        record.want(I, 1);
        record.flush();

        assertThat(record.assignable(I)).as("1 is recorded, but the catalog says 2 again")
                .isEmpty();
        mono.advance(INTERVAL);
        record.flush();

        assertThat(record.assignable(I)).isEqualTo(OptionalInt.of(2));
    }

    @Test
    void aRAISEDuringTheWriteStaysPending() throws Exception {
        Mono mono = new Mono();
        TermRecordWriter record = writer(startedAt7(), mono, r -> r.want(I, 1));
        record.want(I, 3);
        record.flush();

        assertThat(record.assignable(I)).isEmpty();
        mono.advance(INTERVAL);
        record.flush();

        assertThat(record.assignable(I)).isEqualTo(OptionalInt.of(1));
    }

    @Test
    void aWRITEThatFailedKeepsItsChangePending() throws Exception {
        Mono mono = new Mono();
        TermRecordWriter record = writer(startedAt7(), mono, r -> {
            throw new IllegalStateException("store unavailable");
        });
        record.want(I, 3);

        assertThatThrownBy(record::flush).isInstanceOf(IOException.class);
        assertThat(record.assignable(I)).isEmpty();
        mono.advance(INTERVAL);
        record.flush();

        assertThat(record.assignable(I)).isEqualTo(OptionalInt.of(3));
    }

    @Test
    void notDUEGivesTheTimeLeftAndAZeroQuorumIsRefused() throws Exception {
        MemoryBinStore backing = startedAt7();
        Mono mono = new Mono();
        TermRecordWriter record = new TermRecordWriter(backing, "p",
                FastTermStartTest.read(backing, 7), mono, INTERVAL);
        record.want(I, 3);
        record.flush();
        record.want(I, 1);
        mono.advance(Duration.ofMillis(249));

        assertThat(record.flush()).isEqualTo(
                new TermRecordWriter.NotDue(Duration.ofMillis(1).toNanos()));
        assertThatThrownBy(() -> record.want(I, 0)).isInstanceOf(IllegalArgumentException.class);
    }
}
