// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.FastTermStartTest.INDEX;
import static io.github.huyz0.os.biningester.sequencer.FastTermStartTest.latest;
import static io.github.huyz0.os.biningester.sequencer.FastTermStartTest.put;
import static io.github.huyz0.os.biningester.sequencer.FastTermStartTest.read;
import static io.github.huyz0.os.biningester.sequencer.FastTermStartTest.readLatest;
import static io.github.huyz0.os.biningester.sequencer.FastTermStartTest.roster;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.sequencer.FastLeaseFenceHolderTest.Pod;
import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * Every elected term starts its fast term before it serves, and a term that
 * cannot start is given back (ADR-0081 §5 steps 2-3; M13.27d).
 */
class FastTermOpeningTest {

    private static final Duration TTL = Duration.ofSeconds(10);

    /** A term that records whether it was given back. */
    static final class Term implements Closeable {
        boolean closed;

        @Override
        public void close() {
            closed = true;
        }
    }

    private static FastTermOpening opening(BinStore store, FastLeaseFence fence,
            Roster.Incarnation self) {
        return new FastTermOpening(new FastTermStart(store, "p"),
                new EmptyTermCloser(store, "p")::close, fence, self, () -> Map.of(INDEX, 2));
    }

    private static FastLeaseFence quietFence() {
        return new FastLeaseFence(TTL, new SimulatedClock(1_000_000L),
                new FastLeaseFenceHolderTest.Mono());
    }

    @Test
    void aWONTermWritesItsRosterAndLatestWithTheCatalogsQuorums() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        Term term = new Term();

        Optional<FastTermStart.Started> started =
                opening(store, quietFence(), FastTermStartTest.SELF).open(1, term);

        assertThat(started).isPresent();
        assertThat(term.closed).as("served, not given back").isFalse();
        assertThat(readLatest(store)).isEqualTo(1);
        assertThat(read(store, 1).leader()).isEqualTo(FastTermStartTest.SELF);
        assertThat(read(store, 1).termRecord().get(0).walQuorum()).isEqualTo(Map.of(INDEX, 2));
    }

    @Test
    void aDEPOSEDTermIsGivenBackAndWritesNothing() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        put(store, roster(5, -1, "newer", false, 0, 0, false));
        latest(store, 5);
        Term term = new Term();

        Optional<FastTermStart.Started> started =
                opening(store, quietFence(), FastTermStartTest.SELF).open(3, term);

        assertThat(started).isEmpty();
        assertThat(term.closed).isTrue();
        assertThat(store.stat(Roster.key("p", 3))).isEmpty();
    }

    @Test
    void aFAILEDStartGivesTheTermBackAndThrows() throws Exception {
        MemoryBinStore backing = new MemoryBinStore();
        FastTermStartRaceTest.HookedStore store = new FastTermStartRaceTest.HookedStore(backing,
                Roster.key("p", 1), b -> {
                    throw new IOException("store down");
                });
        Term term = new Term();

        assertThatThrownBy(() -> opening(store, quietFence(), FastTermStartTest.SELF)
                .open(1, term)).isInstanceOf(IOException.class);

        assertThat(term.closed).isTrue();
    }

    @Test
    void EARLIEREmptyTermsAreClosedAtTheStart() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        put(store, roster(1, -1, "old1", false, 0, 0, false));
        put(store, roster(2, 1, "old2", false, 0, 0, false));
        latest(store, 2);

        opening(store, quietFence(), FastTermStartTest.SELF).open(3, new Term());

        assertThat(read(store, 1).closed()).isTrue();
        assertThat(read(store, 2).closed()).isTrue();
    }

    @Test
    void aFAILEDCloseStillStartsTheTerm() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        Term term = new Term();
        FastTermOpening opening = new FastTermOpening(new FastTermStart(store, "p"),
                (epoch, unclosed) -> {
                    throw new IOException("store down");
                }, quietFence(), FastTermStartTest.SELF, Map::of);

        assertThat(opening.open(1, term)).isPresent();

        assertThat(term.closed).isFalse();
        assertThat(readLatest(store)).isEqualTo(1);
    }

    /** {@code b} takes the lease over from {@code a}, whose roster names it departed or not. */
    private static Pod takeover(MemoryBinStore store, boolean aDeparted) throws Exception {
        Pod a = FastLeaseFenceHolderTest.pod(store, "a");
        long epoch = a.leases().tryAcquire().orElseThrow().epoch();
        put(store, roster(epoch, -1, "a", aDeparted, 0, 0, false));
        latest(store, epoch);
        Pod b = FastLeaseFenceHolderTest.pod(store, "b");
        b.leases().tryAcquire();
        b.advanceBoth(TTL);
        return b;
    }

    @Test
    void theLEASEsWaitReachesTheRoster() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        Pod b = takeover(store, false);
        long epoch = b.leases().tryAcquire().orElseThrow().epoch();

        FastTermStart.Started started = opening(store, b.fence(),
                FastTermStartTest.incarnation("b")).open(epoch, new Term()).orElseThrow();

        assertThat(started.notBefore()).isEqualTo(b.fence().notBeforeWallMillis());
        assertThat(started.handedOver()).isFalse();
    }

    @Test
    void theREPLACEDHolderDepartedHandsTheTermOver() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        Pod b = takeover(store, true);
        long epoch = b.leases().tryAcquire().orElseThrow().epoch();

        FastTermStart.Started started = opening(store, b.fence(),
                FastTermStartTest.incarnation("b")).open(epoch, new Term()).orElseThrow();

        assertThat(started.handedOver()).as("a, the replaced holder, departed").isTrue();
    }
}
