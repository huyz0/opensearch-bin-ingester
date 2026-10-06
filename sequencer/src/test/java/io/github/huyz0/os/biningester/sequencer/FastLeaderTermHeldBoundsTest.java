// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.FastLeaderTermDepartTest.POD;
import static io.github.huyz0.os.biningester.sequencer.FastLeaderTermDepartTest.header;
import static io.github.huyz0.os.biningester.sequencer.FastTermStartTest.put;
import static io.github.huyz0.os.biningester.sequencer.FastTermStartTest.read;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.sequencer.FastLeaseFenceHolderTest.Mono;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * A HELD's bounds (M13.27k review round 2, P3, P4; M13.27q): a deposed term
 * refuses a HELD and a phase-1 DEPART, and the term's roster is read at most
 * once per {@code min_upload_interval}, however many HELDs a holder over half
 * its cap sends.
 */
class FastLeaderTermHeldBoundsTest {

    private static final Duration INTERVAL = Duration.ofMillis(250);

    private static FastLeaderTerm term1(CountingBinStore store, Mono mono) throws Exception {
        FastTermOpening.Opened opened = new FastTermOpening(new FastTermStart(store, "p"),
                new EmptyTermCloser(store, "p")::close,
                new FastLeaseFence(Duration.ofSeconds(10), new SimulatedClock(1_000_000L),
                        new Mono()), FastTermStartTest.SELF, Map::of)
                .open(1, () -> { }).orElseThrow();
        return new FastLeaderTerm(opened,
                new JoinDesk(new RosterJoins(store, "p", 1, mono, INTERVAL), nanos -> { }),
                key -> 0L, store, "p", mono, INTERVAL);
    }

    @Test
    void aDEPOSEDTermRefusesAHeldAndAPhase1DepartLowerEpoch() throws Exception {
        MemoryBinStore backing = new MemoryBinStore();
        FastLeaderTerm term = term1(new CountingBinStore(backing), new Mono());
        put(backing, read(backing, 1).fencedBy(5));

        assertThat(((FastFrame.Refused) term.answerHeld(header(FastFrame.KIND_HELD, 1),
                new FastFrame.HeldReport(FastFrame.Held.NONE))).reason())
                .isEqualTo(FastFrame.Reason.LOWER_EPOCH);
        assertThat(((FastFrame.Refused) term.answerDepart(header(FastFrame.KIND_DEPART, 1),
                new FastFrame.Depart(POD, 1, FastFrame.Held.NONE))).reason())
                .isEqualTo(FastFrame.Reason.LOWER_EPOCH);
    }

    @Test
    void HELDSWithinOneIntervalReadTheRosterOnce() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        Mono mono = new Mono();
        FastLeaderTerm term = term1(store, mono);
        long before = store.counts().gets();

        for (int i = 0; i < 5; i++) {
            term.answerHeld(header(FastFrame.KIND_HELD, 1),
                    new FastFrame.HeldReport(FastFrame.Held.NONE));
        }
        assertThat(store.counts().gets() - before).as("five HELDs, one read").isEqualTo(1);

        mono.advance(INTERVAL);
        term.answerHeld(header(FastFrame.KIND_HELD, 1),
                new FastFrame.HeldReport(FastFrame.Held.NONE));
        assertThat(store.counts().gets() - before).as("read again an interval later")
                .isEqualTo(2);
    }
}
