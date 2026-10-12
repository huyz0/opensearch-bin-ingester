// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.sequencer.FastLeaseFenceHolderTest.Pod;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * What the server reads to start a term's takeover (M13.33p): the term
 * claimed once, and whether its wait has passed -- the roster's
 * {@code notBefore} AND the lease fence's monotonic wait (ADR-0081 §3).
 */
class FastTakeoverSeamsTest {

    @Test
    void aTAKEOVERIsClaimedOnce() throws Exception {
        FastLeaderTerm term = FastLeaderTermTakeoverDeskTest.term3(new MemoryBinStore(), true);

        assertThat(term.claimTakeover()).isPresent();
        assertThat(term.claimTakeover()).as("one takeover per term").isEmpty();
    }

    @Test
    void aTERMWithNothingUnclosedHasNoTakeoverToClaim() throws Exception {
        assertThat(FastLeaderTermTakeoverDeskTest.term3(new MemoryBinStore(), false)
                .claimTakeover()).isEmpty();
    }

    /** Term 2 opened by b, which took a's lease over once a's expired on b's wall clock. */
    private static FastTermOpening.Opened takenOver(MemoryBinStore store, Pod b) throws Exception {
        Pod a = FastLeaseFenceHolderTest.pod(store, "a");
        a.leases().tryAcquire();
        b.leases().tryAcquire();
        b.wall().advance(Duration.ofHours(1));
        assertThat(b.leases().tryAcquire()).as("expired on b's wall clock: taken").isPresent();
        FastTermStartTest.put(store, FastTermStartTest.roster(1, -1, "x", false, 0, 0, false));
        FastTermStartTest.latest(store, 1);
        return new FastTermOpening(new FastTermStart(store, "p"),
                new EmptyTermCloser(store, "p")::close, b.fence(), FastTermStartTest.SELF,
                Map::of).open(2, () -> { }).orElseThrow();
    }

    @Test
    void aTAKEOVERDecidesNotBeforeTheRostersNotBefore() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        Pod b = FastLeaseFenceHolderTest.pod(store, "b");
        FastTermOpening.Opened opened = takenOver(store, b);
        b.mono().advance(Duration.ofSeconds(11));
        long notBefore = opened.started().notBefore();

        assertThat(opened.mayDecide(notBefore - 1)).as("the lease wait served, not the wall")
                .isFalse();
        assertThat(opened.mayDecide(notBefore)).isTrue();
    }

    @Test
    void aTAKEOVERDecidesOnlyOnceTheMonotonicWaitIsServed() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        Pod b = FastLeaseFenceHolderTest.pod(store, "b");
        FastTermOpening.Opened opened = takenOver(store, b);
        b.mono().advance(Duration.ofMillis(10_999));

        assertThat(opened.mayDecide(b.wall().millis()))
                .as("the wall clock is long past; the monotonic wait is not").isFalse();
        b.mono().advance(Duration.ofMillis(1));
        assertThat(opened.mayDecide(b.wall().millis())).isTrue();
    }
}
