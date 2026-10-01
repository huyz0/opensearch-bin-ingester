// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.sequencer.FastTermStartRaceTest.HookedStore;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * Deposition at every read of the start, the graceful exemption's other half,
 * and a store the walk cannot place (M13.26b review round 1, P1, T1-T3).
 */
class FastTermStartDeposeTest {

    private static FastTermStart.Outcome start(BinStore store, long epoch,
            Optional<String> replaced) throws Exception {
        return new FastTermStart(store, "p").start(epoch, FastTermStartTest.SELF,
                Map.of(), 400, replaced);
    }

    @Test
    void itsOWNRosterFencedByANewerTermDeposes() throws Exception {
        MemoryBinStore backing = new MemoryBinStore();
        FastTermStartTest.put(backing, FastTermStartTest.roster(5, -1, "c", false, 0, 0, false));
        FastTermStartTest.latest(backing, 5);
        start(backing, 7, Optional.empty());
        Roster fencedOwn = FastTermStartTest.read(backing, 7).fencedBy(8);
        HookedStore store = new HookedStore(backing, Roster.key("p", 7),
                b -> FastTermStartTest.put(b, fencedOwn));

        FastTermStart.Outcome again = start(store, 7, Optional.empty());

        assertThat(again).as("a re-run after a lost LATEST answer, fenced meanwhile")
                .isEqualTo(new FastTermStart.Deposed(8));
        assertThat(FastTermStartTest.read(backing, 7).fencedBy()).isEqualTo(8);
    }

    @Test
    void aFENCERefusedAndReReadNewerDeposesWithoutLoweringIt() throws Exception {
        MemoryBinStore backing = new MemoryBinStore();
        FastTermStartTest.put(backing, FastTermStartTest.roster(5, -1, "c", false, 0, 0, false));
        FastTermStartTest.latest(backing, 5);
        HookedStore store = new HookedStore(backing, Roster.key("p", 5),
                b -> FastTermStartTest.put(b,
                        FastTermStartTest.roster(5, -1, "c", false, 0, 8, false)));

        FastTermStart.Outcome outcome = start(store, 7, Optional.empty());

        assertThat(outcome).isEqualTo(new FastTermStart.Deposed(8));
        assertThat(FastTermStartTest.read(backing, 5).fencedBy())
                .as("a fencedBy is only ever raised").isEqualTo(8);
    }

    @Test
    void aNEWERFenceOnAnOlderWalkedRosterDeposesBeforeAnyFence() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        FastTermStartTest.put(store, FastTermStartTest.roster(3, -1, "a", false, 0, 8, false));
        FastTermStartTest.put(store, FastTermStartTest.roster(5, 3, "b", false, 0, 0, false));
        FastTermStartTest.latest(store, 5);

        FastTermStart.Outcome outcome = start(store, 7, Optional.empty());

        assertThat(outcome).isEqualTo(new FastTermStart.Deposed(8));
        assertThat(FastTermStartTest.read(store, 5).fencedBy())
                .as("the walk ends before any fence is written").isZero();
    }

    @Test
    void anUNDEPARTEDReplacedHolderIsNoHandover() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        FastTermStartTest.put(store, FastTermStartTest.roster(5, -1, "c", false, 0, 0, true));
        FastTermStartTest.latest(store, 5);

        FastTermStart.Started started = (FastTermStart.Started) start(store, 7,
                Optional.of("uid-c"));

        assertThat(started.handedOver())
                .as("no term is unclosed, and the replaced holder led one without departing")
                .isFalse();
        assertThat(started.notBefore()).isEqualTo(400);
    }

    @Test
    void aREPLACEDHolderNoWalkFindsIsNoHandover() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        FastTermStartTest.put(store, FastTermStartTest.roster(5, -1, "c", true, 0, 0, false));
        FastTermStartTest.latest(store, 5);

        FastTermStart.Started started = (FastTermStart.Started) start(store, 7,
                Optional.of("uid-x"));

        assertThat(started.handedOver())
                .as("every unclosed leader departed, but the lease named another holder")
                .isFalse();
    }

    @Test
    void aROSTERTheWalkCannotPlaceIsAnIOException() throws Exception {
        MemoryBinStore missing = new MemoryBinStore();
        FastTermStartTest.latest(missing, 5);
        assertThatThrownBy(() -> start(missing, 7, Optional.empty()))
                .as("LATEST names a roster that is not there").isInstanceOf(IOException.class);

        MemoryBinStore foreignLatest = new MemoryBinStore();
        FastTermStartTest.put(foreignLatest,
                FastTermStartTest.roster(7, -1, "other", false, 0, 0, false));
        FastTermStartTest.latest(foreignLatest, 7);
        assertThatThrownBy(() -> start(foreignLatest, 7, Optional.empty()))
                .as("this term's roster, walked, names another leader")
                .isInstanceOf(IOException.class);

        MemoryBinStore foreignOrphan = new MemoryBinStore();
        FastTermStartTest.put(foreignOrphan,
                FastTermStartTest.roster(7, -1, "other", false, 0, 0, false));
        assertThatThrownBy(() -> start(foreignOrphan, 7, Optional.empty()))
                .as("this term's roster, found at creation, names another leader")
                .isInstanceOf(IOException.class);
    }
}
