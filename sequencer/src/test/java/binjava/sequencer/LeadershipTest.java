// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import static binjava.sequencer.DedupFixtures.PREFIX;
import static org.assertj.core.api.Assertions.assertThat;

import binjava.binstore.backend.MemoryBinStore;
import binjava.format.CommitDelta;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

/**
 * Taking a term, giving one up, and knowing you have neither (M5.6d).
 *
 * <p>⚠️ THESE ARE THE SHARP EDGES, and they were found one at a time by review
 * of the class that used to inline them: a close that throws, an election that
 * throws, and a second thread arriving mid-election. Each has a failure mode
 * that is silent, permanent, or both.
 */
class LeadershipTest {

    /** A sequencer that refuses every commit, and can refuse to close too. */
    private static final class Stub implements Sequencer {
        private final IOException closeFailure;
        private boolean closed;

        Stub() {
            this(null);
        }

        Stub(IOException closeFailure) {
            this.closeFailure = closeFailure;
        }

        @Override public CommitDelta commitAll(List<CommitRequest> requests) throws IOException {
            throw new IOException("this stub never commits");
        }

        @Override public void close() throws IOException {
            closed = true;
            if (closeFailure != null) {
                throw closeFailure;
            }
        }
    }

    @Test
    void anElectionThatTHROWSLeavesAFollower_NotAFailedConstructor() throws Exception {
        // ⚠️ COULD NOT ASK IS NOT COULD NOT FORWARD. `LocalSequencer.start`
        // throws for a lock it could not take, a 503 on the lease stat, a 403
        // from a bad role. A pod that could not ASK must still boot and forward:
        // failing here means it cannot start at all during a rolling restart.
        try (Leadership wedged = new Leadership(() -> {
            throw new IOException("injected: 403 on the lease stat");
        })) {
            assertThat(wedged.isHeld()).as("it booted, as a follower").isFalse();
            assertThat(wedged.sequencer()).as("and it has nothing to write with").isNull();
        }
    }

    @Test
    void aVACANCYIsTakenOnDemand_NotOnlyAtConstruction() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        LocalSequencer holder = LocalSequencer.start(
                store, PREFIX, DedupFixtures.manager(store, "poda"), 8).orElseThrow();
        Leadership mine = new Leadership(
                () -> LocalSequencer.start(store, PREFIX,
                        DedupFixtures.manager(store, "podb"), 8));
        try {
            assertThat(mine.isHeld()).as("someone else holds it at boot").isFalse();

            // ⚠️ PROMOTION HAPPENS ON DEMAND, NOT ON A TIMER. A pod that lost
            // the election at boot must be able to lead when the holder dies,
            // or a leader's death is a permanent outage for every follower.
            holder.close();
            assertThat(mine.sequencer()).as("the vacancy is taken").isNotNull();
            assertThat(mine.isHeld()).isTrue();
        } finally {
            mine.close();
        }
    }

    @Test
    void aSecondCallerMIDElectionIsNOT_ParkedBehindIt() throws Exception {
        CountDownLatch inElection = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean bootAlreadyRan = new AtomicBoolean();
        Stub won = new Stub();
        // ⚠️ THE ELECTION IS I/O -- a stat, a get, and on a win a chain
        // recovery whose latency is unbounded in log length. The boot attempt
        // loses; the next one blocks inside the store, which is where a second
        // caller would be parked if the exclusion were a plain lock.
        Leadership slow = new Leadership(() -> {
            if (!bootAlreadyRan.getAndSet(true)) {
                return Optional.empty();
            }
            inElection.countDown();
            try {
                release.await(20, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return Optional.of(won);
        });
        try {
            assertThat(slow.isHeld()).as("the boot election lost").isFalse();
            CompletableFuture<Sequencer> electing =
                    CompletableFuture.supplyAsync(slow::sequencer);
            assertThat(inElection.await(20, TimeUnit.SECONDS))
                    .as("one caller is inside the election").isTrue();

            // ⚠️ THE SECOND CALLER ANSWERS AT ONCE, and "no" is the right
            // answer: it does not need to lead, it needs to know so it can
            // forward. Parking it behind a chain recovery would put a
            // producer's flush behind work done for somebody else. If this
            // blocked, the get below would time out rather than hang the suite.
            CompletableFuture<Sequencer> second =
                    CompletableFuture.supplyAsync(slow::sequencer);
            assertThat(second.get(10, TimeUnit.SECONDS))
                    .as("it was told no rather than made to wait")
                    .isNull();

            release.countDown();
            assertThat(electing.get(20, TimeUnit.SECONDS))
                    .as("and the election that was running still won")
                    .isSameAs(won);
        } finally {
            release.countDown();
            slow.close();
        }
    }

    @Test
    void aFencedSequencerIsDROPPEDEvenWhenItsCloseTHROWS() throws Exception {
        Stub cannotClose = new Stub(new IOException("injected: the release PUT failed"));
        Leadership mine = new Leadership(() -> Optional.of(cannotClose));
        assertThat(mine.isHeld()).isTrue();

        FencedException fence = new FencedException("fenced at epoch 1");
        mine.retire(cannotClose, fence);

        // ⚠️ THE REFERENCE WENT FIRST. Closing before dropping leaves the term
        // on a CLOSED sequencer whose later refusal is deliberately not a
        // FencedException -- so the caller's fenced branch is unreachable for
        // the life of the process and the pod forwards nothing ever again.
        assertThat(mine.isHeld()).as("dropped, though the close threw").isFalse();
        assertThat(cannotClose.closed).as("and it was closed, not merely forgotten").isTrue();

        // ⚠️ REPORTED, NEVER SUBSTITUTED. A close failure that REPLACED the
        // fence would turn a commit that provably appended nothing into an
        // ambiguous one, which `Sequencer.commit` forbids re-sending -- so a
        // producer's write is dropped on every takeover that meets a hiccup.
        assertThat(fence.getSuppressed())
                .as("the close failure is carried by the fence, not in place of it")
                .hasSize(1);
        mine.close();
    }

    @Test
    void retiringTWICERetiresONCE() throws Exception {
        Stub mineNoMore = new Stub();
        Leadership mine = new Leadership(() -> Optional.of(mineNoMore));
        mine.retire(mineNoMore, new FencedException("fenced"));

        // ⚠️ COMPARE-AND-SET, not a null check. Two threads racing into the same
        // fence would otherwise both close the sequencer and both release the
        // lease -- and a second release after a successor has taken the term
        // writes an expired lease over a live one.
        Stub other = new Stub();
        mine.retire(other, new FencedException("fenced again"));
        assertThat(other.closed)
                .as("a sequencer that was never held is not closed by retiring it")
                .isFalse();
        mine.close();
    }

    @Test
    void closeGIVESTheTermBack() throws Exception {
        Stub mine = new Stub();
        Leadership held = new Leadership(() -> Optional.of(mine));
        assertThat(held.isHeld()).isTrue();

        held.close();

        // ⚠️ RELEASED, NOT MERELY FORGOTTEN. `LocalSequencer.close` hands the
        // lease back; a pod that only dropped its reference would leave the
        // renewer renewing, so every follower's takeover waits out the TTL
        // (~10 s, ADR-0007) instead of milliseconds. That is the entire
        // difference `Sequencer.close` is specified to buy.
        assertThat(mine.closed).as("the term was handed back").isTrue();
        assertThat(held.isHeld()).as("and it no longer claims to hold one").isFalse();
    }

    @Test
    void nothingIsELECTEDAfterClose() throws Exception {
        AtomicBoolean asked = new AtomicBoolean();
        Leadership none = new Leadership(() -> {
            asked.set(true);
            return Optional.empty();
        });
        none.close();
        asked.set(false);

        // ⚠️ NO RACE NEEDED FOR THIS ONE. A commit arriving after close -- an
        // in-flight flush, a queued batch -- would otherwise re-acquire the
        // lease the pod has just voluntarily released, with nothing left to
        // give it back: every other pod then reads an unexpired lease and NONE
        // can lead, for a full TTL or for the life of the process.
        assertThat(none.sequencer()).as("a closed pod takes no term").isNull();
        assertThat(asked.get()).as("it did not even ask for one").isFalse();
        assertThat(none.isHeld()).isFalse();
    }

    @Test
    void aTermWonWHILEClosingIsGivenSTRAIGHTBack() throws Exception {
        CountDownLatch inElection = new CountDownLatch(1);
        CountDownLatch closed = new CountDownLatch(1);
        AtomicBoolean bootAlreadyRan = new AtomicBoolean();
        // ⚠️ AND ITS CLOSE FAILS, because `LocalSequencer.close` releases the
        // lease and that `putIfMatch` can 503. Handing the term back must not
        // then throw out of `sequencer()`: the caller asked whether it leads,
        // the answer is no, and a flush that should have been forwarded during
        // a rolling restart would instead fail its producer.
        Stub won = new Stub(new IOException("injected: the release PUT failed"));
        // ⚠️ THE WINDOW IS WIDE, not theoretical: winning takes a tryAcquire, a
        // seal and a recover() whose latency is unbounded in chain length, so a
        // SIGTERM lands inside it easily.
        Leadership slow = new Leadership(() -> {
            if (!bootAlreadyRan.getAndSet(true)) {
                return Optional.empty();
            }
            inElection.countDown();
            try {
                closed.await(20, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return Optional.of(won);
        });
        CompletableFuture<Sequencer> electing = CompletableFuture.supplyAsync(slow::sequencer);
        assertThat(inElection.await(20, TimeUnit.SECONDS)).isTrue();

        // close() reads a null reference here, closes nothing, and reports a
        // clean shutdown -- while the election is still in the store.
        slow.close();
        closed.countDown();

        assertThat(electing.get(20, TimeUnit.SECONDS))
                .as("the term won during close is not handed to a caller")
                .isNull();
        // ⚠️ AND IT WAS GIVEN BACK. Installing it instead leaves a LocalSequencer
        // with a live renewer that nothing will ever stop: every other pod reads
        // an unexpired lease and no pod can lead at all.
        assertThat(won.closed).as("it was closed rather than installed").isTrue();
        assertThat(slow.isHeld()).isFalse();
    }

    @Test
    void retiringASTRANGERLeavesTheLIVETermAlone() throws Exception {
        Stub live = new Stub();
        Leadership mine = new Leadership(() -> Optional.of(live));
        assertThat(mine.isHeld()).isTrue();

        // ⚠️ IDENTITY, NOT EMPTINESS -- and a null check would pass every other
        // test in this file. The interleaving needs no race to reach: two
        // threads both hold reference A and both are fenced; A retires first,
        // a third commit elects and installs B, and then B's LATE retire(A)
        // arrives. Compare-and-set fails on A and leaves B alone. A null check
        // would see "something is held", drop it, and close A -- throwing away
        // the pod's LIVE term while B's renewer keeps the lease unexpired, so
        // no other pod can take over either and the fleet has no leader at all.
        Stub stale = new Stub();
        mine.retire(stale, new FencedException("a term this pod no longer holds"));

        assertThat(mine.isHeld()).as("the live term is untouched").isTrue();
        assertThat(live.closed).as("and was not closed by someone else's fence").isFalse();
        assertThat(stale.closed).as("nor was the stranger adopted and closed").isFalse();
        mine.close();
    }
}
