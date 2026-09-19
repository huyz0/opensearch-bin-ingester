// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import static binjava.sequencer.DedupFixtures.PREFIX;
import static binjava.sequencer.DedupFixtures.counts;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import binjava.binstore.backend.MemoryBinStore;
import binjava.format.CommitDelta;
import binjava.format.RunCommit;
import binjava.format.RunKey;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A forward parked on a holder the watch reports gone is cut short (M8.57,
 * FR-11, NFR-9).
 *
 * <p>⚠️ **MEASURED BY M8.55**: with the {@code EndpointSlice} watch fed, a
 * {@code SIGSTOP}ped leader still cost ~TTL + 0.3 s, because the follower's
 * flush waited in a forward until the peer-commit timeout -- which IS the
 * TTL -- and the evidence was read only by the election after it. A killed
 * leader refuses the connection at once; a frozen one accepts it and says
 * nothing.
 *
 * <p>⚠️ **THE CUT IS THE SAME FAILURE THE TIMEOUT ALREADY GIVES**: an
 * {@code IOException} meaning "may have landed", reconciled as it is today
 * (M5.23, M5.25). It adds no new failure mode, only an earlier one.
 */
@Timeout(value = 30, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class ForwardChallengeTest {

    private static final String A = "pod-a:9000";
    private static final String B = "pod-b:9000";

    private static LeaseConfig config(String podId, String endpoint) {
        return new LeaseConfig(PREFIX, podId, endpoint,
                Duration.ofSeconds(10), Duration.ofSeconds(3));
    }

    private static CommitRequest flush() {
        return new CommitRequest("podb", "i1", 0, "seg/0", counts(3));
    }

    /** A holder that accepts the forward and answers after {@code answerAfter}, or never. */
    private static final class FrozenHolder implements SequencerTransport {
        final CountDownLatch parked = new CountDownLatch(1);
        final Duration answerAfter;
        final CommitDelta answer;

        FrozenHolder(Duration answerAfter, CommitDelta answer) {
            this.answerAfter = answerAfter;
            this.answer = answer;
        }

        @Override
        public CommitDelta send(String endpoint, CommitRequest request) throws IOException {
            parked.countDown();
            try {
                Thread.sleep(answerAfter.toMillis());
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted");
            }
            return answer;
        }

        @Override
        public void close() {
        }
    }

    private static MemoryBinStore leaseHeldBy(String podId, String endpoint) throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        LeaseManager holder = new LeaseManager(store, config(podId, endpoint),
                java.time.Clock.systemUTC());
        assertThat(holder.tryAcquire()).as("the premise: %s holds the lease", podId).isPresent();
        return store;
    }

    @Test
    void aFORWARDParkedOnAHolderTheWATCHReportsGONEFailsPROMPTLY() throws Exception {
        MemoryBinStore store = leaseHeldBy("poda", A);
        FrozenHolder frozen = new FrozenHolder(Duration.ofSeconds(20), null);
        AtomicBoolean gone = new AtomicBoolean();
        LeaseChallenge watch = lease -> gone.get() && lease.holderPodId().equals("poda");

        try (RemoteSequencer remote = new RemoteSequencer(store, config("podb", B), frozen,
                watch)) {
            Thread.ofVirtual().start(() -> {
                try {
                    frozen.parked.await();
                    Thread.sleep(200);
                } catch (InterruptedException ignored) {
                    return;
                }
                gone.set(true);
            });
            long began = System.nanoTime();

            assertThatThrownBy(() -> remote.commit(flush()))
                    .as("⚠️ THE SAME FAILURE A TIMEOUT GIVES: may have landed")
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("poda");
            assertThat(Duration.ofNanos(System.nanoTime() - began))
                    .as("⚠️ CUT SHORT BY THE EVIDENCE, not after the holder's 20 s")
                    .isLessThan(Duration.ofSeconds(2));
        }
    }

    @Test
    void aSLOWForwardToAHolderStillPRESENTIsNOTCut() throws Exception {
        MemoryBinStore store = leaseHeldBy("poda", A);
        CommitDelta answer = new CommitDelta(7, "seg/0", List.of(new RunCommit(
                new RunKey(java.util.UUID.randomUUID(), 0), 3, 0)));
        FrozenHolder slow = new FrozenHolder(Duration.ofMillis(600), answer);

        try (RemoteSequencer remote = new RemoteSequencer(store, config("podb", B), slow,
                lease -> false)) {
            assertThat(remote.commit(flush()))
                    .as("no evidence, no cut: a slow holder is not a gone one")
                    .isSameAs(answer);
        }
    }

    @Test
    void EVIDENCEAboutANOTHERPodDoesNotCutAForwardToThisOne() throws Exception {
        MemoryBinStore store = leaseHeldBy("poda", A);
        CommitDelta answer = new CommitDelta(7, "seg/0", List.of(new RunCommit(
                new RunKey(java.util.UUID.randomUUID(), 0), 3, 0)));
        FrozenHolder slow = new FrozenHolder(Duration.ofMillis(600), answer);
        LeaseChallenge watch = lease -> !lease.holderPodId().equals("poda");

        try (RemoteSequencer remote = new RemoteSequencer(store, config("podb", B), slow,
                watch)) {
            assertThat(remote.commit(flush())).isSameAs(answer);
        }
    }

    @Test
    void theFLEETSequencerHandsItsFORWARDTheSameEvidenceItsElectionUses() throws Exception {
        MemoryBinStore store = leaseHeldBy("poda", A);
        FrozenHolder frozen = new FrozenHolder(Duration.ofSeconds(20), null);
        LeaseChallenge watch = lease -> lease.holderPodId().equals("poda")
                && frozen.parked.getCount() == 0;
        // ⚠️ AN ELECTION THAT NEVER LEADS, so the only way out is the cut.
        Leadership.Election never = () -> java.util.Optional.empty();

        try (FleetSequencer fleet = new FleetSequencer(store, config("podb", B), frozen, never,
                watch)) {
            long began = System.nanoTime();
            assertThatThrownBy(() -> fleet.commit(flush())).isInstanceOf(IOException.class);
            assertThat(Duration.ofNanos(System.nanoTime() - began))
                    .as("⚠️ WIRED, not merely possible: the fleet's forward reads the watch")
                    .isLessThan(Duration.ofSeconds(2));
        }
    }
}
