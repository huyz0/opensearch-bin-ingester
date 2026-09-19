// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import static binjava.sequencer.DedupFixtures.PREFIX;
import static binjava.sequencer.DedupFixtures.counts;
import static binjava.sequencer.DedupFixtures.deltasCarrying;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import binjava.binstore.backend.MemoryBinStore;
import binjava.format.CommitDelta;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The drain's three hazards review measured in round 2 (M8.14a, ADR-0058).
 */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class InboxDrainRaceTest {

    private static final String A = "pod-a:9000";
    private static final String B = "pod-b:9000";

    private static LeaseConfig config(String podId, String endpoint) {
        return new LeaseConfig(PREFIX, podId, endpoint,
                Duration.ofSeconds(10), Duration.ofSeconds(3));
    }

    private static CommitRequest flush(String pod, long seq) {
        return new CommitRequest(pod, "i1", seq, "seg/" + pod + "-" + seq, counts(2));
    }

    @Test
    void aDEFERRINGPodThatBecomesLEADERDrainsItsOwnIntentsBEFOREItsFirstLocalCommit()
            throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        LeaseManager holderA = new LeaseManager(store, config("poda", A), Clock.systemUTC());
        assertThat(holderA.tryAcquire()).isPresent();
        InProcessTransport transport = new InProcessTransport();
        transport.unreachable(A);
        LeaseManager managerB = new LeaseManager(store, config("podb", B), Clock.systemUTC());
        AtomicBoolean mayLead = new AtomicBoolean();

        try (FleetSequencer b = new FleetSequencer(store, config("podb", B), transport,
                () -> mayLead.get()
                        ? LocalSequencer.start(store, PREFIX, managerB, 8,
                                BoundedRecoveryFixture.noRenew())
                        : Optional.empty())) {
            assertThatThrownBy(() -> b.commit(flush("podb", 1)))
                    .isInstanceOf(CommitDeferredException.class);

            holderA.release();
            mayLead.set(true);
            b.commit(flush("podb", 2));
            assertThat(b.leading()).as("the premise: b took the term").isTrue();
        }
        assertThat(deltasCarrying(store, "seg/podb-1"))
                .as("⚠️ THE DEFERRED FLUSH, APPLIED BEFORE flush 2 COULD RAISE b's HIGH MARK "
                        + "over it -- review MEASURED this path unpinned")
                .isEqualTo(1);
        assertThat(deltasCarrying(store, "seg/podb-2")).isEqualTo(1);
    }

    @Test
    void TWOConcurrentDrainsThroughABATCHINGTermCommitEACHIntentONCE() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        LeaseManager manager = new LeaseManager(store, config("poda", A), Clock.systemUTC());
        LocalSequencer local = LocalSequencer.start(store, PREFIX, manager, 8,
                BoundedRecoveryFixture.noRenew()).orElseThrow();
        int intents = 40;
        for (long seq = 0; seq < intents; seq++) {
            Inbox.write(store, PREFIX, flush("podx", seq));
        }
        // ⚠️ BATCHING, AS THE ROUTE's HELD TERM IS: two drains racing one intent
        // land it in one batch, where the dedupe window counts both fresh.
        try (BatchingSequencer term = new BatchingSequencer(local, Duration.ofMillis(5))) {
            CountDownLatch go = new CountDownLatch(1);
            Thread[] drains = new Thread[2];
            for (int i = 0; i < drains.length; i++) {
                drains[i] = Thread.ofVirtual().start(() -> {
                    try {
                        go.await();
                        InboxDrain.drain(store, PREFIX, term);
                    } catch (IOException | InterruptedException ignored) {
                        // the other drain may have deleted what this one listed
                    }
                });
            }
            go.countDown();
            for (Thread drain : drains) {
                drain.join();
            }
        }
        for (long seq = 0; seq < intents; seq++) {
            assertThat(deltasCarrying(store, "seg/podx-" + seq))
                    .as("⚠️ intent %d committed exactly once, not once per drain", seq)
                    .isEqualTo(1);
        }
    }

    @Test
    void anotherPODsStuckIntentDoesNotHOLDThePodThatASKED() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        Inbox.write(store, PREFIX, flush("podx", 5));
        Inbox.write(store, PREFIX, flush("pody", 1));
        Sequencer failsForX = new Sequencer() {
            @Override
            public CommitDelta commitAll(List<CommitRequest> requests) throws IOException {
                CommitRequest r = requests.get(0);
                if (r.podId().equals("podx")) {
                    throw new IOException("stuck");
                }
                return new CommitDelta(1, r.segmentKey(), List.of(new binjava.format.RunCommit(
                        r.recordCounts().keySet().iterator().next(), 2, 0)));
            }

            @Override
            public void close() {
            }
        };

        assertThatCode(() -> InboxDrain.drain(store, PREFIX, failsForX, "pody"))
                .as("⚠️ pody's intents are in: it stops deferring, whatever podx's are doing")
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> InboxDrain.drain(store, PREFIX, failsForX, "podx"))
                .as("and podx, whose intent stuck, keeps deferring")
                .isInstanceOf(IOException.class);
    }
}
