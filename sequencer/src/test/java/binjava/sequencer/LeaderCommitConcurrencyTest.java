// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import binjava.binstore.backend.MemoryBinStore;
import binjava.format.CommitDelta;
import binjava.format.RunCommit;
import binjava.format.RunKey;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A leader's commits never reuse an offset base, however many callers commit
 * at once (M8.49, I2).
 *
 * <p>⚠️ **IN PRODUCTION A LEADER HAS TWO COMMIT PATHS AT ONCE.** Its own flush
 * thread commits through {@code FleetSequencer}. Every follower's forwarded
 * commit arrives on a {@code CommitService} handler thread, which calls the
 * held term directly. Both converge on {@code LocalSequencer.commitAll}, over
 * a {@code CommitLog} that is single-writer by construction: a plain map of
 * offsets and a plain slot counter. Unserialised, two commits can build their
 * deltas from the same offset base. Found by M8.12's review, as an offset
 * reassignment inside one epoch under SIGSTOP. ⚠️ Here, over the in-memory
 * store, the race shows as failed commits on colliding slots rather than as a
 * reused base, so what reds this case is the first assertion.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class LeaderCommitConcurrencyTest {

    private static final RunKey STREAM = new RunKey(UUID.randomUUID(), 0);
    private static final int THREADS = 8;
    private static final int COMMITS = 50;
    private static final int RECORDS = 10;

    @Test
    void concurrentCommitsOnOneLeaderAssignCONTIGUOUSOffsetsANDNEVERReuseOne()
            throws Exception {
        try (MemoryBinStore store = new MemoryBinStore()) {
            Sequencer leader = TestSequencers.leased(store, "p", "leader");
            ConcurrentLinkedQueue<RunCommit> runs = new ConcurrentLinkedQueue<>();
            ConcurrentLinkedQueue<Throwable> failures = new ConcurrentLinkedQueue<>();
            CountDownLatch start = new CountDownLatch(1);
            List<Thread> threads = new ArrayList<>();
            for (int t = 0; t < THREADS; t++) {
                String pod = "pod" + t;
                threads.add(Thread.ofPlatform().start(() -> {
                    try {
                        start.await();
                        for (int c = 0; c < COMMITS; c++) {
                            CommitDelta delta = leader.commitAll(List.of(new CommitRequest(pod,
                                    "i1", c, "bins/" + pod + "/" + c + ".bseg",
                                    Map.of(STREAM, RECORDS))));
                            runs.addAll(delta.allRuns());
                        }
                    } catch (Throwable failed) {
                        failures.add(failed);
                    }
                }));
            }
            start.countDown();
            for (Thread thread : threads) {
                thread.join();
            }

            assertThat(failures).as("no commit failed").isEmpty();
            List<RunCommit> sorted = new ArrayList<>(runs);
            sorted.sort(Comparator.comparingLong(RunCommit::firstOffset));
            assertThat(sorted).hasSize(THREADS * COMMITS);
            long next = 0;
            for (RunCommit run : sorted) {
                assertThat(run.firstOffset())
                        .as("⚠️ I2: EACH RUN STARTS WHERE THE LAST ONE ENDED -- below is a "
                                + "reassigned offset, above is a gap")
                        .isEqualTo(next);
                next = run.firstOffset() + run.recordCount();
            }
            assertThat(next).isEqualTo((long) THREADS * COMMITS * RECORDS);
            leader.close();
        }
    }
}
