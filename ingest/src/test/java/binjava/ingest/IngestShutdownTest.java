// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import static binjava.ingest.IngestTestSupport.appendAsync;
import static binjava.ingest.IngestTestSupport.awaitPending;
import static binjava.ingest.IngestTestSupport.ingest;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import binjava.binstore.CountingBinStore;
import binjava.binstore.backend.MemoryBinStore;
import binjava.format.RunKey;
import binjava.sequencer.LocalSequencer;
import binjava.sequencer.TestSequencers;
import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * What {@code DefaultIngest.close()} owes the cluster, which since M4.6d is two
 * separate obligations rather than one.
 *
 * <p>⚠️ SPLIT OUT OF {@code DefaultIngestTest}, which reached the 500-line limit
 * once M4.6d's sequencer wiring touched every construction site.
 * code-structure.md rule 1: split it, never raise the limit. The move is
 * BYTE-IDENTICAL for {@code closeFlushesWhatIsStillBufferedRatherThanLosingIt}
 * apart from its request count, which M4.6d changes for a stated reason.
 *
 * <p>⚠️ These two tests are here TOGETHER because they constrain each other. The
 * first counts requests and would accept any fifth request as the release; the
 * second names what the release is for and would pass even if the final flush
 * were lost. Neither alone pins {@code close()}.
 */
class IngestShutdownTest {

    @Test
    void closeFlushesWhatIsStillBufferedRatherThanLosingIt() throws Exception {
        // ⚠️ The shutdown path is the one whose failure loses the LAST segment,
        // and while append flushed on every call it was unreachable. A no-op
        // close() must not pass this.
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        SubscriptionHub hub = new SubscriptionHub();
        List<SubscriptionHub.Push> seen = new CopyOnWriteArrayList<>();
        var sub = hub.subscribe(new RunKey(IngestTestSupport.LOGS, 0), seen::add);
        DefaultIngest ingest = ingest(store, hub, IngestTestSupport.NEVER);
        long base = store.counts().total();
        CompletableFuture<AppendResult> inflight = appendAsync(ingest, "logs", 0, 7);
        awaitPending(ingest, 1);
        assertThat(store.counts().total() - base)
                .as("nothing written before the flush").isZero();

        ingest.close();

        assertThat(inflight.get(10, TimeUnit.SECONDS).recordCount()).isEqualTo(7);
        // ⚠️ M2.6: 2 (segment + commit) + 2 (one genuinely new index), and
        // SINCE M4.6d one more: `close()` RELEASES THE LEASE.
        // ⚠️ Stated rather than quietly bumped, because this is a cost threshold
        // moving in the weakening direction. The Sequencer contract calls close
        // "the difference between a failover in milliseconds and one bounded by
        // the lease TTL" (ADR-0007 puts that at ~10 s worst case), and this
        // method IS the pod shutting down. One conditional PUT on a path taken
        // ONCE PER POD LIFETIME buys a ~10 s reduction in cluster-wide failover
        // time; leaving it out would make every graceful shutdown look like a
        // crash to the successor.
        // ⚠️ It is off the hot path by construction -- shutdown, not append --
        // so it changes no per-record or per-segment rate and touches no rule in
        // cost.md's R1-R11.
        assertThat(store.counts().total() - base).isEqualTo(5);
        assertThat(seen).hasSize(1);
        sub.close();
    }

    @Test
    void closeReleasesTheLeaseSoASuccessorSequencesWithoutWaitingOutTheTtl() throws Exception {
        // ⚠️ THE BEHAVIOUR, not the request count.
        // `closeFlushesWhatIsStillBufferedRatherThanLosingIt` sees the release
        // only as a fifth request, and ANY fifth request would satisfy that
        // assertion -- a `stat`, a re-read, a segment written twice. This
        // asserts what the release is FOR: ADR-0007 bounds an ABANDONED lease at
        // its TTL, so a successor acquiring while that TTL is still running is
        // the only evidence the lease was handed back rather than left to lapse.
        //
        // ⚠️ ONE FROZEN CLOCK, SHARED BY BOTH SIDES, and getting this wrong is
        // instructive: with the holder on `systemUTC` and the challenger on a
        // fixed instant, the two disagree about what "expired" means and the test
        // fails against CORRECT code -- the released lease, stamped with the
        // holder's real `now`, looks like the future to the challenger. Sharing
        // one stopped clock makes both halves exact: at T the held lease expires
        // at T+TTL so the challenger provably cannot have it, and after the
        // release its expiry is T, which is expired INCLUSIVE, so the challenger
        // provably can. Neither assertion can be decided by how long a statement
        // took, which is what a wall clock would have left them resting on.
        Clock frozen = Clock.fixed(Instant.parse("2026-09-04T00:00:00Z"), ZoneOffset.UTC);
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        try (DefaultIngest ingest = new DefaultIngest(
                IngestTestSupport.pinnedIntervalConfig(IngestTestSupport.NEVER, 8L << 20),
                store, IngestTestSupport.PREFIX, "pod1",
                TestSequencers.leased(store, IngestTestSupport.PREFIX, "pod1", frozen),
                new SubscriptionHub(), Clock.systemUTC(),
                index -> IngestTestSupport.LOGS)) {

            // ⚠️ THE ANTI-VACUITY HALF, and without it the assertion after close()
            // is worthless: "pod2 acquired" is also what a store holding NO lease
            // at all produces. This shows pod1's term was genuinely held and
            // genuinely exclusive at the instant before the release.
            assertThat(LocalSequencer.start(store, IngestTestSupport.PREFIX,
                            IngestTestSupport.leases(store, "pod2", frozen),
                            IngestTestSupport.SEAL_REDRIVE_BUDGET))
                    .as("while the pod holds a %s lease and the clock does not move, "
                            + "nobody else sequences", IngestTestSupport.LEASE_TTL)
                    .isEmpty();

            ingest.close();

            Optional<LocalSequencer> successor = LocalSequencer.start(store,
                    IngestTestSupport.PREFIX, IngestTestSupport.leases(store, "pod2", frozen),
                    IngestTestSupport.SEAL_REDRIVE_BUDGET);
            assertThat(successor)
                    .as("and after close() the successor takes over on the SAME stopped "
                            + "clock, so the released term's %s TTL provably has not "
                            + "elapsed -- this can only pass if close() wrote the release",
                            IngestTestSupport.LEASE_TTL)
                    .isPresent();
            successor.get().close();
        }
    }

    @Test
    void aFailedLeaseReleaseIsSuppressedRatherThanReplacingAFailedFinalFlush() throws Exception {
        // ⚠️ THE ONE ORDERING THAT LOSES DATA SILENTLY. `close()` flushes and then
        // releases, and the release is I/O that can fail on its own (a stale CAS
        // version after a self-fence -- `LeaseManager.release`'s own javadoc says
        // so -- or a 503 at shutdown). Let that out of the `finally` bare and it
        // REPLACES the exception from the final flush: the operator is told the
        // lease could not be released by a pod that just lost its last segment.
        // ⚠️ This class has paid for the shape once already -- see `drainPushes`,
        // made non-throwing because "an unchecked throw from a finally escaped
        // every catch (IOException) while masking a failed final flush".
        StoreFakes.FailWritesOnCommand failing =
                new StoreFakes.FailWritesOnCommand(new MemoryBinStore());
        CountingBinStore store = new CountingBinStore(failing);
        DefaultIngest ingest = ingest(store, new SubscriptionHub(), IngestTestSupport.NEVER);
        CompletableFuture<AppendResult> inflight = appendAsync(ingest, "logs", 0, 4);
        awaitPending(ingest, 1);

        // ⚠️ AFTER the lease is already held, so the release has a real
        // conditional write to attempt and genuinely fails at it.
        failing.failEveryWriteFromNowOn();

        assertThatThrownBy(ingest::close)
                .as("the LOST SEGMENT is what propagates -- the flush failure, not the release")
                .isInstanceOf(IOException.class)
                .satisfies(thrown -> assertThat(thrown.getSuppressed())
                        .as("and the release failure rides along suppressed, where an "
                                + "operator can still see it, rather than vanishing")
                        .isNotEmpty());

        assertThatThrownBy(() -> inflight.get(10, TimeUnit.SECONDS))
                .as("and the producer is told its append failed rather than being acked")
                .isInstanceOf(ExecutionException.class);
    }

}
