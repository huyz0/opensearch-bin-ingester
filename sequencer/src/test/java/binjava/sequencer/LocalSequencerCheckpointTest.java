// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import binjava.binstore.BinStore;
import binjava.binstore.backend.MemoryBinStore;
import binjava.format.Checkpoint;
import binjava.format.RunKey;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * That the checkpoint writer is actually WIRED, not merely present (M4.8b2).
 *
 * <p>⚠️ THE SEAM IS NOT THE WIRING, and this repository has paid for the
 * difference three times — most recently when
 * {@code LocalSequencer.sleepFor(Duration.ofDays(1))} left the whole build green
 * because every test injected its own ticker and none went through the shipping
 * constructor. {@code CheckpointWriterTriggerTest} drives the writer directly
 * and would pass in full if {@code LocalSequencer.commitAll} never called it.
 * This test is the one that fails in that case.
 */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class LocalSequencerCheckpointTest {

    private static final String PREFIX = "bins/cluster-a";
    private static final Duration TTL = Duration.ofSeconds(10);
    private static final Duration RENEW = Duration.ofSeconds(3);
    private static final UUID A = UUID.fromString("00000000-0000-0000-0000-0000000000aa");
    private static final RunKey RA = new RunKey(A, 0);

    private static final class TestClock extends Clock {
        private long millis = 1_000_000L;

        @Override public long millis() {
            return millis;
        }

        @Override public Instant instant() {
            return Instant.ofEpochMilli(millis);
        }

        @Override public ZoneOffset getZone() {
            return ZoneOffset.UTC;
        }

        @Override public Clock withZone(ZoneId zone) {
            return this;
        }
    }

    private static LeaseManager manager(BinStore store, String podId) {
        return new LeaseManager(store, new LeaseConfig(PREFIX, podId, "", TTL, RENEW),
                new TestClock());
    }

    private static Map<RunKey, Integer> counts(int n) {
        Map<RunKey, Integer> m = new LinkedHashMap<>();
        m.put(RA, n);
        return m;
    }

    @Test
    void commitsThroughTheSequencerReachTheCheckpointWriter() throws Exception {
        // ⚠️ K = 2 IS INJECTED because the shipped default takes a thousand
        // deltas to cross. A test that used the 4-arg `start` would pass whether
        // or not `commitAll` ever called `observe`.
        RecordingBinStore store = new RecordingBinStore(new MemoryBinStore());
        CheckpointWriter.Ticker frozen = () -> new CountDownLatch(1).await();
        LocalSequencer sequencer = LocalSequencer.start(store, PREFIX, manager(store, "poda"), 8,
                LocalSequencer.sleepFor(RENEW), 2, frozen).orElseThrow();
        try {
            sequencer.commit(new CommitRequest("poda", "i1", 0, "seg/0", counts(3)));
            assertThat(store.checkpointKeys())
                    .as("one delta is below K, so nothing is written yet")
                    .isEmpty();

            sequencer.commit(new CommitRequest("poda", "i1", 1, "seg/1", counts(4)));

            assertThat(store.checkpointKeys())
                    .as("the second delta crosses K, THROUGH the sequencer's own commit path")
                    .hasSize(1);
            Checkpoint written = Checkpoint.decode(store.lastCheckpointBody().orElseThrow());
            assertThat(written.streams().get(RA).nextOffset())
                    .as("and it carries what the sequencer actually committed, 3 then 4")
                    .isEqualTo(7);
            assertThat(written.pods())
                    .as("with the pod identity only the request carries")
                    .hasEntrySatisfying("poda", s -> {
                        assertThat(s.lastAppliedFlushSeq()).isEqualTo(1L);
                        assertThat(s.incarnationId()).isNotNull();
                        // ⚠️ THE POINTER'S VALUE, THROUGH THE REAL COMMIT PATH.
                        // `hasPointer()` alone was all this asserted, and
                        // `observe(requests, 0L)` at LocalSequencer's only call
                        // site then left the WHOLE BUILD green -- the writer's
                        // own tests call `observe` directly and recompute the
                        // argument the same way production does, so they mirror
                        // the wiring instead of exercising it. The checkpoint's
                        // own sequence is EXCLUSIVE (one past the last applied
                        // delta), so the pointer is one below it.
                        assertThat(s.sequence())
                                .as("the pointer names the delta the SEQUENCER committed")
                                .isEqualTo(written.sequence() - 1);
                        assertThat(s.epoch()).isEqualTo(1L);
                    });
        } finally {
            sequencer.close();
        }
    }

    @Test
    void aRefusedCheckpointPolicyLeavesTheLEASEFREEForTheNextNode() throws Exception {
        // ⚠️ THIS IS THE FAILURE THAT HAS NO SECOND CHANCE. The writer is built
        // AFTER the renewer thread starts, so an IllegalArgumentException or an
        // NPE from its constructor escapes `start`'s `catch (IOException)` with
        // the lease HELD AND BEING RENEWED -- and no node can take that term
        // again for the life of the process. The remedy is that both checks run
        // BEFORE `tryAcquire`, and nothing pinned that ordering: moving
        // `checkPolicy` below the acquire, or deleting the ticker null-check,
        // each left the whole build green.
        RecordingBinStore store = new RecordingBinStore(new MemoryBinStore());
        CheckpointWriter.Ticker frozen = () -> new CountDownLatch(1).await();

        assertThatThrownBy(() -> LocalSequencer.start(store, PREFIX, manager(store, "poda"), 8,
                LocalSequencer.sleepFor(RENEW), 0, frozen))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> LocalSequencer.start(store, PREFIX, manager(store, "poda"), 8,
                LocalSequencer.sleepFor(RENEW), 2, null))
                .isInstanceOf(NullPointerException.class);

        // ⚠️ THE ASSERTION THAT MATTERS is not the throw -- it is that the term
        // is still available afterwards. A refusal that stranded the lease would
        // pass both lines above and fail here.
        LocalSequencer next = LocalSequencer.start(store, PREFIX, manager(store, "podb"), 8,
                LocalSequencer.sleepFor(RENEW), 2, frozen).orElseThrow();
        try {
            assertThat(next.epoch()).isPositive();
        } finally {
            next.close();
        }
    }

    @Test
    void theShippedCheckpointPolicyActuallyBOUNDSRecovery() {
        // ⚠️ "GREATER THAN ZERO" WAS NEARLY A TAUTOLOGY, and a draft of this test
        // used it: `Long.MAX_VALUE` deltas and a one-day interval -- a policy
        // that never checkpoints in any realistic term -- passed it, which is
        // precisely the `sleepFor(ofDays(1))` failure its sibling javadoc
        // invokes. K = 0 already fails 33 other tests, so the weak form added
        // nothing at all.
        // ⚠️ THE BOUNDS ARE WHAT M4.9 NEEDS, not taste: recovery is "the newest
        // checkpoint plus at most K deltas", so a K nobody crosses leaves that
        // bound meaningless, and an interval longer than a deploy cycle means a
        // short-lived term checkpoints never.
        assertThat(LocalSequencer.CHECKPOINT_EVERY_DELTAS)
                .isBetween(1L, 10_000L);
        assertThat(LocalSequencer.CHECKPOINT_INTERVAL)
                .isGreaterThan(Duration.ZERO)
                .isLessThanOrEqualTo(Duration.ofMinutes(5));
    }
}
