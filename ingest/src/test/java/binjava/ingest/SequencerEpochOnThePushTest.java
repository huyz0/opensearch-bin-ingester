// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import static binjava.ingest.IngestTestSupport.appendOnce;
import static org.assertj.core.api.Assertions.assertThat;

import binjava.binstore.CountingBinStore;
import binjava.binstore.backend.MemoryBinStore;
import binjava.format.CommitDelta;
import binjava.format.RunCommit;
import binjava.format.RunKey;
import binjava.sequencer.CommitRequest;
import binjava.sequencer.Sequencer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The push carries the SEQUENCER's epoch, read from the seam (M5.15d).
 *
 * <p>⚠️ {@code CommitDelta} CARRIES NO EPOCH, which is why this needed a seam
 * accessor at all: the epoch lives on the chain key, package-private in
 * {@code sequencer}, so nothing downstream can derive it from a delta.
 *
 * <p>⚠️ AND THE TWO EPOCHS ARE DIFFERENT COUNTERS (criterion 12). This file
 * asserts the chain's; {@code SessionResumeTest} and {@code SessionResetTest}
 * assert the session's, and that neither moves the other.
 */
class SequencerEpochOnThePushTest {

    private static final RunKey LOGS_0 = new RunKey(IngestTestSupport.LOGS, 0);

    private static final class Watching implements SubscriptionHub.Subscriber {
        private final List<SubscriptionHub.Push> seen = new ArrayList<>();

        @Override
        public SegmentSink open(List<SubscriptionHub.Push> pushes) {
            seen.addAll(pushes);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            return (buffer, offset, length) -> out.write(buffer, offset, length);
        }
    }

    /** A sequencer that answers a distinctive epoch, so a default cannot pass for it. */
    private record AtEpoch(Sequencer delegate, long epoch) implements Sequencer {
        @Override
        public CommitDelta commitAll(List<CommitRequest> requests) throws IOException {
            return delegate.commitAll(requests);
        }

        @Override
        public long epoch() {
            return epoch;
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    @Test
    void aPushCarriesTheEpochTheSEQUENCERReports() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        SubscriptionHub hub = new SubscriptionHub();
        Watching watching = new Watching();

        try (var ignored = hub.subscribe(LOGS_0, watching);
                DefaultIngest ingest = new DefaultIngest(
                        IngestTestSupport.pinnedIntervalConfig(IngestTestSupport.NEVER, 8L << 20),
                        store, IngestTestSupport.PREFIX, "pod1",
                        new AtEpoch(IngestTestSupport.sequencer(store, "pod1"), 37L), hub,
                        Clock.systemUTC(), index -> IngestTestSupport.LOGS)) {
            appendOnce(ingest, "logs", 0, 2);
            long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(10);
            while (watching.seen.isEmpty() && System.nanoTime() < deadline) {
                Thread.onSpinWait();
            }
        }

        assertThat(watching.seen)
                .as("PREMISE: the push arrived at all")
                .isNotEmpty();
        assertThat(watching.seen.get(0).sequencerEpoch())
                .as("37 is this sequencer's answer and nothing else's -- the default sentinel "
                        + "is -1, a fixture's literal is 0 or 1, and the delta carries no "
                        + "epoch at all, so only a read of the seam produces it")
                .isEqualTo(37L);
    }

    @Test
    void aPublisherWithNOChainSaysUNKNOWNRatherThanZero() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        SubscriptionHub hub = new SubscriptionHub();
        Watching watching = new Watching();

        try (var ignored = hub.subscribe(LOGS_0, watching)) {
            hub.publish(new CommitDelta(1, "seg-1", List.of(new RunCommit(LOGS_0, 1, 10L))),
                    "seg-1", new byte[2048], serving(store));
        }

        assertThat(watching.seen.get(0).sequencerEpoch())
                .as("zero is the epoch M4.4b RESERVES for `no lease`, so answering it for a "
                        + "publisher that models no chain is indistinguishable from a commit "
                        + "under the reserved chain -- which is the defect that reservation "
                        + "exists to make visible")
                .isEqualTo(SubscriptionHub.EPOCH_UNKNOWN)
                .isNotEqualTo(0L);
    }

    /**
     * The two epochs are not transposable at the push site (M5.15d's hazard).
     *
     * <p>⚠️ M5.15a's review named this row's risk exactly: two adjacent
     * same-typed {@code long}s in a constructor, where a transposition
     * compiles, encodes, round-trips and passes every test that constructs with
     * its own literals in the declared order. On {@code Push} they are NOT
     * adjacent -- {@code firstOffset} and {@code sequencerEpoch} sit either
     * side of three other components -- and this case is what says so in a way
     * that fails if someone brings them together.
     */
    @Test
    void theOFFSETAndTheEPOCHAreDistinctFieldsWithDistinctValues() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        SubscriptionHub hub = new SubscriptionHub();
        Watching watching = new Watching();

        try (var ignored = hub.subscribe(LOGS_0, watching)) {
            hub.publish(new CommitDelta(1, "seg-1", List.of(new RunCommit(LOGS_0, 1, 500L))),
                    "seg-1", new byte[2048], serving(store), 41L);
        }

        SubscriptionHub.Push push = watching.seen.get(0);
        assertThat(push.firstOffset())
                .as("the offset is the commit log's, and 41 would be the epoch wearing its "
                        + "name")
                .isEqualTo(500L);
        assertThat(push.sequencerEpoch())
                .as("and the epoch is the chain's, distinct from every other number on this "
                        + "push -- 500 here would be the offset wearing the epoch's name, "
                        + "which is the transposition this case exists to catch")
                .isEqualTo(41L);
    }

    /**
     * EVERY mode carries the epoch, not only {@code inline} (M5.15d, round 2's
     * major).
     *
     * <p>⚠️ THE OTHER CASES PUBLISH INLINE-SIZED PAYLOADS, so review measured
     * the PROXY and DIRECT arms of {@code publishSegment} passing the sentinel
     * instead of the parameter and the whole suite staying green. A segment
     * large enough to go PROXY -- the mode the proxy path exists FOR -- would
     * have delivered every push labelled "no chain", so a consumer telling a
     * failover from a session reset would read the wrong answer for exactly the
     * biggest segments.
     */
    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"PROXY", "DIRECT"})
    void everyMODECarriesTheEpoch(String mode) throws Exception {
        boolean direct = "DIRECT".equals(mode);
        CountingBinStore store = new CountingBinStore(new StoreFakes.CanPresign());
        SubscriptionHub hub = new SubscriptionHub();
        Watching watching = new Watching();
        byte[] segment = new byte[4096];
        store.put("seg-cold", binjava.binstore.Body.ofBytes(segment));

        // ⚠️ AN INLINE CAP OF ONE BYTE forces everything else, and `direct`
        // needs the flag AND a store that can presign; the fan-out threshold
        // picks between them.
        SegmentServing serving = new SegmentServing(
                new FetchPolicy(new FetchPolicyConfig(1L, 1L, direct ? 1 : 1000, direct)),
                store.capabilities(), new SegmentProxy(store),
                direct ? new GrantIssuer(store) : null);

        try (var ignored = hub.subscribe(LOGS_0, watching)) {
            // ⚠️ COLD: this pod holds a DIFFERENT segment, so the bytes are not
            // in hand and `inline` is impossible at any size.
            hub.publish(new CommitDelta(1, "seg-cold", List.of(new RunCommit(LOGS_0, 1, 700L))),
                    "seg-some-other-pod-wrote", new byte[] {1}, serving, 53L);
        }

        assertThat(watching.seen).isNotEmpty();
        assertThat(watching.seen.get(0).via())
                .as("PREMISE: this really is the arm the case is named for, not INLINE "
                        + "wearing a parameter")
                .isEqualTo(direct ? binjava.format.FetchMode.DIRECT : binjava.format.FetchMode.PROXY);
        assertThat(watching.seen.get(0).sequencerEpoch())
                .as("and it carries the chain epoch, which the INLINE arm carrying it does "
                        + "not imply -- the three arms pass it separately")
                .isEqualTo(53L);
    }

    private static SegmentServing serving(CountingBinStore store) {
        return new SegmentServing(
                new FetchPolicy(FetchPolicyConfig.defaultsFor(store.capabilities().costs())),
                store.capabilities(), new SegmentProxy(store));
    }
}
