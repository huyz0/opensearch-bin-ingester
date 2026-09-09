// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import binjava.format.ChainEntry;
import binjava.format.CommitDelta;
import binjava.binstore.backend.MemoryBinStore;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A pod that does NOT hold the lease commits, and its records land in the
 * leaseholder's one total order (M5.7, FR-11).
 *
 * <p>⚠️ THE SIMULATION IS EXTENDED, NOT REPLACED. Every seed the four-argument
 * {@code run} has ever taken is bit-identical to what M4 measured; forwarding
 * is a parameter the sweep turns on. Re-basing M4's evidence to add M5's would
 * make the two indistinguishable afterwards.
 *
 * <p>⚠️ THE ASSERTION IS ON THE CHAIN, not on what the driver counted. A driver
 * that incremented {@code forwardedCommits} without the records reaching the
 * log would satisfy every count in this file, which is why the segment keys are
 * read back out of the committed deltas.
 */
@Timeout(value = 120, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class CommitProtocolForwardingTest {

    private static final String PREFIX = "bins/cluster-a";

    /** Every segment key any delta in any chain carries. */
    private static List<String> committedSegments(MemoryBinStore store) throws IOException {
        List<String> keys = new ArrayList<>();
        for (var stat : store.list(PREFIX + "/ctl/log/", null, 10_000).objects()) {
            if (!stat.key().endsWith(".delta")) {
                continue;
            }
            try (var in = store.get(stat.key())) {
                if (ChainEntry.decode(in.readAllBytes()) instanceof CommitDelta delta) {
                    delta.segments().forEach(segment -> keys.add(segment.segmentKey()));
                }
            }
        }
        return keys;
    }

    @Test
    void aPodThatDoesNotHoldTheLeaseCOMMITS_AndItsRecordsAreInTheChain() throws Exception {
        MemoryBinStore backing = new MemoryBinStore();
        var run = CommitProtocolSimulation.run(7, 120, 3,
                CommitProtocolSweepTest.SWEEP_FAULTS, backing,
                ReaderInvariants.ReaderView::of, null, true);

        assertThat(run.forwardAttempts())
                .as("a follower must have TRIED, or nothing below is evidence about forwarding")
                .isPositive();
        assertThat(run.forwardedCommits())
                .as("and some attempt must have SUCCEEDED -- a fleet where every forward is "
                        + "refused holds I1-I5 vacuously for the path this row adds")
                .isPositive();

        // ⚠️ THE DRIVER'S COUNT IS NOT THE EVIDENCE. These are the keys a
        // forwarding pod submitted, matched against what the CHAIN holds -- so
        // a forward that returned offsets without appending, or appended to a
        // chain nobody reads, fails here and passes every count above.
        // ⚠️ AN EXACT COUNT, NOT `containsAnyElementsOf`. Review MEASURED that
        // a one-of-many match needs a single key to land out of the ~4.5 that
        // do per seed, so a driver counting a commit it never made -- `commits++`
        // as the first line of the catch -- passed every assertion here while
        // the sweep's three counter floors read the inflated number.
        List<String> forwardedInChain = committedSegments(backing).stream()
                .filter(key -> key.startsWith("seg/fwd-"))
                .toList();
        assertThat(forwardedInChain)
                .as("every forward the driver counted is IN the chain -- an inflated counter, "
                        + "or a commit counted in the catch, leaves the chain short and fails "
                        + "here")
                .hasSizeGreaterThanOrEqualTo(run.forwardedCommits());
        // ⚠️ GREATER-OR-EQUAL, AND THE GAP IS NOT SLACK. A forward whose reply
        // was lost has still landed: the leaseholder appended and the response
        // never arrived, so the driver could not count it while the chain
        // carries it. That is exactly the ambiguity M5.23 reconciles, and
        // MEASURED here at seed 7: 4 forwarded segments in the chain against 3
        // counted. An EQUAL assertion is therefore false about a correct
        // implementation, which is why it is not made.
        assertThat(forwardedInChain)
                .as("and the chain carries no forward the driver never attempted")
                .hasSizeLessThanOrEqualTo(run.forwardAttempts());
        // ⚠️ AND ATTEMPTS ARE BOUNDED FROM ABOVE, because every floor that reads
        // them is a lower bound and a counter incremented by more than one per
        // attempt satisfies all of them. A round issues at most one forward, so
        // the rounds are the ceiling.
        assertThat(run.forwardAttempts())
                .as("one attempt per round at most -- an inflated counter breaks this and "
                        + "nothing else in the suite")
                .isLessThanOrEqualTo(120);

        assertThat(run.violations())
                .as("I1-I5 hold over a fleet where most pods do not hold the lease")
                .isEmpty();
    }

    /**
     * A forwarded commit is written by the LEASEHOLDER, so the leaseholder's
     * partition is what refuses it.
     *
     * <p>⚠️ THIS IS WHAT THE ACTING-POD SWAP BUYS, and nothing else in the
     * suite constrains it: review MEASURED that deleting the swap, and
     * separately dropping its restore, left every other test green. A forward
     * reads the LEASE as the forwarder and writes the CHAIN as the holder, so a
     * hop that never changes the actor lets a partitioned leaseholder commit.
     */
    @Test
    void aForwardIsWRITTENByTheLeaseholder_SoITSPartitionRefusesIt() throws Exception {
        MemoryBinStore backing = new MemoryBinStore();
        FaultInjectingStore faulty = new FaultInjectingStore(backing, 1,
                new FaultInjectingStore.Faults(0, 0, 0, 0, 0, 0));
        faulty.actingAs("poda");
        // ⚠️ THE LEASE MUST NAME THE ENDPOINT THE TRANSPORT ROUTES BY.
        // `DedupFixtures.manager` writes "", which is what every pod wrote
        // before this row -- and a forwarder reading it addresses nothing.
        LeaseManager leases = new LeaseManager(faulty,
                new LeaseConfig(PREFIX, "poda", ForwardingPods.endpointOf("poda"),
                        java.time.Duration.ofSeconds(10), java.time.Duration.ofSeconds(3)),
                new DedupFixtures.TestClock());
        LocalSequencer leader = LocalSequencer.start(faulty, PREFIX, leases, 8,
                BoundedRecoveryFixture.noRenew()).orElseThrow();
        try {
            ForwardingPods followers = new ForwardingPods(faulty, faulty);
            followers.leads("poda", leader);
            CommitRequest fromB = new CommitRequest("podb", "i1", 0, "seg/fwd-0",
                    DedupFixtures.counts(3));

            assertThat(followers.forward(fromB))
                    .as("a healthy fleet forwards and lands")
                    .isPresent();
            // ⚠️ AND THE ACTOR IS PUT BACK. The hop swaps to the leaseholder for
            // the append and restores the forwarder afterwards, so everything
            // `RemoteSequencer` does NEXT -- the lease re-read on a refusal, 22
            // thousand of them per 1,000 seeds -- is judged against the pod that
            // is asking. Review MEASURED the mutation that survives without
            // this: keeping the swap and dropping only its `finally` restore.
            assertThat(faulty.actingPod())
                    .as("the hop restores the FORWARDER, so its next store call is its own")
                    .isEqualTo("podb");

            // ⚠️ THE LEASEHOLDER IS CUT OFF, the forwarder is not. Without the
            // swap the chain write runs under the FORWARDER's name and this
            // commit succeeds -- a partitioned leader writing the chain.
            faulty.partition("poda");
            CommitRequest alsoFromB = new CommitRequest("podb", "i1", 1, "seg/fwd-1",
                    DedupFixtures.counts(2));

            assertThat(followers.forward(alsoFromB))
                    .as("the write is the LEASEHOLDER's, so the leaseholder's partition stops it")
                    .isEmpty();
        } finally {
            faulty.heal("poda");
            leader.close();
        }
    }

    @Test
    void aFORWARDEDCommitIsAckedInTheLEADERsChain() throws Exception {
        var run = CommitProtocolSimulation.runForwarding(7, 120, 3,
                CommitProtocolSweepTest.SWEEP_FAULTS);

        assertThat(run.forwardedCommits()).isPositive();
        // ⚠️ ONE TOTAL ORDER, WHICH IS THE POINT OF FORWARDING AT ALL. Every
        // acknowledgement -- the leaseholder's own and every forwarded one --
        // is recorded under the epoch whose chain the delta landed in, and
        // `checkAckOrder` reports an ack that precedes its own durable write or
        // jumps a lower-numbered outstanding one. A forwarder acking under its
        // OWN epoch would put an ack in a chain it never wrote.
        // ⚠️ COUNTED AGAINST `commits()`, NOT `isNotEmpty()`. Review MEASURED
        // the weaker form: `acks` is non-empty in every run from the
        // leaseholder's own acks and the store's CONFIRMED events, so deleting
        // the forwarded `acks.add` outright left every assertion green. Each
        // commit contributes exactly one ACK, forwarded or not, so this goes
        // red the moment one population stops being traced.
        long acked = run.acks().stream()
                .filter(AckOrderInvariants.AckEvent::isAck)
                .count();
        assertThat(acked)
                .as("every commit is acked in the trace `checkAckOrder` judges -- the "
                        + "forwarded ones included")
                .isEqualTo(run.commits());
        assertThat(run.violations())
                .as("including I5, which is what an ack in the wrong chain breaks")
                .isEmpty();
    }
}
