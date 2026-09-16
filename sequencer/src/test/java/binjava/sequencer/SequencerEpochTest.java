// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import static binjava.sequencer.DedupFixtures.PREFIX;
import static binjava.sequencer.DedupFixtures.counts;
import static org.assertj.core.api.Assertions.assertThat;

import binjava.binstore.CountingBinStore;
import binjava.binstore.backend.MemoryBinStore;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * Every {@link Sequencer} answers {@link Sequencer#epoch()} for the chain it
 * actually writes or forwards to (M5.15d).
 *
 * <p>⚠️ THE ACCESSOR EXISTS FOR THE PUSH CHANNEL, which carries the sequencer
 * epoch beside a consumer's own session epoch so the two can be told apart
 * (SPEC criterion 12). The ingest side asserts what a PUSH carries; this file
 * asserts what each implementation ANSWERS, which is the half a test fake
 * substituted for the seam hides completely.
 *
 * <p>⚠️ AND THE DEFAULT IS A TRAP WORTH PINNING. {@code Sequencer.epoch()} is a
 * default returning {@code EPOCH_UNKNOWN}, so an implementation that forgets to
 * override it compiles and answers plausibly. These cases are what say each one
 * did not forget.
 */
class SequencerEpochTest {

    private static final String A = "http://a:9000";
    private static final String B = "http://b:9000";

    private static LeaseConfig config(String podId, String endpoint) {
        return new LeaseConfig(PREFIX, podId, endpoint,
                Duration.ofSeconds(10), Duration.ofSeconds(3));
    }

    @Test
    void aLOCALSequencerAnswersTheEpochItLeads() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        try (LocalSequencer local = LocalSequencer.start(store, PREFIX,
                new LeaseManager(store, config("poda", A), java.time.Clock.systemUTC()), 8)
                .orElseThrow()) {
            assertThat(local.epoch())
                    .as("a leader writes one chain and knows which -- and it is never the "
                            + "reserved 0, which M4.4b keeps for `no lease`")
                    .isPositive()
                    .isNotEqualTo(Sequencer.EPOCH_UNKNOWN);
        }
    }

    @Test
    void aREMOTESequencerAnswersWhatItForwardedTOAndReadsNOTHINGToDoIt() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        LocalSequencer leader = LocalSequencer.start(store, PREFIX,
                new LeaseManager(store, config("poda", A), java.time.Clock.systemUTC()), 8)
                .orElseThrow();
        InProcessTransport transport = new InProcessTransport().at(A, leader);
        try (RemoteSequencer remote = new RemoteSequencer(store, config("podb", B), transport)) {
            assertThat(remote.epoch())
                    .as("before any commit this pod has forwarded nothing, so there is no "
                            + "epoch its commits are being routed by -- and 0 would be the "
                            + "reserved unleased chain")
                    .isEqualTo(Sequencer.EPOCH_UNKNOWN);

            remote.commit(new CommitRequest("podb", "i1", 0L, "seg/0", counts(3)));
            long getsAfterCommit = store.counts().gets();

            assertThat(remote.epoch())
                    .as("after forwarding, it answers the epoch of the lease it routed by")
                    .isEqualTo(leader.epoch());
            assertThat(store.counts().gets())
                    .as("and answering costs NO request: every commit reads the lease "
                            + "already, so reading it again here doubles this follower's "
                            + "lease GET rate -- one per flush becoming two, the same object "
                            + "decoded twice, which cost.md R4 calls a read to coalesce")
                    .isEqualTo(getsAfterCommit);
        } finally {
            leader.close();
        }
    }

    @Test
    void readingTheEpochDoesNOTTakeATerm() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        LocalSequencer holder = LocalSequencer.start(store, PREFIX,
                new LeaseManager(store, config("poda", A), java.time.Clock.systemUTC()), 8)
                .orElseThrow();
        InProcessTransport transport = new InProcessTransport().at(A, holder);
        AtomicInteger elections = new AtomicInteger();
        Leadership.Election counting = () -> {
            elections.incrementAndGet();
            return Optional.empty();
        };
        try (FleetSequencer fleet =
                new FleetSequencer(store, config("podb", B), transport, counting)) {
            int atBoot = elections.get();

            fleet.epoch();
            fleet.epoch();
            fleet.epoch();

            assertThat(elections.get())
                    .as("reading a field must not acquire a lease, seal an ancestor and "
                            + "replay a chain -- which is what `Leadership.sequencer()` does, "
                            + "and why this asks for the term already held instead")
                    .isEqualTo(atBoot);
        } finally {
            holder.close();
        }
    }

    @Test
    void aFLEETSequencerLeadingAnswersItsOWNEpoch() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        InProcessTransport transport = new InProcessTransport();
        Leadership.Election electing = () -> LocalSequencer.start(store, PREFIX,
                new LeaseManager(store, config("poda", A), java.time.Clock.systemUTC()), 8);
        try (FleetSequencer fleet =
                new FleetSequencer(store, config("poda", A), transport, electing)) {
            assertThat(fleet.epoch())
                    .as("a pod that holds the term answers its own chain rather than "
                            + "forwarding the question -- which would be a store read for "
                            + "something already in hand")
                    .isPositive()
                    .isNotEqualTo(Sequencer.EPOCH_UNKNOWN);
        }
    }

    @Test
    void aFAKEAnswersUNKNOWNRatherThanZERO() throws Exception {
        try (FakeSequencer fake = new FakeSequencer()) {
            assertThat(fake.epoch())
                    .as("0 is the RESERVED unleased epoch, so a fake answering it is "
                            + "indistinguishable from a commit under the chain M4.4b forbids "
                            + "committing to")
                    .isEqualTo(Sequencer.EPOCH_UNKNOWN)
                    .isNotEqualTo(0L);
        }
    }
}
