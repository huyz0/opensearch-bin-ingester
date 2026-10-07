// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.PREFIX;
import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.counts;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.CommitDelta;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A retry whose own drain of the term it holds is fenced puts that term down,
 * so the next ask reaches whoever holds the lease now (M13.81).
 *
 * <p>⚠️ **M13.80's review P7**: the fence was passed through and nothing
 * retired the term, so an idle pod drained its dead term on every interval and
 * never asked the new leaseholder -- its acked writes invisible again.
 */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class DeferredDrainFenceTest {

    private static final String A = "pod-a:9000";
    private static final String B = "pod-b:9000";
    private static final String C = "pod-c:9000";

    private static LeaseConfig config(String podId, String endpoint) {
        return new LeaseConfig(PREFIX, podId, endpoint,
                Duration.ofSeconds(10), Duration.ofSeconds(3));
    }

    private static CommitRequest flush(long seq) {
        return new CommitRequest("podb", "i1", seq, "seg/podb-" + seq, counts(2));
    }

    /** A term that fails every commit, and fails it as a fence once told to. */
    private static final class Ending implements Sequencer {
        private volatile boolean fenced;
        private volatile boolean closed;

        @Override
        public CommitDelta commitAll(List<CommitRequest> requests) throws IOException {
            if (fenced) {
                throw new FencedException("this term ended");
            }
            throw new IOException("the store hiccuped");
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    @Test
    void aFENCEDRetryPutsItsHeldTermDown() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        InProcessTransport transport = new InProcessTransport().withInbox(store, PREFIX);
        LeaseManager manager = new LeaseManager(store, config("poda", A), Clock.systemUTC());
        FleetSequencer leader = new FleetSequencer(store, config("poda", A), transport,
                () -> LocalSequencer.start(store, PREFIX, manager, 8,
                        BoundedRecoveryFixture.noRenew()));
        Ending term = new Ending();
        boolean[] canLead = {false};
        try (FleetSequencer follower = new FleetSequencer(store, config("podb", B), transport,
                () -> canLead[0] ? Optional.of(term) : Optional.empty())) {
            transport.at(A, leader);
            follower.commit(flush(0));
            transport.unreachable(A);
            assertThatThrownBy(() -> follower.commit(flush(1)))
                    .isInstanceOf(CommitDeferredException.class);
            leader.close();

            canLead[0] = true;
            assertThatThrownBy(() -> follower.commit(flush(2)))
                    .as("the term is taken, and its own drain fails")
                    .isInstanceOf(IOException.class);
            assertThat(follower.leading()).as("the premise: it holds the term").isTrue();

            term.fenced = true;
            follower.retryDeferredDrain();
            assertThat(follower.leading())
                    .as("⚠️ A FENCED TERM IS PUT DOWN, SO THE NEXT ASK GOES TO THE LEASEHOLDER")
                    .isFalse();
            assertThat(term.closed).as("and closed, as a commit's fence closes it").isTrue();

            // ⚠️ M13.81 review T1, T2: the NEXT interval asks whoever holds the
            // lease now -- not backed off, and not counted as drained.
            canLead[0] = false;
            LeaseManager successor = new LeaseManager(store, config("podc", C),
                    Clock.systemUTC());
            try (FleetSequencer now = new FleetSequencer(store, config("podc", C), transport,
                    () -> LocalSequencer.start(store, PREFIX, successor, 8,
                            BoundedRecoveryFixture.noRenew()))) {
                assertThat(now.leading()).as("the premise: a leaseholder again").isTrue();
                transport.at(C, now);
                follower.retryDeferredDrain();
                assertThat(DedupFixtures.deltasCarrying(store, "seg/podb-1"))
                        .as("⚠️ THE DEFERRED FLUSH IS APPLIED BY THE NEW LEASEHOLDER")
                        .isEqualTo(1);
                assertThat(store.list(Inbox.prefixFor(PREFIX), null, 100).objects())
                        .isEmpty();
            }
        }
    }
}
