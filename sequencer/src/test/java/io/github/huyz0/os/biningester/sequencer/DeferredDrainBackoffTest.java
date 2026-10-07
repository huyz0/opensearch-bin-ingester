// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.PREFIX;
import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.counts;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.CommitDelta;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A deferring pod whose drains the leaseholder keeps answering as failed asks
 * less and less often, and never takes the term to ask (M13.80).
 *
 * <p>⚠️ **EACH ASK READS THE WHOLE INBOX** on the leaseholder: a LIST page per
 * 1,000 keys and a GET per pending intent. M13.78's review measured an idle pod
 * whose own intent never applies costing ~28.8M GETs a day at 1,000 intents
 * and a 3 s interval, for ever.
 */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class DeferredDrainBackoffTest {

    private static final String A = "pod-a:9000";
    private static final String B = "pod-b:9000";

    private static LeaseConfig config(String podId, String endpoint) {
        return new LeaseConfig(PREFIX, podId, endpoint,
                Duration.ofSeconds(10), Duration.ofSeconds(3));
    }

    private static FleetSequencer pod(BinStore store, String podId, String endpoint,
            SequencerTransport transport) throws Exception {
        LeaseManager manager = new LeaseManager(store, config(podId, endpoint),
                Clock.systemUTC());
        return new FleetSequencer(store, config(podId, endpoint),
                transport, () -> LocalSequencer.start(store, PREFIX, manager, 8,
                        BoundedRecoveryFixture.noRenew()));
    }

    private static CommitRequest flush(long seq) {
        return new CommitRequest("podb", "i1", seq, "seg/podb-" + seq, counts(2));
    }

    /** Counts the drains a pod asks for. */
    private static final class Asking implements SequencerTransport {
        private final InProcessTransport inner;
        private final AtomicInteger drains = new AtomicInteger();

        Asking(InProcessTransport inner) {
            this.inner = inner;
        }

        @Override
        public CommitDelta send(String endpoint, CommitRequest request) throws IOException {
            return inner.send(endpoint, request);
        }

        @Override
        public void drain(String endpoint, String requester) throws IOException {
            drains.incrementAndGet();
            inner.drain(endpoint, requester);
        }

        @Override
        public void close() {
        }
    }

    /** The ticks, of {@code ticks}, on which a retry asked. */
    private static List<Integer> askedOn(FleetSequencer pod, Asking transport, int ticks) {
        List<Integer> asked = new ArrayList<>();
        for (int tick = 0; tick < ticks; tick++) {
            int before = transport.drains.get();
            pod.retryDeferredDrain();
            if (transport.drains.get() > before) {
                asked.add(tick);
            }
        }
        return asked;
    }

    /** A deferring follower whose asks now reach a leaseholder that cannot apply its intent. */
    private record Answered(FailingInboxStore store, InProcessTransport inner, Asking transport,
            FleetSequencer leader, FleetSequencer follower) implements AutoCloseable {

        static Answered deferring() throws Exception {
            FailingInboxStore store = new FailingInboxStore(new MemoryBinStore());
            InProcessTransport inner = new InProcessTransport().withInbox(store, PREFIX);
            Asking transport = new Asking(inner);
            FleetSequencer leader = pod(store, "poda", A, inner);
            FleetSequencer follower = pod(store, "podb", B, transport);
            inner.at(A, leader);
            follower.commit(flush(0));
            inner.unreachable(A);
            assertThatThrownBy(() -> follower.commit(flush(1)))
                    .isInstanceOf(CommitDeferredException.class);
            inner.at(A, leader);
            store.failing = true;
            return new Answered(store, inner, transport, leader, follower);
        }

        @Override
        public void close() throws Exception {
            follower.close();
            leader.close();
        }
    }

    @Test
    void anANSWEREDFailureAsksAfterDoublingGaps() throws Exception {
        try (Answered pods = Answered.deferring()) {
            assertThat(askedOn(pods.follower(), pods.transport(), 32))
                    .as("⚠️ ONE ASK, THEN SKIPS OF 1, 3, 7, 15 INTERVALS")
                    .containsExactly(0, 1, 3, 7, 15, 31);
        }
    }

    @Test
    void theBackoffIsCAPPEDSoAHealIsStillSeen() throws Exception {
        try (Answered pods = Answered.deferring()) {
            List<Integer> asked = askedOn(pods.follower(), pods.transport(), 600);
            int widest = 0;
            for (int i = 1; i < asked.size(); i++) {
                widest = Math.max(widest, asked.get(i) - asked.get(i - 1));
            }
            assertThat(widest).as("⚠️ AT MOST %d INTERVALS BETWEEN ASKS",
                    FleetSequencer.MAX_SKIPPED_ASKS + 1)
                    .isEqualTo(FleetSequencer.MAX_SKIPPED_ASKS + 1);
        }
    }

    @Test
    void aNEWDeferralAsksOnTheNextInterval() throws Exception {
        // ⚠️ A HEAL's TRIGGER IS A NEW DEFERRAL: a pod backed off by a stuck
        // drain must not make the next flush it defers wait out that backoff.
        try (Answered pods = Answered.deferring()) {
            askedOn(pods.follower(), pods.transport(), 20);

            assertThatThrownBy(() -> pods.follower().commit(flush(2)))
                    .isInstanceOf(CommitDeferredException.class);
            assertThat(askedOn(pods.follower(), pods.transport(), 32))
                    .as("⚠️ ASKED AT ONCE, AND THE DOUBLING STARTS AGAIN FROM ZERO")
                    .containsExactly(0, 1, 3, 7, 15, 31);
        }
    }

    @Test
    void aPodHOLDINGTheTermBacksOffItsOwnFailingDrain() throws Exception {
        // ⚠️ M13.80 review T1: a leaseholder drains locally, through no
        // transport -- so its asks are counted as the inbox reads they cost.
        FailingInboxStore store = new FailingInboxStore(new MemoryBinStore());
        InProcessTransport transport = new InProcessTransport().withInbox(store, PREFIX);
        FleetSequencer leader = pod(store, "poda", A, transport);
        try (FleetSequencer follower = pod(store, "podb", B, transport)) {
            transport.at(A, leader);
            follower.commit(flush(0));
            transport.unreachable(A);
            assertThatThrownBy(() -> follower.commit(flush(1)))
                    .isInstanceOf(CommitDeferredException.class);
            leader.close();
            store.failing = true;
            assertThatThrownBy(() -> follower.commit(flush(2)))
                    .isInstanceOf(IOException.class);
            assertThat(follower.leading()).as("the premise: it holds the term").isTrue();

            List<Integer> read = new ArrayList<>();
            for (int tick = 0; tick < 32; tick++) {
                int before = store.inboxGets.get();
                follower.retryDeferredDrain();
                if (store.inboxGets.get() > before) {
                    read.add(tick);
                }
            }
            assertThat(read).as("⚠️ THE SAME DOUBLING GAPS AS A FOLLOWER's ASKS")
                    .containsExactly(0, 1, 3, 7, 15, 31);
        }
    }

    @Test
    void anUNANSWEREDAskDoesNotBackOff() throws Exception {
        // ⚠️ M13.80's first form backed off on EVERY failure, and
        // PartitionVisibilityIT stranded its trigger again: the retry's asks
        // timed out queued behind the leaseholder's long first drain, the gaps
        // grew, and the one that would have applied the trigger came too late.
        // An ask with no answer read nothing on the leaseholder's side worth
        // backing off for.
        try (Answered pods = Answered.deferring()) {
            pods.inner().unreachable(A);
            assertThat(askedOn(pods.follower(), pods.transport(), 10))
                    .as("asked on every interval")
                    .containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        }
    }

    @Test
    void aRetryNEVERTakesTheTerm() throws Exception {
        // ⚠️ M13.78 review T4: the retry asks about the current term; electing
        // to ask would have an idle pod's background thread take the lease the
        // moment its holder dies.
        MemoryBinStore store = new MemoryBinStore();
        InProcessTransport transport = new InProcessTransport().withInbox(store, PREFIX);
        FleetSequencer leader = pod(store, "poda", A, transport);
        try (FleetSequencer follower = pod(store, "podb", B, transport)) {
            transport.at(A, leader);
            follower.commit(flush(0));
            transport.unreachable(A);
            assertThatThrownBy(() -> follower.commit(flush(1)))
                    .isInstanceOf(CommitDeferredException.class);
            leader.close();

            follower.retryDeferredDrain();
            assertThat(follower.leading()).as("the retry did not elect").isFalse();
        }
    }
}
