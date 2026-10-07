// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.PREFIX;
import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.counts;
import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.deltasCarrying;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.CommitDelta;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A deferring pod keeps asking for its drain until one succeeds, without
 * waiting for its next write (M13.78, ADR-0058 amended).
 *
 * <p>⚠️ **MEASURED BY M13.45**: the drain a pod asks for at the heal can time
 * out on a large inbox and complete on the leaseholder anyway; the flush it
 * then defers waits for the pod's NEXT write, and an idle pod's acked writes
 * stay invisible for as long as it stays idle.
 */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class DeferredDrainRetryTest {

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

    /** A store whose inbox reads fail while {@code failing} is set. */
    private static final class FailingInbox implements BinStore {
        private final BinStore inner;
        private volatile boolean failing;

        FailingInbox(BinStore inner) {
            this.inner = inner;
        }

        @Override
        public java.io.InputStream get(String key) throws IOException {
            if (failing && key.startsWith(Inbox.prefixFor(PREFIX))) {
                throw new IOException("the inbox read failed");
            }
            return inner.get(key);
        }

        @Override
        public java.io.InputStream getRange(String key, long start, long endIncl)
                throws IOException {
            return inner.getRange(key, start, endIncl);
        }

        @Override
        public java.util.Optional<io.github.huyz0.os.biningester.binstore.ObjectStat> stat(
                String key) throws IOException {
            return inner.stat(key);
        }

        @Override
        public io.github.huyz0.os.biningester.binstore.Version put(String key,
                io.github.huyz0.os.biningester.binstore.Body body) throws IOException {
            return inner.put(key, body);
        }

        @Override
        public java.util.Optional<io.github.huyz0.os.biningester.binstore.Version> putIfAbsent(
                String key, io.github.huyz0.os.biningester.binstore.Body body) throws IOException {
            return inner.putIfAbsent(key, body);
        }

        @Override
        public java.util.Optional<io.github.huyz0.os.biningester.binstore.Version> putIfMatch(
                String key, io.github.huyz0.os.biningester.binstore.Body body,
                io.github.huyz0.os.biningester.binstore.Version expected) throws IOException {
            return inner.putIfMatch(key, body, expected);
        }

        @Override
        public io.github.huyz0.os.biningester.binstore.MultipartWriter multipart(String key)
                throws IOException {
            return inner.multipart(key);
        }

        @Override
        public io.github.huyz0.os.biningester.binstore.ListPage list(String prefix,
                String startAfter, int maxKeys) throws IOException {
            return inner.list(prefix, startAfter, maxKeys);
        }

        @Override
        public void delete(java.util.List<String> keys) throws IOException {
            inner.delete(keys);
        }

        @Override
        public io.github.huyz0.os.biningester.binstore.Capabilities capabilities() {
            return inner.capabilities();
        }

        @Override
        public void close() throws IOException {
            inner.close();
        }
    }

    private static int intents(BinStore store) throws IOException {
        return store.list(Inbox.prefixFor(PREFIX), null, 100).objects().size();
    }

    /** Counts the drains a pod asks for, and runs a hook inside each. */
    private static final class Asking implements SequencerTransport {
        private final InProcessTransport inner;
        private final AtomicInteger drains = new AtomicInteger();
        private volatile Runnable duringDrain = () -> { };

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
            duringDrain.run();
        }

        @Override
        public void close() {
        }
    }

    @Test
    void anIDLEDeferringPodsIntentIsAppliedByARetryWithoutAnotherWrite() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        InProcessTransport transport = new InProcessTransport().withInbox(store, PREFIX);
        try (FleetSequencer leader = pod(store, "poda", A, transport);
                FleetSequencer follower = pod(store, "podb", B, transport)) {
            transport.at(A, leader);
            follower.commit(flush(0));
            transport.unreachable(A);
            assertThatThrownBy(() -> follower.commit(flush(1)))
                    .isInstanceOf(CommitDeferredException.class);

            transport.at(A, leader);
            follower.retryDeferredDrain();

            assertThat(deltasCarrying(store, "seg/podb-1"))
                    .as("⚠️ THE DEFERRED FLUSH IS COMMITTED WITH NO FURTHER WRITE FROM ITS POD")
                    .isEqualTo(1);
            assertThat(intents(store)).as("and its intent is gone").isZero();
        }
    }

    @Test
    void aRETRYStillCutOffKeepsDeferringAndTheNextAsksAgain() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        InProcessTransport inner = new InProcessTransport().withInbox(store, PREFIX);
        Asking transport = new Asking(inner);
        try (FleetSequencer leader = pod(store, "poda", A, inner);
                FleetSequencer follower = pod(store, "podb", B, transport)) {
            inner.at(A, leader);
            follower.commit(flush(0));
            inner.unreachable(A);
            assertThatThrownBy(() -> follower.commit(flush(1)))
                    .isInstanceOf(CommitDeferredException.class);
            int asked = transport.drains.get();

            follower.retryDeferredDrain();
            assertThat(transport.drains.get()).as("a retry asks").isEqualTo(asked + 1);
            assertThat(intents(store)).as("cut off still: the intent stays").isEqualTo(1);

            inner.at(A, leader);
            follower.retryDeferredDrain();
            assertThat(transport.drains.get()).as("⚠️ AND A FAILED ASK IS ASKED AGAIN")
                    .isEqualTo(asked + 2);
            assertThat(deltasCarrying(store, "seg/podb-1")).isEqualTo(1);
        }
    }

    @Test
    void aDeferringPodHOLDINGTheTermDrainsItsOwnIntentOnARetry() throws Exception {
        // ⚠️ M13.78 review T1: a pod that deferred, then took the term and
        // failed its own drain, holds the lease -- a retry asking "the
        // leaseholder" asks itself and is refused, and the trigger strands on
        // the leader instead.
        FailingInbox store = new FailingInbox(new MemoryBinStore());
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
                    .as("the term is taken, and its own drain fails")
                    .isInstanceOf(IOException.class);
            assertThat(follower.leading()).as("the premise: it holds the term").isTrue();

            store.failing = false;
            follower.retryDeferredDrain();
            assertThat(deltasCarrying(store, "seg/podb-1"))
                    .as("⚠️ A LEASEHOLDER's RETRY DRAINS LOCALLY").isEqualTo(1);
            assertThat(intents(store)).isZero();
        }
    }

    @Test
    void aCLOSEDPodAsksForNothing() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        InProcessTransport inner = new InProcessTransport().withInbox(store, PREFIX);
        Asking transport = new Asking(inner);
        try (FleetSequencer leader = pod(store, "poda", A, inner)) {
            FleetSequencer follower = pod(store, "podb", B, transport);
            inner.at(A, leader);
            follower.commit(flush(0));
            inner.unreachable(A);
            assertThatThrownBy(() -> follower.commit(flush(1)))
                    .isInstanceOf(CommitDeferredException.class);
            inner.at(A, leader);
            follower.close();
            int asked = transport.drains.get();

            follower.retryDeferredDrain();
            assertThat(transport.drains.get())
                    .as("⚠️ A POD THAT HAS SHUT DOWN SENDS NOTHING MORE").isEqualTo(asked);
        }
    }

    @Test
    void aPodNOTDeferringAsksForNothing() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        InProcessTransport inner = new InProcessTransport().withInbox(store, PREFIX);
        Asking transport = new Asking(inner);
        try (FleetSequencer leader = pod(store, "poda", A, inner);
                FleetSequencer follower = pod(store, "podb", B, transport)) {
            inner.at(A, leader);
            follower.commit(flush(0));
            inner.unreachable(A);
            assertThatThrownBy(() -> follower.commit(flush(1)))
                    .isInstanceOf(CommitDeferredException.class);
            inner.at(A, leader);
            follower.retryDeferredDrain();
            int asked = transport.drains.get();

            follower.retryDeferredDrain();
            follower.retryDeferredDrain();
            assertThat(transport.drains.get())
                    .as("⚠️ ONE REQUEST PER DEFERRING POD PER INTERVAL, AND NONE ONCE DRAINED "
                            + "(cost.md rule 6)")
                    .isEqualTo(asked);
        }
    }

    @Test
    void aFlushDEFERREDDuringARetrysDrainIsNotClearedByIt() throws Exception {
        // ⚠️ THE RETRY RUNS BESIDE THE FLUSH WORKER: a drain that cleared a
        // deferral raised while it was in flight would let the next flush
        // forward past an intent it never applied, and the drain after would
        // delete that intent as at or below the mark -- an acked write lost
        // (ADR-0058's second precondition).
        MemoryBinStore store = new MemoryBinStore();
        InProcessTransport inner = new InProcessTransport().withInbox(store, PREFIX);
        Asking transport = new Asking(inner);
        try (FleetSequencer leader = pod(store, "poda", A, inner);
                FleetSequencer follower = pod(store, "podb", B, transport)) {
            inner.at(A, leader);
            follower.commit(flush(0));
            inner.unreachable(A);
            assertThatThrownBy(() -> follower.commit(flush(1)))
                    .isInstanceOf(CommitDeferredException.class);

            inner.at(A, leader);
            transport.duringDrain = () -> {
                transport.duringDrain = () -> { };
                inner.unreachable(A);
                assertThatThrownBy(() -> follower.commit(flush(2)))
                        .isInstanceOf(CommitDeferredException.class);
                inner.at(A, leader);
            };
            follower.retryDeferredDrain();
            follower.commit(flush(3));
        }
        for (long seq = 1; seq <= 3; seq++) {
            assertThat(deltasCarrying(store, "seg/podb-" + seq))
                    .as("⚠️ FLUSH %d IS COMMITTED EXACTLY ONCE -- 0 is the acked write the "
                            + "retry cleared and the next forward overtook", seq)
                    .isEqualTo(1);
        }
        assertThat(intents(store)).isZero();
    }
}
