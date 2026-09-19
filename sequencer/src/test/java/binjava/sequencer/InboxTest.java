// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import static binjava.sequencer.DedupFixtures.PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import binjava.binstore.CountingBinStore;
import binjava.binstore.backend.MemoryBinStore;
import binjava.format.CommitDelta;
import binjava.format.CommitRequestFrame;
import binjava.format.RunKey;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * A pod that cannot reach its sequencer writes a commit INTENT and defers
 * (M8.14a, ADR-0058, FR-4 as amended, research 03 section 10).
 *
 * <p>⚠️ **ONE INTENT PER POD PER SLOT PER FLUSH, NEVER PER INDEX** (cost.md
 * rule 6): a flush over three indices is one request with three streams in it,
 * and so one PUT.
 *
 * <p>⚠️ **A STORE THAT CANNOT TAKE THE INTENT FAILS THE FLUSH**: a pod cut off
 * from the store as well has nothing durable to ack on, and criterion 12 says
 * its acks STOP.
 */
class InboxTest {

    private static final String A = "pod-a:9000";
    private static final String B = "pod-b:9000";

    private static LeaseConfig config(String podId, String endpoint) {
        return new LeaseConfig(PREFIX, podId, endpoint,
                Duration.ofSeconds(10), Duration.ofSeconds(3));
    }

    /** A flush over THREE indices: one request, three streams. */
    private static CommitRequest flush(long seq) {
        return new CommitRequest("podb", "inc-1", seq, "seg/" + seq, Map.of(
                new RunKey(UUID.randomUUID(), 0), 3,
                new RunKey(UUID.randomUUID(), 0), 4,
                new RunKey(UUID.randomUUID(), 2), 5));
    }

    private static final SequencerTransport UNREACHABLE = new SequencerTransport() {
        @Override
        public CommitDelta send(String endpoint, CommitRequest request) throws IOException {
            throw new IOException("connect timed out: " + endpoint);
        }

        @Override
        public void close() {
        }
    };

    private static MemoryBinStore leaseHeldByA() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        assertThat(new LeaseManager(store, config("poda", A), java.time.Clock.systemUTC())
                .tryAcquire()).isPresent();
        return store;
    }

    private static FleetSequencer follower(binjava.binstore.BinStore store) {
        return new FleetSequencer(store, config("podb", B), UNREACHABLE,
                () -> java.util.Optional.empty());
    }

    @Test
    void anUNREACHABLESequencerMeansONEIntentAndADEFERREDCommit() throws Exception {
        MemoryBinStore backing = leaseHeldByA();
        CountingBinStore store = new CountingBinStore(backing);
        CommitRequest request = flush(7);

        try (FleetSequencer fleet = follower(store)) {
            long putsBefore = store.counts().puts();
            assertThatThrownBy(() -> fleet.commit(request))
                    .as("⚠️ DEFERRED, NOT FAILED: the records and the intent are durable")
                    .isInstanceOf(CommitDeferredException.class);

            assertThat(store.counts().puts() - putsBefore)
                    .as("⚠️ ONE PUT for a flush over three indices, not one per index")
                    .isEqualTo(1);
        }
        byte[] bytes;
        try (InputStream in = backing.get(Inbox.keyFor(PREFIX, request))) {
            bytes = in.readAllBytes();
        }
        CommitRequestFrame intent = CommitRequestFrame.decode(bytes);
        assertThat(new CommitRequest(intent.podId(), intent.incarnationId(), intent.flushSeq(),
                intent.segmentKey(), intent.recordCounts()))
                .as("the intent IS the request, whole: its triple is what dedups the drain")
                .isEqualTo(request);
    }

    @Test
    void aRETRIEDFlushWritesNoSECONDIntent() throws Exception {
        MemoryBinStore backing = leaseHeldByA();
        CommitRequest request = flush(8);

        try (FleetSequencer fleet = follower(backing)) {
            assertThatThrownBy(() -> fleet.commit(request))
                    .isInstanceOf(CommitDeferredException.class);
            assertThatThrownBy(() -> fleet.commit(request))
                    .as("the intent is already durable, so the retry defers too")
                    .isInstanceOf(CommitDeferredException.class);
        }
        assertThat(backing.list(PREFIX + "/ctl/inbox/", null, 100).objects())
                .as("putIfAbsent: one object per flush, however often it is retried")
                .hasSize(1);
    }

    @Test
    void aSTOREThatCannotTakeTheIntentFAILSTheFlush() throws Exception {
        MemoryBinStore backing = leaseHeldByA();
        binjava.binstore.BinStore refusing = (binjava.binstore.BinStore)
                java.lang.reflect.Proxy.newProxyInstance(
                        binjava.binstore.BinStore.class.getClassLoader(),
                        new Class<?>[] {binjava.binstore.BinStore.class}, (proxy, m, args) -> {
                            if (m.getName().equals("putIfAbsent")) {
                                throw new IOException("the store is unreachable too");
                            }
                            try {
                                return m.invoke(backing, args);
                            } catch (java.lang.reflect.InvocationTargetException e) {
                                throw e.getCause();
                            }
                        });

        try (FleetSequencer fleet = follower(refusing)) {
            assertThatThrownBy(() -> fleet.commit(flush(9)))
                    .as("⚠️ NOTHING DURABLE TO ACK ON: criterion 12's acks stop")
                    .isInstanceOf(IOException.class)
                    .isNotInstanceOf(CommitDeferredException.class);
        }
    }
}
