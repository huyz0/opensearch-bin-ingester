// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.sequencer.CommitRequest;
import io.github.huyz0.os.biningester.sequencer.Sequencer;
import java.io.IOException;
import java.time.Clock;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * A {@code DefaultIngest} publishing through the chain holds its own segment
 * before committing and leaves publication to the leaseholder (M10.18, ADR-0075).
 */
class DefaultIngestChainModeTest {

    private static final RunKey KEY = new RunKey(IngestTestSupport.LOGS, 0);

    /** Records what the chain publisher held at the moment of the commit. */
    private static final class Spying implements Sequencer {
        private final Sequencer real;
        private final ChainPublisher[] chain;
        final AtomicLong heldAtCommit = new AtomicLong(-1);
        final List<CommitDelta> committed = new CopyOnWriteArrayList<>();
        volatile boolean fail;
        volatile boolean defer;

        Spying(Sequencer real, ChainPublisher[] chain) {
            this.real = real;
            this.chain = chain;
        }

        @Override
        public CommitDelta commitAll(List<CommitRequest> requests) throws IOException {
            heldAtCommit.set(chain[0].heldBytes());
            if (fail) {
                throw new IOException("the leaseholder is unreachable");
            }
            if (defer) {
                throw new io.github.huyz0.os.biningester.sequencer.CommitDeferredException(
                        "bins/cluster-a/inbox/i1", new IOException("no leaseholder"));
            }
            CommitDelta delta = real.commitAll(requests);
            committed.add(delta);
            return delta;
        }

        @Override
        public long epoch() {
            return real.epoch();
        }

        @Override
        public void close() throws IOException {
            real.close();
        }
    }

    @Test
    void theWriterHoldsItsSegmentBeforeCommittingAndDoesNotPublishItself() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        ChainPublisher[] chain = new ChainPublisher[1];
        Spying sequencer = new Spying(IngestTestSupport.sequencer(store, "pod1"), chain);
        SubscriptionHub hub = new SubscriptionHub();
        List<SubscriptionHub.Push> seen = new CopyOnWriteArrayList<>();
        try (var subscription = hub.subscribe(KEY, SubscriptionHub.assembling(seen::add))) {
            DefaultIngest ingest = new DefaultIngest(
                    IngestTestSupport.pinnedIntervalConfig(IngestTestSupport.NEVER, 4096L),
                    store, IngestTestSupport.PREFIX, "pod1", sequencer, hub,
                    Clock.systemUTC(), index -> IngestTestSupport.LOGS);
            chain[0] = ingest.publishThroughChain(8L << 20);
            AppendResult result = ingest.append(IngestTestSupport.PRINCIPAL, "logs", 0,
                    IngestTestSupport.docs(400)::forEach);
            assertThat(result.recordCount()).isEqualTo(400);
            // ⚠️ CLOSING DRAINS THE INGEST's OWN PUSH QUEUE, so a flush that
            // still published directly has delivered by now -- no race.
            ingest.close();

            assertThat(sequencer.heldAtCommit.get())
                    .as("the bytes were held BEFORE the commit, so a fast push finds them")
                    .isPositive();
            assertThat(seen).as("the flush did not publish its own commit").isEmpty();

            long getsBefore = store.counts().gets();
            chain[0].offer(sequencer.epoch(), sequencer.committed.get(0));
            chain[0].close(); // delivers what is queued
            assertThat(seen).hasSize(1);
            assertThat(store.counts().gets() - getsBefore)
                    .as("published from the bytes the writer held").isZero();
        }
    }

    @Test
    void aDeferredCommitLeavesNothingHeld() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        ChainPublisher[] chain = new ChainPublisher[1];
        Spying sequencer = new Spying(IngestTestSupport.sequencer(store, "pod1"), chain);
        sequencer.defer = true;
        try (DefaultIngest ingest = new DefaultIngest(
                IngestTestSupport.pinnedIntervalConfig(IngestTestSupport.NEVER, 4096L),
                store, IngestTestSupport.PREFIX, "pod1", sequencer, new SubscriptionHub(),
                Clock.systemUTC(), index -> IngestTestSupport.LOGS)) {
            chain[0] = ingest.publishThroughChain(8L << 20);

            ingest.append(IngestTestSupport.PRINCIPAL, "logs", 0,
                    IngestTestSupport.docs(400)::forEach);

            assertThat(sequencer.heldAtCommit.get()).as("the premise: it was held").isPositive();
            assertThat(chain[0].heldBytes())
                    .as("a deferred commit is applied by a drain, whose push holds nothing")
                    .isZero();
            chain[0].close();
        }
    }

    @Test
    void aCommitThatFailsLeavesNothingHeld() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        ChainPublisher[] chain = new ChainPublisher[1];
        Spying sequencer = new Spying(IngestTestSupport.sequencer(store, "pod1"), chain);
        sequencer.fail = true;
        try (DefaultIngest ingest = new DefaultIngest(
                IngestTestSupport.pinnedIntervalConfig(IngestTestSupport.NEVER, 4096L),
                store, IngestTestSupport.PREFIX, "pod1", sequencer, new SubscriptionHub(),
                Clock.systemUTC(), index -> IngestTestSupport.LOGS)) {
            chain[0] = ingest.publishThroughChain(8L << 20);

            assertThatThrownBy(() -> ingest.append(IngestTestSupport.PRINCIPAL, "logs", 0,
                    IngestTestSupport.docs(400)::forEach)).isNotNull();

            assertThat(sequencer.heldAtCommit.get()).as("the premise: it was held").isPositive();
            assertThat(chain[0].heldBytes())
                    .as("a segment no push will ever name is not held forever").isZero();
            chain[0].close();
        }
    }
}
