// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static io.github.huyz0.os.biningester.ingest.IngestTestSupport.LOGS;
import static io.github.huyz0.os.biningester.ingest.IngestTestSupport.PREFIX;
import static io.github.huyz0.os.biningester.ingest.IngestTestSupport.appendOnce;
import static io.github.huyz0.os.biningester.ingest.IngestTestSupport.pinnedIntervalConfig;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.sequencer.FleetSequencer;
import io.github.huyz0.os.biningester.sequencer.InProcessTransport;
import io.github.huyz0.os.biningester.sequencer.LeaseConfig;
import io.github.huyz0.os.biningester.sequencer.LeaseManager;
import io.github.huyz0.os.biningester.sequencer.LocalSequencer;
import io.github.huyz0.os.biningester.sequencer.SequencerTransport;
import io.github.huyz0.os.biningester.sequencer.TestSequencers;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * Two ingesters over one store, and only one of them holds the term (M5.6b).
 *
 * <p>⚠️ THIS TEST IS NOT THE PRODUCTION-FLEET PROOF. It uses
 * {@link InProcessTransport} and a memory store to exercise forwarding at the
 * component seam. The production transport and composition root were added in
 * M8.1/M8.4; their assembled and process-level evidence lives in the M8
 * acceptance tests, not here.
 *
 * <p>⚠️ WHAT THIS TEST DOES NOT PROVE:
 *
 * <ul>
 *   <li>The production socket transport, object-store backend and process
 *       lifecycle are not exercised by this in-memory fixture.</li>
 * </ul>
 *
 * <p>So this is T1 over two simulated pods and a memory store, not the M8
 * assembled or multi-process acceptance evidence. It proves the component
 * commit path only.
 */
class ForwardingIngestTest {

    private static final String A = "pod-a:9000";
    private static final String B = "pod-b:9000";

    /**
     * ⚠️ A TTL AND RENEW INTERVAL LONG ENOUGH THAT THE RENEWER NEVER TICKS.
     * {@code TestSequencers.TTL} is 10 s with a 3 s renew, and the renewer's
     * {@code putIfMatch} is counted by {@link CountingBinStore} like any other
     * PUT -- so the exact request-delta assertion below would read 3 instead of
     * 2 whenever a loaded host let a tick land inside the measured window. The
     * lease never needs to lapse here, so the cheapest fix is to outrun the
     * test rather than to race it.
     */
    private static LeaseConfig config(String podId, String endpoint) {
        return new LeaseConfig(PREFIX, podId, endpoint, Duration.ofHours(1),
                Duration.ofMinutes(30));
    }

    /** A pod's sequencer: it leads if it can, and forwards if it cannot. */
    private static FleetSequencer fleet(BinStore store, String podId, String endpoint,
            InProcessTransport transport) throws IOException {
        LeaseManager leases = new LeaseManager(store, config(podId, endpoint),
                Clock.systemUTC());
        FleetSequencer sequencer = new FleetSequencer(store, config(podId, endpoint), transport,
                () -> LocalSequencer.start(store, PREFIX, leases,
                        TestSequencers.SEAL_REDRIVE_BUDGET));
        if (sequencer.leading()) {
            // ⚠️ A LEADER MUST BE REACHABLE AT ITS OWN ENDPOINT, or the pods
            // forwarding to it have nowhere to send.
            transport.at(endpoint, sequencer);
        }
        return sequencer;
    }

    private static DefaultIngest ingest(BinStore store, String podShortId,
            FleetSequencer sequencer) throws IOException {
        // ⚠️ A FLUSH INTERVAL THAT NEVER FIRES, so every flush below is one the
        // test drove. A timer-driven flush would make the offsets a race between
        // two pods rather than a stated order.
        return new DefaultIngest(pinnedIntervalConfig(Duration.ofDays(1), 8L << 20),
                store, PREFIX, podShortId, sequencer, new SubscriptionHub(),
                Clock.systemUTC(), index -> LOGS);
    }

    @Test
    void aPodThatDoesNotHoldTheLeaseSTILL_INGESTS() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        InProcessTransport transport = new InProcessTransport();
        FleetSequencer leaderSeq = fleet(store, "poda", A, transport);
        FleetSequencer followerSeq = fleet(store, "podb", B, transport);
        assertThat(leaderSeq.leading()).isTrue();
        assertThat(followerSeq.leading())
                .as("the second pod lost the election, which is the normal case")
                .isFalse();

        try (DefaultIngest leader = ingest(store, "poda", leaderSeq);
                DefaultIngest follower = ingest(store, "podb", followerSeq)) {

            assertThat(appendOnce(leader, "logs", 0, 3).firstOffset()).isZero();

            // ⚠️ THE ASSERTION THE WHOLE MILESTONE IS FOR. A producer is routed
            // by a load balancer, so most producers reach a pod that is not the
            // leaseholder. Before this, that pod could not commit at all.
            assertThat(appendOnce(follower, "logs", 0, 2).firstOffset())
                    .as("the follower's records continue the leader's ONE total order")
                    .isEqualTo(3L);
        } finally {
            followerSeq.close();
            leaderSeq.close();
        }
    }

    @Test
    void theFollowerWritesItsOwnSEGMENTButNotTheCHAIN() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        InProcessTransport transport = new InProcessTransport();
        FleetSequencer leaderSeq = fleet(store, "poda", A, transport);
        FleetSequencer followerSeq = fleet(store, "podb", B, transport);

        try (DefaultIngest leader = ingest(store, "poda", leaderSeq);
                DefaultIngest follower = ingest(store, "podb", followerSeq)) {
            appendOnce(leader, "logs", 0, 3);
            long putsBefore = store.counts().puts();
            long listsBefore = store.counts().lists();
            appendOnce(follower, "logs", 0, 2);

            // ⚠️ TWO PUTS AND NO MORE: the follower's own segment, and the ONE
            // commit delta the leaseholder writes on its behalf. Forwarding the
            // segment bytes too, or writing a per-forward intent object, shows
            // up here.
            assertThat(store.counts().puts() - putsBefore)
                    .as("the follower's segment, plus the leaseholder's single delta")
                    .isEqualTo(2L);
            // ⚠️ AND NO LIST, because discovery by listing is how a write path
            // acquires a cost that scales with objects rather than with
            // segments. ⚠️ THIS PINS THE WRITE HALF ONLY: a forwarded commit
            // also spends a stat and two gets on the lease key, which is the
            // per-pod read cost M5.6g owns and MEASURES at ~13% of the
            // write-path bill. Nothing here constrains that, and saying so is
            // the point -- an earlier draft of this comment claimed the whole
            // of cost.md's "segments, never pods" rule on a PUT count alone.
            assertThat(store.counts().lists() - listsBefore)
                    .as("zero LIST on the write path")
                    .isZero();
        } finally {
            followerSeq.close();
            leaderSeq.close();
        }
    }
}
