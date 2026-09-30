// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.sequencer.BatchingSequencer;
import io.github.huyz0.os.biningester.sequencer.FleetSequencer;
import io.github.huyz0.os.biningester.sequencer.InboxDrain;
import io.github.huyz0.os.biningester.sequencer.LeaseChallenge;
import io.github.huyz0.os.biningester.sequencer.LeaseConfig;
import io.github.huyz0.os.biningester.sequencer.LeaseManager;
import io.github.huyz0.os.biningester.sequencer.LocalSequencer;
import io.github.huyz0.os.biningester.sequencer.SequencerTransport;
import java.time.Clock;
import java.time.Duration;

/**
 * The fleet sequencer's construction for {@link Assembly}: the lease, the term
 * a takeover starts, and what runs beside it (M13.1b).
 *
 * <p>⚠️ SPLIT OUT so fast mode's sequencing lands beside this rather than
 * growing {@code Assembly} past its ceiling (M13 criterion 1).
 */
final class SequencerAssembly {

    /**
     * ⚠️ 8, the value every construction site in the tree passes — it bounds
     * how many ancestor seals one takeover will redrive before giving up.
     */
    static final int SEAL_REDRIVE_BUDGET = 8;

    /**
     * How long the leader's commit window stays open once a commit arrives
     * (M8.50).
     *
     * <p>⚠️ **SHORT, BECAUSE THE BATCH COMES FROM THE PUT, NOT THE WAIT.** While
     * one delta is in the store, every commit that arrives queues, and the next
     * window takes them all: that is where one PUT per window comes from under
     * load. The window itself only adds latency to a commit that arrives alone.
     */
    static final Duration COMMIT_WINDOW = Duration.ofMillis(5);

    private SequencerAssembly() {
    }

    static LeaseConfig leaseConfig(ServerConfig config) {
        return new LeaseConfig(config.prefix(), config.podId(), config.endpoint(),
                config.podUid(), config.leaseTtl(), config.leaseRenewInterval());
    }

    /**
     * ⚠️ THE TERM IS TAKEN BY THE RETURNED SEQUENCER'S CONSTRUCTOR, so a caller
     * that fails after this returns must close it or leave the lease held.
     */
    static FleetSequencer create(ServerConfig config, BinStore store,
            SequencerTransport transport, Clock clock, LeaseChallenge challenge,
            RetentionAssembly.LeaseManagerFactory leaseManagerFactory,
            Assembly.BackfillStarter backfillStarter, IngesterMetrics metrics)
            throws java.io.IOException {
        LeaseConfig leases = leaseConfig(config);
        LeaseManager manager = leaseManagerFactory.create(store, leases, clock, challenge);
        return new FleetSequencer(store, leases, transport,
                () -> LocalSequencer.start(store, config.prefix(), manager, SEAL_REDRIVE_BUDGET)
                        .map(term -> {
                            // ⚠️ M8.42: THE CHAIN BELOW THE REPLAY, read once per
                            // takeover and off the election's path. HERE, not in
                            // `LocalSequencer.start`, which M4.9 bounds to a
                            // small constant and tests to the request.
                            // ⚠️ ONLY A TAKEOVER HAS A CHAIN BELOW IT: the first term
                            // of a cluster (epoch 1) would otherwise widen its sweep
                            // over a retention window of empty hours, a LIST each.
                            if (term.epoch() > 1) {
                                backfillStarter.start(store, config.prefix(),
                                        term.chain(), term::serving);
                            }
                            // ⚠️ M8.14a: the intents of pods that died deferring
                            // have nobody else to ask for a drain.
                            InboxDrain.inBackground(store, config.prefix(), term,
                                    metrics::failedIntentBatch);
                            return new BatchingSequencer(term, COMMIT_WINDOW);
                        }), challenge, false, metrics::failedIntentBatch);
    }
}
