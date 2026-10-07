// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.sequencer.BatchingSequencer;
import io.github.huyz0.os.biningester.sequencer.EmptyTermCloser;
import io.github.huyz0.os.biningester.sequencer.FastLeaderTerm;
import io.github.huyz0.os.biningester.sequencer.FastLeaseFence;
import io.github.huyz0.os.biningester.sequencer.FastTermOpening;
import io.github.huyz0.os.biningester.sequencer.FastTermStart;
import io.github.huyz0.os.biningester.sequencer.FleetSequencer;
import io.github.huyz0.os.biningester.sequencer.InboxDrain;
import io.github.huyz0.os.biningester.sequencer.JoinDesk;
import io.github.huyz0.os.biningester.sequencer.LeaseChallenge;
import io.github.huyz0.os.biningester.sequencer.LeaseConfig;
import io.github.huyz0.os.biningester.sequencer.LeaseManager;
import io.github.huyz0.os.biningester.sequencer.LocalSequencer;
import io.github.huyz0.os.biningester.sequencer.MonotonicClock;
import io.github.huyz0.os.biningester.sequencer.RosterJoins;
import io.github.huyz0.os.biningester.sequencer.SequencerTransport;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

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

    /**
     * ADR-0081's {@code min_upload_interval}: at most one roster write of JOINs
     * per interval (§1), however many pods join (M13.27j).
     */
    static final Duration MIN_UPLOAD_INTERVAL = Duration.ofMillis(250);

    private SequencerAssembly() {
    }

    static LeaseConfig leaseConfig(ServerConfig config) {
        return new LeaseConfig(config.prefix(), config.podId(), config.endpoint(),
                config.podUid(), config.leaseTtl(), config.leaseRenewInterval());
    }

    /**
     * A monotonic clock read off {@code wall}, for every entry point but
     * {@code Main}'s (M13.27d): a test's simulated clock is both of its clocks.
     *
     * <p>⚠️ NEVER {@code Main}'S: there the two clocks must be independent, or
     * ADR-0081 §3's two-clock fence is one clock read twice.
     */
    static MonotonicClock following(Clock wall) {
        return () -> wall.millis() * 1_000_000L;
    }

    /**
     * ⚠️ THE TERM IS TAKEN BY THE RETURNED SEQUENCER'S CONSTRUCTOR, so a caller
     * that fails after this returns must close it or leave the lease held.
     *
     * <p>⚠️ EVERY ELECTED TERM STARTS ITS FAST TERM BEFORE IT SERVES (ADR-0081
     * §5 steps 2-3; M13.27d), fast index or not: the sequencer is handed to
     * nobody until {@link FastTermOpening#open} returns, so no default-path
     * commit is admitted before step 3. A term deposed by a newer one, or whose
     * start fails, is given back.
     */
    static FleetSequencer create(ServerConfig config, BinStore store,
            SequencerTransport transport, Clock clock, MonotonicClock mono,
            LeaseChallenge challenge, RetentionAssembly.LeaseManagerFactory leaseManagerFactory,
            Assembly.BackfillStarter backfillStarter, IngesterMetrics metrics)
            throws java.io.IOException {
        return create(config, store, transport, clock, mono, challenge, leaseManagerFactory,
                backfillStarter, metrics, InboxDrain::inBackground);
    }

    /** {@link #retryDeferredDrains(Runnable, Duration)} every renew interval of {@code config}. */
    static AutoCloseable retryDeferredDrains(FleetSequencer fleet, ServerConfig config) {
        return retryDeferredDrains(fleet::retryDeferredDrain, retryInterval(config));
    }

    /** How often a deferring pod asks again: its renew interval, never its TTL. */
    static Duration retryInterval(ServerConfig config) {
        return leaseConfig(config).renewInterval();
    }

    /**
     * Asks a deferring pod's drain again {@code every} interval on its own
     * virtual thread, and returns what stops it (M13.78, ADR-0058 amended).
     *
     * <p>⚠️ **A REQUEST PER DEFERRING POD PER INTERVAL, NOT A LIST TIMER**: a pod
     * not deferring asks nothing, and a leader runs no timer of its own.
     */
    static AutoCloseable retryDeferredDrains(Runnable retry, Duration every) {
        java.util.concurrent.ScheduledExecutorService scheduler =
                java.util.concurrent.Executors.newSingleThreadScheduledExecutor(
                        Thread.ofVirtual().name("deferred-drain").factory());
        long nanos = every.toNanos();
        scheduler.scheduleWithFixedDelay(() -> {
            // ⚠️ A THROW OUT OF A SCHEDULED TASK CANCELS EVERY LATER RUN OF IT,
            // and the pod's deferred flushes would wait for its next write again.
            try {
                retry.run();
            } catch (RuntimeException failed) {
                RETRY_LOG.log(System.Logger.Level.WARNING,
                        "a deferred drain's retry failed; the next interval asks again", failed);
            }
        }, nanos, nanos, TimeUnit.NANOSECONDS);
        return () -> {
            scheduler.shutdownNow();
            scheduler.awaitTermination(5, TimeUnit.SECONDS);
        };
    }

    private static final System.Logger RETRY_LOG =
            System.getLogger(SequencerAssembly.class.getName());

    /** Starts the dead pods' inbox drain on a term just started: {@link InboxDrain}. */
    @FunctionalInterface
    interface DrainStarter {
        void start(BinStore store, String prefix, LocalSequencer term,
                java.util.function.LongConsumer failedBatchSize);
    }

    /**
     * The same, with the inbox drain's start injected, so a test can see when
     * it starts (M13.27o: after the term start, ADR-0081 §5 R3-2).
     */
    static FleetSequencer create(ServerConfig config, BinStore store,
            SequencerTransport transport, Clock clock, MonotonicClock mono,
            LeaseChallenge challenge, RetentionAssembly.LeaseManagerFactory leaseManagerFactory,
            Assembly.BackfillStarter backfillStarter, IngesterMetrics metrics,
            DrainStarter drainStarter) throws java.io.IOException {
        LeaseConfig leases = leaseConfig(config);
        // ⚠️ THE SEQUENCER'S LEASE ONLY reports to the fence; the GC lease's
        // holder exposes nothing.
        FastLeaseFence fence = new FastLeaseFence(config.leaseTtl(), clock, mono);
        LeaseManager manager = leaseManagerFactory.create(store, leases, clock, challenge, fence);
        // ⚠️ AN EMPTY TERM RECORD, UNTIL A FAST COMMIT CAN BE ASSIGNED (M13.27d
        // review round 1, P1): a term recording a wal_quorum stays open until
        // M13.33's takeover decides it, and every later term with it -- the
        // walk growing with history M13.27g removed, for a term in which no
        // fast offset can be assigned before M13.27m. M13.27l records the
        // catalog's values.
        FastTermOpening opening = new FastTermOpening(new FastTermStart(store, config.prefix()),
                new EmptyTermCloser(store, config.prefix())::close, fence,
                new Roster.Incarnation(config.podId(), config.podUid(), config.az(),
                        config.endpoint()), Map::of);
        return new FleetSequencer(store, leases, transport, () -> {
            Optional<LocalSequencer> won =
                    LocalSequencer.start(store, config.prefix(), manager, SEAL_REDRIVE_BUDGET);
            if (won.isEmpty()) {
                return Optional.<BatchingSequencer>empty();
            }
            LocalSequencer term = won.get();
            Optional<FastTermOpening.Opened> opened = opening.open(term.epoch(), term::close);
            if (opened.isEmpty()) {
                return Optional.<BatchingSequencer>empty();
            }
            // ⚠️ THE TERM IS GIVEN BACK IF ANY STEP BELOW THROWS (M13.76): the
            // lease is written and renewed, and the election catches only an
            // IOException -- an unchecked failure here, an Error from a thread
            // that could not start among them, left a renewed lease
            // naming a pod that never published its term, every commit
            // forwarded to it answered 409 until the process ended.
            try {
                return Optional.of(started(config, store, mono, metrics, backfillStarter,
                        drainStarter, term, opened.get()));
            } catch (RuntimeException | Error failed) {
                try {
                    term.close();
                } catch (java.io.IOException alsoFailed) {
                    failed.addSuppressed(alsoFailed);
                }
                throw failed;
            }
        }, challenge, false, metrics::failedIntentBatch);
    }

    /** The term's start after its fast term opened: attached, backfilling, draining, batched. */
    private static BatchingSequencer started(ServerConfig config, BinStore store,
            MonotonicClock mono, IngesterMetrics metrics, Assembly.BackfillStarter backfillStarter,
            DrainStarter drainStarter, LocalSequencer term, FastTermOpening.Opened opened) {
        // ⚠️ M13.27j: the term's JOINs, answered once its roster lists them.
        term.attach(new FastLeaderTerm(opened, new JoinDesk(
                new RosterJoins(store, config.prefix(), term.epoch(), mono,
                        MIN_UPLOAD_INTERVAL), nanos -> TimeUnit.NANOSECONDS.sleep(nanos)),
                term::committedNext, store, config.prefix(), mono,
                MIN_UPLOAD_INTERVAL));
        // ⚠️ M8.42: THE CHAIN BELOW THE REPLAY, read once per
        // takeover and off the election's path. HERE, not in
        // `LocalSequencer.start`, which M4.9 bounds to a
        // small constant and tests to the request.
        // ⚠️ ONLY A TAKEOVER HAS A CHAIN BELOW IT: the first term
        // of a cluster (epoch 1) would otherwise widen its sweep
        // over a retention window of empty hours, a LIST each.
        if (term.epoch() > 1) {
            backfillStarter.start(store, config.prefix(), term.chain(), term::serving);
        }
        // ⚠️ M8.14a: the intents of pods that died deferring
        // have nobody else to ask for a drain.
        drainStarter.start(store, config.prefix(), term, metrics::failedIntentBatch);
        return new BatchingSequencer(term, COMMIT_WINDOW);
    }
}
