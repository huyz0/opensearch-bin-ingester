// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.CrossAzBytes;
import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.Lease;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.http.HttpFastTransport;
import io.github.huyz0.os.biningester.sequencer.EpochFence;
import io.github.huyz0.os.biningester.sequencer.FastFrameRouter;
import io.github.huyz0.os.biningester.sequencer.HeldReports;
import io.github.huyz0.os.biningester.sequencer.JoinedTerms;
import io.github.huyz0.os.biningester.sequencer.LeaderWatch;
import io.github.huyz0.os.biningester.sequencer.TermJoiner;
import java.io.InputStream;
import java.time.Duration;

/**
 * A node's side of the fast peer protocol that is not the leader's (M13.27n):
 * its router metered against the asker's zone, and the watch that joins each
 * term it learns from the lease.
 *
 * <p>⚠️ THE WATCH LOOKS ONCE PER LEASE RENEW INTERVAL (ADR-0081 §1, amended by
 * M13.27n): one GET of the lease per pod per interval, scaling with pods.
 */
final class FastPeer implements AutoCloseable {

    /** Journaled when the watch stops, before readiness fails (M13.27n). */
    static final String WATCH_STOPPED = "fast leader watch stopped";

    private final Thread watching;
    private final java.util.concurrent.atomic.AtomicLong leaseReads;
    private final java.util.concurrent.atomic.AtomicBoolean stopped;

    private FastPeer(Thread watching, java.util.concurrent.atomic.AtomicLong leaseReads,
            java.util.concurrent.atomic.AtomicBoolean stopped) {
        this.watching = watching;
        this.leaseReads = leaseReads;
        this.stopped = stopped;
    }

    /** How many times the watch has read the lease, for a test. */
    long leaseReads() {
        return leaseReads.get();
    }

    /**
     * The node's router, answering as {@code config}'s incarnation behind
     * {@code disk}'s fence, every answer counted against the asker's zone:
     * the one a JOIN's or a DEPART's incarnation names, learned into
     * {@code zones}, and for any other frame its sender's as learned (M13.64)
     * -- unknown, so cross-AZ, only for a sender never seen.
     */
    static FastFrameRouter router(ServerConfig config, FastDisk disk, CrossAzBytes crossAz,
            PeerZones zones) {
        return router(config, disk, crossAz, zones, () -> 0L, null);
    }

    /**
     * The same, its fence raised to {@code ownTerm} before it answers (M13.82),
     * refusing a sender {@code view} does not list (M13.71).
     */
    static FastFrameRouter router(ServerConfig config, FastDisk disk, CrossAzBytes crossAz,
            PeerZones zones, java.util.function.LongSupplier ownTerm,
            io.github.huyz0.os.biningester.http.EndpointSliceView view) {
        return router(config.podUid(), disk.fence(), crossAz, zones, ownTerm, view);
    }

    static FastFrameRouter router(String selfUid, EpochFence fence, CrossAzBytes crossAz,
            PeerZones zones) {
        return router(selfUid, fence, crossAz, zones, () -> 0L,
                (io.github.huyz0.os.biningester.http.EndpointSliceView) null);
    }

    static FastFrameRouter router(String selfUid, EpochFence fence, CrossAzBytes crossAz,
            PeerZones zones, io.github.huyz0.os.biningester.http.EndpointSliceView view) {
        return router(selfUid, fence, crossAz, zones, () -> 0L, view);
    }

    static FastFrameRouter router(String selfUid, EpochFence fence, CrossAzBytes crossAz,
            PeerZones zones, java.util.function.LongSupplier ownTerm,
            io.github.huyz0.os.biningester.http.EndpointSliceView view) {
        return new FastFrameRouter(selfUid, fence, (header, asked, bytes) ->
                crossAz.sent(FastFrame.isControl(header.kind())
                                ? CrossAzBytes.Transport.FAST_CONTROL
                                : CrossAzBytes.Transport.FAST_DATA,
                        azOf(header, asked, zones), bytes), ownTerm, liveness(view));
    }

    /**
     * Live is what the membership view lists, ready or not (M13.71).
     *
     * <p>⚠️ **NO EVIDENCE JUDGES NOTHING**: without membership, or before the
     * view's first event, it lists nobody, and refusing then would refuse
     * every pod. So a node that runs without membership, or whose watch has
     * not yet read a slice, keeps ADR-0084's residual. ⚠️ A watch outage keeps
     * the last state: a pod started during it is refused until the watch
     * reads it, and one deleted during it stays listed until then.
     */
    static FastFrameRouter.Liveness liveness(
            io.github.huyz0.os.biningester.http.EndpointSliceView view) {
        if (view == null) {
            return FastFrameRouter.Liveness.ANY;
        }
        return uid -> !view.hasMembers() || view.lists(uid);
    }

    private static String azOf(FastFrame.Header header, FastFrame.Body asked, PeerZones zones) {
        Roster.Incarnation named = switch (asked) {
            case FastFrame.Join join -> join.incarnation();
            case FastFrame.Depart depart -> depart.incarnation();
            case null, default -> null;
        };
        if (named != null) {
            zones.learn(named, header.epoch());
            return named.az();
        }
        return zones.ofUid(header.senderUid()).orElse(null);
    }

    /** What a JOIN reports: what the pod's journal holds, nothing for a diskless pod. */
    static FastFrame.Held held(FastDisk disk) {
        return disk.journal().map(j -> HeldReports.of(j.held())).orElse(FastFrame.Held.NONE);
    }

    /**
     * The epoch of the term {@code held} serves, or 0 where it serves none
     * (M13.82): what the fast route's fence is raised to before it answers.
     */
    static long ownTerm(io.github.huyz0.os.biningester.sequencer.Sequencer held) {
        return io.github.huyz0.os.biningester.sequencer.LocalSequencer.underneath(held)
                .filter(io.github.huyz0.os.biningester.sequencer.LocalSequencer::serving)
                .map(io.github.huyz0.os.biningester.sequencer.LocalSequencer::epoch)
                .orElse(0L);
    }

    /**
     * Whether this pod leads a SERVING term (M13.27n review round 1, P1): a
     * deposed term stays held until a commit through it throws, and an idle
     * pod makes none -- judged by being held, a deposed idle leader would
     * never read the lease, nor join its successor's term.
     */
    static boolean leading(Assembly assembly) {
        return io.github.huyz0.os.biningester.sequencer.LocalSequencer
                .underneath(assembly.heldTerm())
                .map(io.github.huyz0.os.biningester.sequencer.LocalSequencer::serving)
                .orElse(false);
    }

    /** Whether the watch still runs, for a test. */
    boolean watching() {
        return watching.isAlive();
    }

    /**
     * The joiner a node's watch uses: this pod's incarnation, over
     * {@code transport}, reporting what its journal holds (M13.27n review
     * round 2, T7).
     */
    static TermJoiner joiner(ServerConfig config, FastDisk disk,
            TermJoiner.Transport transport) {
        Roster.Incarnation self = new Roster.Incarnation(config.podId(), config.podUid(),
                config.az(), config.endpoint());
        return new TermJoiner(self, transport, disk.fence(), new JoinedTerms(),
                () -> held(disk));
    }

    /** Reads the sequencer lease: one GET (M13.27r, M13.27p). */
    static LeaderWatch.LeaseReader leaseReader(ServerConfig config, BinStore store) {
        String leaseKey = SequencerAssembly.leaseConfig(config).leaseKey();
        return () -> {
            try (InputStream in = store.get(leaseKey)) {
                return Lease.decode(in.readAllBytes());
            }
        };
    }

    /** Starts the watch that joins every term this pod learns of. */
    static FastPeer start(ServerConfig config, BinStore store, FastDisk disk,
            CrossAzBytes crossAz, PeerZones zones, Duration timeout,
            java.util.function.BooleanSupplier leading, LeaderWatch.Sleeper sleeper) {
        return start(config, store, disk, crossAz, zones, timeout, leading, sleeper,
                java.util.Optional.empty());
    }

    /** The same, its JOINs presenting {@code tls} (ADR-0084; M13.52d). */
    static FastPeer start(ServerConfig config, BinStore store, FastDisk disk,
            CrossAzBytes crossAz, PeerZones zones, Duration timeout,
            java.util.function.BooleanSupplier leading, LeaderWatch.Sleeper sleeper,
            java.util.Optional<io.helidon.common.tls.Tls> tls) {
        // ⚠️ M13.64: a JOIN counted at its leader's zone, learned from its roster.
        TermJoiner joiner = joiner(config, disk, zones.learning(
                new HttpFastTransport(timeout, crossAz, tls), store, config.prefix()));
        java.util.concurrent.atomic.AtomicLong reads = new java.util.concurrent.atomic.AtomicLong();
        LeaderWatch.LeaseReader lease = leaseReader(config, store);
        LeaderWatch watch = new LeaderWatch(config.podUid(), () -> {
            reads.incrementAndGet();
            return lease.read();
        }, joiner, leading);
        java.util.concurrent.atomic.AtomicBoolean stopped =
                new java.util.concurrent.atomic.AtomicBoolean();
        Thread watching = Thread.ofVirtual().name("fast-leader-watch").start(() ->
                watch.run(config.leaseRenewInterval(), sleeper, stopped::get));
        return new FastPeer(watching, reads, stopped);
    }

    @Override
    public void close() {
        stopped.set(true);
        watching.interrupt();
        try {
            if (!watching.join(Duration.ofSeconds(5))) {
                System.getLogger(FastPeer.class.getName()).log(System.Logger.Level.WARNING,
                        "the fast leader watch did not stop within 5 s; it stops at its "
                                + "next look");
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
