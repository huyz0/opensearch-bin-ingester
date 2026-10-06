// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.CrossAzBytes;
import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.Lease;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.http.HttpFastTransport;
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
     * {@code disk}'s fence, every answer counted against the asker's zone
     * where the frame names it -- a JOIN's or a DEPART's incarnation -- and
     * as cross-AZ where it does not.
     */
    static FastFrameRouter router(ServerConfig config, FastDisk disk, CrossAzBytes crossAz) {
        return new FastFrameRouter(config.podUid(), disk.fence(), (header, asked, bytes) ->
                crossAz.sent(CrossAzBytes.Transport.FAST_FRAME, azOf(asked), bytes));
    }

    private static String azOf(FastFrame.Body asked) {
        return switch (asked) {
            case FastFrame.Join join -> join.incarnation().az();
            case FastFrame.Depart depart -> depart.incarnation().az();
            case null, default -> null;
        };
    }

    /** What a JOIN reports: what the pod's journal holds, nothing for a diskless pod. */
    static FastFrame.Held held(FastDisk disk) {
        return disk.journal().map(j -> HeldReports.of(j.held())).orElse(FastFrame.Held.NONE);
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

    /** Starts the watch that joins every term this pod learns of. */
    static FastPeer start(ServerConfig config, BinStore store, FastDisk disk,
            CrossAzBytes crossAz, Duration timeout,
            java.util.function.BooleanSupplier leading, LeaderWatch.Sleeper sleeper) {
        TermJoiner joiner = joiner(config, disk, new HttpFastTransport(timeout, crossAz));
        String leaseKey = SequencerAssembly.leaseConfig(config).leaseKey();
        java.util.concurrent.atomic.AtomicLong reads = new java.util.concurrent.atomic.AtomicLong();
        LeaderWatch watch = new LeaderWatch(config.podUid(), () -> {
            reads.incrementAndGet();
            try (InputStream in = store.get(leaseKey)) {
                return Lease.decode(in.readAllBytes());
            }
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
