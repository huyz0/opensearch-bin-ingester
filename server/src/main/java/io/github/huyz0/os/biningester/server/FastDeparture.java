// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import io.github.huyz0.os.biningester.binstore.backend.MemoryJournalFile;
import io.github.huyz0.os.biningester.format.Lease;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.sequencer.Departure;
import io.github.huyz0.os.biningester.sequencer.FastJournal;
import io.github.huyz0.os.biningester.sequencer.LeaderWatch;
import io.github.huyz0.os.biningester.sequencer.MonotonicClock;
import io.github.huyz0.os.biningester.sequencer.TermJoiner;
import java.io.IOException;
import java.time.Duration;

/**
 * A pod's departure at a graceful stop (ADR-0081 §9; M13.27p): a follower
 * reports what it holds to the leader the lease names -- DEPART phase 1, then
 * HELD -- drops what the answers settle, and once nothing is pending asks to
 * be marked departed (phase 2).
 *
 * <p>⚠️ IT NEVER LEAVES WITH A COPY PENDING: a pending entry, an unreachable
 * leader or an unreadable lease is asked again until the grace ends, and the
 * pod then stops undeparted -- counted as a crash would be, never as a
 * departure that dropped an uncommitted copy.
 *
 * <p>⚠️ A LEADER DEPARTS NOTHING HERE: its shutdown -- stop exposing, upload,
 * mark itself departed -- is M13.35's.
 *
 * <p>⚠️ RE-ASKED ON A PAUSE, NOT ON A RELEASE: ADR-0081 §9 re-sends HELD after
 * each RELEASE, which M13.31 brings; until then the pause stands in for it.
 */
final class FastDeparture {

    /** How long the pod waits between reports. */
    static final Duration PAUSE = Duration.ofMillis(250);

    /**
     * How long a departing pod keeps asking (M13.27p review round 1, P1):
     * ⚠️ FITTED TO {@link ShutdownSequence}'s 30 s budget beside its other
     * bounded steps -- with each exchange bounded by {@link #EXCHANGE_TIMEOUT},
     * the step lasts at most this plus one exchange and one lease read.
     */
    static final Duration GRACE = Duration.ofSeconds(4);

    /** Each DEPART or HELD exchange's timeout, so the grace bounds the step. */
    static final Duration EXCHANGE_TIMEOUT = Duration.ofSeconds(1);

    /** Journaled with the outcome, after the last flush (M13.27p review round 1, P3). */
    static final String JOURNALED = "fast departure: ";

    /** How a departure ended. */
    enum Result { NOT_A_FOLLOWER, DEPARTED, STOPPED_UNDEPARTED }

    private FastDeparture() {
    }

    static Result depart(ServerConfig config, LeaderWatch.LeaseReader lease, FastDisk disk,
            TermJoiner.Transport transport, boolean leading, Duration grace,
            MonotonicClock mono, LeaderWatch.Sleeper sleeper) {
        if (leading) {
            return Result.NOT_A_FOLLOWER;
        }
        Roster.Incarnation self = new Roster.Incarnation(config.podId(), config.podUid(),
                config.az(), config.endpoint());
        FastJournal journal;
        try {
            // ⚠️ A DISKLESS POD HOLDS NOTHING: an empty journal reports so.
            journal = disk.journal().isPresent() ? disk.journal().get()
                    : FastJournal.recover(new MemoryJournalFile(), 1);
        } catch (IOException cannot) {
            return Result.STOPPED_UNDEPARTED;
        }
        Departure departure = new Departure(self, transport, disk.fence(), journal);
        long deadline = mono.nanos() + grace.toNanos();
        boolean askedOnce = false;
        while (mono.nanos() < deadline) {
            try {
                Lease current = lease.read();
                String leader = current.holderPodUid();
                if (leader.isBlank() || leader.equals(config.podUid())) {
                    // ⚠️ NO LEADER TO ASK (a legacy lease, or this pod's own
                    // deposed term) before anything was asked: nothing to
                    // depart; after, keep waiting for one.
                    if (!askedOnce) {
                        return Result.NOT_A_FOLLOWER;
                    }
                } else {
                    askedOnce = true;
                    if (!departure.report(current.epoch(), leader, current.holderEndpoint())) {
                        departure.leave(current.epoch(), leader, current.holderEndpoint());
                        return Result.DEPARTED;
                    }
                }
            } catch (IOException | RuntimeException notYet) {
                askedOnce = true;
            }
            try {
                sleeper.sleep(PAUSE);
            } catch (InterruptedException stopping) {
                // ⚠️ THE FLAG IS NOT SET AGAIN (its review round 1, P2): the
                // shutdown sequence carries on with it clear, so the lease
                // release that follows is not failed at once by it.
                return Result.STOPPED_UNDEPARTED;
            }
        }
        return Result.STOPPED_UNDEPARTED;
    }
}
