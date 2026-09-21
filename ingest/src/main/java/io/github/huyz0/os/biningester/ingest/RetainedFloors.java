// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import io.github.huyz0.os.biningester.format.Checkpoint;
import io.github.huyz0.os.biningester.format.RunKey;
import java.io.IOException;
import java.lang.System.Logger;
import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * Each stream's retained floor, as the serving pod knows it (M8.6, M7.18,
 * ADR-0056).
 *
 * <p>⚠️ **READ ON DEMAND, NEVER ON A TIMER.** It is asked only by a poll that
 * asks for the floor, and a consumer asks only after a shard RESUMES, for a
 * bounded number of polls -- so a node whose shards are all tailing asks
 * nothing at all. A timer that re-read the checkpoint would be a request per
 * interval
 * on every idle pod for ever -- NFR-2 and M8's criterion 3 -- for a number
 * that changes when garbage collection runs.
 *
 * <p>⚠️ **AT MOST ONE READ PER REFRESH INTERVAL, HOWEVER MANY SESSIONS START.**
 * A rolling restart of the OpenSearch nodes reconnects every shard at once; a
 * cache refreshed per session would turn that into a request per shard, which
 * non-negotiable 6 forbids by name. So the rate is bounded by pods over the
 * interval, and it is zero on a pod where no session starts.
 *
 * <p>⚠️ **A FLOOR ONLY RISES, SO THE CACHE ONLY RISES.** A takeover's first
 * checkpoint can report a floor LOWER than the previous term's -- before its
 * first pass it has collected nothing -- and garbage collection never
 * un-deletes. Each stream's value is merged with {@code max}, so a newer but
 * less-informed checkpoint cannot walk a known floor back. ⚠️ And a checkpoint
 * that is not there yet, or a read that fails, keeps what is known: unknown
 * refuses nothing, but a floor already learned is still true.
 *
 * <p>⚠️ **WHAT IT GETS WRONG, IT GETS WRONG LOW.** The value is a checkpoint's,
 * at most one refresh interval plus one checkpoint cadence old. Low refuses
 * less than it should -- a consumer may still meet a 404 in that window -- and
 * never refuses records that are still in the bucket.
 */
public final class RetainedFloors {

    private static final Logger LOG = java.lang.System.getLogger(RetainedFloors.class.getName());

    /**
     * ⚠️ **ONE MINUTE**: the floor moves when a GC pass runs, which is once a
     * pass interval at most, so a staler floor loses nothing a fresher one
     * would have caught -- and a shorter interval only matters during a burst
     * of reconnects, which is exactly when the bound is doing its job.
     */
    public static final Duration DEFAULT_REFRESH = Duration.ofMinutes(1);

    /** Where the newest checkpoint comes from. */
    @FunctionalInterface
    public interface Source {
        /** The current term's newest checkpoint, or empty if it has none yet. */
        Optional<Checkpoint> newest() throws IOException;
    }

    private final Source source;
    private final Clock clock;
    private final long refreshMillis;
    private final Map<RunKey, Long> floors = new HashMap<>();
    private long lastReadMillis;
    private boolean everRead;

    /**
     * Floors that are never known.
     *
     * <p>⚠️ **WHAT A NODE WITHOUT A CHECKPOINT READER SERVES**, and it is
     * exactly the behaviour before ADR-0056: unknown refuses nothing.
     */
    public static RetainedFloors unknown() {
        return new RetainedFloors(Optional::empty, Clock.fixed(java.time.Instant.EPOCH,
                java.time.ZoneOffset.UTC), DEFAULT_REFRESH);
    }

    public RetainedFloors(Source source, Clock clock, Duration refresh) {
        this.source = Objects.requireNonNull(source, "source");
        this.clock = Objects.requireNonNull(clock, "clock");
        Objects.requireNonNull(refresh, "refresh");
        if (refresh.isZero() || refresh.isNegative()) {
            throw new IllegalArgumentException("refresh must be positive: " + refresh);
        }
        this.refreshMillis = refresh.toMillis();
    }

    /**
     * {@code key}'s floor, or empty if none is known.
     *
     * <p>⚠️ **SYNCHRONIZED**, because many sessions start at once in exactly
     * the burst this class exists to bound, and two threads that both saw a
     * stale cache would both read.
     */
    public synchronized OptionalLong floorOf(RunKey key) {
        Objects.requireNonNull(key, "key");
        refreshIfStale();
        Long floor = floors.get(key);
        return floor == null ? OptionalLong.empty() : OptionalLong.of(floor);
    }

    private void refreshIfStale() {
        long now = clock.millis();
        if (everRead && now - lastReadMillis < refreshMillis) {
            return;
        }
        // ⚠️ STAMPED BEFORE THE READ, SO A FAILED READ COUNTS. A store that is
        // down would otherwise be asked again by every session that starts
        // while it is down -- the burst this bound is for, arriving during an
        // outage.
        everRead = true;
        lastReadMillis = now;
        Optional<Checkpoint> newest;
        try {
            newest = source.newest();
        } catch (IOException unreadable) {
            LOG.log(Logger.Level.WARNING, () -> "the retained floor could not be read; the "
                    + "floors already known are kept: " + unreadable);
            return;
        }
        newest.ifPresent(checkpoint -> checkpoint.streams().forEach((key, offsets) ->
                floors.merge(key, offsets.oldestRetainedOffset(), Math::max)));
    }
}
