// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import io.github.huyz0.os.biningester.format.Recovery;
import io.github.huyz0.os.biningester.ingest.AppendResult;
import io.github.huyz0.os.biningester.ingest.FastPath;
import io.github.huyz0.os.biningester.sequencer.FastJournal;
import io.github.huyz0.os.biningester.sequencer.MonotonicClock;
import io.helidon.metrics.api.Counter;
import io.helidon.metrics.api.Gauge;
import io.helidon.metrics.api.Metrics;
import io.helidon.metrics.api.Timer;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Fast mode's metrics (M13.36; SPEC criterion 17, observability.md rule 3):
 * ack and quorum-wait latency, the journal's held bytes, uploads by cause and
 * the offsets recoveries voided -- each a pod-level series with NO label
 * (rule 1). Upload causes are names, not a {@code cause} label, as the front
 * door's refusals are (rule 1's list is closed); which stream a void hit is
 * not exported at all.
 *
 * <p>⚠️ Helidon's registry is process-wide and hands a second registration
 * the first meter, so in a JVM holding several pods these are shared and
 * cumulative, and the journal gauge reads the pod bound last -- as
 * {@link GovernorMetrics}'s gauges do.
 */
final class FastMetrics {

    static final String ACK = "biningester_fast_ack_seconds";
    static final String QUORUM_WAIT = "biningester_fast_quorum_wait_seconds";
    static final String JOURNAL_BYTES = "biningester_fast_journal_bytes";
    static final String UPLOADS_TIMER = "biningester_fast_uploads_timer_total";
    static final String UPLOADS_LEAVE = "biningester_fast_uploads_leave_total";
    static final String VOIDED = "biningester_fast_voided_offsets_total";

    /** Bounded buckets (rule 3), around NFR-15's 10 ms. */
    private static final Duration[] BUCKETS = {Duration.ofMillis(1), Duration.ofMillis(2),
        Duration.ofMillis(5), Duration.ofMillis(10), Duration.ofMillis(25),
        Duration.ofMillis(50), Duration.ofMillis(100), Duration.ofMillis(250),
        Duration.ofMillis(1_000)};

    private static final AtomicReference<FastDisk> JOURNAL = new AtomicReference<>();

    private static final Timer ACK_TIMER = Metrics.globalRegistry().getOrCreate(
            Timer.builder(ACK).buckets(BUCKETS)
                    .description("A fast write's ack: from its COMMIT to its EXPOSED"));
    private static final Timer QUORUM_TIMER = Metrics.globalRegistry().getOrCreate(
            Timer.builder(QUORUM_WAIT).buckets(BUCKETS)
                    .description("A fast batch's wait at the leader, assigned to exposed"));
    private static final Counter TIMER_UPLOADS = Metrics.globalRegistry().getOrCreate(
            Counter.builder(UPLOADS_TIMER)
                    .description("Fast uploads made by the min_upload_interval timer"));
    private static final Counter LEAVE_UPLOADS = Metrics.globalRegistry().getOrCreate(
            Counter.builder(UPLOADS_LEAVE)
                    .description("Fast uploads made by a leader as it leaves its term"));
    private static final Counter VOIDED_OFFSETS = Metrics.globalRegistry().getOrCreate(
            Counter.builder(VOIDED)
                    .description("Offsets a takeover's recovery voided, as counted gaps"));

    static {
        Metrics.globalRegistry().getOrCreate(Gauge.builder(JOURNAL_BYTES, () -> {
            FastDisk disk = JOURNAL.get();
            return disk == null ? 0L : disk.journal().map(FastJournal::heldBytes).orElse(0L);
        }).description("The bytes this pod's fast journal holds"));
    }

    private FastMetrics() {
    }

    /** The journal gauge reads {@code disk}'s from now. */
    static void bindJournal(FastDisk disk) {
        JOURNAL.set(Objects.requireNonNull(disk, "disk"));
    }

    /** {@code path}, its answered appends timed on {@code mono} as acks. */
    static FastPath timed(FastPath path, MonotonicClock mono) {
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(mono, "mono");
        return (stream, records) -> {
            long started = mono.nanos();
            Optional<AppendResult> answered = path.append(stream, records);
            if (answered.isPresent()) {
                ACK_TIMER.record(Duration.ofNanos(mono.nanos() - started));
            }
            return answered;
        };
    }

    static void quorumWaited(long nanos) {
        QUORUM_TIMER.record(Duration.ofNanos(nanos));
    }

    /** An upload was made: by the timer when {@code paced}, else by a leave. */
    static void uploaded(boolean paced) {
        (paced ? TIMER_UPLOADS : LEAVE_UPLOADS).increment();
    }

    /** {@code recovery}'s voids, counted in offsets. */
    static void voided(Recovery recovery) {
        long offsets = 0;
        for (Recovery.VoidRange v : recovery.voids()) {
            offsets += v.toOffsetExclusive() - v.fromOffset();
        }
        if (offsets > 0) {
            VOIDED_OFFSETS.increment(offsets);
        }
    }

    /** The disk the journal gauge reads, null before a node bound one. */
    static FastDisk boundJournal() {
        return JOURNAL.get();
    }

    static Timer ack() {
        return ACK_TIMER;
    }

    static Timer quorumWait() {
        return QUORUM_TIMER;
    }

    static long uploads(boolean paced) {
        return (paced ? TIMER_UPLOADS : LEAVE_UPLOADS).count();
    }

    static long voided() {
        return VOIDED_OFFSETS.count();
    }
}
