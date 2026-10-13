// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.Recovery;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentCommit;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import java.io.IOException;
import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The segments a fast leader uploads outside a flush -- a fast upload's
 * (M13.30a) and a takeover's recovery (M13.33p) -- each kept until the entry
 * committing it names it, then delivered as a flush's is: signalled durable
 * to this pod's peers and pushed to its subscribers at the offsets committed.
 * A segment no entry names is not delivered.
 *
 * <p>⚠️ A RECOVERY's TOO (M13.33w; ADR-0081 §5 step 6): a subscriber pushed a
 * term's entries by PUBLISH, or never pushed them, is pushed the recovered
 * offsets live from the new leader, and trims what overlaps (ADR-0085).
 *
 * <p>⚠️ SPLIT FROM {@link DefaultIngest}, which is held at its split ceiling.
 */
final class UploadDeliveries {

    private final IngestConfig config;
    private final Clock clock;
    private final SegmentPublisher publisher;
    private final DurableSegmentListener durable;
    private final PushQueue pushQueue;
    /**
     * The fast segment published and awaiting its delta (M13.30a): ⚠️ ONE,
     * never a queue of them (its review round 1, P1) -- the leader uploads one
     * at a time, and a retry's segment replaces the one it retries, so no
     * segment is held past its upload.
     */
    private SegmentPublisher.Published awaitingDelta;
    /**
     * One recovery's segments, awaiting its entry: ⚠️ CLEARED BY EACH
     * {@link #recovered}, so a recovery whose entry never landed holds its
     * segments only until the next one's.
     */
    private final Map<String, SegmentPublisher.Published> awaitingRecovery = new HashMap<>();

    UploadDeliveries(IngestConfig config, Clock clock, SegmentPublisher publisher,
            DurableSegmentListener durable, PushQueue pushQueue) {
        this.config = Objects.requireNonNull(config, "config");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.publisher = Objects.requireNonNull(publisher, "publisher");
        this.durable = Objects.requireNonNull(durable, "durable");
        this.pushQueue = Objects.requireNonNull(pushQueue, "pushQueue");
    }

    private SegmentPublisher.Published publish(Map<RunKey, List<SegmentRecord>> runs,
            String what) throws IOException {
        Accumulator segment = new Accumulator(config, clock);
        runs.forEach((key, records) -> records.forEach(r -> segment.add(key, r)));
        return publisher.publish(segment)
                .orElseThrow(() -> new IOException(what + " carries at least one record"));
    }

    String publishRecovery(Map<RunKey, List<SegmentRecord>> runs) throws IOException {
        SegmentPublisher.Published published = publish(runs, "a recovery");
        synchronized (this) {
            awaitingRecovery.put(published.key(), published);
        }
        return published.key();
    }

    String publishFast(Map<RunKey, List<SegmentRecord>> runs) throws IOException {
        SegmentPublisher.Published published = publish(runs, "a fast upload");
        synchronized (this) {
            awaitingDelta = published;
        }
        return published.key();
    }

    void delivered(String segmentKey, CommitDelta delta, long epoch) {
        SegmentPublisher.Published published;
        synchronized (this) {
            published = awaitingDelta;
            if (published == null || !published.key().equals(segmentKey)) {
                return;
            }
            awaitingDelta = null;
        }
        durable.onDurable(segmentKey);
        pushQueue.offer(delta, segmentKey, published.segment(), epoch);
    }

    void recovered(Recovery recovery, long epoch) {
        Map<String, SegmentPublisher.Published> published;
        synchronized (this) {
            published = Map.copyOf(awaitingRecovery);
            awaitingRecovery.clear();
        }
        for (SegmentCommit committed : recovery.segments()) {
            SegmentPublisher.Published kept = published.get(committed.segmentKey());
            if (kept != null) {
                durable.onDurable(committed.segmentKey());
                // one delta per segment, each pushed with its own bytes in hand
                pushQueue.offer(new CommitDelta(recovery.sequence(), List.of(committed)),
                        committed.segmentKey(), kept.segment(), epoch);
            }
        }
    }
}
