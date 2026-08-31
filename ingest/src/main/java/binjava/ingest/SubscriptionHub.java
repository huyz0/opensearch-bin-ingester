// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import binjava.format.CommitDelta;
import binjava.format.RunCommit;
import binjava.format.RunKey;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * Where consumers register and commits are pushed to them (M1.11, FR-5).
 *
 * <p>⚠️ PUSH, NOT POLL, AND THAT IS THE WHOLE COST ARGUMENT. An idle consumer
 * makes NO object-store request — not a reduced number, zero — because nothing
 * asks it to look. Criterion 3 puts 1,600 idle consumers through 3,000 poll
 * intervals of injected clock and requires {@code totalRequests() == 0}; a
 * polling design cannot pass that at any interval, which is why the transport is
 * a seam rather than a loop.
 *
 * <p>⚠️ DELIVERY IS BY STREAM, so a consumer of one partition is not woken by
 * every other partition's commits. Fanning every delta to every subscriber would
 * make wakeups scale with total cluster traffic rather than with the subscriber's
 * own — the same shape as a request that scales with indices.
 *
 * <p>⚠️ A SLOW OR DEAD SUBSCRIBER MUST NOT BLOCK A COMMIT. Delivery failures are
 * isolated per subscriber: the commit has already happened and is durable, so a
 * consumer that cannot keep up falls behind and recovers from the log, it does
 * not stall the writer.
 */
public final class SubscriptionHub {

    /**
     * What a subscriber is handed when its stream advances.
     *
     * <p>⚠️ INLINE: the segment bytes travel WITH the push (ADR-0004 — M1 ships
     * `inline` only). The consumer therefore issues NO object-store request to
     * read what it was just told about, which is what makes criterion 3's zero
     * hold under load and not merely at rest. `proxy` and `direct` are M5.
     */
    public record Push(RunKey key, String segmentKey, int recordCount, long firstOffset,
            byte[] segment) {

        public Push {
            Objects.requireNonNull(key, "key");
            Objects.requireNonNull(segmentKey, "segmentKey");
            Objects.requireNonNull(segment, "segment");
        }

        public long lastOffset() {
            return firstOffset + recordCount - 1;
        }
    }

    /** A registration, closed to unsubscribe. */
    public final class Subscription implements AutoCloseable {
        private final RunKey key;
        private final Consumer<Push> sink;

        private Subscription(RunKey key, Consumer<Push> sink) {
            this.key = key;
            this.sink = sink;
        }

        @Override
        public void close() {
            var list = subscribers.get(key);
            if (list != null) {
                list.remove(this);
                // ⚠️ Drop the empty list too, or a cluster that churns through
                // short-lived consumers accumulates one entry per stream ever
                // seen and never releases it.
                subscribers.remove(key, java.util.List.of());
            }
        }
    }

    private final Map<RunKey, CopyOnWriteArrayList<Subscription>> subscribers =
            new ConcurrentHashMap<>();

    /** Registers interest in one stream. */
    public Subscription subscribe(RunKey key, Consumer<Push> sink) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(sink, "sink");
        Subscription s = new Subscription(key, sink);
        subscribers.computeIfAbsent(key, k -> new CopyOnWriteArrayList<>()).add(s);
        return s;
    }

    public int subscriberCount(RunKey key) {
        var list = subscribers.get(key);
        return list == null ? 0 : list.size();
    }

    public Set<RunKey> subscribedStreams() {
        return Set.copyOf(subscribers.keySet());
    }

    /**
     * Delivers a commit to the streams it touched.
     *
     * <p>⚠️ Called AFTER the delta is durable. A push that preceded durability
     * would let a consumer read an offset the log does not yet contain, and a
     * crash would then have handed out an offset that never existed.
     */
    public void publish(CommitDelta delta) {
        publish(delta, new byte[0]);
    }

    /** Delivers a commit together with the segment bytes it committed. */
    public void publish(CommitDelta delta, byte[] segment) {
        for (RunCommit run : delta.runs()) {
            var list = subscribers.get(run.key());
            if (list == null) {
                continue;
            }
            Push push = new Push(run.key(), delta.segmentKey(), run.recordCount(),
                    run.firstOffset(), segment);
            for (Subscription s : list) {
                try {
                    s.sink.accept(push);
                } catch (RuntimeException slowOrDeadSubscriber) {
                    // ⚠️ Swallowed ON PURPOSE, and only here. The commit is
                    // already durable; a consumer that throws must not roll back
                    // or stall a write that succeeded. It falls behind and
                    // recovers from the commit log, which is what the log is for.
                    continue;
                }
            }
        }
    }
}
