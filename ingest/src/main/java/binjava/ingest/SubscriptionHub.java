// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import binjava.format.CommitDelta;
import binjava.format.RunCommit;
import binjava.format.RunKey;
import binjava.format.SegmentCommit;
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
        // ⚠️ FREE OF THE MIS-PAIRING the overload below refuses, because with
        // no bytes attached there is nothing to mis-pair: each run is delivered
        // under ITS OWN segment key.
        // ⚠️ BUT THIS IS NOT THE BATCHED DELIVERY PATH, and a draft of this
        // comment said it was — claiming "a subscriber that needs the records
        // fetches them", which no subscriber can do. `Push` carries the segment
        // INLINE (ADR-0004) and `ConsumerClient.decodeInto` opens
        // `delivery.segment()` with no store fallback, so a run delivered with
        // `new byte[0]` fails inside the sink and is swallowed below as a
        // slow-or-dead subscriber: silent record loss for the whole window.
        // M4.7's batcher needs a per-segment payload API, not this.
        for (SegmentCommit committed : delta.segments()) {
            for (RunCommit run : committed.runs()) {
                publishRun(committed.segmentKey(), run, new byte[0]);
            }
        }
    }

    /**
     * Delivers a commit together with the segment bytes it committed.
     *
     * @throws IllegalStateException if the delta names several segments — ⚠️
     *     ONE {@code byte[]} CANNOT BE THE PAYLOAD OF MANY SEGMENTS, and this
     *     refusal is the point. {@code Push} carries the bytes INLINE (ADR-0004:
     *     the consumer issues no store request), so the payload is what
     *     {@code ConsumerClient} actually parses and the key is only a label.
     *     Handing every run the same array while pairing each with its own key
     *     fixes the label and leaves the bytes wrong — a subscriber then decodes
     *     another pod's segment. Where its stream is absent that throws inside
     *     the sink and is SWALLOWED here as a slow-or-dead subscriber: silent
     *     record loss. Where two pods committed the same {@code RunKey} in one
     *     window — the case the {@code Sequencer} contract names — the other
     *     pod's segment does contain that stream, so the subscriber decodes the
     *     WRONG RECORDS under its own offsets. Neither throws, and no gate sees
     *     either. ⚠️ M4.7 owns the per-segment payload API this needs; until it
     *     exists, refusing is the only honest answer.
     */
    public void publish(CommitDelta delta, byte[] segment) {
        if (delta.segments().size() != 1) {
            throw new IllegalStateException(
                    "this delta batches " + delta.segments().size() + " segments and one byte[] "
                            + "cannot be the payload of all of them; publish per segment");
        }
        SegmentCommit committed = delta.segments().get(0);
        for (RunCommit run : committed.runs()) {
            publishRun(committed.segmentKey(), run, segment);
        }
    }

    private void publishRun(String segmentKey, RunCommit run, byte[] segment) {
        var list = subscribers.get(run.key());
        if (list == null) {
            return;
        }
        Push push = new Push(run.key(), segmentKey, run.recordCount(),
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
