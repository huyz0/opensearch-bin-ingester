// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import binjava.format.CommitDelta;
import binjava.format.FetchMode;
import binjava.format.RunCommit;
import binjava.format.RunKey;
import binjava.format.SegmentCommit;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
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
     * <p>⚠️ {@link #segment()} IS EMPTY IN EVERY PUSH THE SERVING PATH
     * BUILDS, and a reader who assumes otherwise loses a window silently.
     * {@link SubscriptionHub#publish(CommitDelta, String, byte[],
     * SegmentServing)} writes the segment to the {@link SegmentSink} the
     * subscriber opens -- one hand-off under `inline`, a chunk at a time under
     * `proxy` -- so the array here is {@code EMPTY} at {@code open} time.
     * {@code ConsumerClient.decodeInto} opens {@code delivery.segment()} with
     * no store fallback, so a {@code Subscriber} that read this field would
     * decode nothing and throw, and {@link #publishSegment} catches that as a
     * dead subscriber. ⚠️ {@link #assembling} is the adapter for code that
     * wants the finished array -- and it does NOT fill this field in the push
     * {@code complete} receives either, because {@link #deliver} hands
     * {@code complete} the same {@code EMPTY} push it handed {@code open}.
     * What it does is build a SECOND push carrying the assembled array and
     * pass THAT to the consumer callback it wraps.
     *
     * <p>⚠️ THE BYTES ARE STILL `inline` IN THE SENSE ADR-0004 MEANS IT:
     * the consumer issues NO object-store request to read what it was just
     * told about, which is what makes criterion 3's zero hold under load and
     * not merely at rest. {@code via} says which path put them in the sink;
     * `direct`, where the consumer does read the store, is M5.45b.

     */
    public record Push(RunKey key, String segmentKey, int recordCount, long firstOffset,
            FetchMode via, byte[] segment) {

        public Push {
            Objects.requireNonNull(key, "key");
            Objects.requireNonNull(segmentKey, "segmentKey");
            Objects.requireNonNull(via, "via");
            Objects.requireNonNull(segment, "segment");
        }

        public long lastOffset() {
            return firstOffset + recordCount - 1;
        }
    }

    /** A registration, closed to unsubscribe. */
    public final class Subscription implements AutoCloseable {
        private final RunKey key;
        private final Subscriber subscriber;

        private Subscription(RunKey key, Subscriber subscriber) {
            this.key = key;
            this.subscriber = subscriber;
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


    public int subscriberCount(RunKey key) {
        var list = subscribers.get(key);
        return list == null ? 0 : list.size();
    }

    public Set<RunKey> subscribedStreams() {
        return Set.copyOf(subscribers.keySet());
    }



    /**
     * Where a subscriber takes the bytes of a push.
     *
     * <p>⚠️ A SINK RATHER THAN AN ARRAY, and that is what makes the mode a
     * detail of WHERE THE BYTES COME FROM rather than of what arrives. `inline`
     * writes an array the pod already holds; `proxy` writes chunks off one
     * shared store read serving every subscriber of the segment. Handing over a
     * finished {@code byte[]} instead would force the serving path to
     * materialise the whole segment per subscriber -- buffer-then-forward,
     * which ADR-0004 forbids and M5's SPEC names as criterion 6's falsifier.
     */
    @FunctionalInterface
    public interface Subscriber {

        /**
         * Where this subscriber wants this push's segment bytes written.
         *
         * @throws IOException if it cannot take them; it is dropped from THIS
         *     push and the rest are still served
         */
        SegmentSink open(Push push) throws IOException;

        /** Called once the whole segment has been handed to {@code sink}. */
        default void complete(Push push, SegmentSink sink) throws IOException {
        }
    }

    /**
     * A subscriber that assembles the whole segment and hands it over at once.
     *
     * <p>⚠️ IT PAYS O(SEGMENT) PER SUBSCRIBER ON PURPOSE, and that cost belongs
     * to the subscriber rather than to the serving path: in production this
     * side is a socket, and {@code SegmentSink}'s contract already says a sink
     * that needs to retain bytes copies them itself. ⚠️ SO IT MUST NOT BE USED
     * BY A TEST ASSERTING CRITERION 6 -- the bound that must be flat in K is the
     * SERVICE's, and this adapter is the consumer.
     */
    public static Subscriber assembling(Consumer<Push> onSegment) {
        Objects.requireNonNull(onSegment, "onSegment");
        return new Subscriber() {
            @Override
            public SegmentSink open(Push push) {
                return new AssemblingSink();
            }

            @Override
            public void complete(Push push, SegmentSink sink) {
                byte[] whole = ((AssemblingSink) sink).out.toByteArray();
                onSegment.accept(new Push(push.key(), push.segmentKey(), push.recordCount(),
                        push.firstOffset(), push.via(), whole));
            }
        };
    }

    private static final class AssemblingSink implements SegmentSink {
        private final ByteArrayOutputStream out = new ByteArrayOutputStream();

        @Override
        public void write(byte[] buffer, int offset, int length) {
            out.write(buffer, offset, length);
        }
    }

    /** Registers interest in one stream. */
    public Subscription subscribe(RunKey key, Subscriber subscriber) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(subscriber, "subscriber");
        Subscription s = new Subscription(key, subscriber);
        subscribers.computeIfAbsent(key, k -> new CopyOnWriteArrayList<>()).add(s);
        return s;
    }

    /**
     * Delivers a commit with the INGESTER choosing how the bytes travel.
     *
     * <p>⚠️ THE DECISION IS PER SEGMENT, NOT PER RUN, and that is the whole
     * reason this method gathers subscribers before it chooses. `proxy` streams
     * a WHOLE segment, so a hub that looped its per-{@code RunKey} map would
     * call {@code streamTo} once per run -- ~1,600 GETs for one 8 MiB segment,
     * which is the shard scaling NFR-4 rules out and non-negotiable 6 forbids.
     * M5.40 records that gap; this closes its caller-granularity half. Its
     * per-NODE half stays open: there is no cache, so a LATE subscriber is a
     * second call and a second GET (M5.16).
     *
     * <p>⚠️ FR-6: the CONSUMER CANNOT ASK. Where there is a choice to make
     * it is made here, from the segment, the fan-out and the backend's
     * capabilities; nothing a subscriber supplies reaches {@link FetchPolicy}.
     * A consumer that could demand `direct` at fan-out 300 reproduces the
     * $3,732/month design ADR-0004 rejected.
     *
     * <p>⚠️ AND FOR A SEGMENT THIS POD DOES NOT HOLD THERE IS NO CHOICE, so
     * {@link FetchPolicy} is not consulted at all: `inline` would mean reading
     * the whole segment into memory before forwarding it, which is the
     * buffer-then-forward ADR-0004 forbids. The policy decides only for bytes
     * already in hand.
     *
     * <p>⚠️ A DELTA NAMES MANY PODS' SEGMENTS AND THIS POD HOLDS ONE, which is
     * why the held bytes arrive WITH THE KEY THEY BELONG TO.
     * A removed overload took a bare {@code byte[]} and REFUSED any delta
     * naming more than one segment, because "one byte[] cannot be the payload
     * of all of them" -- true, and the refusal then travelled up to
     * {@code DefaultIngest.pushLoop}, which swallows a
     * {@code RuntimeException} as a slow subscriber, so every batched commit
     * was dropped for every subscriber, silently. Matching by KEY delivers each
     * segment from where its bytes actually are: this pod's from memory, the
     * others from the store.
     *
     * <p>⚠️ THIS IS THE ONLY {@code publish}. M5.47 removed the two that
     * M5.45a had left without a production caller, so there is no longer a
     * shorter one to reach for by accident.
     *
     * @param heldSegmentKey which segment {@code heldBytes} is, or {@code null}
     *     if this pod holds none of them
     * @param heldBytes the segment this pod just wrote -- ⚠️ EVERY OTHER
     *     SEGMENT IN THE DELTA IS READ, and reading is what makes `inline`
     *     impossible for them: inlining bytes we do not hold would mean pulling
     *     a whole segment into memory first, which is the buffer-then-forward
     *     ADR-0004 forbids and M5's SPEC names as criterion 6's falsifier.
     */
    public void publish(CommitDelta delta, String heldSegmentKey, byte[] heldBytes,
            SegmentServing serving) {
        Objects.requireNonNull(delta, "delta");
        Objects.requireNonNull(serving, "serving");
        // ⚠️ A FAILED READ ON ONE SEGMENT DOES NOT DENY THE OTHERS. Letting
        // it escape the loop where it happens would deny every segment AFTER
        // the failing one -- including the one whose bytes are in this pod's
        // hand and need no store at all -- for a commit that is already
        // durable, with `DefaultIngest.pushLoop` catching the throw as a slow
        // subscriber. Silent, for the whole window.
        //
        // ⚠️ ONLY `UncheckedIOException` IS CAUGHT, so that sentence is about
        // a failed READ and nothing else. The `DIRECT` arm below throws
        // `IllegalStateException` and DOES still deny every later segment.
        // M5.45b owns deciding what that arm becomes once it is live.
        //
        // ⚠️ THE RETHROW CHANGES NOTHING OUTSIDE THIS PROCESS TODAY, and an
        // earlier draft of this comment claimed it did. `pushLoop`'s only
        // handler is `catch (RuntimeException e) { continue; }` -- no log, no
        // metric, no `droppedPushes` increment -- so a store outage and a
        // quiet window ARE indistinguishable from outside. M5.48 owns making
        // the difference visible. What the rethrow preserves until then is the
        // option, at the one frame that still knows the read failed.
        UncheckedIOException firstFailure = null;
        for (SegmentCommit committed : delta.segments()) {
            byte[] held = committed.segmentKey().equals(heldSegmentKey) ? heldBytes : null;
            try {
                publishSegment(committed, held, serving);
            } catch (UncheckedIOException storeFailed) {
                if (firstFailure == null) {
                    firstFailure = storeFailed;
                } else {
                    firstFailure.addSuppressed(storeFailed);
                }
            }
        }
        if (firstFailure != null) {
            throw firstFailure;
        }
    }

    /** One subscriber's share of one segment, paired with the run it is for. */
    private record Target(Subscription subscription, RunCommit run) {
    }

    private void publishSegment(SegmentCommit committed, byte[] heldBytes,
            SegmentServing serving) {
        List<Target> targets = new ArrayList<>();
        for (RunCommit run : committed.runs()) {
            var list = subscribers.get(run.key());
            if (list == null) {
                continue;
            }
            for (Subscription s : list) {
                targets.add(new Target(s, run));
            }
        }
        // ⚠️ NO READ FOR NOBODY. A segment nobody subscribes to must cost zero
        // requests, which is what makes criterion 8's zero hold at fan-out
        // rather than merely at rest.
        if (targets.isEmpty()) {
            return;
        }

        FetchMode via = heldBytes == null
                ? FetchMode.PROXY
                : serving.policy().modeFor(
                        new SegmentDelivery(heldBytes.length, true, targets.size(), false),
                        serving.capabilities());

        switch (via) {
            case INLINE -> deliver(committed.segmentKey(), targets, FetchMode.INLINE,
                    sinks -> writeHeldBytes(heldBytes, sinks));
            // ⚠️ FROM THE HELD ARRAY WHEN WE HAVE ONE, and only from the store
            // when we do not. `proxy` names how the bytes reach the CONSUMER;
            // it is never a reason to buy a GET for bytes this pod is already
            // holding, which would be one wasted request per flush.
            case PROXY -> deliver(committed.segmentKey(), targets, FetchMode.PROXY,
                    heldBytes == null
                            ? sinks -> streamFromStore(serving, committed.segmentKey(), sinks)
                            : sinks -> writeHeldBytesChunked(heldBytes, serving, sinks));
            // ⚠️ UNREACHABLE TODAY AND REFUSED RATHER THAN DEGRADED. `direct`
            // needs a grant and a client-side byte source, which is M5.45b;
            // with bytes in hand and no pressure signal `FetchPolicy` cannot
            // choose it, so this arm exists to fail loudly if that changes
            // before the serving half does.
            case DIRECT -> throw new IllegalStateException(
                    "M5.45b owns `direct`; this serving path carries inline and proxy only");
        }
    }

    /** How a mode's bytes reach the sinks that were opened for them. */
    private interface Source {
        void writeTo(List<SegmentSink> sinks) throws IOException;
    }

    /**
     * Opens every subscriber's sink, hands the bytes over ONCE, and completes
     * only the sinks that took them all.
     *
     * <p>⚠️ COMPLETING A SINK THAT THREW MID-STREAM WOULD BE THE WORST OUTCOME
     * AVAILABLE: the subscriber would be handed a TRUNCATED PREFIX it cannot
     * tell from a whole segment, and would decode fewer records under offsets
     * the commit log says are there. {@code SegmentProxy.streamTo} drops a
     * throwing sink and returns only a COUNT, so the hub wraps each sink to
     * learn WHICH. ⚠️ A sink that merely BLOCKS is unhandled ON THIS PATH:
     * M5.41 added a per-chunk deadline to {@code streamTo}'s three-argument
     * overload and this hub calls the two-argument one, because a sink dropped
     * for missing a deadline never throws and {@link Tracking} would not mark
     * it. M5.58 owns closing that before the other overload is wired.
     */
    private void deliver(String segmentKey, List<Target> targets, FetchMode via, Source source) {
        List<Tracking> opened = new ArrayList<>();
        List<Push> pushes = new ArrayList<>();
        List<Target> live = new ArrayList<>();
        for (Target t : targets) {
            Push push = new Push(t.run().key(), segmentKey, t.run().recordCount(),
                    t.run().firstOffset(), via, EMPTY);
            try {
                SegmentSink sink = t.subscription().subscriber.open(push);
                if (sink == null) {
                    continue;
                }
                opened.add(new Tracking(sink));
                pushes.add(push);
                live.add(t);
            } catch (IOException | RuntimeException slowOrDeadSubscriber) {
                // ⚠️ Swallowed ON PURPOSE. The commit is already durable; a
                // consumer that throws must not roll back or stall a write that
                // succeeded. It falls behind and recovers from the log.
                continue;
            }
        }
        if (opened.isEmpty()) {
            return;
        }
        try {
            source.writeTo(List.copyOf(opened));
        } catch (IOException storeFailed) {
            // ⚠️ NOBODY IS COMPLETED, so no subscriber mistakes a prefix for a
            // segment. `DefaultIngest.pushLoop` catches this and drops the push.
            throw new UncheckedIOException(storeFailed);
        }
        for (int i = 0; i < opened.size(); i++) {
            Tracking sink = opened.get(i);
            if (sink.failed) {
                continue;
            }
            try {
                live.get(i).subscription().subscriber.complete(pushes.get(i), sink.delegate);
            } catch (IOException | RuntimeException slowOrDeadSubscriber) {
                continue;
            }
        }
    }

    private void writeHeldBytes(byte[] heldBytes, List<SegmentSink> sinks) {
        for (SegmentSink sink : sinks) {
            try {
                sink.write(heldBytes, 0, heldBytes.length);
            } catch (IOException | RuntimeException slowOrDeadSubscriber) {
                continue;
            }
        }
    }

    /**
     * Hands held bytes over a chunk at a time, chunk OUTER and consumer INNER.
     *
     * <p>⚠️ THE BOUND HERE IS THE HAND-OFF, NOT THE RESIDENCY, and that is
     * a weaker claim than {@link SegmentProxy}'s. These bytes are ALREADY
     * materialised -- this pod wrote them -- so no loop order makes the serving
     * path hold less than a segment. What the chunking buys is that no
     * CONSUMER is handed more than {@code chunkBytes} at once, which is the
     * property that stays flat as K grows and the one criterion 6 measures.
     *
     * <p>⚠️ CHUNK-OUTER IS THEREFORE A MATTER OF INTERLEAVING RATHER THAN
     * OF MEMORY: consumer-outer would give the first consumer the whole
     * segment before the second saw a byte. An earlier version of this
     * paragraph claimed the loop order was the memory bound, which is true of
     * {@link SegmentProxy} -- where the bytes are not yet read -- and not of
     * this method. Review MEASURED consumer-outer surviving every test in the
     * tree, and an earlier draft of this paragraph explained that away by
     * claiming nothing below T4 could observe the order. That is false: sinks
     * that append their own identity to ONE SHARED ORDERED LOG separate the
     * two loops exactly, at this tier, which is what
     * {@code AssembledServingPathFailureTest.everySinkGetsTheFIRSTChunkBefore
     * AnyGetsTheSECOND} now does. The mutation dies.
     */
    private void writeHeldBytesChunked(byte[] heldBytes, SegmentServing serving,
            List<SegmentSink> sinks) {
        int chunk = serving.chunkBytes();
        List<SegmentSink> live = new ArrayList<>(sinks);
        for (int offset = 0; offset < heldBytes.length && !live.isEmpty(); offset += chunk) {
            int length = Math.min(chunk, heldBytes.length - offset);
            for (int i = live.size() - 1; i >= 0; i--) {
                try {
                    live.get(i).write(heldBytes, offset, length);
                } catch (IOException | RuntimeException slowOrDeadSubscriber) {
                    live.remove(i);
                }
            }
        }
    }

    private void streamFromStore(SegmentServing serving, String segmentKey,
            List<SegmentSink> sinks) throws IOException {
        serving.proxy().streamTo(segmentKey, sinks);
    }

    /**
     * A sink that remembers whether it threw, so {@link #deliver} completes only
     * the subscribers that received the whole segment.
     */
    private static final class Tracking implements SegmentSink {
        private final SegmentSink delegate;
        private boolean failed;

        Tracking(SegmentSink delegate) {
            this.delegate = delegate;
        }

        @Override
        public void write(byte[] buffer, int offset, int length) throws IOException {
            try {
                delegate.write(buffer, offset, length);
            } catch (IOException | RuntimeException e) {
                failed = true;
                throw e;
            }
        }
    }

    private static final byte[] EMPTY = new byte[0];

}
