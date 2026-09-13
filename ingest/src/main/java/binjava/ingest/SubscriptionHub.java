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
import java.util.LinkedHashMap;
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
         * Where this subscriber wants ONE segment's bytes written, for every
         * run of it this subscriber holds.
         *
         * <p>⚠️ ONE SINK FOR ALL K PUSHES, WHICH IS THE WHOLE OF
         * M5.40a. A node holding K runs of a segment needs K pushes -- each
         * carries its own run key, record count and first offset -- while
         * needing the BYTES once. The earlier contract took a single
         * {@code Push} and so was opened once per run, handing the same
         * segment over K times: for an 8 MiB segment of ~1,600 runs a node
         * holding ~178 of them took ~1.4 GiB to receive 8 MiB.
         *
         * <p>⚠️ SO THE FAILURE PATHS MERGE TOO, and that is forced
         * rather than chosen. One sink means one byte stream, and a stream that
         * broke part way through has delivered a PREFIX -- no run of it is
         * complete. A throw therefore fails ALL K of this subscriber's runs,
         * never some of them, and {@code complete} is called for none.
         *
         * @param pushes one per run of this segment that this subscriber
         *     holds, never empty. ⚠️ ONE PER SUBSCRIPTION, STRICTLY:
         *     {@link SubscriptionHub#subscribe} does not dedupe, so the same
         *     subscriber registered twice against one {@link RunKey} sees that
         *     run twice here -- and through {@link #assembling} its consumer is
         *     invoked twice with the same key and offset
         * @throws IOException if it cannot take them; this subscriber is
         *     dropped from ALL of them and other subscribers are still served
         */
        SegmentSink open(List<Push> pushes) throws IOException;

        /**
         * Called once the whole segment has been handed to {@code sink}.
         *
         * <p>⚠️ ALL OR NONE, for the reason {@link #open} gives: the
         * sink either took every byte, in which case every run in
         * {@code pushes} is complete, or it did not and none of them is.
         */
        default void complete(List<Push> pushes, SegmentSink sink) throws IOException {
        }
    }

    /**
     * A subscriber that assembles the whole segment and hands it over at once.
     *
     * <p>⚠️ ONE ARRAY, SHARED BY REFERENCE ACROSS A CONSUMER'S RUNS. A
     * subscriber holding K runs of a segment is handed K {@link Push} records
     * whose {@code segment()} is the SAME array -- not K copies. A callback
     * that decodes in place, or zeroes the buffer it was given, corrupts the
     * other K-1; {@link SegmentSink}'s contract already says a consumer that
     * needs to retain bytes copies them itself, and this is where that bites.
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
            public SegmentSink open(List<Push> pushes) {
                return new AssemblingSink();
            }

            @Override
            public void complete(List<Push> pushes, SegmentSink sink) {
                // ⚠️ ONE COPY, HANDED OUT K TIMES. The array is built
                // once and shared by reference across the K pushes; it is not
                // copied per run, which is the cost this adapter's javadoc
                // warns is O(SEGMENT) PER SUBSCRIBER and not per subscription.
                byte[] whole = ((AssemblingSink) sink).out.toByteArray();
                for (Push push : pushes) {
                    onSegment.accept(new Push(push.key(), push.segmentKey(), push.recordCount(),
                            push.firstOffset(), push.via(), whole));
                }
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

    /**
     * One CONSUMER's share of one segment: every run of it that consumer holds.
     *
     * <p>⚠️ ONE PER CONSUMER, NOT ONE PER SUBSCRIPTION, which is
     * M5.40a. This record used to pair one subscription with one run, so a
     * consumer holding K runs of a segment appeared K times and was handed the
     * bytes K times.
     */
    private record Target(Subscriber subscriber, List<RunCommit> runs) {
    }

    /**
     * A {@link Subscriber} keyed by IDENTITY, so grouping cannot be spoofed.
     *
     * <p>⚠️ {@code ==}, NOT {@code equals}. A {@code Subscriber} is
     * caller-supplied and may implement {@code equals} however it likes; two
     * subscribers that merely compare equal are still two consumers, and
     * merging them would hand one of them the other's stream. A plain
     * {@code HashMap<Subscriber, ...>} would do exactly that.
     *
     * <p>⚠️ AND A LINEAR SCAN WOULD BE O(RUNS x CONSUMERS). Comparing
     * each of a segment's runs against every target already built costs 1,600 x
     * ~9 at the scale the M5.40a row uses -- not ruinous, and quadratic only if
     * consumers grew with runs. It is avoided because a hash lookup is simpler
     * to read than a nested loop, not because the tree is on fire.
     * ⚠️ {@link java.util.IdentityHashMap} IS THE JDK CONSTRUCT FOR
     * THIS and is deliberately not used: it does not keep insertion order, and
     * delivery order is worth having stable.
     */
    private record ByIdentity(Subscriber subscriber) {
        @Override
        public boolean equals(Object other) {
            return other instanceof ByIdentity o && o.subscriber == this.subscriber;
        }

        @Override
        public int hashCode() {
            return System.identityHashCode(subscriber);
        }
    }

    private void publishSegment(SegmentCommit committed, byte[] heldBytes,
            SegmentServing serving) {
        // ⚠️ LINKED, so delivery order follows first-subscription order rather
        // than a hash, which makes a failure reproducible run to run.
        //
        // ⚠️ NO TEST DEPENDS ON IT, AND AN EARLIER DRAFT OF THIS COMMENT SAID
        // ONE DID. Review measured `HashMap` here surviving the whole suite:
        // the assertions that look like they care use
        // `containsExactlyInAnyOrder`, and `deliver`'s by-index pairing is
        // between its OWN parallel lists, which cannot disagree with each
        // other. So this is a choice about debuggability, not a pinned
        // property -- said plainly rather than dressed as a constraint.
        Map<ByIdentity, List<RunCommit>> byConsumer = new LinkedHashMap<>();
        for (RunCommit run : committed.runs()) {
            var list = subscribers.get(run.key());
            if (list == null) {
                continue;
            }
            for (Subscription s : list) {
                byConsumer.computeIfAbsent(new ByIdentity(s.subscriber), k -> new ArrayList<>())
                        .add(run);
            }
        }
        List<Target> targets = new ArrayList<>(byConsumer.size());
        byConsumer.forEach((consumer, runs) ->
                targets.add(new Target(consumer.subscriber(), runs)));
        // ⚠️ NO READ FOR NOBODY. A segment nobody subscribes to must cost zero
        // requests, which is what makes criterion 8's zero hold at fan-out
        // rather than merely at rest.
        if (targets.isEmpty()) {
            return;
        }

        FetchMode via = heldBytes == null
                ? FetchMode.PROXY
                : serving.policy().modeFor(
                        // ⚠️ CONSUMERS, NOT SUBSCRIPTIONS, and this number moved
                        // with M5.40a: `targets` used to hold one entry per
                        // (subscription, run), so a handful of nodes holding many
                        // runs each reported a fan-out several times their count.
                        //
                        // ⚠️ IT CHANGES NO MODE TODAY, AND AN EARLIER DRAFT OF
                        // THIS COMMENT CLAIMED OTHERWISE. Review measured it:
                        // `modeFor` reads `segmentFanOut` only through
                        // `wantsDirect`, which first needs
                        // `servingPodUnderPressure || !bytesInServingAz`, and
                        // this caller passes `true` for the second and `false`
                        // for the first -- so the INLINE/PROXY choice here turns
                        // on `batchBytes` alone. The number matters to the
                        // CONTRACT, which M5.43 and M5.45b will read, not to the
                        // branch taken from this line.
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
        List<List<Push>> pushes = new ArrayList<>();
        List<Target> live = new ArrayList<>();
        for (Target t : targets) {
            // ⚠️ K PUSHES, ONE OPEN. Each run keeps its own key, record count
            // and first offset -- a consumer needs all K to know what it just
            // received -- while the BYTES are handed over once.
            List<Push> forThisConsumer = new ArrayList<>(t.runs().size());
            for (RunCommit run : t.runs()) {
                forThisConsumer.add(new Push(run.key(), segmentKey, run.recordCount(),
                        run.firstOffset(), via, EMPTY));
            }
            try {
                SegmentSink sink = t.subscriber().open(List.copyOf(forThisConsumer));
                if (sink == null) {
                    continue;
                }
                opened.add(new Tracking(sink));
                pushes.add(List.copyOf(forThisConsumer));
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
            //
            // ⚠️ AND THE KEY IS IN THE MESSAGE, because this is the last frame
            // that knows it. `pushLoop` holds only the key THIS POD WROTE, and
            // by construction that is never the one that failed -- a read
            // happens only for a segment the pod does NOT hold. A log line up
            // there naming its own key sends an operator to a healthy object.
            throw new UncheckedIOException(
                    "segment " + segmentKey + " could not be read for delivery", storeFailed);
        }
        for (int i = 0; i < opened.size(); i++) {
            Tracking sink = opened.get(i);
            if (sink.failed) {
                continue;
            }
            try {
                live.get(i).subscriber().complete(pushes.get(i), sink.delegate);
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
