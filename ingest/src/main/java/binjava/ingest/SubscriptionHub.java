// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import binjava.format.CommitDelta;
import binjava.format.FetchMode;
import binjava.format.RunKey;
import binjava.format.SegmentCommit;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
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
     * decode nothing and throw, and {@code SegmentServingPath.publishSegment}
     * catches that as a dead subscriber. ⚠️ {@link #assembling} is the adapter for code that
     * wants the finished array -- and it does NOT fill this field in the push
     * {@code complete} receives either, because {@code SegmentServingPath.deliver}
     * hands
     * {@code complete} the same {@code EMPTY} push it handed {@code open}.
     * What it does is build a SECOND push carrying the assembled array and
     * pass THAT to the consumer callback it wraps.
     *
     * <p>⚠️ THE BYTES ARE STILL `inline` IN THE SENSE ADR-0004 MEANS IT:
     * the consumer issues NO object-store request to read what it was just
     * told about, which is what makes criterion 3's zero hold under load and
     * not merely at rest. {@code via} says which path put them in the sink.
     *
     * <p>⚠️ UNDER {@code DIRECT} NOTHING IS PUT IN IT EITHER: the sink is
     * opened and completed with ZERO bytes and the consumer fetches with
     * {@link #grant()}, so a transport reading open-then-complete as "a
     * segment arrived" sees an empty one and must branch on {@code via} first.
     */
    public record Push(RunKey key, String segmentKey, int recordCount, long firstOffset,
            FetchMode via, byte[] segment, binjava.format.Grant grant) {

        /**
         * A push with no grant, which is every {@code inline} and {@code proxy}
         * one.
         *
         * <p>⚠️ IT EXISTS SO THE GRANT COST NO CALL SITES -- the same reason
         * {@code IngestConfig} and {@code SubscriptionEvent} have one. Sweeping
         * the sites is how a fixture silently stops testing what it did, which
         * M5.43's review measured happening to a case two hundred lines from
         * anything its diff touched.
         */
        public Push(RunKey key, String segmentKey, int recordCount, long firstOffset,
                FetchMode via, byte[] segment) {
            this(key, segmentKey, recordCount, firstOffset, via, segment, null);
        }

        public Push {
            Objects.requireNonNull(key, "key");
            Objects.requireNonNull(segmentKey, "segmentKey");
            Objects.requireNonNull(via, "via");
            Objects.requireNonNull(segment, "segment");
            // ⚠️ A GRANT BELONGS TO `direct` AND NOWHERE ELSE. An `inline` push
            // already carries the bytes and a `proxy` push is about to be
            // written them, so a grant on either is a signed URL minted for a
            // consumer that will never fetch with it -- a secret issued for no
            // reason, which security.md rule 4 makes a cost rather than merely
            // waste.
            if (grant != null && via != FetchMode.DIRECT) {
                throw new IllegalArgumentException(
                        "a grant is for `direct`; this push is " + via);
            }
            if (grant == null && via == FetchMode.DIRECT) {
                throw new IllegalArgumentException(
                        "a `direct` push without a grant tells a consumer to fetch and not how");
            }
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

    /**
     * The serving half, split out under M5.65 when this file hit its 700-line
     * cap. What is left here is the REGISTRY -- {@link #subscribe}, the
     * {@link Subscription}, the by-{@link RunKey} map -- plus {@link Push} and
     * {@link Subscriber}, which are the vocabulary of both halves and are named
     * {@code SubscriptionHub.Push} by every transport in the tree.
     */
    private final SegmentServingPath servingPath = new SegmentServingPath(this);


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
         * run of it this subscriber holds -- ⚠️ EXCEPT UNDER
         * {@code FetchMode.DIRECT}, where it is opened and completed EMPTY and
         * the consumer fetches with {@link Push#grant()} instead.
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
     * <p>⚠️ THE BODY MOVED TO {@link AssemblingSubscriber} and this name stayed,
     * because {@code SubscriptionHub.assembling(...)} is what every call site
     * uses. code-structure.md rule 1 forced the move: this file reached 730
     * lines when {@code direct} landed, and an adapter for CONSUMERS is the part
     * of it least about serving.
     */
    public static Subscriber assembling(Consumer<Push> onSegment) {
        return AssemblingSubscriber.of(onSegment);
    }

    /**
     * The session registry, which is the ingester side of SPEC criterion 11.
     *
     * <p>⚠️ IT IS A FIELD OF THE HUB rather than a seam, because a session is
     * state ABOUT subscriptions and the hub is where subscriptions live. It
     * holds no clock, no socket and no store, so non-negotiable 7 has nothing
     * to say about it.
     */
    private final SessionRegistry sessions = new SessionRegistry();

    /**
     * What a resume answers: the epoch it served, and where each stream the
     * session keeps goes on.
     *
     * <p>⚠️ PUBLIC, AND NESTED HERE RATHER THAN ON THE REGISTRY, because the
     * registry is package-private and a public method returning a type an
     * out-of-package caller cannot name is a method that caller cannot use. The
     * first such caller is the transport M8 builds.
     */
    public record Resumed(long sessionEpoch, Map<RunKey, Long> nextOffset) {
    }

    /**
     * A consumer's session: the registration, plus a resume point per stream.
     *
     * <p>⚠️ WHAT IT BUYS OVER {@link #subscribe} is exactly one thing -- a
     * consumer that reconnects is told the offset it must see NEXT on every
     * stream it keeps, so a reconnect is neither a duplicate nor a skip (SPEC
     * criterion 11). A consumer that never reconnects needs none of it, which
     * is why {@code subscribe} is untouched and every transport in the tree
     * still uses it.
     */
    public final class Session implements AutoCloseable {
        private final String id;
        private final Subscriber subscriber;
        private final List<Subscription> registrations = new ArrayList<>();

        private Session(String id, Subscriber subscriber, List<RunKey> keys) {
            this.id = id;
            this.subscriber = subscriber;
            keys.forEach(key -> registrations.add(subscribe(key, subscriber)));
        }

        public String id() {
            return id;
        }

        /** The epoch this session has served, which starts at 0. */
        public long epoch() throws SessionResetException {
            return sessions.epochOf(id);
        }

        public List<RunKey> keys() throws SessionResetException {
            return sessions.keysOf(id);
        }

        /**
         * Drops this session server-side, so its next resume answers RESET.
         *
         * <p>⚠️ IT IS NOT {@link #close()}. Closing unregisters the
         * subscriptions too, which is a consumer going away; a reset leaves the
         * consumer connected and tells it to re-establish. The registrations
         * are left alone deliberately -- a consumer that is about to re-send
         * full state keeps receiving in the meantime, and tearing them down
         * would make a reset a silent gap as well as a signal.
         */
        public void reset() {
            sessions.reset(id);
        }

        /**
         * Answers a {@link SessionResetException} with the consumer's FULL
         * state, on this same session handle.
         *
         * <p>⚠️ THIS IS THE ONLY DUPLICATE-FREE AND GAP-FREE ANSWER, which
         * review measured: re-opening a session hands the consumer every run
         * TWICE, because the old registrations are still in place and
         * {@code subscribe} does not dedupe; closing first leaves a window
         * registered for nothing. Reconciling onto the registrations this
         * handle already holds touches only the difference.
         *
         * <p>⚠️ ADDED BEFORE REMOVED, as in {@link #resume}: a stream present
         * both before and after is never unregistered, so nothing can fall into
         * a gap.
         */
        public synchronized void reestablish(Collection<RunKey> keys) {
            List<RunKey> full = List.copyOf(new LinkedHashSet<>(keys));
            // ⚠️ SUBSCRIBED BEFORE THE REGISTRY IS TOLD, which is the order
            // `resume` takes twelve lines above and for the same reason. The
            // other way round -- registry first -- leaves a window in which a
            // key belongs to the session and is subscribed by nothing: a commit
            // landing there reaches no subscriber, is never recorded in
            // `lastDelivered`, and the next resume answers no offset for it.
            // That is the SILENT SKIP half of criterion 11. This order's worst
            // case is a push delivered but not yet recorded, which is a
            // possible duplicate -- the half a consumer can act on.
            List<Subscription> early = new ArrayList<>();
            try {
                for (RunKey key : full) {
                    if (registrations.stream().noneMatch(r -> r.key.equals(key))) {
                        early.add(subscribe(key, subscriber));
                    }
                }
                sessions.reestablish(id, subscriber, full);
            } catch (RuntimeException failed) {
                // ⚠️ THE SAME ROLLBACK `resume` HAS, for the same reason: a
                // registration the caller was never told about is one nothing
                // can ever close.
                early.forEach(Subscription::close);
                throw failed;
            }
            registrations.addAll(early);
            for (int i = registrations.size() - 1; i >= 0; i--) {
                if (!full.contains(registrations.get(i).key)) {
                    registrations.remove(i).close();
                }
            }
        }

        /**
         * Reconnects at {@code sessionEpoch}, adding and removing streams, and
         * answers where each stream this session keeps goes on.
         *
         * <p>⚠️ THE REGISTRATIONS ARE REBUILT TO MATCH, and the order is
         * add-then-remove rather than close-then-open: a stream carried across
         * the resume is never unregistered, so no push can fall into a gap. A
         * stream that is removed is unregistered after the new set is in place.
         */
        public synchronized Resumed resume(long sessionEpoch,
                Collection<RunKey> add, Collection<RunKey> remove) throws SessionResetException {
            List<RunKey> before = sessions.keysOf(id);
            // ⚠️ REGISTERED BEFORE THE REGISTRY IS TOLD, so an added stream has
            // no window in which it is part of the session and not yet
            // subscribed -- a publish landing there would be dropped, and the
            // consumer would never learn of a commit it had asked for. The
            // reverse order is the one that reads naturally and is wrong.
            //
            // ⚠️ IF THE REGISTRY REFUSES, THE EARLY REGISTRATIONS COME BACK
            // OFF. A refused resume applies none of its deltas, and a
            // registration is a delta the consumer did not get told about.
            List<Subscription> early = new ArrayList<>();
            for (RunKey key : new LinkedHashSet<>(add)) {
                if (!before.contains(key)) {
                    early.add(subscribe(key, subscriber));
                }
            }
            Resumed answer;
            try {
                answer = sessions.resume(id, sessionEpoch, add, remove);
            } catch (RuntimeException | SessionResetException refused) {
                // ⚠️ BOTH, AND THE CHECKED ONE WAS THE GAP. `SessionResetException`
                // arrived with M5.15c and this catch listed only
                // `RuntimeException`, so a session reset interleaving with a
                // resume left the early registrations neither closed nor
                // recorded -- unreachable by `close()` and therefore permanent
                // duplicate delivery.
                early.forEach(Subscription::close);
                throw refused;
            }
            registrations.addAll(early);
            List<RunKey> after = sessions.keysOf(id);
            // ⚠️ REMOVED LAST, for the same reason: a stream carried ACROSS the
            // resume is never unregistered, so nothing can fall into a gap.
            for (int i = registrations.size() - 1; i >= 0; i--) {
                if (!after.contains(registrations.get(i).key)) {
                    registrations.remove(i).close();
                }
            }
            return answer;
        }

        @Override
        public synchronized void close() {
            registrations.forEach(Subscription::close);
            registrations.clear();
            sessions.close(id);
        }
    }

    /**
     * Opens a session for one consumer over {@code keys}.
     *
     * <p>⚠️ ONE SUBSCRIBER FOR EVERY KEY, which is what makes a session cheap:
     * the hub groups by subscriber IDENTITY, so a session over K streams is one
     * consumer to the serving path and takes the segment once (M5.40a, M5.62).
     */
    public Session openSession(Subscriber subscriber, Collection<RunKey> keys) {
        Objects.requireNonNull(subscriber, "subscriber");
        Objects.requireNonNull(keys, "keys");
        // ⚠️ DEDUPED HERE, because the registry dedupes and this did not. A
        // session opened over `[C, C]` subscribed C twice while the registry
        // held one C, so the removal loop kept both registrations and
        // `SegmentServingPath` put run C twice in the same consumer's push
        // list -- a duplicate delivery, which is the exact half of criterion 11
        // this task exists to make impossible.
        List<RunKey> copy = List.copyOf(new LinkedHashSet<>(keys));
        return new Session(sessions.open(subscriber, copy), subscriber, copy);
    }

    /**
     * What a subscriber actually RECEIVED, recorded for its session if it has
     * one. Called by the serving path after {@code complete}, never before.
     */
    void delivered(Subscriber subscriber, List<Push> pushes) {
        sessions.delivered(subscriber, pushes);
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
        // ⚠️ ONLY `UncheckedIOException` IS CAUGHT, and M5.45d WIDENED WHAT
        // THAT COVERS: `mintGrant` wraps a signing failure in one for exactly
        // this reason, so a signer outage on segment 1 no longer denies 2..n.
        // An earlier draft said the `DIRECT` arm throws `IllegalStateException`
        // and does deny them; that arm serves now, and what is left of it is
        // the null-issuer guard -- a misconfiguration, not an outage.
        //
        // ⚠️ AND THE RETHROW IS VISIBLE OUTSIDE THIS PROCESS, which M5.48
        // bought: `pushLoop` increments `undeliverablePushes` and logs a
        // WARNING naming the segment. An earlier draft called this handler a
        // bare `continue` with no log, metric or counter.
        UncheckedIOException firstFailure = null;
        for (SegmentCommit committed : delta.segments()) {
            byte[] held = committed.segmentKey().equals(heldSegmentKey) ? heldBytes : null;
            try {
                servingPath.publishSegment(committed, held, serving);
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
     * The subscribers registered for one stream, in first-subscription order.
     *
     * <p>⚠️ IT IS THE SEAM THE SERVING PATH READS THE REGISTRY THROUGH, and it
     * is deliberately not public: {@link SegmentServingPath} needs the
     * subscriber behind each {@link Subscription}, and that field is private to
     * this class. ⚠️ DUPLICATES ARE KEPT. {@link #subscribe} does not dedupe,
     * so a subscriber registered twice against one key appears twice here --
     * which is what {@code Subscriber.open}'s "one per subscription, strictly"
     * paragraph promises a caller.
     */
    List<Subscriber> subscribersFor(RunKey key) {
        var list = subscribers.get(key);
        if (list == null) {
            return List.of();
        }
        List<Subscriber> found = new ArrayList<>(list.size());
        for (Subscription s : list) {
            found.add(s.subscriber);
        }
        return found;
    }

}
