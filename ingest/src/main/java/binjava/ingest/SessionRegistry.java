// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import binjava.format.RunKey;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Where a consumer's session lives on the INGESTER side (M5.15b, FR-6).
 *
 * <p>⚠️ THE PROPERTY IS "THE NEXT RECORD", NOT "ROUGHLY WHERE IT WAS". A
 * consumer that reconnects with {@code (sessionId, sessionEpoch)} and a set of
 * {@code add}/{@code remove} deltas is told, per stream it keeps, the offset it
 * must see next -- one past the last offset this hub actually DELIVERED to it.
 * No duplicate and no skip is the whole criterion (SPEC criterion 11), and both
 * halves are failures a "roughly" answer produces.
 *
 * <p>⚠️ DELIVERED, NOT PUSHED, and the difference is where the defect would
 * live. {@link SegmentServingPath} completes only the sinks that took every
 * byte; a sink that threw mid-stream received a truncated prefix and is NOT
 * completed. Recording at push time would move a session's resume point past
 * records the consumer never got, which is a SKIP -- the silent half of the
 * criterion, and the half a consumer cannot detect.
 *
 * <p>⚠️ THE SESSION EPOCH IS KIP-227's, NOT THE SEQUENCER'S TERM, which is the
 * confusion SPEC criterion 12 exists to forbid. It orders requests WITHIN one
 * session so a retried resume is idempotent: resuming at an epoch this registry
 * has already served is answered with the SAME answer rather than advancing,
 * and resuming at one it has passed is refused. A sequencer failover does not
 * touch it, and bumping it says nothing about the chain.
 *
 * <p>⚠️ IT MINTS THE IDENTIFIER, which is the decision M5.14's review left
 * open. A consumer-supplied id would let two consumers claim one session --
 * accidentally, by copying a config -- and the second would resume from the
 * first's offsets, which is a skip with no symptom.
 */
final class SessionRegistry {

    /**
     * ⚠️ IDENTITY, NOT {@code equals}. A {@code Subscriber} is caller-supplied
     * and may implement {@code equals} however it likes; two that merely compare
     * equal are two consumers, and merging their sessions hands one the other's
     * resume point. Same rule as {@code SegmentServingPath.ByIdentity}, and the
     * same reason.
     */
    private record ByIdentity(SubscriptionHub.Subscriber subscriber) {
        @Override
        public boolean equals(Object other) {
            return other instanceof ByIdentity o && o.subscriber == this.subscriber;
        }

        @Override
        public int hashCode() {
            return System.identityHashCode(subscriber);
        }
    }

    /** The deltas a resume carried, so a retry can be told from a new request. */
    private record Request(long epoch, List<RunKey> add, List<RunKey> remove) {
    }

    private static final class State {
        private final SubscriptionHub.Subscriber subscriber;
        private final Map<RunKey, Long> lastDelivered = new LinkedHashMap<>();
        private final List<RunKey> keys = new ArrayList<>();
        private long epoch;
        private Request lastRequest;
        private SubscriptionHub.Resumed lastAnswer;

        State(SubscriptionHub.Subscriber subscriber) {
            this.subscriber = subscriber;
        }
    }

    private final Map<String, State> byId = new HashMap<>();
    private final Map<ByIdentity, String> idOf = new HashMap<>();

    /** Opens a session for {@code subscriber} over {@code keys}, at epoch 0. */
    synchronized String open(SubscriptionHub.Subscriber subscriber, Collection<RunKey> keys) {
        Objects.requireNonNull(subscriber, "subscriber");
        // ⚠️ ONE SESSION PER SUBSCRIBER INSTANCE, REFUSED RATHER THAN
        // OVERWRITTEN. `idOf` maps identity to one id, so a second open on the
        // same subscriber used to clobber the first -- which then stopped
        // recording deliveries and would resume BEHIND the truth, a duplicate
        // with no symptom. A consumer that wants two sessions brings two
        // subscribers, which is also what the hub's grouping means by two
        // consumers.
        if (idOf.containsKey(new ByIdentity(subscriber))) {
            throw new IllegalStateException("this subscriber already holds session "
                    + idOf.get(new ByIdentity(subscriber))
                    + "; close it before opening another, or use a second subscriber");
        }
        State state = new State(subscriber);
        state.keys.addAll(keys);
        String id = UUID.randomUUID().toString();
        byId.put(id, state);
        idOf.put(new ByIdentity(subscriber), id);
        return id;
    }

    /** The streams this session currently holds, in the order they were added. */
    synchronized List<RunKey> keysOf(String session) {
        return List.copyOf(state(session).keys);
    }

    synchronized long epochOf(String session) {
        return state(session).epoch;
    }

    /**
     * Records what a subscriber actually received, so a resume can answer with
     * the next record rather than with the last one again.
     *
     * <p>⚠️ THE HIGHEST WINS, NOT THE LATEST. Deliveries to one consumer are
     * ordered by the push thread today, but a session's resume point must not
     * depend on that: an out-of-order record that moved the point BACKWARDS
     * would re-deliver everything between, which is the duplicate half of the
     * criterion.
     */
    synchronized void delivered(SubscriptionHub.Subscriber subscriber,
            List<SubscriptionHub.Push> pushes) {
        String id = idOf.get(new ByIdentity(subscriber));
        if (id == null) {
            // ⚠️ A SESSIONLESS SUBSCRIBER IS NOT AN ERROR. `subscribe` still
            // exists and every transport in the tree uses it; a session is what
            // a consumer opts into when it wants to resume.
            return;
        }
        State state = byId.get(id);
        for (SubscriptionHub.Push push : pushes) {
            state.lastDelivered.merge(push.key(), push.lastOffset(), Math::max);
        }
    }

    /**
     * Resumes {@code session} at {@code sessionEpoch}, applying the key deltas.
     *
     * <p>⚠️ THE SAME EPOCH IS ANSWERED, NOT RE-APPLIED. A resume is a request
     * like any other and its reply can be lost; a consumer that resends must
     * get the same answer rather than a second application of its deltas. That
     * is what the epoch is FOR -- KIP-227's ordering within a session -- and
     * without it a lost reply costs the consumer the very records this method
     * exists to place exactly.
     *
     * @throws IllegalStateException if {@code sessionEpoch} is behind what this
     *     registry has already served, which is a reordered or replayed request
     *     rather than a retry, and answering it would move the session
     *     backwards
     */
    synchronized SubscriptionHub.Resumed resume(String session, long sessionEpoch,
            Collection<RunKey> add, Collection<RunKey> remove) {
        State state = state(session);
        if (sessionEpoch < state.epoch) {
            throw new IllegalStateException("session " + session + " is at epoch " + state.epoch
                    + " and this request carries " + sessionEpoch
                    + "; answering it would move the session backwards");
        }
        Request request = new Request(sessionEpoch, List.copyOf(add), List.copyOf(remove));
        if (sessionEpoch == state.epoch && state.lastAnswer != null) {
            // ⚠️ THE REQUEST IS COMPARED, NOT ONLY THE EPOCH. Keyed on the
            // epoch alone, a genuinely NEW request that reuses one has its
            // deltas silently dropped: the consumer believes its `add` took
            // effect and waits forever on a stream nothing registered it for.
            // A retry is the same request; anything else at the same epoch is a
            // protocol error and is refused loudly rather than half-applied.
            if (request.equals(state.lastRequest)) {
                return state.lastAnswer;
            }
            throw new IllegalStateException("session " + session + " already served epoch "
                    + sessionEpoch + " with different deltas; a retry resends the same request "
                    + "and a new request carries a new epoch");
        }
        state.keys.removeAll(List.copyOf(remove));
        for (RunKey key : add) {
            if (!state.keys.contains(key)) {
                state.keys.add(key);
            }
        }
        state.lastDelivered.keySet().retainAll(state.keys);
        state.epoch = sessionEpoch;

        Map<RunKey, Long> next = new LinkedHashMap<>();
        for (RunKey key : state.keys) {
            Long last = state.lastDelivered.get(key);
            // ⚠️ A STREAM WITH NOTHING DELIVERED IS ABSENT, not zero. Zero is a
            // real offset, and answering it for a stream this session has never
            // seen would tell a consumer to expect the beginning of a run that
            // may be long gone -- a request for records nothing will push.
            if (last != null) {
                next.put(key, last + 1);
            }
        }
        state.lastRequest = request;
        state.lastAnswer = new SubscriptionHub.Resumed(sessionEpoch, Map.copyOf(next));
        return state.lastAnswer;
    }

    synchronized void close(String session) {
        State state = byId.remove(session);
        if (state != null) {
            idOf.remove(new ByIdentity(state.subscriber));
        }
    }

    private State state(String session) {
        State state = byId.get(Objects.requireNonNull(session, "session"));
        if (state == null) {
            throw new IllegalStateException("no such session: " + session
                    + "; a consumer whose session is gone re-subscribes rather than resuming");
        }
        return state;
    }
}
