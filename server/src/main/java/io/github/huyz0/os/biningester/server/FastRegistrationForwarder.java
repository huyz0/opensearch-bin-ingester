// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.IndexRegistration;
import io.github.huyz0.os.biningester.ingest.IndexCatalog;
import io.github.huyz0.os.biningester.sequencer.JoinedTerms;
import io.github.huyz0.os.biningester.sequencer.MonotonicClock;
import io.github.huyz0.os.biningester.sequencer.TermJoiner;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * A writer's fast indices, told to its leader (M13.83b; ADR-0081 §4,
 * ADR-0082 §2): the plugin registers an index only with the ingester its node
 * talks to, so every {@code wal=true} registration in this pod's catalog is
 * forwarded as a REGISTER to the leader of the newest term it joined, and so
 * is a forwarded index's switch to {@code wal=false} (M13.34ad) -- the
 * leader's term record is fed from its own catalog, and a fast batch of an
 * index it never recorded waits.
 *
 * <p>⚠️ ONCE PER REGISTRATION PER LEADER: sent again only on a change, a new
 * leader, or a send that failed or was refused; nothing when nothing changed,
 * and nothing by the pod that leads -- its catalog is the one the record is
 * fed from. Frames scale with index changes and leader changes, never
 * records.
 *
 * <p>⚠️ PACED ON A MISS (review round 1, P1/P2): it runs on every pass of the
 * exposure drain, which a fast batch's acks wake. A send that failed or was
 * refused stops the pass -- the leader that missed one would miss the rest,
 * and each miss can wait a whole exchange timeout -- and nothing more is sent
 * until {@link #RETRY_INTERVAL} has passed, so a leader that is down or has
 * not rostered this pod yet costs one frame a second, not one per batch.
 */
final class FastRegistrationForwarder {

    /** How long after a missed send the next is tried. */
    static final Duration RETRY_INTERVAL = Duration.ofSeconds(1);

    private final String selfUid;
    private final TermJoiner.Transport transport;
    private final MonotonicClock mono;
    /** No send before this monotonic instant, after a miss. */
    private long retryAt;
    private boolean missed;
    /** What each leader epoch took, by index name. */
    private long takenEpoch = -1;
    private final Map<String, IndexRegistration> taken = new HashMap<>();

    FastRegistrationForwarder(String selfUid, TermJoiner.Transport transport,
            MonotonicClock mono) {
        this.selfUid = Objects.requireNonNull(selfUid, "selfUid");
        this.transport = Objects.requireNonNull(transport, "transport");
        this.mono = Objects.requireNonNull(mono, "mono");
    }

    /**
     * Forwards what the leader has not taken.
     *
     * @param leaders the newest joined term's leader
     * @param leading whether this pod leads a term itself
     */
    synchronized void forward(IndexCatalog catalog,
            Supplier<Optional<JoinedTerms.Leader>> leaders, boolean leading) {
        if (leading) {
            return;
        }
        Optional<JoinedTerms.Leader> leader = leaders.get();
        if (leader.isEmpty()) {
            return;
        }
        if (missed && mono.nanos() - retryAt < 0) {
            return;
        }
        missed = false;
        JoinedTerms.Leader to = leader.get();
        if (to.epoch() != takenEpoch) {
            taken.clear();
            takenEpoch = to.epoch();
        }
        // ⚠️ AND EACH INDEX THIS LEADER TOOK AS FAST THAT IS NO LONGER FAST
        // HERE (M13.34ad): its switch to the default path, or the leader keeps
        // the fast registration this pod told it.
        java.util.List<IndexRegistration> owed = new java.util.ArrayList<>(
                catalog.fastRegistrations());
        for (IndexRegistration was : taken.values()) {
            if (was.wal()) {
                catalog.resolve(was.indexName())
                        .filter(now -> now.indexName().equals(was.indexName()) && !now.wal())
                        .ifPresent(owed::add);
            }
        }
        for (IndexRegistration registration : owed) {
            if (registration.equals(taken.get(registration.indexName()))) {
                continue;
            }
            byte[] frame;
            try {
                frame = FastFrame.encode(to.epoch(), selfUid, to.uid(),
                        new FastFrame.Register(registration));
            } catch (IllegalArgumentException tooLarge) {
                // ⚠️ review round 1, T2: larger than a REGISTER may be (ADR-0082
                // §2), so never one -- and no miss, which would hold back the rest.
                continue;
            }
            try {
                FastFrame.Frame answer = FastFrame.decode(transport.exchange(to.endpoint(), frame));
                if (answer.body() instanceof FastFrame.HeldStatusReport) {
                    taken.put(registration.indexName(), registration);
                    continue;
                }
            } catch (IOException | IllegalArgumentException unanswered) {
                // an unanswered REGISTER is not forwarded
            }
            missed = true;
            retryAt = mono.nanos() + RETRY_INTERVAL.toNanos();
            return;
        }
    }
}
