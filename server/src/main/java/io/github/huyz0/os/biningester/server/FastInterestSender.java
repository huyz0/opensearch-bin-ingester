// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.FastPublishFrame;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.ingest.IndexCatalog;
import io.github.huyz0.os.biningester.sequencer.JoinedTerms;
import io.github.huyz0.os.biningester.sequencer.MonotonicClock;
import io.github.huyz0.os.biningester.sequencer.TermJoiner;
import java.io.IOException;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * This pod's fast streams with a subscriber, told to its leader (ADR-0081 §6;
 * M13.30g): every stream of a {@code wal=true} index a subscriber on this
 * pod's hub follows is sent as an INTEREST to the leader of the newest term
 * it joined, so the leader PUBLISHes its exposed entries here before the
 * upload.
 *
 * <p>⚠️ AS A DIFFERENCE, ONCE PER LEADER: what the leader took is kept, and an
 * INTEREST names only the streams added and removed since; a new leader starts
 * from nothing (its interest is per term) and is sent every stream. Nothing
 * when nothing changed, and nothing by the pod that leads, as REGISTER's
 * forwarder does not. Frames scale with subscriber changes and leader changes,
 * never records.
 *
 * <p>⚠️ PACED ON A MISS, as {@link FastRegistrationForwarder} is: a send that
 * failed or was refused leaves the leader's interest as it was, and nothing
 * more is sent until {@link #RETRY_INTERVAL} has passed.
 */
final class FastInterestSender {

    /** How long after a missed send the next is tried. */
    static final Duration RETRY_INTERVAL = Duration.ofSeconds(1);

    private final String selfUid;
    private final TermJoiner.Transport transport;
    private final MonotonicClock mono;
    /** No send before this monotonic instant, after a miss. */
    private long retryAt;
    private boolean missed;
    /** What the leader of {@code takenEpoch} took. */
    private long takenEpoch = -1;
    private final Set<RunKey> taken = new HashSet<>();

    FastInterestSender(String selfUid, TermJoiner.Transport transport, MonotonicClock mono) {
        this.selfUid = Objects.requireNonNull(selfUid, "selfUid");
        this.transport = Objects.requireNonNull(transport, "transport");
        this.mono = Objects.requireNonNull(mono, "mono");
    }

    /** Of {@code subscribed}, the streams of the catalog's fast indices. */
    static Set<RunKey> fastStreams(Set<RunKey> subscribed, IndexCatalog catalog) {
        Set<UUID> fast = catalog.fastRegistrations().stream()
                .map(r -> RunKey.ofIndexUuid(r.indexUuid(), 0).indexId())
                .collect(Collectors.toSet());
        return subscribed.stream().filter(k -> fast.contains(k.indexId()))
                .collect(Collectors.toSet());
    }

    /**
     * Sends the leader what changed in {@code wanted} since it last took it.
     *
     * @param wanted this pod's fast streams with a subscriber
     * @param leaders the newest joined term's leader
     * @param leading whether this pod leads a term itself
     */
    synchronized void send(Set<RunKey> wanted, Supplier<Optional<JoinedTerms.Leader>> leaders,
            boolean leading) {
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
        List<RunKey> added = wanted.stream().filter(k -> !taken.contains(k)).sorted().toList();
        List<RunKey> removed = taken.stream().filter(k -> !wanted.contains(k)).sorted().toList();
        if (added.isEmpty() && removed.isEmpty()) {
            return;
        }
        try {
            FastFrame.Frame answer = FastFrame.decode(transport.exchange(to.endpoint(),
                    FastFrame.encode(to.epoch(), selfUid, to.uid(),
                            new FastPublishFrame.Interest(added, removed))));
            if (answer.body() instanceof FastFrame.HeldStatusReport) {
                taken.clear();
                taken.addAll(wanted);
                return;
            }
        } catch (IOException | IllegalArgumentException unanswered) {
            // an unanswered INTEREST changed nothing at the leader
        }
        missed = true;
        retryAt = mono.nanos() + RETRY_INTERVAL.toNanos();
    }
}
