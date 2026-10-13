// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.sequencer.FastFrameRouter;
import io.github.huyz0.os.biningester.sequencer.FastLeaderTerm;
import io.github.huyz0.os.biningester.sequencer.LocalSequencer;
import io.github.huyz0.os.biningester.sequencer.Sequencer;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * The loss of a holder of an unreleased entry, found by the leader's drain
 * (ADR-0081 §7; M13.31f): a pod holding a copy the chain has not committed
 * that the EndpointSlice no longer lists is reported to the term's desk,
 * which asks an upload at once (M13.31e) -- shrinking the window in which a
 * second loss is quorum loss.
 *
 * <p>⚠️ READ FROM MEMORY, NO REQUEST: the EndpointSlice view the membership
 * watch keeps. A pod whose UID is deleted leaves the slice too, so no
 * per-holder Kubernetes lookup is made -- it would scale with holders times
 * drains. With no view, or one that has heard no event, nobody is lost.
 *
 * <p>⚠️ ONCE PER LOSS: a lost holder is reported once, and again only after
 * the slice lists it and then drops it anew; a new term starts from nothing.
 * Only {@code holderLost} is asked -- never {@code QuorumFrontier.lost}, which
 * drops the pod's unexposed copies first (M13.31e review round 1, P1).
 */
final class FastHolderLoss {

    private long epoch = -1;
    private final Set<String> reported = new HashSet<>();

    /** Reports each newly lost holder of the leading term's unreleased entries. */
    synchronized void check(Sequencer held, FastDisk disk, Roster.Incarnation self,
            FastFrameRouter.Liveness live) {
        Objects.requireNonNull(live, "live");
        Optional<FastLeaderTerm> term = LocalSequencer.underneath(held)
                .filter(LocalSequencer::serving).flatMap(LocalSequencer::fastTerm);
        if (term.isEmpty() || disk.journal().isEmpty()) {
            return;
        }
        var desk = term.get().commitDesk(disk.journal().get(), self);
        for (String uid : newlyLost(term.get().epoch(), desk.holders(), live)) {
            desk.holderLost(uid);
        }
    }

    /**
     * Of {@code holders}, those of term {@code termEpoch} lost since last
     * asked: unlisted and not yet reported -- a holder listed again, or no
     * longer a holder, reported anew on its next loss; a new term from
     * nothing.
     */
    synchronized java.util.List<String> newlyLost(long termEpoch, Set<String> holders,
            FastFrameRouter.Liveness live) {
        if (termEpoch != epoch) {
            epoch = termEpoch;
            reported.clear();
        }
        reported.removeIf(uid -> live.live(uid) || !holders.contains(uid));
        java.util.List<String> lost = new java.util.ArrayList<>();
        for (String uid : holders) {
            if (!live.live(uid) && reported.add(uid)) {
                lost.add(uid);
            }
        }
        return lost;
    }
}
