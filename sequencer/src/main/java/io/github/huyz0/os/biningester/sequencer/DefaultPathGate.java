// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.format.RunKey;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * What a default-path run may not do at a fast term's leader, split out of
 * {@link FastLeaderTerm} at its ceiling:
 *
 * <ul>
 *   <li>⚠️ WAIT FOR A TAKEOVER'S DECISION (ADR-0081 §5 step 3's tail;
 *       M13.33n): a run of an index recorded in a walked term, until its
 *       stream is decided -- held too before any desk is built, as no
 *       takeover ran yet;
 *   <li>⚠️ LAND ON A FAST STREAM (ADR-0081 §8, ADR-0082 §7; M13.34c): a run of
 *       an index the leader's catalog has in fast mode -- its offsets are the
 *       fast cursor's, which a default commit would take from under it.
 *       Until a writer asks for the redirecting answer (M13.34e) the whole
 *       request is refused, DEFINITELY ({@link CommitRefusedException}): a
 *       forward answers it and never defers it.
 * </ul>
 *
 * <p>⚠️ STATED LIMITS, M13.34c's and M13.34d's to close: the catalog is
 * learnt once per drain tick, so a commit between a catalog's flip to fast
 * and the next tick is admitted; an inbox intent deferred EARLIER (the leader
 * unreachable, the writer's registration stale) for a stream now fast is
 * refused by every drain, and stalls that pod's inbox until the redirecting
 * answer exists; and a switch to {@code wal=false} admits default commits as
 * soon as the catalog says so, though a fast tail is uncommitted.
 */
final class DefaultPathGate {

    /** The indices with a term-record value in a term this one walked. */
    private final Set<UUID> walked = ConcurrentHashMap.newKeySet();
    /** The catalog's fast indices, as last told; the term's starting ones first. */
    private volatile Set<UUID> catalogFast;

    DefaultPathGate(FastTermOpening.Opened opened) {
        Objects.requireNonNull(opened, "opened");
        for (Roster r : opened.stillOpen()) {
            r.termRecord().forEach(t -> walked.addAll(t.walQuorum().keySet()));
        }
        Set<UUID> starting = new HashSet<>();
        opened.started().own().termRecord().forEach(t -> starting.addAll(t.walQuorum().keySet()));
        catalogFast = Set.copyOf(starting);
    }

    /** No takeover will run, its leader holding no journal: nothing is held (M13.33n). */
    void noTakeover() {
        walked.clear();
    }

    /** Whether a default-path run of {@code stream} waits for the takeover over {@code desk}. */
    boolean holds(RunKey stream, CommitDesk desk) {
        if (!walked.contains(stream.indexId())) {
            return false;
        }
        return desk == null || desk.leader().awaitsDecision(stream);
    }

    /** The catalog's fast indices now: what {@link #fast} answers from. */
    void catalogIs(Set<UUID> fast) {
        catalogFast = Set.copyOf(fast);
    }

    /** Whether the catalog has {@code stream}'s index in fast mode. */
    boolean fast(RunKey stream) {
        return catalogFast.contains(stream.indexId());
    }
}
