// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.Roster;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A leader's shutdown (ADR-0081 §9): it FIRST stops assigning and exposing
 * (M13.35a), uploads and commits what it exposed (M13.35c), then marks itself
 * departed in its roster -- closing it in the same write only if no earlier
 * term was open when it started, a takeover it may not have finished being
 * its successor's -- and in each earlier unclosed roster listing it where it
 * holds no entry of that term (M13.35d), so its successor is handed over
 * with no fence and no collection.
 *
 * <p>⚠️ NEVER WITH AN EXPOSED ENTRY UNCOMMITTED: the leader then stays
 * undeparted, its stop counted as a crash would be.
 *
 * <p>⚠️ AN EARLIER TERM IT HOLDS ANY ENTRY OF STAYS UNDEPARTED, its deletion
 * counted there as a crash would be: an entry still in the journal is not
 * known committed, and §9 departs a term only with none pending -- a
 * stricter test than pending, never a looser one.
 */
final class LeaderLeave {

    private LeaderLeave() {
    }

    /**
     * @param leader the term's desk's leader, null when no desk was built
     * @param journal the leader's fast journal, if it has one
     * @throws IOException the store failed; the leader stops undeparted
     */
    static FastLeaderTerm.Left leave(FastWriteLeader leader, Optional<FastJournal> journal,
            FastUploader.Last last, FastTermOpening.Opened opened, RosterDepartures departures)
            throws IOException {
        if (leader != null && leader.stop()) {
            try {
                last.upload();
            } catch (IOException failed) {
                return FastLeaderTerm.Left.STAYED;
            }
            if (leader.stop()) {
                return FastLeaderTerm.Left.STAYED;
            }
        }
        String self = opened.started().own().leader().podUid();
        boolean close = opened.stillOpen().isEmpty();
        if (departures.leave(self, close) instanceof RosterDepartures.Deposed) {
            return FastLeaderTerm.Left.DEPOSED;
        }
        Set<Long> holding = journal.map(j -> j.held().stream()
                .map(FastJournalRecord.Entry::epoch).collect(Collectors.toSet()))
                .orElse(Set.of());
        List<Long> earlier = opened.stillOpen().stream().map(Roster::epoch)
                .filter(e -> !holding.contains(e)).toList();
        if (!earlier.isEmpty()
                && departures.depart(self, earlier) instanceof RosterDepartures.Deposed) {
            return FastLeaderTerm.Left.DEPOSED;
        }
        return close ? FastLeaderTerm.Left.DEPARTED_AND_CLOSED : FastLeaderTerm.Left.DEPARTED;
    }
}
