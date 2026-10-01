// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.RunKey;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * A holder's side of HELD and HELD_STATUS (ADR-0081 §5.7, §9; M13.26h): what
 * its journal holds, by stream and by {@code (epoch, assignedAfter)} group,
 * and what the leader's answer lets it drop.
 *
 * <p>⚠️ ONLY WHAT THE ANSWER SAYS IS SAFE IS DROPPED: a stream's entries below
 * its committed next offset are released, a group's part at or above the
 * offset a decision supersedes it from is dropped, and a group the leader
 * answers of a closed term is dropped whole -- a committed or superseded one
 * is settled by the release and the drop at the superseding offset alone. A
 * PENDING group is kept -- a departing pod waits for it, and never drops an
 * uncommitted live copy, so its departure is never counted as quorum loss.
 */
public final class HeldReports {

    private HeldReports() {
    }

    /** The HELD body for {@code held}, a journal's held entries. */
    public static FastFrame.Held of(List<FastJournalRecord.Entry> held) {
        Map<RunKey, Map<List<Long>, long[]>> byStream = new TreeMap<>();
        Comparator<List<Long>> group = Comparator.<List<Long>, Long>comparing(g -> g.get(0))
                .thenComparing(g -> g.get(1));
        for (FastJournalRecord.Entry e : held) {
            long[] span = byStream.computeIfAbsent(e.key(), k -> new TreeMap<>(group))
                    .computeIfAbsent(List.of(e.epoch(), e.assignedAfter()),
                            g -> new long[] {Long.MAX_VALUE, Long.MIN_VALUE});
            span[0] = Math.min(span[0], e.firstOffset());
            span[1] = Math.max(span[1], e.endOffset() - 1);
        }
        List<FastFrame.HeldStream> streams = new ArrayList<>();
        byStream.forEach((key, groups) -> {
            List<FastFrame.HeldGroup> list = new ArrayList<>();
            groups.forEach((g, span) -> list.add(
                    new FastFrame.HeldGroup(g.get(0), g.get(1), span[0], span[1])));
            streams.add(new FastFrame.HeldStream(key, list));
        });
        return new FastFrame.Held(streams);
    }

    /**
     * Applies the leader's answer to {@code journal}, durably.
     *
     * @return whether any group is still PENDING -- a departing pod waits,
     *     and reports again after its next RELEASE
     */
    public static boolean apply(FastJournal journal, FastFrame.HeldStatus status)
            throws IOException {
        boolean pending = false;
        for (FastFrame.StreamStatus s : status.streams()) {
            journal.release(s.stream(), s.releaseBelow());
            for (FastFrame.GroupStatus g : s.groups()) {
                if (g.supersededFrom() != Long.MAX_VALUE) {
                    journal.drop(s.stream(), g.epoch(), g.assignedAfter(), g.supersededFrom());
                }
                // ⚠️ ONLY A CLOSED TERM IS DROPPED WHOLE (M13.26f review round 2,
                // P4; M13.26h review round 2, P8): the release below the
                // committed next offset and the drop at the superseding offset
                // settle all a COMMITTED or SUPERSEDED answer covers, and a
                // whole-group drop would take an entry appended to the group
                // after the report -- REPLICA batches may arrive out of order,
                // so one below the superseding offset, not superseded, and
                // acked. A closed term takes no new entry: the epoch fence
                // refuses its late REPLICA.
                switch (g.status()) {
                    case CLOSED_TERM ->
                            journal.drop(s.stream(), g.epoch(), g.assignedAfter(), 0);
                    case COMMITTED, SUPERSEDED -> { }
                    case PENDING -> pending = true;
                }
            }
        }
        journal.sync();
        return pending;
    }
}
