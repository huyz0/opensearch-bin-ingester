// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.format.RunKey;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A fast term's publication at its leader (ADR-0081 §6; M13.30d): which
 * rostered pod is interested in which stream, and what each exposure step
 * owes each of them -- each exposed entry once, in offset order per stream.
 *
 * <p>⚠️ PER TERM: a new term starts with no interest, and every pod with a
 * subscriber on a fast stream sends its INTEREST again on the leader change.
 *
 * <p>⚠️ BEST EFFORT AND BOUNDED: what one exposure step owes is taken whole
 * by the next drain; a PUBLISH that cannot be sent is dropped, and its pod's
 * subscribers read the entry from the chain once uploaded, as they would
 * without publication. Nothing owed outlives a drain.
 */
public final class PublishDesk {

    private final Map<Roster.Incarnation, Set<RunKey>> interest = new HashMap<>();
    private final Map<Roster.Incarnation, List<FastJournalRecord.Entry>> owed =
            new LinkedHashMap<>();

    /** {@code pod}'s interest changed: {@code added} streams, {@code removed} ones. */
    public synchronized void interest(Roster.Incarnation pod, List<RunKey> added,
            List<RunKey> removed) {
        Objects.requireNonNull(pod, "pod");
        Set<RunKey> streams = interest.computeIfAbsent(pod, p -> new HashSet<>());
        streams.addAll(added);
        streams.removeAll(removed);
        if (streams.isEmpty()) {
            interest.remove(pod);
        }
    }

    /**
     * One exposure step's entries, each owed to every pod interested in its
     * stream, in offset order per stream.
     */
    public synchronized void exposed(List<FastJournalRecord.Entry> entries) {
        if (entries.isEmpty() || interest.isEmpty()) {
            return;
        }
        List<FastJournalRecord.Entry> ordered = new ArrayList<>(entries);
        ordered.sort(Comparator.comparing((FastJournalRecord.Entry e) -> e.key().indexId())
                .thenComparingInt(e -> e.key().partitionId())
                .thenComparingLong(FastJournalRecord.Entry::firstOffset));
        interest.forEach((pod, streams) -> {
            for (FastJournalRecord.Entry e : ordered) {
                if (streams.contains(e.key())) {
                    owed.computeIfAbsent(pod, p -> new ArrayList<>()).add(e);
                }
            }
        });
    }

    /**
     * {@code pod}'s interest is exactly {@code streams} (M13.30k): the leader
     * pod's own, read from its hub on each drain rather than sent as a frame.
     */
    public synchronized void interestExactly(Roster.Incarnation pod, Set<RunKey> streams) {
        Objects.requireNonNull(pod, "pod");
        if (streams.isEmpty()) {
            interest.remove(pod);
        } else {
            interest.put(pod, new HashSet<>(streams));
        }
    }

    /** {@code pod} is no longer rostered as itself: its interest is forgotten. */
    public synchronized void forget(Roster.Incarnation pod) {
        interest.remove(pod);
        owed.remove(pod);
    }

    /** What is owed, by pod, taken: the next drain starts from nothing. */
    public synchronized Map<Roster.Incarnation, List<FastJournalRecord.Entry>> take() {
        Map<Roster.Incarnation, List<FastJournalRecord.Entry>> taken = new LinkedHashMap<>(owed);
        owed.clear();
        return taken;
    }
}
