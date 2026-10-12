// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.FastTakeoverFrame;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * A member's answer to a successor's FENCE (ADR-0081 §5 step 4; M13.33b):
 * what closed terms left is dropped, durably, and the page asked of every
 * entry it holds of the fenced terms is answered as COLLECTED.
 *
 * <p>⚠️ THE EPOCH IS RAISED BEFORE THIS RUNS: the router raises the pod's
 * fence durably to a higher frame's epoch before any handler, so nothing of
 * the fenced terms is journaled after the answer is read.
 *
 * <p>⚠️ ONE FIXED ORDER, PAGED BY NUMBER: by term, {@code assignedAfter},
 * stream and first offset, cut into pages of at most {@code pageBytes} -- an
 * entry larger alone on its page. With the fence raised, what a member holds
 * of the fenced terms does not change while it is collected, so a page asked
 * again is the same page and a lost answer skips nothing.
 */
public final class FenceAnswer {

    private static final Comparator<FastJournalRecord.Entry> ORDER =
            Comparator.comparingLong(FastJournalRecord.Entry::epoch)
                    .thenComparingLong(FastJournalRecord.Entry::assignedAfter)
                    .thenComparing(e -> e.key().indexId())
                    .thenComparingInt(e -> e.key().partitionId())
                    .thenComparingLong(FastJournalRecord.Entry::firstOffset);

    private final FastJournal journal;
    private final long pageBytes;
    private final HolderGate gate;

    /** @param pageBytes the most entry bytes one COLLECTED page carries */
    public FenceAnswer(FastJournal journal, long pageBytes) {
        this(journal, pageBytes, new HolderGate());
    }

    /**
     * The same, under the pod's {@code gate} (M13.33b review round 1, P1): a
     * REPLICA of a fenced term that passed the router's fence just before the
     * raise appends under the gate and re-checks the fence there, so it lands
     * before a page is read or not at all.
     */
    public FenceAnswer(FastJournal journal, long pageBytes, HolderGate gate) {
        this.gate = Objects.requireNonNull(gate, "gate");
        this.journal = Objects.requireNonNull(journal, "journal");
        if (pageBytes < 1) {
            throw new IllegalArgumentException("a page carries at least one byte");
        }
        this.pageBytes = pageBytes;
    }

    /**
     * The answer to {@code fence}, sent under {@code header}.
     *
     * @throws IOException the journal failed; nothing is answered and the
     *     successor asks again
     */
    public FastFrame.Body answer(FastFrame.Header header, FastTakeoverFrame.Fence fence)
            throws IOException {
        Objects.requireNonNull(header, "header");
        Objects.requireNonNull(fence, "fence");
        List<Long> fenced = fence.unclosedEpochs();
        if (header.epoch() <= fenced.get(fenced.size() - 1)) {
            return new FastFrame.Refused(FastFrame.Reason.NOT_ROSTERED, Optional.empty(),
                    "a FENCE of term " + header.epoch() + " does not take over " + fenced);
        }
        return gate.exclusively(() -> page(fence));
    }

    private FastFrame.Body page(FastTakeoverFrame.Fence fence) throws IOException {
        List<Long> fenced = fence.unclosedEpochs();
        dropClosedThrough(fence.closedThrough());
        Set<Long> terms = new HashSet<>(fenced);
        List<FastJournalRecord.Entry> held = new ArrayList<>();
        for (FastJournalRecord.Entry e : journal.held()) {
            if (terms.contains(e.epoch())) {
                held.add(e);
            }
        }
        held.sort(ORDER);
        List<List<FastJournalRecord.Entry>> pages = pages(held);
        long asked = fence.page();
        if (asked >= pages.size()) {
            return new FastTakeoverFrame.Collected(asked, true, List.of());
        }
        return new FastTakeoverFrame.Collected(asked, asked == pages.size() - 1,
                pages.get((int) asked));
    }

    /** Drops, durably, every group of a term at or below {@code closedThrough}. */
    private void dropClosedThrough(long closedThrough) throws IOException {
        boolean dropped = false;
        for (FastJournalRecord.Entry e : journal.held()) {
            if (e.epoch() <= closedThrough) {
                journal.drop(e.key(), e.epoch(), e.assignedAfter(), 0);
                dropped = true;
            }
        }
        if (dropped) {
            journal.sync();
        }
    }

    private List<List<FastJournalRecord.Entry>> pages(List<FastJournalRecord.Entry> held) {
        List<List<FastJournalRecord.Entry>> pages = new ArrayList<>();
        List<FastJournalRecord.Entry> page = new ArrayList<>();
        long bytes = 0;
        for (FastJournalRecord.Entry e : held) {
            long size = e.encode().length;
            if (!page.isEmpty() && bytes + size > pageBytes) {
                pages.add(page);
                page = new ArrayList<>();
                bytes = 0;
            }
            page.add(e);
            bytes += size;
        }
        if (!page.isEmpty()) {
            pages.add(page);
        }
        return pages;
    }
}
