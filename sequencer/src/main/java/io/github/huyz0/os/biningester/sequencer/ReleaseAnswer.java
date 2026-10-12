// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.FastTakeoverFrame;
import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

/**
 * A holder's answer to a RELEASE (ADR-0081 §5 step 7; M13.28b): from the
 * leader of a term it joined, each named stream's entries wholly below its
 * offset are released, then answered by an empty HELD_STATUS.
 *
 * <p>⚠️ NOT FSYNCED BEFORE THE ANSWER: a released entry is committed, so one
 * a crash brings back is only held again, and costs one HELD report
 * (§5.7) -- never an fsync per RELEASE.
 *
 * <p>⚠️ REFUSED NOT_ROSTERED from anyone but the term's leader: a reused name
 * or address never drops another term's copies.
 */
public final class ReleaseAnswer {

    private final FastJournal journal;
    private final JoinedTerms joined;

    public ReleaseAnswer(FastJournal journal, JoinedTerms joined) {
        this.journal = Objects.requireNonNull(journal, "journal");
        this.joined = Objects.requireNonNull(joined, "joined");
    }

    /**
     * The answer to {@code release}, sent under {@code header}.
     *
     * @throws IOException the journal failed; the leader sends it again
     */
    public FastFrame.Body answer(FastFrame.Header header, FastTakeoverFrame.Release release)
            throws IOException {
        Objects.requireNonNull(header, "header");
        Objects.requireNonNull(release, "release");
        Optional<String> leader = joined.leaderOf(header.epoch());
        if (leader.isEmpty() || !leader.get().equals(header.senderUid())) {
            return new FastFrame.Refused(FastFrame.Reason.NOT_ROSTERED, Optional.empty(),
                    header.senderUid() + " is not the leader of a term this pod joined");
        }
        for (FastTakeoverFrame.ReleaseAt at : release.streams()) {
            journal.release(at.stream(), at.below());
        }
        return new FastFrame.HeldStatusReport(FastFrame.HeldStatus.NONE);
    }
}
