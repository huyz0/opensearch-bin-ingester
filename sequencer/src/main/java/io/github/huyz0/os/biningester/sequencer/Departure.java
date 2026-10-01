// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.Roster;
import java.io.IOException;
import java.util.Objects;

/**
 * A non-leader pod's graceful departure (ADR-0081 §9; M13.26h): it stops
 * accepting entries, asks the leader for an upload and reports what it holds
 * (DEPART phase 1), drops what the answer lets it drop, reports again after
 * each RELEASE while anything is pending (HELD), and only then asks to be
 * marked departed (DEPART phase 2) -- answered by an empty HELD_STATUS once
 * every {@code DEPARTED} write has landed.
 *
 * <p>⚠️ IT NEVER DROPS AN UNCOMMITTED LIVE COPY: what is pending stays, so
 * routine churn is never counted as quorum loss. If the grace period ends
 * first the pod stops undeparted, and counts as a crash would.
 *
 * <p>⚠️ EVERY ANSWER IS FENCED BEFORE IT IS READ, as {@link TermJoiner}'s
 * is: addressed to this incarnation, sent by the leader, admitted by the
 * pod's {@link EpochFence}, of the term asked.
 */
public final class Departure {

    private final Roster.Incarnation self;
    private final TermJoiner.Transport transport;
    private final EpochFence fence;
    private final FastJournal journal;
    private volatile boolean departing;
    /**
     * The term and leader a phase-1 DEPART -- the ask for an upload -- was
     * answered by; a new leader is asked again (M13.26f review round 2, P5).
     */
    private long reportedEpoch = -1;
    private String reportedLeader = "";

    public Departure(Roster.Incarnation self, TermJoiner.Transport transport, EpochFence fence,
            FastJournal journal) {
        this.self = Objects.requireNonNull(self, "self");
        this.transport = Objects.requireNonNull(transport, "transport");
        this.fence = Objects.requireNonNull(fence, "fence");
        this.journal = Objects.requireNonNull(journal, "journal");
    }

    /** Whether this pod still accepts entries: false from the first report on. */
    public boolean accepting() {
        return !departing;
    }

    /**
     * Reports what the journal holds -- DEPART phase 1 the first time, a HELD
     * after -- and applies the answer.
     *
     * @return whether anything is still pending: report again after the next
     *     RELEASE, and leave only once this returns false
     */
    public synchronized boolean report(long epoch, String leaderUid, String leaderEndpoint)
            throws IOException {
        departing = true;
        FastFrame.Held held = HeldReports.of(journal.held());
        // ⚠️ PHASE 1 UNTIL ONE IS ANSWERED (M13.26f review round 1, P2): it is
        // the ask for an upload, and a retry after it failed must ask again.
        boolean reported = reportedEpoch == epoch && reportedLeader.equals(leaderUid);
        FastFrame.Body ask = reported ? new FastFrame.HeldReport(held)
                : new FastFrame.Depart(self, 1, held);
        FastFrame.HeldStatus status = exchange(epoch, leaderUid, leaderEndpoint, ask);
        reportedEpoch = epoch;
        reportedLeader = leaderUid;
        // ⚠️ PENDING WHILE THE JOURNAL HOLDS ANYTHING (M13.26f review round 3,
        // P6): an entry appended after the report is in no answer, and the
        // leader settled none of it.
        boolean answeredPending = HeldReports.apply(journal, status);
        return answeredPending || !journal.held().isEmpty();
    }

    /**
     * Asks to be marked departed in every unclosed roster listing this pod.
     *
     * @throws IOException the answer was not the empty HELD_STATUS that says
     *     every {@code DEPARTED} write landed -- the pod has not departed
     */
    public void leave(long epoch, String leaderUid, String leaderEndpoint) throws IOException {
        departing = true;
        if (!journal.held().isEmpty()) {
            throw new IOException("the journal still holds " + journal.held().size()
                    + " entries: report until nothing is pending, then leave");
        }
        FastFrame.HeldStatus status = exchange(epoch, leaderUid, leaderEndpoint,
                new FastFrame.Depart(self, 2, FastFrame.Held.NONE));
        if (!status.streams().isEmpty()) {
            throw new IOException("a departure answered with entries still to settle");
        }
    }

    private FastFrame.HeldStatus exchange(long epoch, String leaderUid, String endpoint,
            FastFrame.Body ask) throws IOException {
        if (epoch < fence.highest()) {
            throw new TermJoiner.Fenced(fence.highest(), "term " + epoch
                    + " is below the fence at " + fence.highest());
        }
        byte[] answer = transport.exchange(endpoint,
                FastFrame.encode(epoch, self.podUid(), leaderUid, ask));
        FastFrame.Header header = FastFrame.header(answer);
        if (!header.targetUid().equals(self.podUid()) || !header.senderUid().equals(leaderUid)) {
            throw new IOException("a departure answer not from the leader to this pod");
        }
        if (!fence.admit(header.epoch())) {
            throw new TermJoiner.Fenced(fence.highest(), "a departure answer of epoch "
                    + header.epoch() + ", below the fence");
        }
        if (header.epoch() != epoch) {
            throw new IOException("a departure answered by term " + header.epoch()
                    + ", not " + epoch);
        }
        return switch (FastFrame.decode(answer).body()) {
            case FastFrame.HeldStatusReport report -> report.status();
            case FastFrame.Refused refused -> throw new IOException("the departure was refused: "
                    + refused.reason() + " " + refused.text());
            default -> throw new IOException("a departure answered by kind " + header.kind());
        };
    }
}
