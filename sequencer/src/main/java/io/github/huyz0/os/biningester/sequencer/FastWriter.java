// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.FastWriteFrame;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.format.RunKey;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * The writer's side of one fast batch (ADR-0081 §2; M13.29a): COMMIT to the
 * term's leader; on ASSIGNED, journal this pod's copy with its offsets where
 * one is required, fsync, and CONFIRM it; and answer the batch written only
 * once it is EXPOSED -- the writer acks its producer on nothing earlier.
 *
 * <p>⚠️ EVERY ANSWER IS FENCED BEFORE IT IS READ, as {@link Departure}'s is:
 * addressed to this incarnation, sent by the leader, admitted by the pod's
 * {@link EpochFence}, of the term asked -- an answer of a newer term deposes
 * the one asked.
 *
 * <p>⚠️ A COMMIT IS ONLY EVER SENT UNDER THIS POD's OWN KEY (M13.52g, its
 * review P2): the leader refuses another pod's key NOT_ROSTERED although the
 * sender IS rostered, so a writer that sent one would re-join and resend the
 * same COMMIT for ever. Sent under its own, a NOT_ROSTERED means re-join.
 *
 * <p>⚠️ A RETRY REUSES THE KEY: the leader answers a retried batch from its
 * offsets if exposed in the term, and assigns it anew otherwise, the
 * duplicate removed by {@code _id} as the default path's retries are.
 */
public final class FastWriter {

    /** How one batch ended. */
    public sealed interface Outcome permits Written, Retry, NotFast, Rejoin, Fenced {
    }

    /** Exposed: the producer may be acked, at these offsets. */
    public record Written(FastWriteFrame.Assigned assigned) implements Outcome {
    }

    /** Held, discarded or unexposed in time: send it again under the same key. */
    public record Retry(String why) implements Outcome {
    }

    /** The leader takes no fast write: send the batch the default way. */
    public record NotFast(String why) implements Outcome {
    }

    /** This pod is not rostered, or the leader is leaving: join the term again. */
    public record Rejoin(String why) implements Outcome {
    }

    /** The term asked is deposed: write to the newer one. */
    public record Fenced(long fence) implements Outcome {
    }

    private final Roster.Incarnation self;
    private final TermJoiner.Transport transport;
    private final EpochFence fence;
    private final Optional<FastJournal> journal;
    private final ExposedInbox inbox;
    private final Duration exposedWait;

    /**
     * @param journal this pod's fast journal; a diskless writer holds no copy
     * @param exposedWait how long a batch waits for its EXPOSED before it is
     *     retried
     */
    public FastWriter(Roster.Incarnation self, TermJoiner.Transport transport, EpochFence fence,
            Optional<FastJournal> journal, ExposedInbox inbox, Duration exposedWait) {
        this.self = Objects.requireNonNull(self, "self");
        this.transport = Objects.requireNonNull(transport, "transport");
        this.fence = Objects.requireNonNull(fence, "fence");
        this.journal = Objects.requireNonNull(journal, "journal");
        this.inbox = Objects.requireNonNull(inbox, "inbox");
        this.exposedWait = Objects.requireNonNull(exposedWait, "exposedWait");
    }

    /**
     * Writes {@code commit} through term {@code epoch}'s leader.
     *
     * @throws IllegalArgumentException the commit's key names another pod
     * @throws IOException the leader did not answer, or answered as no leader
     *     of this term would
     */
    public Outcome write(long epoch, String leaderUid, String leaderEndpoint,
            FastWriteFrame.Commit commit) throws IOException {
        if (!commit.key().podId().equals(self.podId())) {
            throw new IllegalArgumentException("a COMMIT under pod " + commit.key().podId()
                    + "'s key, not this pod's " + self.podId());
        }
        if (epoch < fence.highest()) {
            return new Fenced(fence.highest());
        }
        Optional<FastFrame.Body> answer = exchange(epoch, leaderUid, leaderEndpoint, commit);
        if (answer.isEmpty()) {
            return new Fenced(fence.highest());
        }
        return switch (answer.get()) {
            case FastWriteFrame.Assigned assigned -> written(epoch, leaderUid, leaderEndpoint,
                    commit, assigned);
            case FastFrame.Refused refused -> refusal(refused);
            default -> throw new IOException("a COMMIT answered by kind " + answer.get().kind());
        };
    }

    private Outcome written(long epoch, String leaderUid, String leaderEndpoint,
            FastWriteFrame.Commit commit, FastWriteFrame.Assigned assigned) throws IOException {
        List<FastWriteFrame.RunAt> required = new ArrayList<>();
        List<FastWriteFrame.RunAt> all = new ArrayList<>();
        for (FastWriteFrame.AssignedRun run : assigned.runs()) {
            all.add(new FastWriteFrame.RunAt(run.stream(), run.firstOffset()));
            if (run.copyRequired()) {
                required.add(new FastWriteFrame.RunAt(run.stream(), run.firstOffset()));
            }
        }
        if (!required.isEmpty()) {
            boolean journaled = journalCopy(epoch, commit, assigned);
            Optional<FastFrame.Body> confirmed = exchange(epoch, leaderUid, leaderEndpoint,
                    new FastWriteFrame.Confirm(commit.key(), assigned.assignedAfter(), required,
                            journaled));
            if (confirmed.isEmpty()) {
                return new Fenced(fence.highest());
            }
            if (confirmed.get() instanceof FastFrame.Refused refused) {
                return refusal(refused);
            }
            // ⚠️ ONLY THE ACKNOWLEDGEMENT COUNTS (M13.29a review round 1, P3):
            // another kind is no leader's answer to a CONFIRM.
            if (!(confirmed.get() instanceof FastFrame.HeldStatusReport)) {
                throw new IOException("a CONFIRM answered by kind " + confirmed.get().kind());
            }
        }
        try {
            // ⚠️ M13.34t: a DISCARDED of the batch's key ends the wait at once.
            if (inbox.await(epoch, commit.key(), assigned.assignedAfter(), all, exposedWait)) {
                return new Written(assigned);
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted waiting for an EXPOSED", interrupted);
        }
        return new Retry("discarded, or not exposed within " + exposedWait);
    }

    /**
     * Journals the required runs' entries with their offsets and fsyncs:
     * false, keeping none, without a journal or past its cap.
     */
    private boolean journalCopy(long epoch, FastWriteFrame.Commit commit,
            FastWriteFrame.Assigned assigned) throws IOException {
        if (journal.isEmpty()) {
            return false;
        }
        FastJournal j = journal.get();
        Map<RunKey, FastWriteFrame.CommitRun> byStream = new HashMap<>();
        commit.runs().forEach(run -> byStream.put(run.stream(), run));
        List<FastJournalRecord.Entry> entries = new ArrayList<>();
        long bytes = 0;
        for (FastWriteFrame.AssignedRun run : assigned.runs()) {
            FastWriteFrame.CommitRun sent = byStream.get(run.stream());
            if (!run.copyRequired() || sent == null) {
                continue;
            }
            FastJournalRecord.Entry e = new FastJournalRecord.Entry(epoch, run.stream(),
                    run.firstOffset(), run.walQuorum(), assigned.assignedAfter(), commit.key(),
                    sent.records());
            entries.add(e);
            bytes += e.encode().length;
        }
        if (!j.fits(bytes)) {
            return false;
        }
        List<FastJournalRecord.Entry> appended = new ArrayList<>();
        for (FastJournalRecord.Entry e : entries) {
            if (!j.append(e)) {
                for (FastJournalRecord.Entry gone : appended) {
                    j.drop(gone.key(), gone.epoch(), gone.assignedAfter(), gone.firstOffset());
                }
                j.sync();
                return false;
            }
            appended.add(e);
        }
        j.sync();
        return true;
    }

    private Outcome refusal(FastFrame.Refused refused) {
        return switch (refused.reason()) {
            case BACKPRESSURE, DISCARDED -> new Retry(refused.reason() + ": " + refused.text());
            case NOT_FAST -> new NotFast(refused.text());
            case NOT_ROSTERED, DEPARTING -> new Rejoin(refused.reason() + ": " + refused.text());
            case LOWER_EPOCH -> new Fenced(fence.highest());
        };
    }

    /** The leader's answer to {@code ask}: empty where the term asked is deposed. */
    private Optional<FastFrame.Body> exchange(long epoch, String leaderUid, String endpoint,
            FastFrame.Body ask) throws IOException {
        byte[] answer = transport.exchange(endpoint,
                FastFrame.encode(epoch, self.podUid(), leaderUid, ask));
        FastFrame.Header header = FastFrame.header(answer);
        if (!header.targetUid().equals(self.podUid()) || !header.senderUid().equals(leaderUid)) {
            throw new IOException("a fast write answer not from the leader to this pod");
        }
        if (!fence.admit(header.epoch()) || header.epoch() > epoch) {
            return Optional.empty();
        }
        if (header.epoch() != epoch) {
            throw new IOException("a fast write answered by term " + header.epoch()
                    + ", not " + epoch);
        }
        return Optional.of(FastFrame.decode(answer).body());
    }

    /**
     * Splits {@code runs} into batches whose every run is at most
     * {@code bound} records (M13.27e): the leader refuses a longer run, since
     * a run takes one first offset and could never fit under {@code B}.
     */
    public static List<List<FastWriteFrame.CommitRun>> splitAtBound(
            List<FastWriteFrame.CommitRun> runs, long bound) {
        if (bound < 1) {
            throw new IllegalArgumentException("a bound is at least one record");
        }
        // ⚠️ AN int STEP, NEVER A CAST BOUND (M13.29a review round 1, P2): a
        // run is at most an int long, so no bound past one splits anything.
        int step = (int) Math.min(bound, Integer.MAX_VALUE);
        List<List<FastWriteFrame.CommitRun>> batches = new ArrayList<>();
        for (FastWriteFrame.CommitRun run : runs) {
            int size = run.records().size();
            for (int from = 0, i = 0; from < size; from += Math.min(step, size - from), i++) {
                if (batches.size() == i) {
                    batches.add(new ArrayList<>());
                }
                batches.get(i).add(new FastWriteFrame.CommitRun(run.stream(),
                        run.records().subList(from, from + Math.min(step, size - from))));
            }
        }
        return batches;
    }
}
