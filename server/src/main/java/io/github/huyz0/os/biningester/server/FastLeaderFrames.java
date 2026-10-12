// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.FastWriteFrame;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.sequencer.FastFrameRouter;
import io.github.huyz0.os.biningester.sequencer.FastLeaderTerm;
import io.github.huyz0.os.biningester.sequencer.LocalSequencer;
import io.github.huyz0.os.biningester.sequencer.Sequencer;
import io.github.huyz0.os.biningester.sequencer.TermJoiner;
import java.io.IOException;
import java.util.Optional;

/**
 * A node's handler of the frames a leader answers -- JOIN (M13.27o), DEPART
 * and HELD (M13.27k), COMMIT (M13.27s), CONFIRM (M13.27v): the term this pod leads answers them, and a pod leading
 * no term refuses them {@code NOT_ROSTERED} -- the sender learned an old
 * leader, and asks the new one.
 */
final class FastLeaderFrames {

    private FastLeaderFrames() {
    }

    /**
     * A COMMIT (M13.27s), answered by the term this pod leads over its fast
     * journal: ⚠️ A POD WITHOUT ONE HOLDS NO FAST WRITE, so refuses
     * {@code NOT_FAST} -- the writer sends the batch the default way.
     */
    static FastFrame.Body answerCommit(Sequencer held, FastDisk disk, Roster.Incarnation self,
            FastFrame.Header header, FastWriteFrame.Commit commit) throws IOException {
        Optional<FastLeaderTerm> term =
                LocalSequencer.underneath(held).flatMap(LocalSequencer::fastTerm);
        if (term.isEmpty()) {
            return new FastFrame.Refused(FastFrame.Reason.NOT_ROSTERED, Optional.empty(),
                    "this pod leads no term");
        }
        if (disk.journal().isEmpty()) {
            return new FastFrame.Refused(FastFrame.Reason.NOT_FAST, Optional.empty(),
                    "this pod holds no fast journal");
        }
        return term.get().answerCommit(header, commit,
                term.get().commitDesk(disk.journal().get(), self));
    }

    /**
     * The catalog's fast indices recorded in the term this pod leads, if any
     * (M13.27l; ADR-0081 §4): wanted, then flushed at most once per
     * {@code min_upload_interval}; a failed write is retried next call.
     */
    static void recordCatalog(Sequencer held, java.util.Map<java.util.UUID, Integer> fast) {
        Optional<FastLeaderTerm> term =
                LocalSequencer.underneath(held).flatMap(LocalSequencer::fastTerm);
        if (term.isEmpty()) {
            return;
        }
        term.get().catalogSays(fast);
        try {
            term.get().flushTermRecord();
        } catch (IOException failed) {
            // ⚠️ THE CHANGES STAY PENDING, their batches waiting; the next
            // tick writes them.
            System.getLogger(FastLeaderFrames.class.getName()).log(
                    System.Logger.Level.WARNING, "the term record was not written", failed);
        }
    }

    /** A CONFIRM (M13.27v), counted by the same desk its COMMIT was assigned by. */
    static FastFrame.Body answerConfirm(Sequencer held, FastDisk disk, Roster.Incarnation self,
            FastFrame.Header header, FastWriteFrame.Confirm confirm) throws IOException {
        Optional<FastLeaderTerm> term =
                LocalSequencer.underneath(held).flatMap(LocalSequencer::fastTerm);
        if (term.isEmpty()) {
            return new FastFrame.Refused(FastFrame.Reason.NOT_ROSTERED, Optional.empty(),
                    "this pod leads no term");
        }
        if (disk.journal().isEmpty()) {
            return new FastFrame.Refused(FastFrame.Reason.NOT_FAST, Optional.empty(),
                    "this pod holds no fast journal");
        }
        return term.get().answerConfirm(header, confirm,
                term.get().commitDesk(disk.journal().get(), self));
    }

    /**
     * Registers the write's leader frames -- COMMIT (M13.27s) and CONFIRM
     * (M13.27v) -- each answer waking {@code exposed} (M13.27w): either can
     * complete a batch's quorum. ⚠️ EVEN A REFUSAL WAKES IT: a wake that
     * finds nothing owed costs one in-memory drain.
     */
    static void registerWrite(java.util.function.BiConsumer<Integer, FastFrameRouter.Handler> handle,
            java.util.function.Supplier<Sequencer> held, FastDisk disk, Roster.Incarnation self,
            ExposedDrain exposed) {
        handle.accept(FastWriteFrame.KIND_COMMIT, (header, body) -> {
            FastFrame.Body answer = answerCommit(held.get(), disk, self, header,
                    (FastWriteFrame.Commit) body);
            exposed.wake();
            return answer;
        });
        handle.accept(FastWriteFrame.KIND_CONFIRM, (header, body) -> {
            FastFrame.Body answer = answerConfirm(held.get(), disk, self, header,
                    (FastWriteFrame.Confirm) body);
            exposed.wake();
            return answer;
        });
    }

    /**
     * Sends the EXPOSEDs the leading term's desk owes (M13.27w); nothing on
     * a pod leading no term or holding no fast journal. ⚠️ A ROSTER READ
     * THAT FAILS SENDS NOTHING, and the exposures stay owed for the next
     * answer's drain.
     */
    static void sendExposed(Sequencer held, FastDisk disk, Roster.Incarnation self,
            TermJoiner.Transport transport) {
        Optional<FastLeaderTerm> term =
                LocalSequencer.underneath(held).flatMap(LocalSequencer::fastTerm);
        if (term.isEmpty() || disk.journal().isEmpty()) {
            return;
        }
        try {
            term.get().sendExposed(term.get().commitDesk(disk.journal().get(), self), transport);
        } catch (IOException rosterUnread) {
            // owed still: the next COMMIT or CONFIRM answer drains again
        }
    }

    /**
     * Uploads what the leading term has exposed and the chain has not
     * committed (M13.31a; ADR-0081 §7), at the offsets assigned; nothing on a
     * pod leading no serving term or holding no fast journal. ⚠️ A FAILED
     * UPLOAD LEAVES THE ENTRIES HELD AND EXPOSED: the next one retries them.
     */
    static void upload(Sequencer held, FastDisk disk, Roster.Incarnation self,
            io.github.huyz0.os.biningester.sequencer.RecoveryWriter.SegmentUpload upload,
            TermJoiner.Transport transport) {
        Optional<LocalSequencer> local = LocalSequencer.underneath(held)
                .filter(LocalSequencer::serving);
        Optional<FastLeaderTerm> term = local.flatMap(LocalSequencer::fastTerm);
        if (term.isEmpty() || disk.journal().isEmpty()) {
            return;
        }
        FastLeaderTerm fast = term.get();
        io.github.huyz0.os.biningester.sequencer.FastUploader.Chain chain =
                new io.github.huyz0.os.biningester.sequencer.FastUploader.Chain() {
                    @Override
                    public long next(io.github.huyz0.os.biningester.format.RunKey stream) {
                        return local.get().committedNext(stream);
                    }

                    @Override
                    public java.util.Map<io.github.huyz0.os.biningester.format.RunKey, Long>
                            commitAt(String segmentKey,
                                    java.util.Map<io.github.huyz0.os.biningester.format.RunKey,
                                            Long> firsts,
                                    java.util.Map<io.github.huyz0.os.biningester.format.RunKey,
                                            Integer> counts) throws IOException {
                        io.github.huyz0.os.biningester.format.CommitDelta delta = local.get()
                                .commitAt(new io.github.huyz0.os.biningester.sequencer
                                        .CommitRequest(self.podId(), "fast" + fast.epoch(),
                                                fast.uploadSeq(firsts), segmentKey, counts),
                                        firsts);
                        fast.uploadCommitted(firsts);
                        return committedNext(delta);
                    }
                };
        try {
            fast.upload(fast.commitDesk(disk.journal().get(), self), disk.journal().get(), chain,
                    upload, transport);
        } catch (IOException | RuntimeException failed) {
            System.getLogger(FastLeaderFrames.class.getName()).log(
                    System.Logger.Level.WARNING, "the fast upload failed; the next retries",
                    failed);
        }
    }

    /** Each stream's committed next offset after {@code delta}: what it actually holds. */
    static java.util.Map<io.github.huyz0.os.biningester.format.RunKey, Long> committedNext(
            io.github.huyz0.os.biningester.format.CommitDelta delta) {
        java.util.Map<io.github.huyz0.os.biningester.format.RunKey, Long> next =
                new java.util.HashMap<>();
        for (io.github.huyz0.os.biningester.format.SegmentCommit segment : delta.segments()) {
            for (io.github.huyz0.os.biningester.format.RunCommit run : segment.runs()) {
                next.merge(run.key(), run.firstOffset() + run.recordCount(), Math::max);
            }
        }
        return next;
    }

    /**
     * Sends the REPLICAs the leading term's desk owes and counts their acks
     * (M13.27x); nothing on a pod leading no term or holding no fast journal.
     * ⚠️ A ROSTER READ THAT FAILS SENDS NOTHING: the copies stay owed for the
     * next drain.
     */
    static void sendReplicas(Sequencer held, FastDisk disk, Roster.Incarnation self,
            TermJoiner.Transport transport) {
        Optional<FastLeaderTerm> term =
                LocalSequencer.underneath(held).flatMap(LocalSequencer::fastTerm);
        if (term.isEmpty() || disk.journal().isEmpty()) {
            return;
        }
        try {
            term.get().sendReplicas(term.get().commitDesk(disk.journal().get(), self), transport);
        } catch (IOException rosterUnread) {
            // owed still: the next COMMIT or CONFIRM answer drains again
        }
    }

    static FastFrame.Body answer(Sequencer held, FastFrame.Header header, FastFrame.Body body)
            throws IOException {
        Optional<FastLeaderTerm> term =
                LocalSequencer.underneath(held).flatMap(LocalSequencer::fastTerm);
        if (term.isEmpty()) {
            return new FastFrame.Refused(FastFrame.Reason.NOT_ROSTERED, Optional.empty(),
                    "this pod leads no term");
        }
        return switch (body) {
            case FastFrame.Join join -> term.get().answerJoin(header, join);
            case FastFrame.Depart depart -> term.get().answerDepart(header, depart);
            case FastFrame.HeldReport report -> term.get().answerHeld(header, report);
            default -> throw new IllegalArgumentException("a leader answers no kind "
                    + body.kind() + " here");
        };
    }
}
