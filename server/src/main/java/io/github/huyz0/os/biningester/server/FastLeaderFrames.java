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
