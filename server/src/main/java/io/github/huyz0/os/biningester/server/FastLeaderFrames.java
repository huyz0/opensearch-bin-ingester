// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.FastWriteFrame;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.sequencer.FastLeaderTerm;
import io.github.huyz0.os.biningester.sequencer.LocalSequencer;
import io.github.huyz0.os.biningester.sequencer.Sequencer;
import java.io.IOException;
import java.util.Optional;

/**
 * A node's handler of the frames a leader answers -- JOIN (M13.27o), DEPART
 * and HELD (M13.27k), COMMIT (M13.27s): the term this pod leads answers them, and a pod leading
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
