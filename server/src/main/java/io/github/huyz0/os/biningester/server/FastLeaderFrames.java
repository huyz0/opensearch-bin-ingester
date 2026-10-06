// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.sequencer.FastLeaderTerm;
import io.github.huyz0.os.biningester.sequencer.LocalSequencer;
import io.github.huyz0.os.biningester.sequencer.Sequencer;
import java.io.IOException;
import java.util.Optional;

/**
 * A node's handler of the frames a leader answers -- JOIN (M13.27o), DEPART
 * and HELD (M13.27k): the term this pod leads answers them, and a pod leading
 * no term refuses them {@code NOT_ROSTERED} -- the sender learned an old
 * leader, and asks the new one.
 */
final class FastLeaderFrames {

    private FastLeaderFrames() {
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
