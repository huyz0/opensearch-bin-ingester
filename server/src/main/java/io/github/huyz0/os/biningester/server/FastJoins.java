// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.sequencer.FastLeaderTerm;
import io.github.huyz0.os.biningester.sequencer.LocalSequencer;
import io.github.huyz0.os.biningester.sequencer.Sequencer;
import java.io.IOException;
import java.util.Optional;

/**
 * A node's JOIN handler (M13.27j): the term this pod leads answers it, and a
 * pod leading no term refuses it {@code NOT_ROSTERED} -- the joiner learned an
 * old leader, and asks the new one.
 */
final class FastJoins {

    private FastJoins() {
    }

    static FastFrame.Body answer(Sequencer held, FastFrame.Header header, FastFrame.Join join)
            throws IOException {
        Optional<FastLeaderTerm> term =
                LocalSequencer.underneath(held).flatMap(LocalSequencer::fastTerm);
        if (term.isEmpty()) {
            return new FastFrame.Refused(FastFrame.Reason.NOT_ROSTERED, Optional.empty(),
                    "this pod leads no term");
        }
        return term.get().answerJoin(header, join);
    }
}
