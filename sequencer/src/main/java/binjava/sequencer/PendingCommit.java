// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import binjava.format.CommitDelta;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * One caller's outstanding submission to {@link BatchingSequencer}, and the
 * rules for handing that caller back what the committer got.
 *
 * <p>⚠️ EXTRACTED FROM {@code BatchingSequencer} because that file reached the
 * 500-line limit, which code-structure.md rule 1 says to answer by splitting and
 * never by raising. This is the seam that was actually there: the batcher owns
 * WHEN a window closes, and this owns WHAT a caller is told — which is a
 * contract question about {@code Sequencer}, not a batching question.
 */
final class PendingCommit {

    private final List<CommitRequest> requests;
    private final CompletableFuture<CommitDelta> done = new CompletableFuture<>();

    PendingCommit(List<CommitRequest> requests) {
        this.requests = requests;
    }

    List<CommitRequest> requests() {
        return requests;
    }

    void complete(CommitDelta delta) {
        done.complete(delta);
    }

    void fail(Throwable cause) {
        done.completeExceptionally(cause);
    }

    CommitDelta await() throws IOException {
        try {
            return done.join();
        } catch (CompletionException wrapped) {
            Throwable cause = wrapped.getCause();
            if (cause instanceof IOException io) {
                // ⚠️ UNWRAPPED, because the Sequencer contract distinguishes
                // an IOException (ambiguous: the PUT may have landed) from
                // everything else, and a caller that has to unwrap a
                // CompletionException to learn which it got will eventually
                // not bother.
                throw io;
            }
            if (cause instanceof RuntimeException runtime) {
                throw runtime;
            }
            // ⚠️ AN ERROR PROPAGATES AS AN ERROR. Wrapping an
            // OutOfMemoryError as IOException would tell the caller exactly
            // the wrong thing: the Sequencer contract says an IOException is
            // AMBIGUOUS — the PUT may have landed — which invites a retry,
            // and retrying into an OOM is how a pod turns a bad window into
            // a bad hour. It is also not this caller's memory that ran out;
            // it is the committer's, and that is not a condition a caller
            // can be asked to handle.
            if (cause instanceof Error fatal) {
                throw fatal;
            }
            throw new IOException(cause);
        }
    }
}
