// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.FastPublishFrame;
import io.github.huyz0.os.biningester.sequencer.FastFrameRouter;
import io.github.huyz0.os.biningester.sequencer.JoinedTerms;
import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;

/**
 * A PUBLISH, taken by a pod with subscribers on its streams (ADR-0081 §6;
 * M13.30i): only from the leader of a term this pod joined, and only that
 * term's entries, each handed to the subscription hub at its offsets under
 * its fast key ({@code DefaultIngest.deliverPushed}, M13.30j).
 *
 * <p>⚠️ A LOWER EPOCH IS REFUSED BEFORE THIS HANDLER: the router admits a
 * frame's epoch against the pod's fence, which a successor's FENCE raised
 * (ADR-0081 §3), so a deposed leader's PUBLISH never reaches the hub.
 *
 * <p>⚠️ BEST EFFORT, as the leader's send is: an entry the hub cannot take is
 * read from the chain once uploaded, and the answer says nothing about it.
 */
final class FastPublishHandler {

    /** Where a pushed entry goes: the hub, by its fast key. */
    @FunctionalInterface
    interface Pushed {
        void deliver(FastJournalRecord.Entry entry) throws IOException;
    }

    private FastPublishHandler() {
    }

    static void register(BiConsumer<Integer, FastFrameRouter.Handler> handle, JoinedTerms joined,
            Pushed pushed) {
        Objects.requireNonNull(joined, "joined");
        Objects.requireNonNull(pushed, "pushed");
        handle.accept(FastPublishFrame.KIND_PUBLISH,
                (header, body) -> answer(joined, pushed, header, (FastPublishFrame.Publish) body));
    }

    static FastFrame.Body answer(JoinedTerms joined, Pushed pushed, FastFrame.Header header,
            FastPublishFrame.Publish publish) {
        Optional<String> leader = joined.leaderOf(header.epoch());
        if (leader.isEmpty() || !leader.get().equals(header.senderUid())) {
            return new FastFrame.Refused(FastFrame.Reason.NOT_ROSTERED, Optional.empty(),
                    header.senderUid() + " is not the leader of a term this pod joined");
        }
        for (FastJournalRecord.Entry e : publish.entries()) {
            if (e.epoch() != header.epoch()) {
                return new FastFrame.Refused(FastFrame.Reason.NOT_ROSTERED, Optional.empty(),
                        "an entry of term " + e.epoch() + " in term " + header.epoch()
                                + "'s PUBLISH");
            }
        }
        for (FastJournalRecord.Entry e : publish.entries()) {
            try {
                pushed.deliver(e);
            } catch (IOException | RuntimeException undelivered) {
                // read from the chain once uploaded, as without publication
            }
        }
        return new FastFrame.HeldStatusReport(FastFrame.HeldStatus.NONE);
    }
}
