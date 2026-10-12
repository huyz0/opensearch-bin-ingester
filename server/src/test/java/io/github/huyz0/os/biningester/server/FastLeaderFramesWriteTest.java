// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.FastWriteFrame;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.sequencer.FastFrameRouter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The write's leader frames are registered with the EXPOSED drain (M13.27w
 * review round 1, T1): a COMMIT or a CONFIRM answer wakes it, and the drain's
 * send reaches nothing on a pod leading no term.
 *
 * <p>⚠️ THE TERM-PRESENT SEND IS NOT PINNED HERE: a node's term assigns
 * nothing until M13.27l records a {@code wal_quorum}, so it owes no EXPOSED;
 * {@code FastLeaderTermExposedTest} pins the term's send.
 */
@Timeout(30)
class FastLeaderFramesWriteTest {

    private static final Roster.Incarnation SELF =
            new Roster.Incarnation("pod1", "uid-pod1", "az-a", "");

    private static Map<Integer, FastFrameRouter.Handler> registered(CountDownLatch woke) {
        Map<Integer, FastFrameRouter.Handler> handlers = new HashMap<>();
        FastLeaderFrames.registerWrite(handlers::put, () -> null, null, SELF,
                new ExposedDrain(woke::countDown));
        return handlers;
    }

    private static FastJournalRecord.IdempotencyKey key() {
        return new FastJournalRecord.IdempotencyKey("p", new UUID(9, 9), 1);
    }

    @Test
    void aCOMMITAnswerWakesTheDrain() throws Exception {
        CountDownLatch woke = new CountDownLatch(1);
        FastFrameRouter.Handler commit = registered(woke).get(FastWriteFrame.KIND_COMMIT);

        FastFrame.Body answer = commit.answer(
                new FastFrame.Header(FastWriteFrame.KIND_COMMIT, 1, "uid-p", "uid-pod1"),
                new FastWriteFrame.Commit(key(), List.of(new FastWriteFrame.CommitRun(
                        new RunKey(new UUID(1, 1), 0), List.of(new SegmentRecord("d",
                                OpType.INDEX, OptionalLong.empty(), new byte[] {1}))))));

        assertThat(((FastFrame.Refused) answer).reason()).as("this pod leads no term")
                .isEqualTo(FastFrame.Reason.NOT_ROSTERED);
        assertThat(woke.await(10, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void aCONFIRMAnswerWakesTheDrain() throws Exception {
        CountDownLatch woke = new CountDownLatch(1);
        FastFrameRouter.Handler confirm = registered(woke).get(FastWriteFrame.KIND_CONFIRM);

        FastFrame.Body answer = confirm.answer(
                new FastFrame.Header(FastWriteFrame.KIND_CONFIRM, 1, "uid-p", "uid-pod1"),
                new FastWriteFrame.Confirm(key(), 0, List.of(new FastWriteFrame.RunAt(
                        new RunKey(new UUID(1, 1), 0), 0)), true));

        assertThat(((FastFrame.Refused) answer).reason())
                .isEqualTo(FastFrame.Reason.NOT_ROSTERED);
        assertThat(woke.await(10, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void aPODLeadingNoTermSendsNothing() {
        AtomicInteger sent = new AtomicInteger();

        FastLeaderFrames.sendExposed(null, null, SELF, (endpoint, frame) -> {
            sent.incrementAndGet();
            return new byte[0];
        });

        assertThat(sent.get()).isZero();
    }
}
