// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.FastFrameRouterTest.SELF;
import static io.github.huyz0.os.biningester.sequencer.FastFrameRouterTest.fence;
import static io.github.huyz0.os.biningester.sequencer.FastFrameRouterTest.join;
import static io.github.huyz0.os.biningester.sequencer.FastFrameRouterTest.refused;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.FastFrame;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * A frame from a UID that is no live incarnation is refused, unread, and
 * raises nothing (M13.71, ADR-0084 decision 8's residual).
 *
 * <p>⚠️ **THE CERTIFICATE OUTLIVES THE POD.** A leaked key passes the binding
 * check under its own UID until the certificate expires, the pod it was issued
 * to long replaced: a {@code Long.MAX_VALUE} frame from it deposed every
 * leader it reached, for good, and a JOIN at the fence -- which every answer
 * names -- was rostered and could commit.
 */
class FastFrameRouterLivenessTest {

    private static FastFrameRouter router(EpochFence fence, long ownTerm,
            FastFrameRouter.Liveness liveness) {
        return new FastFrameRouter(SELF, fence, FastFrameRouter.AnswerMeter.NONE,
                () -> ownTerm, liveness);
    }

    @Test
    void aSenderNOTLIVECannotRaiseTheFence() throws Exception {
        EpochFence fence = fence();
        fence.raise(4);
        FastFrameRouter router = router(fence, 0, uid -> false);
        AtomicInteger calls = new AtomicInteger();
        router.handle(FastFrame.KIND_JOIN, (header, body) -> {
            calls.incrementAndGet();
            return refused("");
        });

        FastFrame.Frame answer = FastFrame.decode(
                router.answer(join(Long.MAX_VALUE, SELF)).orElseThrow());

        assertThat(fence.highest()).as("⚠️ THE FENCE IS UNMOVED").isEqualTo(4);
        assertThat(((FastFrame.Refused) answer.body()).reason())
                .isEqualTo(FastFrame.Reason.NOT_ROSTERED);
        assertThat(calls).as("and the frame is not handled").hasValue(0);
    }

    @Test
    void aSenderNOTLIVEIsRefusedAtTheFenceToo() throws Exception {
        // ⚠️ M13.71 review P2: every answer names the fence, so a gone pod's
        // JOIN at it was rostered, and its COMMITs counted.
        EpochFence fence = fence();
        fence.raise(4);
        FastFrameRouter router = router(fence, 0, uid -> false);
        AtomicInteger calls = new AtomicInteger();
        router.handle(FastFrame.KIND_JOIN, (header, body) -> {
            calls.incrementAndGet();
            return refused("joined");
        });

        FastFrame.Frame answer = FastFrame.decode(router.answer(join(4, SELF)).orElseThrow());

        assertThat(calls).as("not handled").hasValue(0);
        assertThat(((FastFrame.Refused) answer.body()).reason())
                .isEqualTo(FastFrame.Reason.NOT_ROSTERED);
    }

    @Test
    void theREFUSALIsAnsweredUnderTheTermLed() throws Exception {
        // ⚠️ M13.71 review P1, T3: a leader whose fence lags its term answered a
        // first JOIN's refusal under the lower epoch, and the joiner -- its own
        // fence read from the newer lease -- took it as a deposition for good.
        EpochFence fence = fence();
        fence.raise(4);
        FastFrameRouter router = router(fence, 5, uid -> false);
        router.handle(FastFrame.KIND_JOIN, (header, body) -> refused(""));

        FastFrame.Frame answer = FastFrame.decode(router.answer(join(5, SELF)).orElseThrow());

        assertThat(((FastFrame.Refused) answer.body()).reason())
                .isEqualTo(FastFrame.Reason.NOT_ROSTERED);
        assertThat(answer.header().epoch())
                .as("refused under the term led, so the joiner asks again").isEqualTo(5);
    }

    @Test
    void EVERYKindIsJudgedNotJustAJoin() throws Exception {
        // ⚠️ M13.71 review T2: a guard on JOIN alone left a gone pod's HELD,
        // COMMIT or DEPART -- or a kind with no handler -- raising the fence.
        EpochFence fence = fence();
        fence.raise(4);
        FastFrameRouter router = router(fence, 0, uid -> false);

        FastFrame.Frame answer = FastFrame.decode(router.answer(FastFrame.encode(Long.MAX_VALUE,
                "uid-p", SELF, new FastFrame.HeldReport(FastFrame.Held.NONE))).orElseThrow());

        assertThat(fence.highest()).isEqualTo(4);
        assertThat(((FastFrame.Refused) answer.body()).reason())
                .isEqualTo(FastFrame.Reason.NOT_ROSTERED);
    }

    @Test
    void aLIVESenderStillRaisesIt() throws Exception {
        EpochFence fence = fence();
        fence.raise(4);
        FastFrameRouter router = router(fence, 0, uid -> uid.equals("uid-p"));
        router.handle(FastFrame.KIND_JOIN, (header, body) -> refused(""));

        router.answer(join(9, SELF));

        assertThat(fence.highest()).isEqualTo(9);
    }
}
