// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.FastFrameRouterTest.SELF;
import static io.github.huyz0.os.biningester.sequencer.FastFrameRouterTest.fence;
import static io.github.huyz0.os.biningester.sequencer.FastFrameRouterTest.join;
import static io.github.huyz0.os.biningester.sequencer.FastFrameRouterTest.refused;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.FastFrame;
import org.junit.jupiter.api.Test;

/**
 * A pod answers under at least the term it leads (M13.82).
 *
 * <p>⚠️ **MEASURED BY M13.71's REVIEW (P1)**: a pod's fence is read from the
 * lease once, at boot, so a pod that took the term by failover kept the
 * fence of the term before it. Its answers carried that lower epoch, and a
 * pod started since -- its fence read from the newer lease -- took any answer
 * as a deposition and never joined the term, for the term's whole life.
 */
class FastFrameRouterOwnTermTest {

    @Test
    void aLEADERRaisesItsFenceToItsOwnTermBeforeItAnswers() throws Exception {
        EpochFence fence = fence();
        fence.raise(4);
        FastFrameRouter router = new FastFrameRouter(SELF, fence,
                FastFrameRouter.AnswerMeter.NONE, () -> 5L);
        router.handle(FastFrame.KIND_JOIN, (header, body) -> refused(""));

        // a frame at the fence it booted with, which raises nothing by itself
        FastFrame.Frame answer = FastFrame.decode(router.answer(join(4, SELF)).orElseThrow());

        assertThat(answer.header().epoch())
                .as("⚠️ ANSWERED UNDER THE TERM IT LEADS, NOT THE ONE IT BOOTED IN")
                .isEqualTo(5);
        assertThat(fence.highest()).as("and its fence raised to it").isEqualTo(5);
        assertThat(((FastFrame.Refused) answer.body()).reason())
                .as("so a frame of the term before is below its fence")
                .isEqualTo(FastFrame.Reason.LOWER_EPOCH);
    }
}
