// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.FastFrameRouterTest.SELF;
import static io.github.huyz0.os.biningester.sequencer.FastFrameRouterTest.fence;
import static io.github.huyz0.os.biningester.sequencer.FastFrameRouterTest.join;
import static io.github.huyz0.os.biningester.sequencer.FastFrameRouterTest.refused;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.FastFrame;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

/**
 * Refused UNREAD (ADR-0082 §2; M13.27h review round 1, T1): a frame below the
 * fence or addressed to another incarnation is answered from its header alone,
 * so a deposed leader learns it is deposed even when its body cannot be read.
 */
class FastFrameRouterHeaderTest {

    private static byte[] torn(byte[] frame) {
        return Arrays.copyOf(frame, frame.length - 1);
    }

    @Test
    void aTORNFrameBelowTheFenceIsStillRefusedLowerEpoch() throws Exception {
        EpochFence fence = fence();
        fence.raise(5);
        FastFrameRouter router = new FastFrameRouter(SELF, fence);
        router.handle(FastFrame.KIND_JOIN, (header, body) -> refused(""));

        FastFrame.Frame answer = FastFrame.decode(router.answer(torn(join(3, SELF)))
                .orElseThrow());

        assertThat(((FastFrame.Refused) answer.body()).reason())
                .isEqualTo(FastFrame.Reason.LOWER_EPOCH);
        assertThat(answer.header().epoch()).isEqualTo(5);
    }

    @Test
    void aTORNFrameToAnotherIncarnationIsStillRefusedNotRostered() throws Exception {
        FastFrameRouter router = new FastFrameRouter(SELF, fence());
        router.handle(FastFrame.KIND_JOIN, (header, body) -> refused(""));

        FastFrame.Frame answer = FastFrame.decode(router.answer(torn(join(3, "uid-x")))
                .orElseThrow());

        assertThat(((FastFrame.Refused) answer.body()).reason())
                .isEqualTo(FastFrame.Reason.NOT_ROSTERED);
    }
}
