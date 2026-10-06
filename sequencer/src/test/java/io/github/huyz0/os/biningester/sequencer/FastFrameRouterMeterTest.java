// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.FastFrameRouterTest.SELF;
import static io.github.huyz0.os.biningester.sequencer.FastFrameRouterTest.fence;
import static io.github.huyz0.os.biningester.sequencer.FastFrameRouterTest.join;
import static io.github.huyz0.os.biningester.sequencer.FastFrameRouterTest.refused;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.FastFrame;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Every answer the router sends is metered by its size, with the frame it
 * answered when one was read (M13.27n; M13.27h review round 1, P1).
 */
class FastFrameRouterMeterTest {

    private record Metered(FastFrame.Body asked, int bytes) {
    }

    @Test
    void anANSWEREDFrameIsMeteredWithWhatItAsked() throws Exception {
        List<Metered> metered = new ArrayList<>();
        FastFrameRouter router = new FastFrameRouter(SELF, fence(),
                (header, asked, bytes) -> metered.add(new Metered(asked, bytes)));
        router.handle(FastFrame.KIND_JOIN, (header, body) -> refused("x"));

        byte[] answer = router.answer(join(3, SELF)).orElseThrow();

        assertThat(metered).hasSize(1);
        assertThat(metered.get(0).asked()).isInstanceOf(FastFrame.Join.class);
        assertThat(metered.get(0).bytes()).isEqualTo(answer.length);
    }

    @Test
    void aFRAMERefusedFromItsHeaderIsMeteredWithNothingAsked() throws Exception {
        List<Metered> metered = new ArrayList<>();
        FastFrameRouter router = new FastFrameRouter(SELF, fence(),
                (header, asked, bytes) -> metered.add(new Metered(asked, bytes)));

        byte[] answer = router.answer(join(3, "uid-x")).orElseThrow();

        assertThat(metered).containsExactly(new Metered(null, answer.length));
    }
}
