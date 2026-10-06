// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.Roster;
import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * A pod's one receiver of fast frames (ADR-0082 §2; M13.27h): the header
 * first, so the frame is refused unread when it is addressed to another
 * incarnation or below the epoch fence; then the handler of its kind.
 */
class FastFrameRouterTest {

    static final String SELF = "uid-l";
    static final Roster.Incarnation POD = new Roster.Incarnation("p", "uid-p", "az-b", "");

    static EpochFence fence() throws IOException {
        return EpochFence.start(new MemoryBinStore(), "k", Optional.empty());
    }

    static byte[] join(long epoch, String target) {
        return FastFrame.encode(epoch, POD.podUid(), target,
                new FastFrame.Join(POD, FastFrame.Held.NONE));
    }

    static FastFrame.Refused refused(String text) {
        return new FastFrame.Refused(FastFrame.Reason.NOT_FAST, Optional.empty(), text);
    }

    @Test
    void aFRAMEOfAHandledKindIsAnsweredByItsHandlerToItsSender() throws Exception {
        FastFrameRouter router = new FastFrameRouter(SELF, fence());
        router.handle(FastFrame.KIND_JOIN, (header, body) ->
                refused("seen " + ((FastFrame.Join) body).incarnation().podId()));

        FastFrame.Frame answer = FastFrame.decode(router.answer(join(3, SELF)).orElseThrow());

        assertThat(answer.header().senderUid()).isEqualTo(SELF);
        assertThat(answer.header().targetUid()).isEqualTo("uid-p");
        assertThat(answer.header().epoch()).isEqualTo(3);
        assertThat(((FastFrame.Refused) answer.body()).text()).isEqualTo("seen p");
    }

    @Test
    void aFRAMEToAnotherIncarnationIsRefusedUnreadAndRaisesNothing() throws Exception {
        EpochFence fence = fence();
        FastFrameRouter router = new FastFrameRouter(SELF, fence);
        AtomicInteger calls = new AtomicInteger();
        router.handle(FastFrame.KIND_JOIN, (header, body) -> {
            calls.incrementAndGet();
            return refused("");
        });

        FastFrame.Frame answer = FastFrame.decode(router.answer(join(9, "uid-x")).orElseThrow());

        assertThat(((FastFrame.Refused) answer.body()).reason())
                .isEqualTo(FastFrame.Reason.NOT_ROSTERED);
        assertThat(answer.header().targetUid()).isEqualTo("uid-p");
        assertThat(calls).hasValue(0);
        assertThat(fence.highest()).as("another incarnation's frame raises nothing").isZero();
    }

    @Test
    void aLOWEREpochIsRefusedAtTheFenceUnread() throws Exception {
        EpochFence fence = fence();
        fence.raise(5);
        FastFrameRouter router = new FastFrameRouter(SELF, fence);
        AtomicInteger calls = new AtomicInteger();
        router.handle(FastFrame.KIND_JOIN, (header, body) -> {
            calls.incrementAndGet();
            return refused("");
        });

        FastFrame.Frame answer = FastFrame.decode(router.answer(join(3, SELF)).orElseThrow());

        assertThat(((FastFrame.Refused) answer.body()).reason())
                .isEqualTo(FastFrame.Reason.LOWER_EPOCH);
        assertThat(answer.header().epoch()).as("the sender learns the fence").isEqualTo(5);
        assertThat(calls).hasValue(0);
    }

    @Test
    void aHIGHEREpochRaisesTheFenceBeforeTheHandlerRuns() throws Exception {
        EpochFence fence = fence();
        FastFrameRouter router = new FastFrameRouter(SELF, fence);
        AtomicLong seen = new AtomicLong(-1);
        router.handle(FastFrame.KIND_JOIN, (header, body) -> {
            seen.set(fence.highest());
            return refused("");
        });

        router.answer(join(7, SELF));

        assertThat(seen).hasValue(7);
    }

    @Test
    void aKINDWithNoHandlerIsNotAnswered() throws Exception {
        FastFrameRouter router = new FastFrameRouter(SELF, fence());

        assertThat(router.answer(join(3, SELF))).isEmpty();
    }

    @Test
    void aMALFORMEDFrameIsRefusedAsMalformed() throws Exception {
        FastFrameRouter router = new FastFrameRouter(SELF, fence());
        router.handle(FastFrame.KIND_JOIN, (header, body) -> refused(""));
        byte[] good = join(3, SELF);

        assertThatThrownBy(() -> router.answer(new byte[] {1, 2, 3}))
                .isInstanceOf(FastFrameRouter.Malformed.class);
        assertThatThrownBy(() -> router.answer(Arrays.copyOf(good, good.length - 1)))
                .as("a good header over a torn body").isInstanceOf(FastFrameRouter.Malformed.class);
    }

    @Test
    void aHANDLERsFailureIsNotMalformed() throws Exception {
        FastFrameRouter router = new FastFrameRouter(SELF, fence());
        router.handle(FastFrame.KIND_JOIN, (header, body) -> {
            throw new IOException("store down");
        });

        assertThatThrownBy(() -> router.answer(join(3, SELF)))
                .isInstanceOf(IOException.class)
                .isNotInstanceOf(FastFrameRouter.Malformed.class);
    }
}
