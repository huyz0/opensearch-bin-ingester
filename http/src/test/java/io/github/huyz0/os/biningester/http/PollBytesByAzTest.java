// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.CrossAzBytes;
import io.github.huyz0.os.biningester.binstore.CrossAzBytes.Transport;
import io.github.huyz0.os.biningester.format.FetchMode;
import io.github.huyz0.os.biningester.http.PollAnswer.AnswerFrame;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * How one poll answer's bytes are split across M9 criterion 6's transports
 * (M9.2, NFR-5), asserted without a socket.
 *
 * <p>⚠️ **THIS FILE EXISTS BECAUSE THREE MUTATIONS SURVIVED THE SOCKET
 * CASES.** Review MEASURED it: the attribution was reachable only through a
 * live {@code WebServer} publish, every such case published {@code INLINE},
 * and so {@code PROXY} attributed to the poll, {@code DIRECT} attributed to a
 * proxy read, and the overhead call deleted outright all left
 * {@code :http:test} green. A fan-out mode is a one-line switch and the whole
 * per-transport split rests on it, so it is asserted where every arm is
 * cheap to reach.
 */
class PollBytesByAzTest {

    private static final String HERE = "az-a";
    private static final String THERE = "az-b";

    @Test
    void anINLINEPayloadIsATTRIBUTEDToTheInlinePush() {
        CrossAzBytes counter = new CrossAzBytes(HERE);
        PollAnswer.countAnswer(counter, THERE, 0,
                List.of(new AnswerFrame(FetchMode.INLINE, 900)));
        assertThat(counter.crossAzBytes(Transport.INLINE_PUSH)).isEqualTo(900);
        assertThat(counter.crossAzBytes(Transport.PROXY_READ)).isZero();
        assertThat(counter.crossAzBytes(Transport.CONSUMER_POLL)).isZero();
    }

    @Test
    void aPROXYFrameIsATTRIBUTEDToTheProxyReadAndNOTToThePoll() {
        // ⚠️ THE LARGEST TERM OF NFR-5 (cost.md rules 10-11) HAS ITS OWN
        // COLUMN, and it must not be swept into the poll's own bytes: a
        // measurement that reported the proxy path as 0 while it carried
        // traffic is the one reading criterion 6 cannot survive.
        CrossAzBytes counter = new CrossAzBytes(HERE);
        PollAnswer.countAnswer(counter, THERE, 0,
                List.of(new AnswerFrame(FetchMode.PROXY, 64)));
        assertThat(counter.crossAzBytes(Transport.PROXY_READ)).isEqualTo(64);
        assertThat(counter.crossAzBytes(Transport.CONSUMER_POLL)).isZero();
        assertThat(counter.crossAzBytes(Transport.INLINE_PUSH)).isZero();
    }

    @Test
    void aDIRECTGrantIsTheP0LLSOwnBytesAndNEVERAProxyRead() {
        // ⚠️ THE DEFECT `transportOf`'s JAVADOC CLAIMS TO PREVENT. A `direct`
        // consumer fetches from the object store with a signed URL, so what
        // leaves this socket is the grant frame; counting it as a proxy read
        // would put a segment's worth of bytes in a column they never
        // travelled through.
        CrossAzBytes counter = new CrossAzBytes(HERE);
        PollAnswer.countAnswer(counter, THERE, 0,
                List.of(new AnswerFrame(FetchMode.DIRECT, 120)));
        assertThat(counter.crossAzBytes(Transport.CONSUMER_POLL)).isEqualTo(120);
        assertThat(counter.crossAzBytes(Transport.PROXY_READ)).isZero();
    }

    @Test
    void theAnswersOWNBytesAreCOUNTEDAndBelongToTheSocket() {
        // ⚠️ RED WITH THE OVERHEAD CALL DELETED, which the socket cases could
        // not be: every one of them built its answer from
        // `RetainedFloors.unknown()`, so the overhead was always 0 and the
        // call returned early having constrained nothing.
        CrossAzBytes counter = new CrossAzBytes(HERE);
        PollAnswer.countAnswer(counter, THERE, 48, List.of());
        assertThat(counter.crossAzBytes(Transport.CONSUMER_POLL)).isEqualTo(48);
        assertThat(counter.crossAzBytes()).isEqualTo(48);
    }

    @Test
    void oneAnswerCarryingEVERYModeSplitsAndSumsToTheTotal() {
        CrossAzBytes counter = new CrossAzBytes(HERE);
        PollAnswer.countAnswer(counter, THERE, 16,
                List.of(new AnswerFrame(FetchMode.INLINE, 1),
                        new AnswerFrame(FetchMode.PROXY, 2),
                        new AnswerFrame(FetchMode.DIRECT, 4),
                        new AnswerFrame(FetchMode.INLINE, 8)));
        assertThat(counter.crossAzBytes(Transport.INLINE_PUSH)).isEqualTo(9);
        assertThat(counter.crossAzBytes(Transport.PROXY_READ)).isEqualTo(2);
        assertThat(counter.crossAzBytes(Transport.CONSUMER_POLL)).isEqualTo(20);
        assertThat(counter.crossAzBytes()).isEqualTo(31);
    }

    @Test
    void aConsumerInTHISZoneSpendsNoCrossAzBytesInAnyColumn() {
        CrossAzBytes counter = new CrossAzBytes(HERE);
        PollAnswer.countAnswer(counter, HERE, 16,
                List.of(new AnswerFrame(FetchMode.INLINE, 1),
                        new AnswerFrame(FetchMode.PROXY, 2),
                        new AnswerFrame(FetchMode.DIRECT, 4)));
        assertThat(counter.crossAzBytes()).isZero();
        assertThat(counter.sameAzBytes(Transport.INLINE_PUSH)).isEqualTo(1);
        assertThat(counter.sameAzBytes(Transport.PROXY_READ)).isEqualTo(2);
        assertThat(counter.sameAzBytes(Transport.CONSUMER_POLL)).isEqualTo(20);
    }

    @Test
    void aFrameCARRIESItsModeAndItsBytesAsIDENTITY() {
        // ⚠️ THE MODE IS HALF OF WHAT A FRAME IS. Two frames of the same size
        // published in different modes belong in different columns, so a
        // frame that compared or printed as its bytes alone would make the
        // split unreadable in exactly the place it is being debugged.
        AnswerFrame inline = new AnswerFrame(FetchMode.INLINE, 900);
        assertThat(inline).isEqualTo(new AnswerFrame(FetchMode.INLINE, 900))
                .hasSameHashCodeAs(new AnswerFrame(FetchMode.INLINE, 900));
        assertThat(inline).isNotEqualTo(new AnswerFrame(FetchMode.PROXY, 900));
        assertThat(inline.hashCode())
                .as("⚠️ A CONSTANT HASH IS NOT A HASH: these frames go into collections "
                        + "while an answer is being built, and one bucket for every frame "
                        + "is a quiet quadratic on the poll path")
                .isNotEqualTo(new AnswerFrame(FetchMode.PROXY, 900).hashCode());
        assertThat(inline).isNotEqualTo(new AnswerFrame(FetchMode.INLINE, 901));
        assertThat(inline.toString()).contains("INLINE").contains("900");
    }

    @Test
    void aConsumerThatNAMEDNoZoneIsUNATTRIBUTEDInEveryColumn() {
        CrossAzBytes counter = new CrossAzBytes(HERE);
        PollAnswer.countAnswer(counter, null, 16,
                List.of(new AnswerFrame(FetchMode.PROXY, 2)));
        assertThat(counter.unknownPeerBytes()).isEqualTo(18);
        assertThat(counter.crossAzBytes()).isEqualTo(18);
        assertThat(counter.sameAzBytes()).isZero();
    }
}
