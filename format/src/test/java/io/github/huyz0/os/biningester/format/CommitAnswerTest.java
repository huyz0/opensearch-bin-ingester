// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The default-path commit answer (ADR-0082 §7; ADR-0081 §8; M13.34a): its
 * three goldens from an independent encoder (golden_answer.py, written from
 * the ADR's table), and the answers and bytes it refuses.
 */
class CommitAnswerTest {

    private static final RunKey AA = new RunKey(
            UUID.fromString("00000000-0000-0000-0000-0000000000aa"), 3);
    private static final RunKey BB = new RunKey(
            UUID.fromString("00000000-0000-0000-0000-0000000000bb"), 0);
    private static final RunKey CC = new RunKey(
            UUID.fromString("00000000-0000-0000-0000-0000000000cc"), 1);
    private static final String KEY = "bins/seg/000000000000002a.seg";

    private static byte[] golden(String name) throws IOException {
        return FastFrameTest.golden(name);
    }

    /** {@code chain-delta-v0.bin}'s delta: sequence 5, runs AA and BB. */
    private static CommitDelta delta(long sequence) {
        return new CommitDelta(sequence, List.of(new SegmentCommit(KEY, List.of(
                new RunCommit(AA, 2, 10), new RunCommit(BB, 7, 0)))));
    }

    @Test
    void anAnswerOfOneDeltaEncodesToItsGoldenBytes() throws Exception {
        assertThat(new CommitAnswer(List.of(delta(5)), List.of()).encode())
                .isEqualTo(golden("commit-answer-one-delta-v1.bin"));
    }

    @Test
    void anAnswerOfTwoDeltasEncodesToItsGoldenBytes() throws Exception {
        assertThat(new CommitAnswer(List.of(delta(5), delta(6)), List.of()).encode())
                .as("a paused run's later delta of the same segment")
                .isEqualTo(golden("commit-answer-two-deltas-v1.bin"));
    }

    @Test
    void anAnswerWithARedirectedRunEncodesToItsGoldenBytes() throws Exception {
        assertThat(new CommitAnswer(List.of(delta(5)), List.of(CC)).encode())
                .isEqualTo(golden("commit-answer-redirect-v1.bin"));
    }

    @Test
    void theGOLDENAnswersDecodeToTheirDeltasAndRedirects() throws Exception {
        assertThat(CommitAnswer.decode(golden("commit-answer-one-delta-v1.bin")))
                .isEqualTo(new CommitAnswer(List.of(delta(5)), List.of()));
        assertThat(CommitAnswer.decode(golden("commit-answer-two-deltas-v1.bin")).deltas())
                .extracting(CommitDelta::sequence).containsExactly(5L, 6L);
        assertThat(CommitAnswer.decode(golden("commit-answer-redirect-v1.bin")).redirected())
                .containsExactly(CC);
    }

    @Test
    void anAnswerOfNothingOrWithDeltasOutOfOrderOrARunRedirectedTwiceIsRefused() {
        assertThatThrownBy(() -> new CommitAnswer(List.of(), List.of()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CommitAnswer(List.of(delta(6), delta(5)), List.of()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CommitAnswer(List.of(delta(5), delta(5)), List.of()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CommitAnswer(List.of(delta(5)), List.of(CC, CC)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void bytesThatAreNoAnswerAreRefusedNotPartlyRead() throws Exception {
        byte[] good = golden("commit-answer-redirect-v1.bin");
        byte[] wrongMagic = good.clone();
        wrongMagic[0] = 0;
        byte[] wrongVersion = good.clone();
        wrongVersion[4] = 2;
        byte[] trailing = java.util.Arrays.copyOf(good, good.length + 1);
        byte[] torn = java.util.Arrays.copyOf(good, good.length - 1);
        byte[] hugeDelta = good.clone();
        ByteBuffer.wrap(hugeDelta).putInt(7, 0x7FFFFFFF);
        byte[] hugeRedirectCount = good.clone();
        ByteBuffer.wrap(hugeRedirectCount).putInt(good.length - 24, 0x7FFFFFFF);

        for (byte[] bytes : List.of(wrongMagic, wrongVersion, trailing, torn, hugeDelta,
                hugeRedirectCount)) {
            assertThatThrownBy(() -> CommitAnswer.decode(bytes)).isInstanceOf(IOException.class);
        }
    }
}
