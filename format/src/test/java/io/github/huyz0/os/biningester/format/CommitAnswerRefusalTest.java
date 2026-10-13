// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * A commit answer's bytes that parse but that no answer can be -- or whose
 * nested delta no delta can be -- are refused as an {@link IOException}, never
 * an unchecked throw (M13.34a review round 1, P1, T1); and a run both
 * committed and redirected is no answer (P3).
 */
class CommitAnswerRefusalTest {

    private static final RunKey AA = new RunKey(
            UUID.fromString("00000000-0000-0000-0000-0000000000aa"), 3);
    private static final RunKey CC = new RunKey(
            UUID.fromString("00000000-0000-0000-0000-0000000000cc"), 1);

    private static byte[] golden(String name) throws IOException {
        return FastFrameTest.golden(name);
    }

    /** {@code BCAN}, version 1, then {@code deltas} and {@code redirected} laid out by hand. */
    private static byte[] answer(List<byte[]> deltas, List<RunKey> redirected) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.writeBytes(ByteBuffer.allocate(4).putInt(CommitAnswer.MAGIC).array());
        out.write(1);
        out.writeBytes(ByteBuffer.allocate(2).putShort((short) deltas.size()).array());
        for (byte[] d : deltas) {
            out.writeBytes(ByteBuffer.allocate(4).putInt(d.length).array());
            out.writeBytes(d);
        }
        out.writeBytes(ByteBuffer.allocate(4).putInt(redirected.size()).array());
        for (RunKey r : redirected) {
            out.writeBytes(ByteBuffer.allocate(20).putLong(r.indexId().getMostSignificantBits())
                    .putLong(r.indexId().getLeastSignificantBits()).putInt(r.partitionId())
                    .array());
        }
        return out.toByteArray();
    }

    @Test
    void bytesThatParseButThatNoAnswerCanBeAreAnIOException() throws Exception {
        byte[] five = golden("chain-delta-v0.bin");
        byte[] six = five.clone();
        six[8] = 6;

        for (byte[] bytes : List.of(answer(List.of(), List.of()),
                answer(List.of(six, five), List.of()),
                answer(List.of(five), List.of(CC, CC)),
                answer(List.of(five), List.of(AA)))) {
            assertThatThrownBy(() -> CommitAnswer.decode(bytes)).isInstanceOf(IOException.class);
        }
    }

    @Test
    void aNESTEDDeltaThatNoDeltaCanBeIsAnIOException() throws Exception {
        byte[] bytes = golden("commit-answer-one-delta-v1.bin");
        // the first run's record count: 11 bytes of answer header, then the
        // delta's magic, version, sequence, key length and key, run count,
        // index UUID and partition
        int count = 11 + 4 + 4 + 1 + 1 + 29 + 1 + 16 + 1;
        assertThat(bytes[count]).as("the premise: its two records").isEqualTo((byte) 2);
        byte[] none = Arrays.copyOf(bytes, bytes.length);
        none[count] = 0;

        assertThatThrownBy(() -> CommitAnswer.decode(none)).isInstanceOf(IOException.class);
    }

    @Test
    void aRUNBothCommittedAndRedirectedIsNoAnswer() {
        CommitDelta five = new CommitDelta(5, List.of(new SegmentCommit("bins/a.seg",
                List.of(new RunCommit(AA, 2, 10)))));

        assertThatThrownBy(() -> new CommitAnswer(List.of(five), List.of(AA)))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
