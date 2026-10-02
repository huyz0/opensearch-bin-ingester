// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The fast write's frames (ADR-0082 §2; M13.27c), held to golden bytes from an
 * independent encoder, and refused when they cannot be placed.
 */
class FastWriteFrameTest {

    private static final FastJournalRecord.IdempotencyKey KEY =
            new FastJournalRecord.IdempotencyKey("ingester-2",
                    new UUID(0x0102030405060708L, 0x1112131415161718L), 42);
    private static final List<SegmentRecord> RECORDS = List.of(
            new SegmentRecord("d1", OpType.INDEX, OptionalLong.empty(),
                    "{\"a\":1}".getBytes(StandardCharsets.UTF_8)),
            new SegmentRecord("d2", OpType.INDEX, OptionalLong.of(7),
                    "{}".getBytes(StandardCharsets.UTF_8)));
    private static final List<FastWriteFrame.RunAt> AT_100 =
            List.of(new FastWriteFrame.RunAt(FastFrameTest.STREAM, 100));

    private static FastWriteFrame.Commit commit() {
        return new FastWriteFrame.Commit(KEY,
                List.of(new FastWriteFrame.CommitRun(FastFrameTest.STREAM, RECORDS)));
    }

    private static FastJournalRecord.Entry entry() {
        return new FastJournalRecord.Entry(7, FastFrameTest.STREAM, 100, 2, 4, KEY, RECORDS);
    }

    @Test
    void eachKINDEncodesToItsGoldenBytes() throws Exception {
        assertThat(FastFrame.encode(7, "uid-2", "uid-1", commit()))
                .isEqualTo(FastFrameTest.golden("fast-commit-v1.bin"));
        assertThat(FastFrame.encode(7, "uid-1", "uid-2", new FastWriteFrame.Assigned(4,
                List.of(new FastWriteFrame.AssignedRun(FastFrameTest.STREAM, 100, 2, true)))))
                .isEqualTo(FastFrameTest.golden("fast-assigned-v1.bin"));
        assertThat(FastFrame.encode(7, "uid-2", "uid-1",
                new FastWriteFrame.Confirm(KEY, 4, AT_100, true)))
                .isEqualTo(FastFrameTest.golden("fast-confirm-v1.bin"));
        assertThat(FastFrame.encode(7, "uid-1", "uid-2", new FastWriteFrame.Exposed(4, AT_100)))
                .isEqualTo(FastFrameTest.golden("fast-exposed-v1.bin"));
        assertThat(FastFrame.encode(7, "uid-1", "uid-3",
                new FastWriteFrame.Replica(List.of(entry()))))
                .isEqualTo(FastFrameTest.golden("fast-replica-v1.bin"));
        assertThat(FastFrame.encode(7, "uid-3", "uid-1", new FastWriteFrame.ReplicaAck(4, AT_100)))
                .isEqualTo(FastFrameTest.golden("fast-replica-ack-v1.bin"));
    }

    @Test
    void eachGOLDENFrameDecodesToItsBody() throws Exception {
        // ⚠️ RECORDS HOLD byte[] PAYLOADS, which record equality compares by
        // reference: a decoded COMMIT and REPLICA are held to the golden bytes
        // by re-encoding them, field by field where it is not a payload.
        FastWriteFrame.Commit decoded = (FastWriteFrame.Commit) FastFrame.decode(
                FastFrameTest.golden("fast-commit-v1.bin")).body();
        assertThat(decoded.key()).isEqualTo(KEY);
        assertThat(FastFrame.encode(7, "uid-2", "uid-1", decoded))
                .isEqualTo(FastFrameTest.golden("fast-commit-v1.bin"));
        assertThat(FastFrame.decode(FastFrameTest.golden("fast-assigned-v1.bin")).body())
                .isEqualTo(new FastWriteFrame.Assigned(4, List.of(
                        new FastWriteFrame.AssignedRun(FastFrameTest.STREAM, 100, 2, true))));
        assertThat(FastFrame.decode(FastFrameTest.golden("fast-confirm-v1.bin")).body())
                .isEqualTo(new FastWriteFrame.Confirm(KEY, 4, AT_100, true));
        assertThat(FastFrame.decode(FastFrameTest.golden("fast-exposed-v1.bin")).body())
                .isEqualTo(new FastWriteFrame.Exposed(4, AT_100));
        FastWriteFrame.Replica replica = (FastWriteFrame.Replica) FastFrame.decode(
                FastFrameTest.golden("fast-replica-v1.bin")).body();
        assertThat(replica.entries()).hasSize(1);
        assertThat(replica.entries().get(0).encode()).isEqualTo(entry().encode());
        assertThat(FastFrame.decode(FastFrameTest.golden("fast-replica-ack-v1.bin")).body())
                .isEqualTo(new FastWriteFrame.ReplicaAck(4, AT_100));
    }

    @Test
    void aCOMMITRunWhoseRecordsDoNotFillItsLengthIsRefused() throws Exception {
        byte[] commit = FastFrameTest.golden("fast-commit-v1.bin").clone();
        // ⚠️ THE RUN'S RECORD COUNT: its 20 bytes of records end the frame,
        // after the count and the byte length. Claiming 1 of its 2 records
        // leaves bytes the records do not fill.
        int countAt = commit.length - 20 - 8;
        assertThat(java.nio.ByteBuffer.wrap(commit).getInt(countAt)).isEqualTo(2);
        java.nio.ByteBuffer.wrap(commit).putInt(countAt, 1);

        assertThatThrownBy(() -> FastFrame.decode(commit)).isInstanceOf(IOException.class);
    }

    @Test
    void aFLAGOtherThanZeroOrOneIsRefused() throws Exception {
        byte[] confirm = FastFrameTest.golden("fast-confirm-v1.bin").clone();
        confirm[confirm.length - 1] = 2;

        assertThatThrownBy(() -> FastFrame.decode(confirm)).isInstanceOf(IOException.class);
    }

    @Test
    void aREPLICAEntryThatIsNotOneWholeJournalEntryIsRefused() throws Exception {
        byte[] replica = FastFrameTest.golden("fast-replica-v1.bin").clone();
        replica[replica.length - 3] ^= 1;

        assertThatThrownBy(() -> FastFrame.decode(replica)).isInstanceOf(IOException.class);
    }

    @Test
    void anASSIGNEDRunOutsideItsRangesIsRefused() {
        assertThatThrownBy(() -> new FastWriteFrame.AssignedRun(FastFrameTest.STREAM, 0, 4, false))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new FastWriteFrame.Exposed(-1, AT_100))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new FastWriteFrame.Commit(KEY, List.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
