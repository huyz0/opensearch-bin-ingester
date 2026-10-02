// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

/**
 * The write frames' remaining decode guards (M13.27c review round 1, T4).
 */
class FastWriteFrameGuardsTest {

    /** The header of a golden frame: magic, version, kind, epoch, two UIDs. */
    private static int headerLength(byte[] frame) {
        int at = 14;
        for (int i = 0; i < 2; i++) {
            at += 1 + frame[at];
        }
        return at;
    }

    @Test
    void aCOMMITRunClaimingMoreRecordsThanItsBytesIsRefusedBeforeItSizesAList()
            throws Exception {
        byte[] golden = FastFrameTest.golden("fast-commit-v1.bin");
        ByteBuffer.wrap(golden).putInt(golden.length - 20 - 8, 0x7FFF_FFFF);

        assertThatThrownBy(() -> FastFrame.decode(golden)).isInstanceOf(IOException.class);
    }

    @Test
    void aREPLICAEntryWithBytesAfterItIsRefused() throws Exception {
        byte[] golden = FastFrameTest.golden("fast-replica-v1.bin");
        int lengthAt = headerLength(golden) + 4;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.writeBytes(Arrays.copyOf(golden, golden.length));
        out.write(0);
        byte[] longer = out.toByteArray();
        ByteBuffer.wrap(longer).putInt(lengthAt, ByteBuffer.wrap(golden).getInt(lengthAt) + 1);

        assertThatThrownBy(() -> FastFrame.decode(longer)).isInstanceOf(IOException.class);
    }

    @Test
    void aREPLICASlotHoldingTwoEntriesIsRefused() throws Exception {
        byte[] golden = FastFrameTest.golden("fast-replica-v1.bin");
        int lengthAt = headerLength(golden) + 4;
        int entryLength = ByteBuffer.wrap(golden).getInt(lengthAt);
        byte[] entry = Arrays.copyOfRange(golden, lengthAt + 4, lengthAt + 4 + entryLength);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.writeBytes(Arrays.copyOf(golden, lengthAt));
        out.writeBytes(ByteBuffer.allocate(4).putInt(entryLength * 2).array());
        out.writeBytes(entry);
        out.writeBytes(entry);

        assertThatThrownBy(() -> FastFrame.decode(out.toByteArray()))
                .isInstanceOf(IOException.class);
    }

    @Test
    void anASSIGNEDRunUnderAZeroQuorumIsRefused() throws Exception {
        byte[] golden = FastFrameTest.golden("fast-assigned-v1.bin");
        golden[golden.length - 2] = 0;

        assertThatThrownBy(() -> FastFrame.decode(golden)).isInstanceOf(IOException.class);
    }

    @Test
    void aRUNCountPastTheBytesIsRefusedAtTheCount() throws Exception {
        byte[] golden = FastFrameTest.golden("fast-exposed-v1.bin");
        ByteBuffer.wrap(golden).putInt(golden.length - 28 - 4, 2);

        assertThatThrownBy(() -> FastFrame.decode(golden)).isInstanceOf(IOException.class)
                .hasMessageContaining("elements");
    }
}
