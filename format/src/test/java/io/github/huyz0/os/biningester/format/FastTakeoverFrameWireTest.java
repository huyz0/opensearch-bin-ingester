// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The takeover frames' paging and wire refusals (M13.33a review round 1,
 * T1-T2): a page not marked last stays not last -- the successor counts a
 * member answered only on its last page -- and a RELEASE or COLLECTED whose
 * bytes break the record's rules is refused as malformed.
 */
class FastTakeoverFrameWireTest {

    private static final UUID INDEX = new UUID(0x0102030405060708L, 0x1112131415161718L);

    private static FastJournalRecord.Entry entry() throws Exception {
        return ((FastWriteFrame.Replica) FastFrame.decode(
                FastFrameTest.golden("fast-replica-v1.bin")).body()).entries().get(0);
    }

    /** A frame of {@code kind} whose body is {@code body}, under the takeover's header. */
    private static byte[] frame(int kind, byte[] body) {
        byte[] header = FastFrame.encode(9, "uid-m", "uid-s",
                new FastTakeoverFrame.Collected(0, true, List.of()));
        // the empty page's body is 9 bytes: the header is what precedes them
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(header, 0, header.length - 9);
        byte[] bytes = out.toByteArray();
        bytes[5] = (byte) kind;
        ByteArrayOutputStream whole = new ByteArrayOutputStream();
        whole.writeBytes(bytes);
        whole.writeBytes(body);
        return whole.toByteArray();
    }

    @Test
    void aPAGENotMarkedLastStaysNotLast() throws Exception {
        FastTakeoverFrame.Collected first = new FastTakeoverFrame.Collected(0, false,
                List.of(entry()));

        FastTakeoverFrame.Collected decoded = (FastTakeoverFrame.Collected) FastFrame.decode(
                FastFrame.encode(9, "uid-m", "uid-s", first)).body();

        assertThat(decoded.page()).isZero();
        assertThat(decoded.last()).as("the member has more pages").isFalse();
        assertThat(decoded.entries()).hasSize(1);
    }

    @Test
    void aRELEASENamingAStreamTwiceOnTheWireIsRefused() {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        FastFrame.u32(body, 2);
        for (int i = 0; i < 2; i++) {
            FastFrame.runKey(body, new RunKey(INDEX, 3));
            FastFrame.i64(body, 10);
        }

        assertThatThrownBy(() -> FastFrame.decode(frame(FastTakeoverFrame.KIND_RELEASE,
                body.toByteArray()))).isInstanceOf(IOException.class);
    }

    @Test
    void aCOLLECTEDEntryThatIsNotOneWholeJournalEntryIsRefused() throws Exception {
        byte[] whole = entry().encode();
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        FastFrame.u32(body, 0);
        body.write(1);
        FastFrame.u32(body, 1);
        FastFrame.u32(body, whole.length - 1);
        body.write(whole, 0, whole.length - 1);

        assertThatThrownBy(() -> FastFrame.decode(frame(FastTakeoverFrame.KIND_COLLECTED,
                body.toByteArray()))).isInstanceOf(IOException.class);
    }
}
