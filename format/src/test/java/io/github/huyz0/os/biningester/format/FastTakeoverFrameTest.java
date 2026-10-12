// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The takeover's frames (ADR-0082 §2; M13.33a): FENCE, COLLECTED and
 * RELEASE, held to golden bytes from an independent encoder, and refused when
 * they cannot be placed.
 */
class FastTakeoverFrameTest {

    private static final UUID INDEX = new UUID(0x0102030405060708L, 0x1112131415161718L);

    private static FastJournalRecord.Entry entry() throws Exception {
        return ((FastWriteFrame.Replica) FastFrame.decode(
                FastFrameTest.golden("fast-replica-v1.bin")).body()).entries().get(0);
    }

    private static FastTakeoverFrame.Release release() {
        return new FastTakeoverFrame.Release(List.of(
                new FastTakeoverFrame.ReleaseAt(new RunKey(INDEX, 3), 102),
                new FastTakeoverFrame.ReleaseAt(new RunKey(INDEX, 4), 0)));
    }

    @Test
    void eachKINDEncodesToItsGoldenBytes() throws Exception {
        assertThat(FastFrame.encode(9, "uid-s", "uid-m",
                new FastTakeoverFrame.Fence(List.of(7L, 8L), 6)))
                .isEqualTo(FastFrameTest.golden("fast-fence-v1.bin"));
        assertThat(FastFrame.encode(9, "uid-m", "uid-s",
                new FastTakeoverFrame.Collected(3, true, List.of(entry()))))
                .isEqualTo(FastFrameTest.golden("fast-collected-v1.bin"));
        assertThat(FastFrame.encode(7, "uid-1", "uid-3", release()))
                .isEqualTo(FastFrameTest.golden("fast-release-v1.bin"));
    }

    @Test
    void eachGOLDENFrameDecodesToItsBody() throws Exception {
        assertThat(FastFrame.decode(FastFrameTest.golden("fast-fence-v1.bin")).body())
                .isEqualTo(new FastTakeoverFrame.Fence(List.of(7L, 8L), 6));
        FastTakeoverFrame.Collected collected = (FastTakeoverFrame.Collected) FastFrame.decode(
                FastFrameTest.golden("fast-collected-v1.bin")).body();
        assertThat(collected.page()).isEqualTo(3);
        assertThat(collected.last()).isTrue();
        assertThat(collected.entries()).singleElement()
                .satisfies(e -> assertThat(e.encode()).isEqualTo(entry().encode()));
        assertThat(FastFrame.decode(FastFrameTest.golden("fast-release-v1.bin")).body())
                .isEqualTo(release());
    }

    @Test
    void anEMPTYPageAndAnEmptyReleaseRoundTrip() throws Exception {
        // ⚠️ A member holding nothing answers one empty page marked last.
        FastTakeoverFrame.Collected none = new FastTakeoverFrame.Collected(0, true, List.of());
        assertThat(FastFrame.decode(FastFrame.encode(9, "uid-m", "uid-s", none)).body())
                .isEqualTo(none);
        FastTakeoverFrame.Release nothing = new FastTakeoverFrame.Release(List.of());
        assertThat(FastFrame.decode(FastFrame.encode(7, "uid-1", "uid-3", nothing)).body())
                .isEqualTo(nothing);
    }

    @Test
    void aFENCEsEpochsAreAscendingAboveItsClosedThrough() {
        assertThatThrownBy(() -> new FastTakeoverFrame.Fence(List.of(8L, 7L), 6))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new FastTakeoverFrame.Fence(List.of(7L, 7L), 6))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new FastTakeoverFrame.Fence(List.of(6L), 6))
                .as("a closed term is fenced by no one").isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new FastTakeoverFrame.Fence(List.of(), 6))
                .as("nothing to fence sends no FENCE").isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new FastTakeoverFrame.Fence(List.of(1L), -1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aRELEASENamesEachStreamOnceAtANonNegativeOffset() {
        RunKey s = new RunKey(INDEX, 3);
        assertThatThrownBy(() -> new FastTakeoverFrame.Release(List.of(
                new FastTakeoverFrame.ReleaseAt(s, 1), new FastTakeoverFrame.ReleaseAt(s, 2))))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new FastTakeoverFrame.ReleaseAt(s, -1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aCOLLECTEDPageIsAU32() {
        assertThatThrownBy(() -> new FastTakeoverFrame.Collected(-1, true, List.of()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new FastTakeoverFrame.Collected(FastFrame.MAX_U32 + 1, true,
                List.of())).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aMALFORMEDTakeoverFrameIsRefused() throws Exception {
        byte[] collected = FastFrameTest.golden("fast-collected-v1.bin");
        int last = indexOfLastFlag(collected);
        byte[] badFlag = collected.clone();
        badFlag[last] = 2;
        assertThatThrownBy(() -> FastFrame.decode(badFlag)).isInstanceOf(IOException.class);

        byte[] fence = FastFrameTest.golden("fast-fence-v1.bin");
        byte[] trailing = Arrays.copyOf(fence, fence.length + 1);
        assertThatThrownBy(() -> FastFrame.decode(trailing)).isInstanceOf(IOException.class);

        byte[] descending = fence.clone();
        // the two epochs, 7 then 8, swapped: each is the last byte of its i64
        descending[fence.length - 17] = 8;
        descending[fence.length - 9] = 7;
        assertThatThrownBy(() -> FastFrame.decode(descending)).isInstanceOf(IOException.class);

        byte[] release = FastFrameTest.golden("fast-release-v1.bin");
        byte[] cut = Arrays.copyOf(release, release.length - 1);
        assertThatThrownBy(() -> FastFrame.decode(cut)).isInstanceOf(IOException.class);
    }

    /** The {@code last} byte: after the header and the page's u32. */
    private static int indexOfLastFlag(byte[] collected) throws Exception {
        int header = FastFrame.encode(9, "uid-m", "uid-s",
                new FastTakeoverFrame.Collected(0, true, List.of())).length - 9;
        return header + 4;
    }
}
