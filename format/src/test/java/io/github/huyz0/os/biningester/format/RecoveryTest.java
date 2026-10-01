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
 * The recovery chain entry: a takeover's commits and voids in one entry
 * (M13.25, ADR-0081 §5 invariant c, ADR-0082 §5).
 *
 * <p>⚠️ ONE ENTRY, BECAUSE THE CHAIN'S FOLD TAKES THE MAXIMUM: written as a
 * void entry beside ordinary deltas, a crash between them leaves an exposed
 * hole below the next offset or a void past a surviving copy (M13.22 review
 * round 1, P3). The golden files come from an independent encoder of the
 * ADR's layout.
 */
class RecoveryTest {

    private static final RunKey A = new RunKey(new UUID(0x0123456789ABCDEFL, -0x0123456789ABCDF0L), 3);
    private static final RunKey B = new RunKey(new UUID(0x1111111111111111L, 0x2222222222222222L), 0);

    private static Recovery stored() {
        return new Recovery(42,
                List.of(new SegmentCommit("seg/recovered-1",
                        List.of(new RunCommit(A, 2, 100), new RunCommit(B, 1, 7)))),
                List.of(new Recovery.VoidRange(A, 102, 65636),
                        new Recovery.VoidRange(B, 8, 65543)));
    }

    private static Recovery voidsOnly() {
        return new Recovery(43, List.of(), List.of(new Recovery.VoidRange(A, 65636, 131172)));
    }

    private static byte[] golden(String name) throws IOException {
        try (var in = RecoveryTest.class.getResourceAsStream("/golden/" + name)) {
            assertThat(in).as("missing golden file %s", name).isNotNull();
            return in.readAllBytes();
        }
    }

    @Test
    void aRECOVERYEncodesToItsStoredBytes() throws Exception {
        assertThat(stored().encode()).isEqualTo(golden("chain-recovery-v1.bin"));
        assertThat(voidsOnly().encode()).isEqualTo(golden("chain-recovery-voids-only-v1.bin"));
    }

    @Test
    void theSTOREDBytesDecodeAsARecoveryThroughTheChainDecoder() throws Exception {
        ChainEntry read = ChainEntry.decode(golden("chain-recovery-v1.bin"));

        assertThat(read).isEqualTo(stored());
        assertThat(ChainEntry.decode(golden("chain-recovery-voids-only-v1.bin")))
                .isEqualTo(voidsOnly());
    }

    @Test
    void itsCOMMITSReadAsADeltaAndAVoidsOnlyEntryHasNone() {
        assertThat(stored().delta()).hasValueSatisfying(delta -> {
            assertThat(delta.sequence()).isEqualTo(42);
            assertThat(delta.segments()).isEqualTo(stored().segments());
        });
        assertThat(voidsOnly().delta()).as("a recovery that only voids commits no segment")
                .isEmpty();
    }

    @Test
    void anEMPTYRecoveryIsRefused() {
        assertThatThrownBy(() -> new Recovery(1, List.of(), List.of()))
                .as("neither commits nor voids: an entry that moves nothing")
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aVOIDMustBeNonEmptyNonNegativeSortedAndDisjoint() {
        assertThatThrownBy(() -> new Recovery.VoidRange(A, 5, 5))
                .as("from == to voids nothing").isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Recovery.VoidRange(A, -1, 5))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Recovery(1, List.of(), List.of(
                        new Recovery.VoidRange(B, 0, 1), new Recovery.VoidRange(A, 0, 1))))
                .as("voids are sorted by RunKey (ADR-0082 §5)")
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Recovery(1, List.of(), List.of(
                        new Recovery.VoidRange(A, 0, 10), new Recovery.VoidRange(A, 5, 20))))
                .as("two voids of one stream overlapping")
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aTORNOrForgedRecoveryIsRefusedNotSkipped() throws Exception {
        byte[] whole = golden("chain-recovery-v1.bin");

        assertThatThrownBy(() -> ChainEntry.decode(Arrays.copyOf(whole, whole.length - 1)))
                .isInstanceOf(IOException.class);
        // a single void [0, 1): its last byte is the uvarint `to`; zero makes to == from
        byte[] forged = new Recovery(43, List.of(), List.of(
                new Recovery.VoidRange(A, 0, 1))).encode();
        forged[forged.length - 1] = 0;
        assertThatThrownBy(() -> ChainEntry.decode(forged))
                .as("a void with to == from on the wire is corrupt input: an IOException, "
                        + "never an unchecked throw out of a recovery path")
                .isInstanceOf(IOException.class);
    }
}
