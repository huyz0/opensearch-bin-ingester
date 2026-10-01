// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The three fast-mode settings on the wire (M13.23, criterion 7, ADR-0082 §1).
 *
 * <p>⚠️ THE V2 FILES WERE NOT WRITTEN BY THIS ENCODER. They came from an
 * independent encoder of ADR-0082's layout, checked against the stored v1 file
 * first, so a byte here disagreeing with {@link IndexRegistration#encode} is a
 * disagreement with the decision record, not with yesterday's code.
 *
 * <p>⚠️ AN INDEX AT THE DEFAULTS STILL ENCODES AS V1, which is what makes
 * ADR-0082's "mixed fleets work in both directions of a rolling upgrade" true:
 * an older ingester refuses an unknown version, so a plugin that wrote v2 for
 * every index would make every index unroutable on every older ingester the
 * moment the plugin upgraded first. Only an index that actually sets one of
 * the three needs the newer ingester.
 */
class IndexRegistrationV2GoldenTest {

    private static final String UUID = "nVzgup36TLqWp7VBBREj1w";

    private static byte[] golden(String name) throws IOException {
        try (var in = IndexRegistrationV2GoldenTest.class.getResourceAsStream("/golden/" + name)) {
            assertThat(in).as("missing golden file %s", name).isNotNull();
            return in.readAllBytes();
        }
    }

    private static IndexRegistration walAt(int quorum) {
        return IndexRegistration.unsplit(UUID, "logs", 3).withFastSettings(60_000, true, quorum);
    }

    private static IndexRegistration timerOnly() {
        return new IndexRegistration(UUID, "logs-000002", List.of("logs", "logs-write"),
                8, 32, 4, 1).withFastSettings(250, false, 2);
    }

    @Test
    void aWALIndexEncodesToItsStoredBytesAtEveryQuorum() throws Exception {
        for (int q = 1; q <= 3; q++) {
            assertThat(walAt(q).encode())
                    .as("wal_quorum %d: the bytes ADR-0082 §1 lays out, from an independent "
                            + "encoder", q)
                    .isEqualTo(golden("index-registration-v2-wal-q" + q + ".bin"));
        }
    }

    @Test
    void aTIMEROnlyChangeIsV2WithNoQuorumByte() throws Exception {
        byte[] stored = golden("index-registration-v2-timer.bin");

        assertThat(timerOnly().encode())
                .as("a non-default flush_timer alone makes the frame v2; with wal=false the "
                        + "quorum byte is absent, as ADR-0082 says it is read only when wal=1")
                .isEqualTo(stored);
        assertThat(ByteBuffer.wrap(stored).getInt(4)).isEqualTo(IndexRegistration.VERSION_2);
    }

    @Test
    void theSTOREDV2BytesDecodeToWhatWroteThem() throws Exception {
        for (int q = 1; q <= 3; q++) {
            assertThat(IndexRegistration.decode(golden("index-registration-v2-wal-q" + q + ".bin")))
                    .isEqualTo(walAt(q));
        }
        assertThat(IndexRegistration.decode(golden("index-registration-v2-timer.bin")))
                .isEqualTo(timerOnly());
    }

    @Test
    void aV1FrameDecodesAtTheThreeDEFAULTS() throws Exception {
        IndexRegistration read = IndexRegistration.decode(golden("index-registration-v1.bin"));

        assertThat(read.flushTimerMillis())
                .as("a v1 sender is an older plugin: its index has the default timer")
                .isEqualTo(5_000);
        assertThat(read.wal()).as("and is wal=false (ADR-0082 §1)").isFalse();
        assertThat(read.walQuorum()).isEqualTo(2);
    }

    @Test
    void anIndexAtTheDEFAULTSStillEncodesAsV1() throws Exception {
        assertThat(IndexRegistration.unsplit(UUID, "logs", 3)
                        .withFastSettings(5_000, false, 2).encode())
                .as("an older ingester reads every index that sets none of the three -- the "
                        + "rolling upgrade works with the plugin upgraded first")
                .isEqualTo(golden("index-registration-v1.bin"));
    }

    @Test
    void aQUORUMOutsideOneToThreeIsRefused() {
        IndexRegistration base = IndexRegistration.unsplit(UUID, "logs", 3);

        for (int q : new int[] {0, 4, -1}) {
            assertThatThrownBy(() -> base.withFastSettings(5_000, true, q))
                    .as("wal_quorum %d names no AZ count this fleet has", q)
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void aFLUSHTimerAtOrBelowZeroIsRefused() {
        IndexRegistration base = IndexRegistration.unsplit(UUID, "logs", 3);

        assertThatThrownBy(() -> base.withFastSettings(0, false, 2))
                .as("a zero deadline is an upload per record, not a setting")
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> base.withFastSettings(-1, false, 2))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void withWALOffTheQuorumIsTheDEFAULTSoEqualityMeansTheSameWire() {
        assertThat(IndexRegistration.unsplit(UUID, "logs", 3).withFastSettings(5_000, false, 3))
                .as("the quorum is not on the wire when wal=false, so a registration holding "
                        + "one would not survive its own round trip -- and the registrar diffs "
                        + "by equality, so it would re-push a change the ingester never sees")
                .isEqualTo(IndexRegistration.unsplit(UUID, "logs", 3));
    }

    /**
     * ⚠️ THE QUORUM BYTE STAYS IN PLACE. Corrupting the wal byte of a
     * {@code wal=false} frame would let a decoder that reads any non-zero byte
     * as {@code true} throw anyway -- for running out of bytes looking for a
     * quorum -- and pass this case for the wrong reason.
     */
    @Test
    void aWALByteOtherThanZeroOrOneIsRefusedNotGuessed() throws Exception {
        byte[] bytes = golden("index-registration-v2-wal-q3.bin");
        bytes[bytes.length - 2] = 2;

        assertThatThrownBy(() -> IndexRegistration.decode(bytes))
                .as("a wal byte of 2 is a shape this build does not know")
                .isInstanceOf(IOException.class);
    }

    @Test
    void aV2FrameCutShortIsRefused() throws Exception {
        byte[] whole = golden("index-registration-v2-wal-q3.bin");

        assertThatThrownBy(() -> IndexRegistration.decode(Arrays.copyOf(whole, whole.length - 1)))
                .as("a wal=1 frame missing its quorum byte")
                .isInstanceOf(IOException.class);
        assertThatThrownBy(() -> IndexRegistration.decode(Arrays.copyOf(whole, whole.length - 3)))
                .as("a frame torn inside the timer")
                .isInstanceOf(IOException.class);
    }
}
