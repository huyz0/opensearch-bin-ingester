// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * ADR-0082 §1's layout, written out by hand, against the golden files AND the
 * encoder (M13.23 review round 1, T4).
 *
 * <p>⚠️ THE GOLDEN FILES' PROVENANCE IS IN THE TREE. They were first produced
 * by an independent encoder that lived outside it; this class is that encoder,
 * field by field from the decision record, so anyone can re-run the check
 * that the files say what the ADR says -- and it builds its varints and
 * strings itself rather than calling {@code SegmentWriter}, which the
 * production encoder uses.
 */
class IndexRegistrationV2LayoutTest {

    private static final String UUID = "nVzgup36TLqWp7VBBREj1w";

    private static void uvarint(ByteArrayOutputStream out, long n) {
        while ((n & ~0x7FL) != 0) {
            out.write((int) ((n & 0x7F) | 0x80));
            n >>>= 7;
        }
        out.write((int) n);
    }

    private static void string(ByteArrayOutputStream out, String s) {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        uvarint(out, b.length);
        out.writeBytes(b);
    }

    /** ADR-0082 §1: v1's fields, then i64 timer, u8 wal, u8 quorum only when wal = 1. */
    private static byte[] byHand(String name, List<String> aliases, int shards, int routingShards,
            int factor, int partition, long timer, boolean wal, int quorum) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.writeBytes(ByteBuffer.allocate(8).putInt(0x42495247).putInt(2).array());
        string(out, UUID);
        string(out, name);
        uvarint(out, aliases.size());
        aliases.forEach(a -> string(out, a));
        uvarint(out, shards);
        uvarint(out, routingShards);
        uvarint(out, factor);
        uvarint(out, partition);
        out.writeBytes(ByteBuffer.allocate(8).putLong(timer).array());
        out.write(wal ? 1 : 0);
        if (wal) {
            out.write(quorum);
        }
        return out.toByteArray();
    }

    private static byte[] golden(String name) throws IOException {
        try (var in = IndexRegistrationV2LayoutTest.class.getResourceAsStream("/golden/" + name)) {
            assertThat(in).as("missing golden file %s", name).isNotNull();
            return in.readAllBytes();
        }
    }

    @Test
    void theGOLDENFilesAndTheEncoderBothSayWhatTheADRSays() throws Exception {
        for (int q = 1; q <= 3; q++) {
            byte[] adr = byHand("logs", List.of(), 3, 3, 1, 1, 60_000, true, q);

            assertThat(golden("index-registration-v2-wal-q" + q + ".bin"))
                    .as("the stored file at wal_quorum %d", q).isEqualTo(adr);
            assertThat(IndexRegistration.unsplit(UUID, "logs", 3)
                            .withFastSettings(60_000, true, q).encode())
                    .as("the encoder at wal_quorum %d", q).isEqualTo(adr);
        }
        byte[] timer = byHand("logs-000002", List.of("logs", "logs-write"), 8, 32, 4, 1, 250,
                false, 2);
        assertThat(golden("index-registration-v2-timer.bin")).isEqualTo(timer);
        assertThat(new IndexRegistration(UUID, "logs-000002", List.of("logs", "logs-write"),
                        8, 32, 4, 1).withFastSettings(250, false, 2).encode())
                .isEqualTo(timer);
    }
}
