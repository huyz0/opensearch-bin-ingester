// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The fast frames' header and the JOIN, JOINED and REFUSED kinds (ADR-0082
 * §2; M13.26d), held to golden bytes from an independent encoder.
 */
class FastFrameTest {

    static final RunKey STREAM = new RunKey(new UUID(1, 1), 3);

    static byte[] golden(String name) throws IOException {
        try (var in = FastFrameTest.class.getResourceAsStream("/golden/" + name)) {
            assertThat(in).as("missing golden file %s", name).isNotNull();
            return in.readAllBytes();
        }
    }

    static FastFrame.Join join() {
        return new FastFrame.Join(
                new Roster.Incarnation("ingester-2", "uid-2", "az-b", "http://10.0.0.2:8080"),
                new FastFrame.Held(List.of(new FastFrame.HeldStream(STREAM, List.of(
                        new FastFrame.HeldGroup(5, 0, 100, 120),
                        new FastFrame.HeldGroup(6, 2, 121, 130))))));
    }

    static FastFrame.Joined joined() {
        return new FastFrame.Joined(4, new FastFrame.HeldStatus(List.of(
                new FastFrame.StreamStatus(STREAM, 110, List.of(
                        new FastFrame.GroupStatus(5, 0, Long.MAX_VALUE,
                                FastFrame.Status.COMMITTED),
                        new FastFrame.GroupStatus(6, 2, 125, FastFrame.Status.PENDING))))));
    }

    static FastFrame.Refused discarded() {
        return new FastFrame.Refused(FastFrame.Reason.DISCARDED,
                Optional.of(new FastJournalRecord.IdempotencyKey("ingester-3",
                        new UUID(0x0102030405060708L, 0x1112131415161718L), 42)),
                "discarded");
    }

    @Test
    void eachKINDEncodesToItsGoldenBytes() throws Exception {
        assertThat(FastFrame.encode(7, "uid-2", "uid-1", join()))
                .isEqualTo(golden("fast-join-v1.bin"));
        assertThat(FastFrame.encode(7, "uid-1", "uid-2", joined()))
                .isEqualTo(golden("fast-joined-v1.bin"));
        assertThat(FastFrame.encode(7, "uid-1", "uid-3", discarded()))
                .isEqualTo(golden("fast-refused-discarded-v1.bin"));
        assertThat(FastFrame.encode(9, "uid-1", "uid-3", new FastFrame.Refused(
                FastFrame.Reason.LOWER_EPOCH, Optional.empty(), "lower epoch")))
                .isEqualTo(golden("fast-refused-v1.bin"));
    }

    @Test
    void eachGOLDENFrameDecodesToItsBody() throws Exception {
        assertThat(FastFrame.decode(golden("fast-join-v1.bin")))
                .isEqualTo(new FastFrame.Frame(new FastFrame.Header(12, 7, "uid-2", "uid-1"),
                        join()));
        assertThat(FastFrame.decode(golden("fast-joined-v1.bin")).body()).isEqualTo(joined());
        assertThat(FastFrame.decode(golden("fast-refused-discarded-v1.bin")).body())
                .isEqualTo(discarded());
        assertThat(FastFrame.decode(golden("fast-refused-v1.bin")).header().epoch())
                .isEqualTo(9);
    }

    @Test
    void theHEADERReadsAloneForTheFence() throws Exception {
        assertThat(FastFrame.header(golden("fast-joined-v1.bin")))
                .isEqualTo(new FastFrame.Header(15, 7, "uid-1", "uid-2"));
    }
}
