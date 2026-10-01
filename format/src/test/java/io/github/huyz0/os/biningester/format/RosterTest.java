// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The roster object and {@code LATEST} (ADR-0082 §3; M13.25c, landed by
 * M13.26b). The golden files come from an independent encoder written from
 * the ADR's text, so encoder and decoder are each held to the bytes.
 */
class RosterTest {

    static final UUID I1 = UUID.fromString("00000000-0000-0001-0000-000000000001");
    static final UUID I2 = UUID.fromString("00000000-0000-0001-0000-000000000002");
    static final UUID HIGH = UUID.fromString("f0000000-0000-0000-0000-000000000000");

    static byte[] golden(String name) throws IOException {
        try (var in = RosterTest.class.getResourceAsStream("/golden/" + name)) {
            assertThat(in).as("missing golden file %s", name).isNotNull();
            return in.readAllBytes();
        }
    }

    static Roster full() {
        Roster.Incarnation one = new Roster.Incarnation("ingester-1", "uid-1", "az-a",
                "http://10.0.0.1:8080");
        Roster.Incarnation two = new Roster.Incarnation("ingester-2", "uid-2", "az-b",
                "http://10.0.0.2:8080");
        return new Roster(7, 5, one,
                List.of(new Roster.Member(one, Roster.State.ROSTERED),
                        new Roster.Member(two, Roster.State.DEPARTED)),
                List.of(new Roster.TermRecord(0, new java.util.TreeMap<>(Map.of(I2, 1, I1, 2))),
                        new Roster.TermRecord(1, new java.util.TreeMap<>(Map.of(HIGH, 3, I1, 1)))),
                List.of(new Roster.Decision(0, new RunKey(I1, 3), 4242),
                        new Roster.Decision(2, new RunKey(I2, 0), 65536)),
                1727740800000L, 9, false);
    }

    static Roster first() {
        Roster.Incarnation zero = new Roster.Incarnation("ingester-0", "uid-0", "az-c", "");
        return new Roster(1, -1, zero, List.of(new Roster.Member(zero, Roster.State.ROSTERED)),
                List.of(new Roster.TermRecord(0, new java.util.TreeMap<>())), List.of(), 0, 0,
                true);
    }

    @Test
    void theROSTEREncodesToItsGoldenBytes() throws Exception {
        assertThat(full().encode()).isEqualTo(golden("roster-v1.json"));
        assertThat(first().encode()).isEqualTo(golden("roster-first-v1.json"));
    }

    @Test
    void theGOLDENRostersDecodeToTheirRecords() throws Exception {
        assertThat(Roster.decode(golden("roster-v1.json"))).isEqualTo(full());
        assertThat(Roster.decode(golden("roster-first-v1.json"))).isEqualTo(first());
    }

    @Test
    void LATESTEncodesAndDecodesItsGoldenBytes() throws Exception {
        assertThat(Roster.encodeLatest(7)).isEqualTo(golden("roster-latest-v1.json"));
        assertThat(Roster.decodeLatest(golden("roster-latest-v1.json"))).isEqualTo(7);
    }

    @Test
    void theKEYSAreFixedLengthUnderThePrefix() {
        assertThat(Roster.key("p", 7)).isEqualTo("p/ctl/fast/0/0000000000000007.roster");
        assertThat(Roster.latestKey("p")).isEqualTo("p/ctl/fast/0/LATEST");
    }

    @Test
    void aMEMBERIsFoundByItsUidAndTheLeadersDepartureRead() {
        Roster r = full();

        assertThat(r.member("uid-2").orElseThrow().state()).isEqualTo(Roster.State.DEPARTED);
        assertThat(r.member("uid-9")).isEmpty();
        assertThat(r.leaderDeparted()).isFalse();
        assertThat(r.fencedBy(12).fencedBy()).isEqualTo(12);
        assertThat(r.after(6, 99).predecessor()).isEqualTo(6);
        assertThat(r.after(6, 99).notBefore()).isEqualTo(99);
    }
}
