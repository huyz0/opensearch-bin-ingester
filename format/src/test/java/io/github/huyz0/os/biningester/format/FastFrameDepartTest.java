// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * DEPART, HELD and HELD_STATUS (ADR-0082 §2; M13.26f), held to golden bytes
 * from an independent encoder.
 */
class FastFrameDepartTest {

    private static final Roster.Incarnation POD =
            new Roster.Incarnation("ingester-2", "uid-2", "az-b", "http://10.0.0.2:8080");

    private static FastFrame.Held held() {
        return new FastFrame.Held(List.of(new FastFrame.HeldStream(FastFrameTest.STREAM,
                List.of(new FastFrame.HeldGroup(7, 1, 200, 210)))));
    }

    private static FastFrame.HeldStatus status() {
        return new FastFrame.HeldStatus(List.of(new FastFrame.StreamStatus(FastFrameTest.STREAM,
                205, List.of(
                        new FastFrame.GroupStatus(7, 1, Long.MAX_VALUE, FastFrame.Status.SUPERSEDED),
                        new FastFrame.GroupStatus(7, 2, 300, FastFrame.Status.CLOSED_TERM),
                        new FastFrame.GroupStatus(8, 0, 400, FastFrame.Status.PENDING)))));
    }

    @Test
    void eachKINDEncodesToItsGoldenBytes() throws Exception {
        assertThat(FastFrame.encode(7, "uid-2", "uid-1", new FastFrame.Depart(POD, 1, held())))
                .isEqualTo(FastFrameTest.golden("fast-depart-report-v1.bin"));
        assertThat(FastFrame.encode(7, "uid-2", "uid-1",
                new FastFrame.Depart(POD, 2, FastFrame.Held.NONE)))
                .isEqualTo(FastFrameTest.golden("fast-depart-leave-v1.bin"));
        assertThat(FastFrame.encode(7, "uid-2", "uid-1", new FastFrame.HeldReport(held())))
                .isEqualTo(FastFrameTest.golden("fast-held-v1.bin"));
        assertThat(FastFrame.encode(7, "uid-1", "uid-2", new FastFrame.HeldStatusReport(status())))
                .isEqualTo(FastFrameTest.golden("fast-held-status-v1.bin"));
    }

    @Test
    void eachGOLDENFrameDecodesToItsBody() throws Exception {
        assertThat(FastFrame.decode(FastFrameTest.golden("fast-depart-report-v1.bin")).body())
                .isEqualTo(new FastFrame.Depart(POD, 1, held()));
        assertThat(FastFrame.decode(FastFrameTest.golden("fast-depart-leave-v1.bin")).body())
                .isEqualTo(new FastFrame.Depart(POD, 2, FastFrame.Held.NONE));
        assertThat(FastFrame.decode(FastFrameTest.golden("fast-held-v1.bin")).body())
                .isEqualTo(new FastFrame.HeldReport(held()));
        assertThat(FastFrame.decode(FastFrameTest.golden("fast-held-status-v1.bin")).body())
                .isEqualTo(new FastFrame.HeldStatusReport(status()));
    }

    @Test
    void aPHASEOtherThanOneOrTwoOrAPhaseTwoReportIsRefused() throws Exception {
        assertThatThrownBy(() -> new FastFrame.Depart(POD, 3, FastFrame.Held.NONE))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new FastFrame.Depart(POD, 2, held()))
                .isInstanceOf(IllegalArgumentException.class);
        byte[] bad = FastFrameTest.golden("fast-depart-leave-v1.bin").clone();
        bad[bad.length - 1] = 3;

        assertThatThrownBy(() -> FastFrame.decode(bad)).isInstanceOf(IOException.class);
    }
}
