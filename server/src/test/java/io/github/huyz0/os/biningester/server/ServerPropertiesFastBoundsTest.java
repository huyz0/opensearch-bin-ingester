// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.http.FastFrameService;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * A fast pod's journal and segments are bounded against the peer route's
 * frame (M13.27z; M13.27h's review round 1, P2; M13.27u's review round 1,
 * P1): every batch a writer may send fits one frame, and the leader's half
 * of the cap holds any batch one frame carries.
 *
 * <p>⚠️ **A COMMIT THE LEADER's HALF COULD NEVER HOLD WAITED FOR EVER**, and
 * a segment larger than a frame could never be sent at all -- so neither is
 * representable.
 */
class ServerPropertiesFastBoundsTest {

    private static Map<String, String> fast() {
        Map<String, String> settings = new HashMap<>();
        settings.put(ServerProperties.POD_ID, "pod1");
        settings.put(ServerProperties.POD_UID, "uid-pod1");
        settings.put(ServerProperties.PEER_TLS, "off");
        settings.put(ServerProperties.PEER_PORT, "0");
        settings.put(ServerProperties.POD_AZ, "az-a");
        settings.put(ServerProperties.TRUST_DOMAIN, "cluster-a");
        settings.put(ServerProperties.PREFIX, "bins/cluster-a");
        settings.put(ServerProperties.STORE_KIND, "memory");
        settings.put(ServerProperties.ENDPOINT, "http://pod1:8080");
        settings.put(ServerProperties.HTTP_PORT, "8080");
        settings.put(ServerProperties.PRODUCER_SUBJECT, "producer-1");
        settings.put(ServerProperties.PRODUCER_ALLOWED_INDICES, "logs");
        settings.put(ServerProperties.FAST_JOURNAL_DIR, "/var/fast");
        return settings;
    }

    @Test
    void aCAPBelowFourFramesIsRefusedNamingIt() {
        Map<String, String> settings = fast();
        settings.put(ServerProperties.FAST_JOURNAL_CAP,
                Long.toString(4 * FastFrameService.MAX_FRAME_BYTES - 1));

        assertThatThrownBy(() -> ServerProperties.parse(settings))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(ServerProperties.FAST_JOURNAL_CAP);
    }

    @Test
    void aCAPOfFourFramesIsAccepted() {
        Map<String, String> settings = fast();
        settings.put(ServerProperties.FAST_JOURNAL_CAP,
                Long.toString(4 * FastFrameService.MAX_FRAME_BYTES));

        assertThat(ServerProperties.parse(settings).fastJournal().orElseThrow().capBytes())
                .isEqualTo(256L << 20);
    }

    @Test
    void aSEGMENTPastHalfAFrameIsRefusedOnAFastPod() {
        Map<String, String> settings = fast();
        settings.put(ServerProperties.MAX_SEGMENT_BYTES,
                Long.toString(FastFrameService.MAX_FRAME_BYTES / 2 + 1));

        assertThatThrownBy(() -> ServerProperties.parse(settings))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(ServerProperties.MAX_SEGMENT_BYTES);
    }

    @Test
    void theSAMESegmentIsAcceptedOnADisklessPod() {
        Map<String, String> settings = fast();
        settings.remove(ServerProperties.FAST_JOURNAL_DIR);
        settings.put(ServerProperties.MAX_SEGMENT_BYTES,
                Long.toString(FastFrameService.MAX_FRAME_BYTES / 2 + 1));

        assertThat(ServerProperties.parse(settings).ingest().maxSegmentBytes())
                .isEqualTo(FastFrameService.MAX_FRAME_BYTES / 2 + 1);
    }
}
