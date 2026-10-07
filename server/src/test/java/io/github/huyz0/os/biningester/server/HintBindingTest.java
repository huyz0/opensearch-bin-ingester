// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.DurableSegmentSignalFrame;
import io.github.huyz0.os.biningester.format.SegmentKey;
import io.github.huyz0.os.biningester.http.DurableSegmentSignalService;
import io.github.huyz0.os.biningester.http.EndpointSliceView;
import io.github.huyz0.os.biningester.http.PeerBinding;
import java.nio.file.Path;
import java.security.cert.Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * A durable-segment hint's writer is its client certificate's pod (ADR-0084
 * decision 8; M13.52g), over and above the EndpointSlice's address: here the
 * view authorizes every hint, so only the binding can refuse one.
 */
class HintBindingTest {

    private static final String FLEET = "spiffe://cluster-a/ns/test/pod";

    private static Optional<Certificate[]> chain(String name) throws Exception {
        String cert = Path.of(HintBindingTest.class.getResource("/peer-tls/" + name + ".pem")
                .toURI()).toString();
        String key = cert.replace(".pem", ".key");
        String ca = Path.of(HintBindingTest.class.getResource("/peer-tls/ca.pem").toURI())
                .toString();
        PeerTls read = Main.peerTls(new PeerConfig(PeerConfig.Mode.MUTUAL, 9443, Optional.of(
                new PeerConfig.Files(cert, key, ca))), message -> { }).orElseThrow();
        return Optional.of(read.chain().toArray(new Certificate[0]));
    }

    private static boolean accepted(Optional<Certificate[]> chain) throws Exception {
        EndpointSliceView view = new EndpointSliceView();
        view.apply("{\"type\":\"ADDED\",\"object\":{\"metadata\":{\"name\":\"ingesters\"},"
                + "\"endpoints\":[{\"addresses\":[\"10.0.0.1\"],\"zone\":\"az-a\","
                + "\"conditions\":{\"ready\":true},\"targetRef\":{\"name\":\"pod1\"}}]}} ");
        List<String> warmed = new ArrayList<>();
        DurableSegmentSignalService service = new DurableSegmentSignalService(view,
                (segmentKey, writerAz) -> warmed.add(segmentKey), PeerBinding.within(FLEET));
        boolean accepted = service.accept(chain, "10.0.0.1", new DurableSegmentSignalFrame(
                "pod1", "az-a", new SegmentKey("bins/cluster-a", 1, "pod1", 1, 48).key())
                .encode());
        assertThat(warmed).hasSize(accepted ? 1 : 0);
        return accepted;
    }

    @Test
    void theWRITERsOwnCertificateIsAccepted() throws Exception {
        assertThat(accepted(chain("pod1"))).isTrue();
    }

    @Test
    void anOTHERPodsCertificateIsRefused() throws Exception {
        assertThat(accepted(chain("pod2"))).isFalse();
    }

    @Test
    void theWRITERsNameUnderAnotherFleetIsRefused() throws Exception {
        assertThat(accepted(chain("otherns"))).isFalse();
    }
}
