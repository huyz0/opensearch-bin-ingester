// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The shapes a peer certificate's SAN may take (ADR-0084 decision 8; M13.52f
 * review round 1, T1-T4, P3): read at the URI, where a certificate fixture
 * would need one file per edge.
 */
class PeerIdentityShapeTest {

    @Test
    void theSMALLESTValidShapeIsOnePathSegment() {
        assertThat(PeerIdentity.parse("spiffe://td/ns/name-0/uid"))
                .isEqualTo(new PeerIdentity("spiffe://td/ns", "name-0", "uid"));
    }

    @Test
    void anEMPTYSegmentIsRefusedRatherThanReadAsAPrefix() {
        assertThatThrownBy(() -> PeerIdentity.parse("spiffe://cluster-a//pod-1/uid-pod1"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("empty segment");
    }

    @Test
    void EVERYDashIsRemovedFromThePodName() {
        assertThat(new PeerIdentity("spiffe://td/ns", "bin-ingester-0", "uid").podId())
                .isEqualTo("biningester0");
    }

    @Test
    void aNONSpiffeUriSanIsNotCounted() {
        assertThat(PeerIdentity.ofUris(List.of("https://example.test/pod-1",
                "spiffe://td/ns/pod-1/uid")).podUid()).isEqualTo("uid");
    }

    @Test
    void anIDENTITYOfWouldRefuseCannotBeBuilt() {
        assertThatThrownBy(() -> new PeerIdentity("spiffe://cluster-a", "pod-1", "uid"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new PeerIdentity("https://cluster-a/ns", "pod-1", "uid"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new PeerIdentity("spiffe://cluster-a/ns", "", "uid"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
