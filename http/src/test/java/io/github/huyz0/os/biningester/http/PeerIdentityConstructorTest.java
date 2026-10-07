// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

/**
 * The rest of what {@link PeerIdentity}'s constructor refuses (M13.52f review
 * round 2, T5, carried into M13.52g): no value a route could read but
 * {@code of} would refuse.
 */
class PeerIdentityConstructorTest {

    @Test
    void aTRAILINGSlashPrefixIsRefused() {
        assertThatThrownBy(() -> new PeerIdentity("spiffe://cluster-a/", "pod-1", "uid"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void anEMPTYUidIsRefused() {
        assertThatThrownBy(() -> new PeerIdentity("spiffe://cluster-a/ns", "pod-1", ""))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aSLASHInTheNameOrUidIsRefused() {
        assertThatThrownBy(() -> new PeerIdentity("spiffe://cluster-a/ns", "pod/1", "uid"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new PeerIdentity("spiffe://cluster-a/ns", "pod-1", "u/id"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
