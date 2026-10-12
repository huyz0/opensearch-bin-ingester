// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.Roster;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * The node's REPLICA send reaches nothing on a pod leading no term (M13.27x).
 *
 * <p>⚠️ THE TERM-PRESENT SEND IS NOT PINNED HERE: a node's term assigns
 * nothing until M13.27l records a {@code wal_quorum}, so it owes no REPLICA;
 * {@code FastLeaderTermReplicaTest} pins the term's send.
 */
class FastLeaderFramesReplicaTest {

    @Test
    void aPODLeadingNoTermSendsNoReplica() {
        AtomicInteger sent = new AtomicInteger();

        FastLeaderFrames.sendReplicas(null, null,
                new Roster.Incarnation("pod1", "uid-pod1", "az-a", ""), (endpoint, frame) -> {
                    sent.incrementAndGet();
                    return new byte[0];
                });

        assertThat(sent.get()).isZero();
    }
}
