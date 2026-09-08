// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import static binjava.sequencer.DedupFixtures.PREFIX;
import static binjava.sequencer.DedupFixtures.counts;
import static binjava.sequencer.DedupFixtures.deltasCarrying;
import static binjava.sequencer.DedupFixtures.firstOffsetOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import binjava.binstore.CountingBinStore;
import binjava.binstore.backend.MemoryBinStore;
import binjava.format.CommitDelta;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * A pod that does not hold the lease commits by forwarding (M5.4, M5.5).
 *
 * <p>⚠️ THIS IS THE CORRECTNESS HOLE M4 LEFT OPEN, stated twice in its own
 * SPEC: "at the end of M4 a multi-pod deployment is not yet correct". A lease
 * means exactly one sequencer writes the chain; without forwarding, every other
 * pod simply cannot commit.
 */
class RemoteSequencerTest {

    private static final String A = "pod-a:9000";
    private static final String B = "pod-b:9000";
    private static final String C = "pod-c:9000";

    /**
     * A lease manager that ADVERTISES its endpoint.
     *
     * <p>⚠️ `DedupFixtures.manager` writes an empty endpoint, which is fine for
     * every test that never forwards -- and useless here, because the endpoint
     * IS the thing under test: a forwarder reads it out of the lease.
     */
    private static LeaseManager leaderAt(binjava.binstore.BinStore store, String podId,
            String endpoint) {
        return new LeaseManager(store, config(podId, endpoint), java.time.Clock.systemUTC());
    }

    private static LeaseConfig config(String podId, String endpoint) {
        return new LeaseConfig(PREFIX, podId, endpoint,
                Duration.ofSeconds(10), Duration.ofSeconds(3));
    }

    private static CommitRequest flush(long seq, String segment) {
        return new CommitRequest("podx", "i1", seq, segment, counts(3));
    }

    @Test
    void aNonLeaseholderCommitsThroughTheLEASEHOLDER() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        LocalSequencer leader = LocalSequencer.start(
                store, PREFIX, leaderAt(store, "poda", A), 8).orElseThrow();
        InProcessTransport transport = new InProcessTransport().at(A, leader);
        try (RemoteSequencer remote =
                new RemoteSequencer(store, config("podb", B), transport)) {
            CommitDelta delta = remote.commit(flush(0, "seg/0"));

            assertThat(firstOffsetOf(delta, "seg/0"))
                    .as("the forwarding pod gets real offsets from the leaseholder's order")
                    .isZero();
        } finally {
            leader.close();
        }
        assertThat(deltasCarrying(store, "seg/0"))
                .as("and exactly one delta carries it").isEqualTo(1);
    }

    @Test
    void theENDPOINTComesFromTheLeaseAndNotFromConfiguration() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        LocalSequencer leader = LocalSequencer.start(
                store, PREFIX, leaderAt(store, "poda", A), 8).orElseThrow();
        InProcessTransport transport = new InProcessTransport().at(A, leader);
        try (RemoteSequencer remote =
                new RemoteSequencer(store, config("podb", B), transport)) {
            remote.commit(flush(0, "seg/0"));
            // ⚠️ THE LEASE IS THE TRUTH (ADR-0012). `config` names THIS pod's
            // own endpoint, B; the send must go to A, which is where the lease
            // says the holder is. A forwarder that used its own configuration,
            // or cached a peer, would send commits to a pod that does not hold
            // the term.
            assertThat(transport.sentTo()).containsExactly(A);
        } finally {
            leader.close();
        }
    }

    @Test
    void aLeaseThatMovedBEFORETheCommitIsFollowedWithNoRefusalAtAll() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        LocalSequencer first = LocalSequencer.start(
                store, PREFIX, leaderAt(store, "poda", A), 8).orElseThrow();
        InProcessTransport transport = new InProcessTransport().at(A, first);
        try (RemoteSequencer remote =
                new RemoteSequencer(store, config("podb", B), transport)) {
            remote.commit(flush(0, "seg/0"));
            first.close();
            transport.gone(A);
            LocalSequencer second = LocalSequencer.start(
                    store, PREFIX, leaderAt(store, "podc", C), 8).orElseThrow();
            transport.at(C, second);
            try {
                assertThat(firstOffsetOf(remote.commit(flush(1, "seg/1")), "seg/1"))
                        .as("offsets continue the SAME total order across the takeover")
                        .isEqualTo(3L);
            } finally {
                second.close();
            }
            // ⚠️ NO WASTED SEND. Reading the lease per commit means a takeover
            // that completed before the commit costs nothing extra -- the
            // forwarder simply addresses the new holder. That is the cheap
            // path, and it is the common one.
            assertThat(transport.sentTo()).containsExactly(A, C);
        }
    }

    @Test
    void aLeaseThatMovesDURINGTheCommitIsFollowedAfterOneRefusal() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        LocalSequencer first = LocalSequencer.start(
                store, PREFIX, leaderAt(store, "poda", A), 8).orElseThrow();
        InProcessTransport inner = new InProcessTransport().at(A, first);

        // ⚠️ THE RACE THE FOLLOW EXISTS FOR: the lease is read, and the term
        // moves before the send lands. A forwarder that cached the endpoint, or
        // gave up on the refusal, would lose this commit -- and the refusal
        // says plainly that nothing was applied, so resending is safe.
        SequencerTransport movesUnderneath = new SequencerTransport() {
            private boolean moved;

            @Override
            public CommitDelta send(String endpoint, CommitRequest request)
                    throws IOException {
                if (!moved && A.equals(endpoint)) {
                    moved = true;
                    first.close();
                    inner.gone(A);
                    LocalSequencer second = LocalSequencer.start(
                            store, PREFIX, leaderAt(store, "podc", C), 8).orElseThrow();
                    inner.at(C, second);
                    throw new NotTheLeaseholderException("fenced: the term moved");
                }
                return inner.send(endpoint, request);
            }

            @Override
            public void close() throws IOException {
                inner.close();
            }
        };

        try (RemoteSequencer remote =
                new RemoteSequencer(store, config("podb", B), movesUnderneath)) {
            assertThat(firstOffsetOf(remote.commit(flush(0, "seg/0")), "seg/0"))
                    .as("the refused commit reaches the new holder and lands once")
                    .isZero();
        }
        assertThat(inner.sentTo())
                .as("the retry went to the endpoint the RE-READ lease named")
                .containsExactly(C);
        assertThat(deltasCarrying(store, "seg/0"))
                .as("and exactly one delta carries it -- a refusal is not a duplicate")
                .isEqualTo(1);
    }

    @Test
    void aRefusalWhenTheLeaseHasNOTMovedIsREPORTED_NotLoopedOn() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        LocalSequencer leader = LocalSequencer.start(
                store, PREFIX, leaderAt(store, "poda", A), 8).orElseThrow();
        // The lease names A, but nothing is listening there.
        InProcessTransport transport = new InProcessTransport();
        try (RemoteSequencer remote =
                new RemoteSequencer(store, config("podb", B), transport)) {
            // ⚠️ NOT AN INFINITE FOLLOW. If the store still names the same
            // holder, resending is asking a pod that has already said no. The
            // truthful answer is that there is no reachable sequencer, and a
            // caller can then fail its producers rather than hang.
            assertThatThrownBy(() -> remote.commit(flush(0, "seg/0")))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("has not moved");
            assertThat(transport.sentTo())
                    .as("exactly two attempts: one, a lease re-read, and no more")
                    .hasSize(1);
        } finally {
            leader.close();
        }
    }

    @Test
    void anAMBIGUOUSFailureIsNOTResent() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        LocalSequencer leader = LocalSequencer.start(
                store, PREFIX, leaderAt(store, "poda", A), 8).orElseThrow();
        // ⚠️ THE DISTINCTION THAT KEEPS I2. A plain IOException does not say
        // whether the commit was applied -- it may have landed and lost its
        // reply. `Sequencer.commit`'s contract says resending one "commits the
        // same records again", because an ambiguous commit is never recorded as
        // applied. Reconciling that from the chain is M5.23; until then the
        // failure propagates.
        SequencerTransport flaky = new SequencerTransport() {
            @Override
            public CommitDelta send(String endpoint, CommitRequest request)
                    throws IOException {
                throw new IOException("injected: the reply was lost");
            }

            @Override
            public void close() {
            }
        };
        try (RemoteSequencer remote =
                new RemoteSequencer(store, config("podb", B), flaky)) {
            assertThatThrownBy(() -> remote.commit(flush(0, "seg/0")))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("the reply was lost");
        } finally {
            leader.close();
        }
        assertThat(deltasCarrying(store, "seg/0"))
                .as("nothing was resent, so nothing could be duplicated").isZero();
    }

    @Test
    void forwardingAddsNoOBJECTSTOREWriteOfItsOwn() throws Exception {
        MemoryBinStore backing = new MemoryBinStore();
        CountingBinStore store = new CountingBinStore(backing);
        LocalSequencer leader = LocalSequencer.start(
                store, PREFIX, leaderAt(store, "poda", A), 8).orElseThrow();
        InProcessTransport transport = new InProcessTransport().at(A, leader);
        try (RemoteSequencer remote =
                new RemoteSequencer(store, config("podb", B), transport)) {
            long putsBefore = store.counts().puts();
            remote.commit(flush(0, "seg/0"));
            // ⚠️ THE COST ARGUMENT, ASSERTED. Forwarding is a pod-to-pod RPC:
            // the only write is the leaseholder's own single chain PUT, which it
            // would have made anyway. A design that wrote an intent object per
            // forwarded commit -- the degraded `ctl/inbox` path, deferred to M8
            // -- would cost one extra PUT per (pod, flush) and shows up here.
            assertThat(store.counts().puts() - putsBefore)
                    .as("one PUT for the delta, and nothing for the forwarding itself")
                    .isEqualTo(1L);
        } finally {
            leader.close();
        }
    }

    @Test
    void aBATCHIsRefusedRatherThanSilentlySendingTheFirst() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        LocalSequencer leader = LocalSequencer.start(
                store, PREFIX, leaderAt(store, "poda", A), 8).orElseThrow();
        InProcessTransport transport = new InProcessTransport().at(A, leader);
        try (RemoteSequencer remote =
                new RemoteSequencer(store, config("podb", B), transport)) {
            // ⚠️ SILENTLY SENDING THE FIRST WOULD LOSE THE REST. Batching many
            // pods' flushes into one delta is the leaseholder's job (M4.7); a
            // forwarding pod has one flush of its own and the wire has no
            // encoding for more.
            assertThatThrownBy(() ->
                    remote.commitAll(List.of(flush(0, "seg/0"), flush(1, "seg/1"))))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("one request per send");
        } finally {
            leader.close();
        }
        assertThat(deltasCarrying(store, "seg/0")).isZero();
    }
}
