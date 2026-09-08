// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import binjava.binstore.Body;
import binjava.binstore.backend.MemoryBinStore;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * I5's ack clause, judged against a trace the STORE produced (M4.50).
 *
 * <p>⚠️ THE DEFECT THIS CLOSES. {@link CommitProtocolSimulation} used to append
 * both halves of the trace from one {@code commit()} return, adjacently and
 * single-threaded:
 *
 * <pre>
 *   CommitDelta acked = leader.commit(req);
 *   acks.add(AckEvent.confirmed(epoch, acked.sequence()));
 *   acks.add(AckEvent.acked(epoch, acked.sequence()));
 * </pre>
 *
 * The confirmation therefore preceded the acknowledgement BY CONSTRUCTION, so
 * {@link AckOrderInvariants#checkAckOrder} could not fail however the writer
 * behaved -- a writer acking window N+1 before window N confirmed left the
 * 1,000-seed sweep green. I5 was asserted and not testable, and I5 is the one
 * the research corpus singles out as "the one a plausible implementation
 * violates by accident".
 *
 * <p>⚠️ SO THE CONFIRMATION COMES FROM THE STORE, which sees every PUT in the
 * order they actually complete and cannot be talked out of the truth by the
 * driver. The acknowledgement still comes from the writer returning. The two
 * now have independent sources, which is the whole point: a trace whose halves
 * share an author cannot catch a disagreement between them.
 */
class AckTraceStoreTest {

    private static final String PREFIX = "bins/cluster-a";
    private static final long EPOCH = 3L;

    private static Body body(String s) {
        byte[] b = s.getBytes();
        return new Body(b.length, () -> new ByteArrayInputStream(b));
    }

    private static String deltaKey(long seq) {
        return new LogKeys(PREFIX, EPOCH).keyFor(seq);
    }

    @Test
    void theTraceRECORDSTheOrderTheStoreActuallyConfirmedIn() throws Exception {
        List<AckOrderInvariants.AckEvent> trace = new ArrayList<>();
        AckTraceStore store = new AckTraceStore(new MemoryBinStore(), trace::add);

        // Sequence 1 lands first, then 0. A store may complete writes in any
        // order -- that is pipelining, which ADR-0011 permits.
        store.putIfAbsent(deltaKey(1), body("one"));
        store.putIfAbsent(deltaKey(0), body("zero"));

        assertThat(trace).as("the store's own completion order, not the caller's")
                .containsExactly(
                        AckOrderInvariants.AckEvent.confirmed(EPOCH, 1),
                        AckOrderInvariants.AckEvent.confirmed(EPOCH, 0));
    }

    @Test
    void anACKAheadOfItsOwnCONFIRMATIONIsCaught() throws Exception {
        List<AckOrderInvariants.AckEvent> trace = new ArrayList<>();
        AckTraceStore store = new AckTraceStore(new MemoryBinStore(), trace::add);

        // ⚠️ THIS IS THE SHAPE M4.50 NAMES: the writer acknowledges sequence 1
        // while sequence 0 is still outstanding. With both events synthesised
        // from one return this was unrepresentable; with the confirmation
        // coming from the store it is just what happens.
        store.putIfAbsent(deltaKey(1), body("one"));
        trace.add(AckOrderInvariants.AckEvent.acked(EPOCH, 1));
        store.putIfAbsent(deltaKey(0), body("zero"));
        trace.add(AckOrderInvariants.AckEvent.acked(EPOCH, 0));

        assertThat(AckOrderInvariants.checkAckOrder(trace))
                .as("acking 1 while 0 was unconfirmed violates I5's ack clause")
                .hasSize(1)
                .allSatisfy(v -> assertThat(v.toString())
                        .contains("I5").contains("lower-numbered write 0"));
    }

    @Test
    void aWriterThatACKSInOrderIsCLEAN() throws Exception {
        List<AckOrderInvariants.AckEvent> trace = new ArrayList<>();
        AckTraceStore store = new AckTraceStore(new MemoryBinStore(), trace::add);

        // ⚠️ THE NEGATIVE CONTROL, and it is doing real work: pipelining is
        // PERMITTED. Both writes are outstanding together and 1 confirms
        // first; what matters is only that 0 is ACKED first. A checker that
        // forbade this would forbid throughput, which ADR-0011 explicitly
        // allows -- "pipelining is permitted; acknowledgement is not".
        store.putIfAbsent(deltaKey(1), body("one"));
        store.putIfAbsent(deltaKey(0), body("zero"));
        trace.add(AckOrderInvariants.AckEvent.acked(EPOCH, 0));
        trace.add(AckOrderInvariants.AckEvent.acked(EPOCH, 1));

        assertThat(AckOrderInvariants.checkAckOrder(trace))
                .as("out-of-order CONFIRMATION with in-order ACK is legal")
                .isEmpty();
    }

    @Test
    void aLOSTWriteIsNotConfirmed() throws Exception {
        List<AckOrderInvariants.AckEvent> trace = new ArrayList<>();
        MemoryBinStore backing = new MemoryBinStore();
        AckTraceStore store = new AckTraceStore(backing, trace::add);

        store.putIfAbsent(deltaKey(0), body("first"));
        // ⚠️ THE SECOND PUT LOSES -- putIfAbsent is write-once, which is what
        // I1 rests on. A losing write confirmed nothing and must not appear,
        // or a duplicated in-flight PUT would forge a confirmation for a write
        // that never landed and silently satisfy an ack that had no business
        // being satisfied.
        store.putIfAbsent(deltaKey(0), body("second"));

        assertThat(trace).as("only the winner confirms").hasSize(1);
    }

    @Test
    void aNonDeltaKeyIsNotPartOfTheChainsAckTrace() throws Exception {
        List<AckOrderInvariants.AckEvent> trace = new ArrayList<>();
        AckTraceStore store = new AckTraceStore(new MemoryBinStore(), trace::add);

        // ⚠️ CHECKPOINTS AND THE POINTER SORT UNDER THE SAME PREFIX (M4.8b1),
        // and neither is a commit. Counting one as a confirmation would move
        // the chain's floor and could mask a genuinely unconfirmed commit.
        store.putIfAbsent(new LogKeys(PREFIX, EPOCH).checkpointKeyFor(7), body("ckpt"));
        store.putIfAbsent(new LogKeys(PREFIX, EPOCH).latestCheckpointKey(), body("ptr"));
        store.putIfAbsent(PREFIX + "/seg/whatever", body("segment"));

        assertThat(trace).as("only .delta keys are chain commits").isEmpty();
    }
}
