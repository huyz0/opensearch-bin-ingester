// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import binjava.sequencer.Invariants.Violation;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * I5's ACK-ORDERING clause, which leaves no trace in the stored bytes (M4.11).
 *
 * <p>architecture.md states I5 as two clauses. The first — nothing acknowledged
 * beyond a {@code SEAL} in its own chain — is structural and {@link Invariants}
 * already checks it. This is the second, from ADR-0011: <b>a commit may be
 * acknowledged only after every lower-numbered write in the chain has been
 * confirmed</b>.
 *
 * <p>⚠️ THE ORDER A WRITER ACKNOWLEDGED IN IS NOT IN THE STORE. Two runs that
 * produce byte-identical chains can differ in whether one of them acked a
 * commit before a lower-numbered write was confirmed, so no predicate over the
 * objects can see this. It needs the TRACE, which is why the checker takes one.
 *
 * <p>⚠️ AND THE TEST MUST FAIL AGAINST A NAIVE PIPELINING IMPLEMENTATION.
 * `CommitLog` is strictly serial today, so it satisfies I5 BY ACCIDENT: a test
 * driven through it would pass while constraining nothing, which is the trap
 * this row's own description names. So the naive implementation is written
 * here, and the checker is measured against it.
 *
 * <p>⚠️ PIPELINING IS PERMITTED — ADR-0011 says so in as many words, and a
 * checker that merely forbade concurrent writes would contradict the decision
 * it exists to enforce. {@link #pipeliningThatWAITSToAckIsPERMITTED} is what
 * separates "acked out of order" from "wrote out of order", and without it the
 * whole rule collapses into "be serial".
 */
class AckOrderTest {

    /** A writer that confirms and acks strictly in sequence order. */
    private static List<AckOrderInvariants.AckEvent> serial(int n) {
        List<AckOrderInvariants.AckEvent> trace = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            trace.add(AckOrderInvariants.AckEvent.confirmed(1, i));
            trace.add(AckOrderInvariants.AckEvent.acked(1, i));
        }
        return trace;
    }

    /**
     * A writer that pipelines and acks each write as its own PUT returns —
     * the naive implementation ADR-0011 forbids. Write 1 confirms first, so it
     * is acked while write 0 is still in flight.
     */
    private static List<AckOrderInvariants.AckEvent> naivePipelined() {
        return List.of(
                AckOrderInvariants.AckEvent.confirmed(1, 1),
                AckOrderInvariants.AckEvent.acked(1, 1),
                AckOrderInvariants.AckEvent.confirmed(1, 0),
                AckOrderInvariants.AckEvent.acked(1, 0));
    }

    /**
     * A writer that pipelines the WRITES but holds each ack until every lower
     * write is confirmed. Both PUTs are in flight together; neither is acked
     * early.
     */
    private static List<AckOrderInvariants.AckEvent> pipelinedButOrderedAcks() {
        return List.of(
                AckOrderInvariants.AckEvent.confirmed(1, 1),
                AckOrderInvariants.AckEvent.confirmed(1, 0),
                AckOrderInvariants.AckEvent.acked(1, 0),
                AckOrderInvariants.AckEvent.acked(1, 1));
    }

    @Test
    void aSerialWriterViolatesNOTHING() {
        assertThat(AckOrderInvariants.checkAckOrder(serial(4))).isEmpty();
    }

    /** ⚠️ THE CASE THE ROW EXISTS FOR: the checker must CATCH this. */
    @Test
    void aNAIVEPipeliningWriterVIOLATESI5() {
        List<Violation> found = AckOrderInvariants.checkAckOrder(naivePipelined());

        assertThat(found).as("acking 1 while 0 is unconfirmed is the ADR-0011 violation")
                .hasSize(1);
        assertThat(found.get(0).invariant()).isEqualTo("I5");
        // ⚠️ DISTINCT LABELS, not `contains("1").contains("0")`, which review
        // measured satisfied by the two sequences in EITHER role -- swapping
        // them in the message survived.
        assertThat(found.get(0).detail())
                .contains("acked sequence 1").contains("lower-numbered write 0");
    }

    /**
     * ⚠️ PIPELINING IS PERMITTED, AND THIS IS THE ASSERTION THAT SAYS SO. Both
     * writes are outstanding at once and the checker reports nothing, because
     * what I5 forbids is the ACK order, not the WRITE order. A checker without
     * this case is satisfied by "never pipeline", which is not the rule.
     */
    @Test
    void pipeliningThatWAITSToAckIsPERMITTED() {
        assertThat(AckOrderInvariants.checkAckOrder(pipelinedButOrderedAcks())).isEmpty();
    }

    /**
     * ⚠️ AN ACK OF AN UNCONFIRMED WRITE IS A VIOLATION EVEN WITH NO LOWER
     * SEQUENCE OUTSTANDING. Acking what was never confirmed at all is the same
     * harm — a producer told its records are durable when nothing landed.
     */
    @Test
    void ackingAWriteThatWasNEVERConfirmedIsAVIOLATION() {
        List<Violation> found = AckOrderInvariants.checkAckOrder(List.of(AckOrderInvariants.AckEvent.acked(1, 0)));

        assertThat(found).hasSize(1);
        assertThat(found.get(0).invariant()).isEqualTo("I5");
    }

    /** An empty trace is not a pass with evidence; it is nothing to judge. */
    @Test
    void anEmptyTraceReportsNoVIOLATIONAndNoEVIDENCE() {
        assertThat(AckOrderInvariants.checkAckOrder(List.of())).isEmpty();
    }

    /**
     * ⚠️ EVERY OUT-OF-ORDER ACK IS REPORTED, not just the first. A checker that
     * stops at one hides how far a run drifted, and M4.13 pins exact counts.
     */
    @Test
    void EVERYOutOfOrderAckIsReportedNotJustTheFirst() {
        List<Violation> found = AckOrderInvariants.checkAckOrder(List.of(
                AckOrderInvariants.AckEvent.confirmed(1, 2),
                AckOrderInvariants.AckEvent.acked(1, 2),
                AckOrderInvariants.AckEvent.confirmed(1, 1),
                AckOrderInvariants.AckEvent.acked(1, 1),
                AckOrderInvariants.AckEvent.confirmed(1, 0),
                AckOrderInvariants.AckEvent.acked(1, 0)));

        assertThat(found).as("acking 2 then 1 early are two distinct violations").hasSize(2);
    }

    /**
     * ⚠️ DEPTH THREE, WHICH IS ADR-0011's OWN SCENARIO: "a leader that issues
     * N+1, N+2, N+3 concurrently could have N+2 succeed while N+1 loses to a
     * seal". Every other trace here is depth two, where the culprit is always
     * the ADJACENT sequence -- and review measured that a checker scanning only
     * {@code seq - 1} passes all of them. This is the case that separates
     * "scan every lower write" from "check your predecessor".
     */
    @Test
    void anAckOverAGapOfTWOIsCaught() {
        List<Violation> found = AckOrderInvariants.checkAckOrder(List.of(
                AckOrderInvariants.AckEvent.confirmed(1, 1),
                AckOrderInvariants.AckEvent.confirmed(1, 2),
                AckOrderInvariants.AckEvent.acked(1, 2),
                AckOrderInvariants.AckEvent.confirmed(1, 0)));

        assertThat(found).as("0 was unconfirmed when 2 was acked, with 1 confirmed between them")
                .hasSize(1);
        assertThat(found.get(0).detail())
                .as("and it must name WHICH is the ack and WHICH the unconfirmed lower")
                .contains("acked sequence 2")
                .contains("lower-numbered write 0");
    }

    /**
     * ⚠️ A CHAIN'S FIRST COMMIT IS NOT AT SEQUENCE 0, and asserting the clean
     * case only at base 0 hid that. {@code CommitLog.apply} advances
     * `nextSequence` past the {@code CONTINUE} at slot 0, so in a real epoch
     * the first COMMIT sits at 1 -- which review measured the base-0 scan
     * reporting as two violations of a strictly serial writer.
     */
    @Test
    void aSerialWriterWhoseChainSTARTSAtOneViolatesNOTHING() {
        assertThat(AckOrderInvariants.checkAckOrder(List.of(
                AckOrderInvariants.AckEvent.confirmed(1, 1),
                AckOrderInvariants.AckEvent.acked(1, 1),
                AckOrderInvariants.AckEvent.confirmed(1, 2),
                AckOrderInvariants.AckEvent.acked(1, 2))))
                .as("the base is the chain's own floor, not the literal 0")
                .isEmpty();
    }

    /**
     * ⚠️ SEQUENCE NUMBERS RESTART PER EPOCH -- `ChainReplay` says so: "OFFSETS
     * CROSS, SEQUENCE NUMBERS DO NOT". One `confirmed` set spanning the trace
     * let chain A's writes satisfy chain B's lookups, and review measured the
     * consequence: chain B acking its own sequence 2 having confirmed NOTHING
     * of chain B reported zero violations. Silent, and a failover simulation is
     * the named consumer.
     */
    @Test
    void ONECHAINSConfirmationsDoNotSatisfyANOTHERS() {
        List<Violation> found = AckOrderInvariants.checkAckOrder(List.of(
                AckOrderInvariants.AckEvent.confirmed(1, 0),
                AckOrderInvariants.AckEvent.acked(1, 0),
                AckOrderInvariants.AckEvent.confirmed(1, 1),
                AckOrderInvariants.AckEvent.acked(1, 1),
                AckOrderInvariants.AckEvent.confirmed(1, 2),
                AckOrderInvariants.AckEvent.acked(1, 2),
                // epoch 2 acks its own sequence 2 having confirmed nothing.
                AckOrderInvariants.AckEvent.acked(2, 2)));

        assertThat(found).as("epoch 2's ack is unbacked by epoch 2's own confirmations")
                .hasSize(1);
        assertThat(found.get(0).detail()).contains("epoch 2");
    }

    /**
     * ⚠️ ONE ACK IS AT MOST ONE VIOLATION. An ack that is BOTH unconfirmed and
     * preceded by unconfirmed lowers must not be counted twice -- M4.13 pins
     * exact counts, and double-reporting inflates them.
     */
    @Test
    void anAckThatIsBothUnconfirmedAndOutOfOrderCountsONCE() {
        List<Violation> found = AckOrderInvariants.checkAckOrder(List.of(
                AckOrderInvariants.AckEvent.confirmed(1, 0),
                AckOrderInvariants.AckEvent.acked(1, 2)));

        assertThat(found).hasSize(1);
    }

    /**
     * ⚠️ DEPTH THREE ON THE **PERMITTED** SIDE, which is where the rule can
     * still collapse into "be serial". Review measured the watermark's
     * {@code while} weakened to an {@code if} passing every other test here:
     * one confirm closing a TWO-WIDE gap happens only at depth 3, and depth 3
     * had been added for the violation side alone. This is ADR-0011's own
     * scenario -- N+1, N+2, N+3 outstanding together -- acked in order.
     */
    @Test
    void legalPipeliningAtDepthTHREEIsPERMITTED() {
        assertThat(AckOrderInvariants.checkAckOrder(List.of(
                AckOrderInvariants.AckEvent.confirmed(1, 1),
                AckOrderInvariants.AckEvent.confirmed(1, 2),
                // ⚠️ THIS ONE CONFIRM CLOSES A TWO-WIDE GAP: the watermark must
                // advance 0 -> 3 in one step, which an `if` cannot do.
                AckOrderInvariants.AckEvent.confirmed(1, 0),
                AckOrderInvariants.AckEvent.acked(1, 0),
                AckOrderInvariants.AckEvent.acked(1, 1),
                AckOrderInvariants.AckEvent.acked(1, 2))))
                .as("all three outstanding at once, acked in order: legal")
                .isEmpty();
    }

    /**
     * ⚠️ THE WATERMARK IS PER CHAIN TOO, and the sibling test does not show it:
     * its epoch-2 event is an ack of a sequence epoch 2 never confirmed, so it
     * stops at the own-write arm and never reaches the ordering comparison.
     * Review measured the watermark keyed on a constant passing all ten tests
     * while a SUCCESSOR acking out of order went unreported -- the silent false
     * negative the sibling's javadoc claims to have closed.
     */
    @Test
    void aSUCCESSORChainAckingOutOfOrderIsCaught() {
        List<Violation> found = AckOrderInvariants.checkAckOrder(List.of(
                AckOrderInvariants.AckEvent.confirmed(1, 0),
                AckOrderInvariants.AckEvent.acked(1, 0),
                AckOrderInvariants.AckEvent.confirmed(1, 1),
                AckOrderInvariants.AckEvent.acked(1, 1),
                AckOrderInvariants.AckEvent.confirmed(1, 2),
                AckOrderInvariants.AckEvent.acked(1, 2),
                // epoch 2 confirms 0 and 2, then acks 2 with its OWN 1 in flight
                AckOrderInvariants.AckEvent.confirmed(2, 0),
                AckOrderInvariants.AckEvent.confirmed(2, 2),
                AckOrderInvariants.AckEvent.acked(2, 2)));

        assertThat(found).as("epoch 1 ran ahead; its watermark must not cover epoch 2")
                .hasSize(1);
        assertThat(found.get(0).detail()).contains("epoch 2").contains("lower-numbered write 1");
    }

    /**
     * ⚠️ THE FLOOR IS PER CHAIN, not one global minimum. Review measured a
     * flattened base passing all ten while reporting two false violations of a
     * serial chain based above another chain's floor -- round 1's literal-0
     * false positive, one level up.
     */
    @Test
    void TWOChainsAtDIFFERENTFloorsAreJudgedSeparately() {
        assertThat(AckOrderInvariants.checkAckOrder(List.of(
                AckOrderInvariants.AckEvent.confirmed(1, 0),
                AckOrderInvariants.AckEvent.acked(1, 0),
                AckOrderInvariants.AckEvent.confirmed(2, 5),
                AckOrderInvariants.AckEvent.acked(2, 5),
                AckOrderInvariants.AckEvent.confirmed(2, 6),
                AckOrderInvariants.AckEvent.acked(2, 6))))
                .as("epoch 2's floor is 5, and epoch 1's 0 must not be imposed on it")
                .isEmpty();
    }

    /**
     * ⚠️ THE MESSAGE NAMES THE LOWEST UNCONFIRMED WRITE, not the chain's floor.
     * Review measured {@code + w} replaced by the floor passing all ten: on a
     * trace whose floor IS confirmed, the two differ, and a report naming a
     * write that was confirmed sends a reader to the wrong slot.
     */
    @Test
    void theMessageNamesTheLOWESTUnconfirmedWriteNotTheFloor() {
        List<Violation> found = AckOrderInvariants.checkAckOrder(List.of(
                AckOrderInvariants.AckEvent.confirmed(1, 0),
                AckOrderInvariants.AckEvent.confirmed(1, 2),
                AckOrderInvariants.AckEvent.acked(1, 2)));

        assertThat(found).hasSize(1);
        assertThat(found.get(0).detail())
                .as("0 IS confirmed; 1 is the one still missing")
                .contains("lower-numbered write 1")
                .doesNotContain("lower-numbered write 0");
    }
}
