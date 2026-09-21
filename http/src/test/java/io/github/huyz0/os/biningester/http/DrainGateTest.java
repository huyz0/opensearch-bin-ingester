// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * What the front door admits while the node drains (M8.7, research 08 §7).
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class DrainGateTest {

    @Test
    void readinessFAILSOnceAndNeverComesBACK() {
        DrainGate gate = new DrainGate();
        assertThat(gate.ready()).as("a new node is ready").isTrue();

        gate.failReadiness();
        gate.refuseBulk();
        gate.releasePollers();

        assertThat(gate.ready()).as("⚠️ A DRAINING NODE IS NEVER READY AGAIN").isFalse();
    }

    @Test
    void aWAITINGPollIsWOKENAndANewOneIsREFUSED() throws Exception {
        DrainGate gate = new DrainGate();
        CountDownLatch entered = new CountDownLatch(1);
        AtomicReference<String> outcome = new AtomicReference<>();
        Thread poller = Thread.ofVirtual().start(() -> {
            assertThat(gate.enterPoll()).isTrue();
            entered.countDown();
            try {
                // the 30 s long-poll a consumer is really sent
                new LinkedBlockingQueue<>().poll(30, TimeUnit.SECONDS);
                outcome.set("timed out");
            } catch (InterruptedException woken) {
                outcome.set("woken");
            } finally {
                gate.exitPoll();
            }
        });
        entered.await();
        assertThat(gate.awaitNoPollers(Duration.ZERO)).as("the premise: one poll is open")
                .isEqualTo(1);

        gate.releasePollers();

        assertThat(gate.awaitNoPollers(Duration.ofSeconds(10)))
                .as("⚠️ WOKEN, NOT LEFT TO WAIT OUT ITS 30 s").isZero();
        poller.join();
        assertThat(outcome.get()).isEqualTo("woken");
        assertThat(gate.enterPoll()).as("⚠️ AND NO NEW POLL IS ADMITTED").isFalse();
        assertThat(gate.pollsRefused()).isTrue();
    }

    @Test
    void theGATESInterruptIsCLEAREDWhenThePollExits() throws Exception {
        // ⚠️ A poll that was between its admission and its wait keeps the
        // interrupt as a flag. Left set, the handler thread would fail its
        // next blocking call for a reason nobody gave.
        DrainGate gate = new DrainGate();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch released = new CountDownLatch(1);
        AtomicBoolean interruptedBeforeExit = new AtomicBoolean();
        AtomicBoolean interruptedAfterExit = new AtomicBoolean(true);
        Thread poller = Thread.ofVirtual().start(() -> {
            gate.enterPoll();
            entered.countDown();
            try {
                released.await();
            } catch (InterruptedException unexpected) {
                Thread.currentThread().interrupt();
            }
            interruptedBeforeExit.set(Thread.currentThread().isInterrupted());
            gate.exitPoll();
            interruptedAfterExit.set(Thread.currentThread().isInterrupted());
        });
        entered.await();
        gate.releasePollers();
        released.countDown();
        poller.join();

        assertThat(interruptedBeforeExit.get()).as("the premise: the gate interrupted it")
                .isTrue();
        assertThat(interruptedAfterExit.get()).as("⚠️ CLEARED ON EXIT").isFalse();
    }

    @Test
    void aBULKInsideTheDoorIsWAITEDForAndANewOneIsREFUSED() throws Exception {
        DrainGate gate = new DrainGate();
        assertThat(gate.enterBulk()).isTrue();
        assertThat(gate.enterBulk()).isTrue();

        gate.refuseBulk();

        assertThat(gate.enterBulk()).as("⚠️ NO NEW BULK IS ADMITTED").isFalse();
        assertThat(gate.awaitNoBulk(Duration.ofMillis(50)))
                .as("⚠️ BOUNDED: two are still inside, and the wait says so").isEqualTo(2);

        Thread.ofVirtual().start(() -> {
            gate.exitBulk();
            gate.exitBulk();
        });
        assertThat(gate.awaitNoBulk(Duration.ofSeconds(10)))
                .as("⚠️ AND IT ENDS WHEN THE LAST ONE LEAVES").isZero();
        assertThat(gate.bulkInFlight()).isZero();
    }

    @Test
    void theSWITCHESAreINDEPENDENT() {
        // ⚠️ §7 throws them in order, one step at a time. A gate that shut
        // bulk when it released polls would refuse a producer before the
        // requests inside had been waited for.
        DrainGate gate = new DrainGate();
        gate.failReadiness();
        assertThat(gate.enterPoll()).as("readiness alone refuses no poll").isTrue();
        gate.exitPoll();
        assertThat(gate.enterBulk()).as("readiness alone refuses no bulk").isTrue();
        gate.exitBulk();

        gate.releasePollers();
        assertThat(gate.enterBulk()).as("⚠️ RELEASING POLLS REFUSES NO BULK").isTrue();
        gate.exitBulk();

        DrainGate other = new DrainGate();
        other.refuseBulk();
        assertThat(other.enterPoll()).as("⚠️ REFUSING BULK REFUSES NO POLL").isTrue();
        other.exitPoll();
        assertThat(other.ready()).as("and fails no readiness").isTrue();
    }
}
