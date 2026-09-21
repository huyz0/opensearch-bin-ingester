// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.client;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * The reconnect backoff (M8.21, research 08 §7 step 2).
 *
 * <p>⚠️ **AT T0, BECAUSE THE SOCKET CASES CANNOT SEE THIS.** The channel's
 * twenty cases over a real socket stayed green with the sleep, the jitter, the
 * doubling and the ceiling all removed — review measured it. A consumer whose
 * ingester is down then hot-loops against it, once per subscription, which is
 * the one failure mode a retry is supposed to prevent.
 */
class SubscriptionBackoffTest {

    @Test
    void aRETRYWaitsAndTheWaitIsJITTERED() {
        // ⚠️ EVERY CONSUMER ON EVERY NODE RECONNECTS AT THE SAME INSTANT AFTER
        // A DEPLOYMENT. An unjittered retry turns that into a synchronised
        // storm against a node that has just started.
        Set<Long> seen = new HashSet<>();
        for (int i = 0; i < 200; i++) {
            long millis = HttpSubscriptionTransport.jitteredMillis(Duration.ofMillis(400));
            assertThat(millis)
                    .as("⚠️ HALF THE BACKOFF EITHER SIDE OF IT, never zero: a retry that "
                            + "rounds to 0 ms is the hot loop this exists to prevent")
                    .isBetween(200L, 599L);
            seen.add(millis);
        }
        assertThat(seen)
                .as("⚠️ THE SPREAD IS THE POINT RATHER THAN THE DELAY. A fixed wait is a "
                        + "herd that arrives late rather than one that does not arrive")
                .hasSizeGreaterThan(50);
    }

    @Test
    void aSUBMillisecondBackoffStillWAITS() {
        // ⚠️ A RETRY THAT ROUNDS TO ZERO IS THE HOT LOOP. The constructor's
        // guard refuses a zero floor, and a caller may still hand one of a few
        // hundred microseconds -- which is zero milliseconds for every draw,
        // and `Thread.sleep(0)` does not wait at all.
        for (int i = 0; i < 50; i++) {
            assertThat(HttpSubscriptionTransport.jitteredMillis(Duration.ofNanos(1_000)))
                    .as("⚠️ AT LEAST A MILLISECOND, always")
                    .isGreaterThanOrEqualTo(1L);
        }
    }

    @Test
    void theBACKOFFDOUBLESAndSTOPSAtTheCEILING() {
        // ⚠️ UNBOUNDED DOUBLING REACHES HOURS, and a consumer whose ingester
        // came back an hour ago is a delivery gap nobody can see. ⚠️ AND IT
        // MUST ACTUALLY GROW: a retry that stays at the floor against a node
        // that is down is the storm, one subscription at a time.
        Duration ceiling = Duration.ofMillis(500);
        assertThat(HttpSubscriptionTransport.grow(Duration.ofMillis(100), ceiling))
                .isEqualTo(Duration.ofMillis(200));
        assertThat(HttpSubscriptionTransport.grow(Duration.ofMillis(200), ceiling))
                .isEqualTo(Duration.ofMillis(400));
        assertThat(HttpSubscriptionTransport.grow(Duration.ofMillis(400), ceiling))
                .as("⚠️ CLAMPED, not 800 ms")
                .isEqualTo(ceiling);
        assertThat(HttpSubscriptionTransport.grow(ceiling, ceiling))
                .as("⚠️ AND IT STAYS THERE")
                .isEqualTo(ceiling);
    }

    @Test
    void aTRUNCATEDFrameIsREFUSEDRatherThanReturnedShort() throws Exception {
        // ⚠️ A SHORT FRAME IS A TORN ONE, and returning what arrived would hand
        // `SubscriptionEvent.decode` a prefix -- which decodes to whatever the
        // missing bytes would have said. The answer this reads from is always
        // in memory, so short means torn rather than not yet arrived.
        byte[] torn = new byte[] {0, 0, 0, 8, 1, 2, 3};
        assertThat(catching(() -> StreamFraming.readFrame(new java.io.ByteArrayInputStream(torn))))
                .hasMessageContaining("ended after 3 of 8");

        byte[] noLength = new byte[] {0, 0};
        assertThat(catching(() -> StreamFraming.readFrame(
                new java.io.ByteArrayInputStream(noLength))))
                .hasMessageContaining("inside a frame length");
    }

    @Test
    void anANSWERIsBOUNDEDWHILEItIsREADRatherThanAfter() throws Exception {
        // ⚠️ THE MEASUREMENT NEVER RUNS IF THE ALLOCATION HAPPENS FIRST. This
        // pins that the refusal arrives before the whole body has been
        // accumulated: the stream here is longer than the cap and the exception
        // names the cap rather than the length.
        java.io.InputStream endless = new java.io.InputStream() {
            @Override
            public int read() {
                return 7;
            }

            @Override
            public int read(byte[] b, int off, int len) {
                java.util.Arrays.fill(b, off, off + len, (byte) 7);
                return len;
            }
        };
        assertThat(catching(() -> StreamFraming.readBounded(endless, 64 << 10)))
                .as("⚠️ REFUSED ON THE WAY IN. A cap applied after `readAllBytes` never runs "
                        + "at all -- the allocation is what fails, with an Error the poll "
                        + "loop does not catch")
                .hasMessageContaining("exceeded 65536 bytes");

        assertThat(StreamFraming.readBounded(new java.io.ByteArrayInputStream(new byte[100]), 100))
                .as("⚠️ AND EXACTLY THE CAP IS ACCEPTED, or the bound is off by one")
                .hasSize(100);
    }

    private static Throwable catching(ThrowingCall call) {
        try {
            call.run();
        } catch (Throwable thrown) {
            return thrown;
        }
        throw new AssertionError("nothing was thrown");
    }

    @FunctionalInterface
    private interface ThrowingCall {
        void run() throws Exception;
    }
}
