// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.sequencer.FastLeaseFenceHolderTest.Pod;
import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * The replaced lease's holder, which a term start judges §3's graceful
 * exemption by (ADR-0081 §3, §5 step 2; M13.27d).
 */
class FastLeaseFenceReplacedHolderTest {

    private static final Duration TTL = Duration.ofSeconds(10);

    @Test
    void aTAKEOVERNamesTheReplacedHolder() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        Pod a = FastLeaseFenceHolderTest.pod(store, "a");
        a.leases().tryAcquire();
        Pod b = FastLeaseFenceHolderTest.pod(store, "b");
        b.leases().tryAcquire();
        b.advanceBoth(TTL);

        assertThat(b.leases().tryAcquire()).isPresent();

        assertThat(b.fence().replacedHolderUid()).contains("uid-a");
    }

    @Test
    void theFIRSTTermNamesNone() throws Exception {
        Pod a = FastLeaseFenceHolderTest.pod(new MemoryBinStore(), "a");

        a.leases().tryAcquire();

        assertThat(a.fence().replacedHolderUid()).isEmpty();
    }

    @Test
    void aLEGACYLeaseNamingNoUidNamesNone() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        SimulatedClock wall = new SimulatedClock(1_000_000L);
        LeaseManager legacy = new LeaseManager(store,
                new LeaseConfig("p", "a", "", TTL, Duration.ofSeconds(3)), wall);
        legacy.tryAcquire();
        Pod b = FastLeaseFenceHolderTest.pod(store, "b");
        b.leases().tryAcquire();
        b.advanceBoth(TTL);

        assertThat(b.leases().tryAcquire()).isPresent();

        assertThat(b.fence().replacedHolderUid()).isEmpty();
    }

    @Test
    void eachTERMNamesTheHolderItReplacedNotAnEarlierOne() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        Pod a = FastLeaseFenceHolderTest.pod(store, "a");
        a.leases().tryAcquire();
        Pod b = FastLeaseFenceHolderTest.pod(store, "b");
        b.leases().tryAcquire();
        b.advanceBoth(TTL);
        b.leases().tryAcquire();
        b.leases().release();
        a.advanceBoth(TTL);

        assertThat(a.leases().tryAcquire()).as("b's released lease, taken back").isPresent();

        assertThat(a.fence().replacedHolderUid()).contains("uid-b");
    }
}
