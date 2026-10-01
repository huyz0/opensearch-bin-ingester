// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.sequencer.FastLeaseFenceAmbiguityTest.Outcome;
import io.github.huyz0.os.biningester.sequencer.FastLeaseFenceAmbiguityTest.ScriptedStore;
import io.github.huyz0.os.biningester.sequencer.FastLeaseFenceHolderTest.Pod;
import java.io.IOException;
import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * Writes settled by the OTHER read paths (M13.26 review round 2): a takeover
 * settled by {@code renew}, a pod's takeover of its own expired lease, a
 * settled renewal's wall-clock half, and a term found lost by a renewal's
 * re-read.
 */
class FastLeaseFenceSettleTest {

    private static final Duration TTL = Duration.ofSeconds(10);
    private static final long START = 1_000_000L;

    @Test
    void aTAKEOVERSettledByARenewalOwesTheWait() throws Exception {
        MemoryBinStore backing = new MemoryBinStore();
        Pod a = FastLeaseFenceHolderTest.pod(backing, "a");
        a.leases().tryAcquire();
        ScriptedStore bStore = new ScriptedStore(backing);
        Pod b = FastLeaseFenceHolderTest.pod(bStore, "b");
        b.leases().tryAcquire();
        b.advanceBoth(TTL);
        bStore.then(Outcome.LANDED_ANSWER_LOST, Outcome.LOST);
        assertThatThrownBy(() -> b.leases().tryAcquire()).isInstanceOf(IOException.class);

        assertThatThrownBy(() -> b.leases().renew())
                .as("the renewal's re-read settles the takeover; its own write is lost")
                .isInstanceOf(IOException.class);

        assertThat(b.leases().held()).isPresent();
        assertThat(b.fence().successorMayAssign()).isFalse();
        assertThat(b.fence().notBeforeWallMillis()).isEqualTo(START + 11_000L);
    }

    @Test
    void aTAKEOVEROfItsOwnExpiredLeaseSettledLateOwesTheWait() throws Exception {
        ScriptedStore store = new ScriptedStore(new MemoryBinStore());
        Pod a = FastLeaseFenceHolderTest.pod(store, "a");
        a.leases().tryAcquire();
        a.advanceBoth(Duration.ofSeconds(11));
        store.then(Outcome.LANDED_ANSWER_LOST);
        assertThatThrownBy(() -> a.leases().tryAcquire())
                .as("its own lease expired: taken over, the answer lost")
                .isInstanceOf(IOException.class);

        assertThat(a.leases().tryAcquire()).isPresent();

        assertThat(a.fence().notBeforeWallMillis())
                .as("the term replaced a lease, its own old one, and records it")
                .isEqualTo(START + 11_000L);
        assertThat(a.fence().successorMayAssign()).isFalse();
    }

    @Test
    void aSETTLEDRenewalTakesTheFoundLeasesWallExpiry() throws Exception {
        ScriptedStore store = new ScriptedStore(new MemoryBinStore());
        Pod a = FastLeaseFenceHolderTest.pod(store, "a");
        a.leases().tryAcquire();
        a.wall().advance(Duration.ofSeconds(2));
        store.then(Outcome.LANDED_ANSWER_LOST, Outcome.LOST);
        assertThatThrownBy(() -> a.leases().renew()).isInstanceOf(IOException.class);
        a.wall().advance(Duration.ofSeconds(1));
        assertThatThrownBy(() -> a.leases().renew()).isInstanceOf(IOException.class);

        a.wall().advance(Duration.ofMillis(7_999));
        assertThat(a.fence().mayExpose())
                .as("the landed renewal's expiry is +12 s, its margin ends at +11 s").isTrue();
        a.wall().advance(Duration.ofMillis(1));

        assertThat(a.fence().mayExpose()).as("a stopped monotonic clock; the wall bound holds")
                .isFalse();
    }

    @Test
    void aTERMFoundLostByARenewalsRereadStopsExposing() throws Exception {
        MemoryBinStore backing = new MemoryBinStore();
        ScriptedStore aStore = new ScriptedStore(backing);
        Pod a = FastLeaseFenceHolderTest.pod(aStore, "a");
        a.leases().tryAcquire();
        aStore.then(Outcome.LOST);
        assertThatThrownBy(() -> a.leases().renew()).isInstanceOf(IOException.class);
        Pod b = FastLeaseFenceHolderTest.pod(backing, "b");
        b.wall().advance(Duration.ofSeconds(11));
        b.leases().tryAcquire();

        assertThat(a.leases().renew()).as("the re-read finds b's term").isEmpty();

        assertThat(a.fence().mayExpose()).isFalse();
    }
}
