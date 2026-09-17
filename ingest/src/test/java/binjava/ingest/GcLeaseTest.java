// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import binjava.binstore.Body;
import binjava.binstore.CountingBinStore;
import binjava.binstore.backend.MemoryBinStore;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * GC is a leased role, and a fenced pass deletes nothing (M7.9, FR-9,
 * research 06 §4).
 *
 * <p>⚠️ THE ASSERTION IS THE ABSENCE OF A DELETE, NOT THE PRESENCE OF AN
 * EXCEPTION. A loser that throws on its next store call has already deleted,
 * and the delete is the irreversible half — two roles sweeping at once is two
 * processes deciding what is unread from two different views of the watermark
 * table.
 */
class GcLeaseTest {

    private final MemoryBinStore backing = new MemoryBinStore();
    private final CountingBinStore store = new CountingBinStore(backing);

    /** A lease a test moves by hand. */
    private static final class TestLease implements GcLease {
        private final AtomicBoolean held = new AtomicBoolean();
        private final AtomicBoolean available = new AtomicBoolean(true);
        final AtomicInteger releases = new AtomicInteger();

        @Override
        public boolean acquire() {
            return available.compareAndSet(true, false) && held.compareAndSet(false, true);
        }

        @Override
        public boolean stillHeld() {
            return held.get();
        }

        @Override
        public void release() {
            releases.incrementAndGet();
            if (held.compareAndSet(true, false)) {
                available.set(true);
            }
        }

        void fence() {
            held.set(false);
        }
    }

    @Test
    void aPodThatDoesNotHOLDTheLeaseRunsNOTHING() {
        TestLease taken = new TestLease();
        assertThat(taken.acquire()).isTrue();
        List<String> ran = new ArrayList<>();
        new LeasedGc(new TestLeaseSharing(taken), store).runIfLeader(s -> ran.add("ran"));
        assertThat(ran).isEmpty();
        assertThat(store.counts().total())
                .as("⚠️ AND IT COSTS NOTHING AT ALL. Five pods out of six hold no lease "
                        + "every interval forever, so a follower that listed to find out, "
                        + "or read a checkpoint before checking, would make the fleet pay "
                        + "per pod per interval -- NFR-2 through the GC door")
                .isZero();
        assertThat(taken.stillHeld())
                .as("⚠️ AND THE INCUMBENT STILL HOLDS IT. A `release()` in a `finally` "
                        + "with no early return hands back a role another pod is MID-PASS "
                        + "with -- two GC roles at once, which is the one thing the lease "
                        + "exists to prevent, and it is invisible to a request count")
                .isTrue();
        assertThat(taken.releases.get())
                .as("and nothing was released on this pod's behalf either")
                .isZero();
    }

    @Test
    void aPodThatHOLDSTheLeaseRunsThePass() {
        List<String> ran = new ArrayList<>();
        new LeasedGc(new TestLease(), store).runIfLeader(s -> ran.add("ran"));
        assertThat(ran).containsExactly("ran");
    }

    @Test
    void aPassThatLOSESTheLeaseIssuesNOFurtherDELETE() throws Exception {
        for (int i = 0; i < 6; i++) {
            backing.put("bucket/data/obj-" + i, Body.ofBytes(new byte[] {1}));
        }
        TestLease lease = new TestLease();
        LeasedGc gc = new LeasedGc(lease, store);
        gc.runIfLeader(fenced -> {
            try {
                fenced.delete(List.of("bucket/data/obj-0", "bucket/data/obj-1"));
                lease.fence();
                fenced.delete(List.of("bucket/data/obj-2", "bucket/data/obj-3"));
                fenced.delete(List.of("bucket/data/obj-4", "bucket/data/obj-5"));
            } catch (IOException expectedAfterFencing) {
                // the pass stops here, which is what a real GC's catch does
            }
        });
        assertThat(store.counts().deletes())
                .as("⚠️ ONE CALL, NOT THREE. The fence is checked BEFORE each batch "
                        + "reaches the store, so the two batches after the loss never "
                        + "become requests -- a check only at the start of the pass would "
                        + "let a whole pass's worth of deletes through after the lease "
                        + "moved")
                .isEqualTo(1);
        assertThat(backing.stat("bucket/data/obj-2"))
                .as("and the objects are still there, which is the half that matters")
                .isNotEmpty();
        assertThat(backing.stat("bucket/data/obj-0")).isEmpty();
    }

    @Test
    void aFencedDELETEDoesNotTakeTheNodeDown() {
        TestLease lease = new TestLease();
        LeasedGc gc = new LeasedGc(lease, store);
        assertThatCode(() -> gc.runIfLeader(fenced -> {
            lease.fence();
            try {
                fenced.delete(List.of("bucket/data/obj-0"));
            } catch (IOException expected) {
                // a real pass logs and moves on
            }
        }))
                .as("losing a lease is the ordinary outcome of a pause or a slow renew, "
                        + "not an incident -- and a throw out of the scheduled pass would "
                        + "stop GC on this pod permanently")
                .doesNotThrowAnyException();
    }

    @Test
    void theLEASEIsRELEASEDAfterThePass() {
        TestLease lease = new TestLease();
        new LeasedGc(lease, store).runIfLeader(s -> { });
        assertThat(lease.releases.get())
                .as("a lease held past the pass makes the next pod wait a whole TTL for "
                        + "work that finished in milliseconds")
                .isEqualTo(1);
    }

    @Test
    void theLEASEIsRELEASEDEvenWhenThePassTHROWS() {
        TestLease lease = new TestLease();
        LeasedGc gc = new LeasedGc(lease, store);
        assertThatCode(() -> gc.runIfLeader(s -> {
            throw new IllegalStateException("the pass blew up");
        }))
                .as("a pass that throws must not take the schedule down with it")
                .doesNotThrowAnyException();
        assertThat(lease.releases.get())
                .as("⚠️ AND THE LEASE MUST NOT BE STRANDED. A pass that died holding it "
                        + "pins GC to a dead pod for a whole TTL, which is exactly when "
                        + "storage is growing")
                .isEqualTo(1);
    }

    @Test
    void theFENCEDStoreOnlyGuardsDELETE() throws Exception {
        backing.put("bucket/data/obj-0", Body.ofBytes(new byte[] {1, 2, 3}));
        TestLease lease = new TestLease();
        new LeasedGc(lease, store).runIfLeader(fenced -> {
            lease.fence();
            try {
                assertThat(fenced.stat("bucket/data/obj-0")).isNotEmpty();
            } catch (IOException impossible) {
                throw new AssertionError(impossible);
            }
        });
        assertThat(store.counts().stats())
                .as("reading after a lease is lost is harmless and stopping it would turn "
                        + "an ordinary loss into a cascade of failures -- only the "
                        + "irreversible verb is fenced")
                .isEqualTo(1);
    }

    /**
     * A lease another pod already holds.
     *
     * <p>⚠️ ITS {@code release} DELEGATES rather than doing nothing, and that
     * is what makes the follower case an assertion: a `release()` in a `finally`
     * with no early return hands back a role the INCUMBENT is mid-pass with,
     * and an empty body here would leave that invisible.
     */
    private record TestLeaseSharing(TestLease taken) implements GcLease {
        @Override
        public boolean acquire() {
            return taken.acquire();
        }

        @Override
        public boolean stillHeld() {
            return taken.stillHeld();
        }

        @Override
        public void release() {
            taken.release();
        }
    }
}
