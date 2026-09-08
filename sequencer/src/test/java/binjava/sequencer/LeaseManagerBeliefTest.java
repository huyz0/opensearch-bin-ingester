// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import binjava.binstore.backend.MemoryBinStore;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;

/**
 * ⚠️ {@code held()} MUST NOT REPORT A TERM THIS INSTANCE NO LONGER HOLDS
 * (M4.3e). Every other path already clears the belief when it learns it is
 * fenced — {@code adopt} does it on a lost conditional write, {@code renew}
 * does it, {@code refreshVersion} does it — and {@code adopt}'s own comment
 * says leaving a superseded lease in {@code held} "would make {@code held()}
 * report a term this instance does not hold". {@code tryAcquire}'s
 * unexpired-lease early return was the one branch that did exactly that.
 *
 * <p>⚠️ NOT A BLANKET CLEAR, which is why this is more than a one-line fix: an
 * instance may legitimately be looking at its OWN unexpired lease, and clearing
 * there would make a healthy holder forget its term merely for having asked.
 * The condition is the same (epoch, podId) identity {@code refreshVersion}
 * uses, and for the same reason.
 */
class LeaseManagerBeliefTest {

    private static final Duration TTL = Duration.ofSeconds(10);
    private static final Duration RENEW = Duration.ofSeconds(3);

    /** ⚠️ ADVANCED, never slept on. */
    private static final class TestClock extends Clock {
        private long millis = 1_000_000L;

        @Override public long millis() {
            return millis;
        }

        void advance(Duration d) {
            millis += d.toMillis();
        }

        @Override public Instant instant() {
            return Instant.ofEpochMilli(millis);
        }

        @Override public ZoneOffset getZone() {
            return ZoneOffset.UTC;
        }

        @Override public Clock withZone(ZoneId z) {
            return this;
        }
    }

    private static LeaseManager manager(MemoryBinStore store, String podId, TestClock clock) {
        return new LeaseManager(store, new LeaseConfig("bins/cluster-a", podId, "",
                TTL, RENEW), clock);
    }

    @Test
    void aSupersededHolderThatAsksAgainStopsBelievingItHoldsTheTerm() throws Exception {
        // ⚠️ The realistic path to this branch: a node that lost the lease
        // while partitioned comes back and calls `tryAcquire` -- NOT `renew`,
        // because it is trying to become leader again -- and finds the
        // successor's lease alive. Before M4.3e it was told "no" while
        // `held()` went on reporting the term it had lost, so anything gating
        // on `held()` kept sequencing under an epoch someone else had
        // superseded.
        MemoryBinStore store = new MemoryBinStore();
        TestClock clock = new TestClock();
        LeaseManager a = manager(store, "podA", clock);
        assertThat(a.tryAcquire()).isPresent();

        // ⚠️ The successor REUSES podA's name, and this is the second time
        // that mattered -- M4.3d's control had the same hole one branch over.
        // A StatefulSet gives stable pod names, so the realistic successor to
        // a dead podA is a restarted podA. With a `podB` successor BOTH halves
        // of the identity differ and either one alone still discriminates, so
        // the test passes even with the epoch conjunct deleted -- while a
        // restarted podA would then match on holder, keep its belief, and go
        // on reporting a superseded term. That is the exact defect this task
        // removes, surviving the test written to remove it.
        // ⚠️ So this pins the EPOCH half. The podId half is defence-in-depth
        // and NOT pinned, for the reason `isOwnTerm` gives: no in-protocol
        // path mints a same-epoch lease under a different holder.
        clock.advance(TTL);
        LeaseManager b = manager(store, "podA", clock);
        assertThat(b.tryAcquire()).isPresent();
        assertThat(b.held().orElseThrow().epoch())
                .as("a new term under the same name -- only the epoch tells them apart")
                .isEqualTo(2);

        assertThat(a.tryAcquire()).as("the successor holds it, and it has not expired").isEmpty();
        assertThat(a.held())
                .as("so the old process must stop claiming the term it lost")
                .isEmpty();
    }

    @Test
    void askingAgainDoesNotDiscardABeliefAnAmbiguousWriteWouldHaveVindicated() throws Exception {
        // ⚠️ M4.3d left `ambiguous` set when a renew's outcome is unknown, so
        // the NEXT call re-reads before trusting its version. This branch runs
        // in between, and a plausible hardening -- "if our version is in
        // doubt, drop the belief" -- would be strictly worse than the harm
        // M4.3d fixed: `renew` short-circuits on `held == null`, so
        // `refreshVersion` could never run and the recovery would be
        // unreachable. The node would stand down for a full TTL and recover
        // only by burning an epoch.
        // ⚠️ Asserting `held()` alone is not enough: it passes if the belief
        // survives but the FLAG does not. The following `renew()` is what
        // proves the flag survived, because only a refresh can supply the
        // version that write needs.
        MemoryBinStore backing = new MemoryBinStore();
        AmbiguousPutStore store =
                new AmbiguousPutStore(backing, AmbiguousPutStore.Mode.LANDED);
        TestClock clock = new TestClock();
        LeaseManager m = new LeaseManager(store, new LeaseConfig("bins/cluster-a", "podA", "",
                TTL, RENEW), clock);
        assertThat(m.tryAcquire()).isPresent();

        clock.advance(RENEW);
        assertThatThrownBy(m::renew).isInstanceOf(java.io.IOException.class);

        assertThat(m.tryAcquire()).as("nothing to acquire -- it is its own term").isEmpty();
        assertThat(m.held()).as("and asking did not discard the belief").isPresent();
        clock.advance(RENEW);
        assertThat(m.renew())
                .as("nor the ambiguity flag -- this write needs the refresh's version")
                .isPresent();
    }

    @Test
    void aHolderLookingAtItsOwnUnexpiredLeaseKeepsIt() throws Exception {
        // ⚠️ THE CONTROL. A blanket clear on this branch would pass the test
        // above and make a healthy leader forget its own term for the sole
        // offence of having called `tryAcquire` twice.
        MemoryBinStore store = new MemoryBinStore();
        TestClock clock = new TestClock();
        LeaseManager a = manager(store, "podA", clock);
        assertThat(a.tryAcquire()).isPresent();

        assertThat(a.tryAcquire())
                .as("nothing to acquire -- it already holds it")
                .isEmpty();
        assertThat(a.held())
                .as("but it still holds it, and asking did not change that")
                .isPresent();
        assertThat(a.renew()).as("and it can still renew").isPresent();
    }

    @Test
    void anInstanceHoldingNothingIsUnchangedByAnUnexpiredLease() throws Exception {
        // ⚠️ The clear must not assume there is a belief to compare against:
        // a fresh instance losing a race reaches this branch with `held` null.
        MemoryBinStore store = new MemoryBinStore();
        TestClock clock = new TestClock();
        assertThat(manager(store, "podA", clock).tryAcquire()).isPresent();

        LeaseManager late = manager(store, "podB", clock);
        assertThat(late.tryAcquire()).isEmpty();
        assertThat(late.held()).isEmpty();
    }
}
