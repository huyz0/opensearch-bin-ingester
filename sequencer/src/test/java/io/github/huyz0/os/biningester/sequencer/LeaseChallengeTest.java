// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.Lease;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * An unexpired lease is taken on evidence that its holder is gone, and on
 * nothing less (M8.13, NFR-9).
 *
 * <p>⚠️ **THE CLOCK IS FIXED**, so the lease under challenge is unexpired by
 * construction. A wall clock would let the TTL lapse between two statements,
 * and an acquisition would then pass for a reason that is not the challenge.
 */
class LeaseChallengeTest {

    private static final Clock FIXED = Clock.fixed(Instant.parse("2026-09-19T12:00:00Z"),
            ZoneOffset.UTC);

    private static LeaseManager manager(MemoryBinStore store, String pod,
            LeaseChallenge challenge) {
        return new LeaseManager(store, new LeaseConfig("p", pod, "http://" + pod + ":8080",
                TestSequencers.TTL, java.time.Duration.ofSeconds(3)), FIXED, challenge);
    }

    @Test
    void anUNEXPIREDLeaseIsTAKENWhenItsHolderIsGONEUnderAGreaterEpoch() throws Exception {
        try (MemoryBinStore store = new MemoryBinStore()) {
            Lease first = manager(store, "podA", LeaseChallenge.NEVER).tryAcquire()
                    .orElseThrow();

            Optional<Lease> taken = manager(store, "podB",
                    lease -> lease.holderPodId().equals("podA")).tryAcquire();

            assertThat(first.isExpiredAt(FIXED.millis()))
                    .as("the premise: the challenged lease is unexpired").isFalse();
            assertThat(taken).as("⚠️ TAKEN ON EVIDENCE, BEFORE THE TTL").isPresent();
            assertThat(taken.get().holderPodId()).isEqualTo("podB");
            assertThat(taken.get().epoch()).as("⚠️ UNDER A STRICTLY GREATER EPOCH")
                    .isGreaterThan(first.epoch());
        }
    }

    @Test
    void withoutEVIDENCEAnUnexpiredLeaseIsLEFTAlone() throws Exception {
        try (MemoryBinStore store = new MemoryBinStore()) {
            manager(store, "podA", LeaseChallenge.NEVER).tryAcquire().orElseThrow();

            assertThat(manager(store, "podB", LeaseChallenge.NEVER).tryAcquire())
                    .as("no evidence: the TTL alone bounds failover").isEmpty();
            assertThat(manager(store, "podB", lease -> false).tryAcquire()).isEmpty();
        }
    }

    @Test
    void aPodNEVERChallengesITSOWNTerm() throws Exception {
        // ⚠️ A draining leader's endpoint leaves the slice while it is still
        // flushing. If it challenged itself, its own restarted election would
        // burn an epoch and seal its own chain under a live flush.
        try (MemoryBinStore store = new MemoryBinStore()) {
            Lease held = manager(store, "podA", LeaseChallenge.NEVER).tryAcquire()
                    .orElseThrow();

            Optional<Lease> again = manager(store, "podA", lease -> true).tryAcquire();

            assertThat(again).as("⚠️ NOT TAKEN FROM ITSELF, however strong the evidence")
                    .isEmpty();
            assertThat(new LeaseManager(store, new LeaseConfig("p", "podC", "",
                    TestSequencers.TTL, java.time.Duration.ofSeconds(3)), FIXED,
                    LeaseChallenge.NEVER).tryAcquire())
                    .as("and the term is still podA's, epoch " + held.epoch()).isEmpty();
        }
    }

    @Test
    void theCHALLENGEDHolderLEARNSItOnItsNextRenew() throws Exception {
        // ⚠️ BEING WRONG COSTS A FAILOVER, NEVER A WRITE: a holder that was
        // alive after all is told at its next renewal, and stops sequencing.
        try (MemoryBinStore store = new MemoryBinStore()) {
            LeaseManager holder = manager(store, "podA", LeaseChallenge.NEVER);
            holder.tryAcquire().orElseThrow();
            manager(store, "podB", lease -> true).tryAcquire().orElseThrow();

            assertThat(holder.renew()).as("⚠️ FENCED: the renew comes back empty").isEmpty();
        }
    }
}
