// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import static binjava.sequencer.FaultFixtures.body;
import static binjava.sequencer.FaultFixtures.read;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import binjava.binstore.Body;
import binjava.binstore.backend.MemoryBinStore;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * What the injector does when it is NOT injecting, plus accessor hygiene.
 *
 * <p>⚠️ SPLIT OUT AT 524 LINES against the 500-line limit -- code-structure.md
 * rule 1: split it, never raise the limit. The seam is the one review kept
 * finding: the CLEAN paths are a separate concern from the faulting arms, and
 * they were unconstrained four separate times while the faulting arms were
 * pinned. Round 7 found the ambiguous arms used a free key and a matching
 * version; round 8 found the clean conditional writes had neither payload nor
 * version checked; round 9 found the clean READS -- the three verbs `recover`
 * walks a chain with -- could each return nothing at all.
 *
 * <p>⚠️ WHY A BROKEN CLEAN PATH IS WORSE THAN A BROKEN FAULT: it corrupts the
 * control run. A clean `list` returning an empty page makes the sweep report
 * truncated chains and offsets resuming at 0 for EVERY seed, which is
 * indistinguishable from M4.16's unsealed-ancestor I2 violation -- so the rounds
 * that follow are spent diagnosing the protocol for a defect the harness wrote.
 */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class FaultInjectingStoreDelegationTest {

    @Test
    void theVerbsThatInjectNOTHINGStillDELEGATEFaithfully() throws Exception {
        // ⚠️ THE ROW SAYS `put`, `getRange`, `multipart` and `delete` INJECT
        // NOTHING, which tells a reader they are pass-throughs -- and round-5 and
        // round-6 review both measured that nothing checked they still PASS
        // THROUGH. Surviving mutations included swapping getRange's bounds,
        // making delete a no-op, and making close a no-op. A decorator that is
        // wrong on the verbs it does not fault is a decorator that quietly
        // changes the baseline every faulted run is compared against.
        MemoryBinStore backing = new MemoryBinStore();
        FaultInjectingStore store = new FaultInjectingStore(backing, 1L,
                FaultInjectingStore.Faults.none());

        store.put("p", body("hello"));
        assertThat(backing.stat("p")).as("put reaches the delegate").isPresent();
        try (var in = store.getRange("p", 1, 3)) {
            assertThat(new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8))
                    .as("getRange passes its bounds through in the right ORDER -- swapped, this "
                            + "reads a different window or throws")
                    .isEqualTo("ell");
        }
        store.delete(java.util.List.of("p"));
        assertThat(backing.stat("p")).as("delete reaches the delegate").isEmpty();
    }

    @Test
    void theCLEANPathWritesTheCALLERSExactBytesAndHONOURSTheVersion() throws Exception {
        // ⚠️ THE NON-FAULTING PATH IS THE BASELINE EVERY FAULTED RUN IS COMPARED
        // AGAINST, and round-8 review measured it unconstrained in two ways: the
        // clean `putIfAbsent` could write a same-length zeroed array, and the
        // clean `putIfMatch` could drop the version for an unconditional `put`.
        // Every other test in this file enables a fault, so nothing exercised the
        // path taken on the overwhelming majority of calls. A decorator that is
        // wrong when it injects NOTHING corrupts the control run, and the sweep
        // would read the difference as a protocol defect.
        MemoryBinStore backing = new MemoryBinStore();
        FaultInjectingStore store = new FaultInjectingStore(backing, 1L,
                FaultInjectingStore.Faults.none());

        store.putIfAbsent("clean", body("exact-payload"));
        assertThat(read(backing, "clean"))
                .as("the clean putIfAbsent lands the caller's bytes, not a placeholder")
                .isEqualTo("exact-payload");

        // ⚠️ A STALE VERSION on the clean path: the CAS must still LOSE.
        var stale = backing.putIfAbsent("cas2", body("v1")).orElseThrow();
        backing.putIfMatch("cas2", body("v2"), stale);
        assertThat(store.putIfMatch("cas2", body("v3"), stale))
                .as("the clean putIfMatch is still CONDITIONAL -- dropping the version here "
                        + "would let a fenced writer's write land through the harness itself")
                .isEmpty();
        assertThat(read(backing, "cas2"))
                .as("and nothing changed").isEqualTo("v2");

        // ⚠️ And it SUCCEEDS on a current version, or the assertion above would be
        // satisfied by a putIfMatch that never writes at all.
        var current = backing.stat("cas2").orElseThrow().version();
        assertThat(store.putIfMatch("cas2", body("v4"), current))
                .as("a matching version still writes").isPresent();
        assertThat(read(backing, "cas2")).isEqualTo("v4");
    }

    @Test
    void theCLEANREADSReturnWhatTheDelegateHoldsAndNotAnEmptyView() throws Exception {
        // ⚠️ THE SWEEP'S RECOVERY PATH IS EXACTLY THESE THREE VERBS, and round-9
        // review measured all three clean returns unconstrained: `stat` could
        // return empty, `get` could return zero bytes, `list` could return an
        // empty page, and all 20 tests stayed green. Every read-back assertion in
        // both files reads the BACKING store rather than the decorator, so the
        // decorator's own reads were never observed at all.
        // ⚠️ WHY IT IS THE WORST PLACE FOR THIS GAP: `LocalSequencer.recover`
        // walks the chain with `list` and `get`. A broken clean read makes the
        // sweep report empty or truncated chains and offsets resuming at 0 for
        // every seed -- indistinguishable from M4.16's unsealed-ancestor I2
        // violation, and the next rounds would be spent diagnosing the protocol
        // for a defect the harness fabricated.
        MemoryBinStore backing = new MemoryBinStore();
        backing.putIfAbsent("r/1", body("one"));
        backing.putIfAbsent("r/2", body("two"));
        FaultInjectingStore store = new FaultInjectingStore(backing, 1L,
                FaultInjectingStore.Faults.none());

        assertThat(store.stat("r/1"))
                .as("stat reports what is there, through the decorator").isPresent();
        try (var in = store.get("r/1")) {
            assertThat(new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8))
                    .as("get returns the delegate's BYTES, not an empty stream")
                    .isEqualTo("one");
        }
        assertThat(store.list("r/", null, 10).objects())
                .as("list returns the delegate's page, not an empty one -- this is the call "
                        + "recovery walks the chain with")
                .hasSize(2);
    }

    @Test
    void bothACCESSORSHandBackACOPYAndNotAViewOfTheirLiveState() throws Exception {
        // ⚠️ `theInjectedRecordIsASNAPSHOT...` pinned only the UNMODIFIABLE half
        // of its name: round-9 review measured `List.copyOf` ->
        // `Collections.unmodifiableList` surviving, because an unmodifiable VIEW
        // also throws on add. And `drawCounts()`'s copy had no test at all.
        // ⚠️ BOTH MATTER FOR THE SAME REASON: they are the two things a sweep
        // reads to prove it faulted anything. A view captured before a phase and
        // compared after is the SAME OBJECT, so the comparison reports no change
        // having compared nothing -- the silent-simulator failure produced by the
        // accessor rather than by a probability.
        FaultInjectingStore store = new FaultInjectingStore(new MemoryBinStore(), 1L,
                new FaultInjectingStore.Faults(1.0, 0, 0, 0));
        assertThatThrownBy(() -> store.stat("a")).isInstanceOf(IOException.class);

        List<FaultInjectingStore.Injected> before = store.injected();
        Map<String, Integer> countsBefore = store.drawCounts();
        int size = before.size();
        int draws = countsBefore.getOrDefault("unreachable", 0);

        assertThatThrownBy(() -> store.stat("b")).isInstanceOf(IOException.class);

        assertThat(before)
                .as("the list handed back earlier is a SNAPSHOT -- it did not grow when a "
                        + "second fault fired, which a live view would have")
                .hasSize(size);
        assertThat(countsBefore.getOrDefault("unreachable", 0))
                .as("and the draw counts handed back earlier did not move either")
                .isEqualTo(draws);
        assertThat(store.injected()).as("while the store's own record DID advance")
                .hasSizeGreaterThan(size);
    }
}
