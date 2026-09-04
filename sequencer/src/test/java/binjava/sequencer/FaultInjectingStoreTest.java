// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import static binjava.sequencer.FaultFixtures.body;
import static binjava.sequencer.FaultFixtures.read;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import binjava.binstore.Body;
import binjava.binstore.CountingBinStore;
import binjava.binstore.backend.MemoryBinStore;
import binjava.format.RunKey;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The injector has to be tested before anything it tests can be believed.
 *
 * <p>⚠️ THE RISK THE SPEC NAMES IS "THE SIMULATION PROVES THE SIMULATOR" — a
 * fault-injecting store that never injects the fault that matters. A seed that
 * silently injects nothing produces a green run that demonstrates the absence of
 * testing, and nothing downstream can tell that from a real pass. So every fault
 * class here is asserted to FIRE and to be OBSERVABLE, not merely configured.
 */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class FaultInjectingStoreTest {

    private static final UUID A = UUID.fromString("00000000-0000-0000-0000-0000000000aa");

    private static Map<RunKey, Integer> counts(int n) {
        Map<RunKey, Integer> m = new LinkedHashMap<>();
        m.put(new RunKey(A, 0), n);
        return m;
    }


    @Test
    void anAmbiguousPutLANDSWhileReportingFailure() throws Exception {
        // ⚠️ THE DEFINING PROPERTY, and the one a store that only fails cleanly
        // cannot express. Every ambiguity defect this module has recorded --
        // M4.3d's self-fencing renew, M4.3g's acquisition, M4.3h's single
        // refresh -- lives in the gap between "it failed" and "it failed AFTER
        // committing".
        MemoryBinStore backing = new MemoryBinStore();
        FaultInjectingStore store = new FaultInjectingStore(backing, 1L,
                new FaultInjectingStore.Faults(0, 1.0, 0));

        assertThatThrownBy(() -> store.putIfAbsent("k", body("v")))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("landed but the response was lost");

        assertThat(backing.stat("k"))
                .as("the caller saw a failure and the object IS THERE")
                .isPresent();
        assertThat(store.injected())
                .as("and the run can prove which fault it exercised")
                .extracting(FaultInjectingStore.Injected::kind)
                .containsExactly("ambiguousPut");
    }

    @Test
    void aDuplicatedPutIsHarmlessBecausePutIfAbsentIsWriteOnce() throws Exception {
        // ⚠️ I1 ("no commit-log sequence number is written twice") rests on the
        // store's write-once primitive, NOT on this repository's code. A retried
        // in-flight PUT must therefore be a no-op, and that is a property of the
        // BACKEND worth asserting explicitly: if it ever stopped holding, I1
        // would break with nothing here being wrong.
        // ⚠️ THE SLOT MUST ALREADY BELONG TO SOMEBODY ELSE. A first draft had
        // the duplicate retry into an EMPTY key, where it writes the same bytes
        // it just wrote -- so an implementation that clobbered with `put` was
        // indistinguishable from one that lost with `putIfAbsent`, and the test
        // passed against both. `tdd-red.sh` refused to mint a record for it,
        // which is the gate doing exactly its job.
        // ⚠️ The realistic shape is the dangerous one: writer A's PUT is still
        // in flight when writer B WINS the slot, and A's retry arrives after.
        MemoryBinStore backing = new MemoryBinStore();
        backing.putIfAbsent("k", body("B-got-here-first"));
        FaultInjectingStore store = new FaultInjectingStore(backing, 7L,
                new FaultInjectingStore.Faults(0, 0, 1.0));

        assertThat(store.putIfAbsent("k", body("A-retrying")))
                .as("A loses the slot, which is an empty Optional and not an error")
                .isEmpty();
        assertThat(store.injected())
                .as("and the duplicate genuinely fired -- otherwise this proves nothing")
                .isNotEmpty();
        try (var in = backing.get("k")) {
            assertThat(new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8))
                    .as("B's bytes survive: a duplicated in-flight PUT cannot overwrite "
                            + "the winner, which is what I1 rests on")
                    .isEqualTo("B-got-here-first");
        }
    }

    @Test
    void aStoreConfiguredWithNoFaultsInjectsNothingAtAll() throws Exception {
        // ⚠️ THE NEGATIVE CONTROL. Without it an injector that fired
        // unconditionally would satisfy every test above, and every "clean run"
        // baseline the simulation compares against would be quietly faulted.
        MemoryBinStore backing = new MemoryBinStore();
        FaultInjectingStore store = new FaultInjectingStore(backing, 42L,
                FaultInjectingStore.Faults.none());
        for (int i = 0; i < 200; i++) {
            store.putIfAbsent("k" + i, body("v"));
        }
        assertThat(store.injected()).as("a clean baseline is genuinely clean").isEmpty();
    }

    @Test
    void anInjectedFaultCHANGESWhatTheCommitLogDoes() throws Exception {
        // ⚠️ CRITERION 1: a fault class must have a seed where it changes the
        // OUTCOME, not merely one where it fires. An injector wired to a store
        // nobody consults would satisfy every other test in this file.
        MemoryBinStore clean = new MemoryBinStore();
        CommitLog healthy = new CommitLog(clean, "bins", 1);
        healthy.commit("seg/0", counts(3));
        assertThat(healthy.nextSequence()).as("the clean run commits").isEqualTo(1);

        MemoryBinStore faulted = new MemoryBinStore();
        CommitLog unlucky = new CommitLog(new FaultInjectingStore(faulted, 1L,
                new FaultInjectingStore.Faults(1.0, 0, 0)), "bins", 1);

        assertThatThrownBy(() -> unlucky.commit("seg/0", counts(3)))
                .as("the same call against a faulted store does something DIFFERENT")
                .isInstanceOf(IOException.class);
        assertThat(faulted.stat(unlucky.keyFor(0)))
                .as("and nothing was written, so the difference is observable in the store")
                .isEmpty();
    }

    @Test
    void anAmbiguousPutIfMatchALSOLandsBecauseTheLEASEIsTheCASThatMatters() throws Exception {
        // ⚠️ `putIfMatch` IS THE LEASE'S ONLY CAS -- takeover, renew and release
        // all go through it -- so it is the primitive behind all three
        // ambiguity defects the sibling test names. Round-1 review measured this
        // arm as deletable: removing the landing so the branch throws WITHOUT
        // writing left every test in this file green, because the only coverage
        // was on `putIfAbsent`. A decorator that fails the lease CAS cleanly can
        // never reproduce a self-fence.
        MemoryBinStore backing = new MemoryBinStore();
        var first = backing.putIfAbsent("k", body("v1"));
        assertThat(first).as("the fixture needs a version to match against").isPresent();

        FaultInjectingStore store = new FaultInjectingStore(backing, 1L,
                new FaultInjectingStore.Faults(0, 1.0, 0));

        assertThatThrownBy(() -> store.putIfMatch("k", body("v2"), first.get()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("landed but the response was lost");

        assertThat(read(backing, "k"))
                .as("the caller saw a failure and the NEW value is what is stored -- "
                        + "which is exactly how a renew loses its version and self-fences")
                .isEqualTo("v2");
        assertThat(store.injected()).extracting(FaultInjectingStore.Injected::kind)
                .as("and the run can prove which fault it exercised")
                .containsExactly("ambiguousPut");
    }

    @Test
    void aProbabilityOUTSIDEZeroToOneIsREFUSEDRatherThanSilentlyDisablingItsClass() {
        // ⚠️ THE GUARD HAD NO TEST AT ALL, and round-3 review measured that
        // deleting its whole body left the suite green -- a guard whose javadoc
        // calls itself "rung 1 of gate-design" and which nothing could falsify.
        // ⚠️ NaN IS THE CASE A NAIVE REWRITE LOSES: `p < 0 || p > 1` accepts it,
        // and `nextDouble() < NaN` is always false, so the class silently never
        // fires while `injected()` still shows the others working.
        assertThatThrownBy(() -> new FaultInjectingStore.Faults(-0.05, 0, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unreachable");
        assertThatThrownBy(() -> new FaultInjectingStore.Faults(0, 1.5, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ambiguousPut");
        assertThatThrownBy(() -> new FaultInjectingStore.Faults(0, 0, Double.NaN))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("duplicatePut");
        assertThatThrownBy(
                () -> new FaultInjectingStore.Faults(Double.POSITIVE_INFINITY, 0, 0))
                .isInstanceOf(IllegalArgumentException.class);
        // ⚠️ And the ends of the range are ACCEPTED, or the guard would be a
        // different bug: 0 disables a class on purpose and 1 is how a test forces
        // one to fire on every call.
        assertThat(new FaultInjectingStore.Faults(0, 0, 1.0).duplicatePut()).isEqualTo(1.0);
    }

    @Test
    void aDUPLICATEDPutREALLYREACHESTheStoreTwiceAndTheFirstResultIsWhatTheCallerGets()
            throws Exception {
        // ⚠️ THE CLASS WAS RECORDED BUT NEVER OBSERVABLY INJECTED, measured by
        // round-3 review: deleting the second PUT and keeping only the
        // `injected.add` left every test green, because an empty Optional is
        // MemoryBinStore's answer whether the retry lost or never happened.
        // Observing the DELEGATE is the only way to tell those apart.
        CountingBinStore counting = new CountingBinStore(new MemoryBinStore());
        FaultInjectingStore store = new FaultInjectingStore(counting, 1L,
                new FaultInjectingStore.Faults(0, 0, 1.0));

        long before = counting.counts().puts();
        var result = store.putIfAbsent("fresh", body("v"));

        assertThat(counting.counts().puts() - before)
                .as("the retry REACHED the store -- two conditional writes, not one")
                .isEqualTo(2);
        // ⚠️ AND THE CALLER GETS THE FIRST RESULT, not the retry's. On a free key
        // the first attempt WINS, so returning the duplicate's empty Optional
        // would have the harness tell a caller it lost a write-once slot it
        // actually owns -- an I1 violation fabricated by the injector itself.
        assertThat(result)
                .as("the first attempt won and the caller is told so")
                .isPresent();
        assertThat(store.injected()).extracting(FaultInjectingStore.Injected::kind)
                .containsExactly("duplicatePut");
        assertThat(store.injected()).extracting(FaultInjectingStore.Injected::key)
                .as("and the record names WHICH key it faulted, not just which class")
                .containsExactly("fresh");
    }

    @Test
    void theREADSideFaultsTooAndEVERYClassRecordsWHICHKeyItHit() throws Exception {
        // ⚠️ TWO HOLES IN ONE TEST, both measured by round-4 review as leaving the
        // whole suite green. (1) `stat`, `get` and `list` could each become pure
        // pass-throughs: every other test drives a WRITE, and the read side is
        // exactly what `recover`'s chain walk uses, so the sweep that is supposed
        // to fault RECOVERY would only ever fault its writes. (2) The
        // `unreachable` record could be DELETED or RELABELLED -- and that record
        // is the file's whole reason for existing, since the class javadoc says
        // the only defence against a silent simulator is being able to assert
        // which fault a run saw.
        FaultInjectingStore store = new FaultInjectingStore(new MemoryBinStore(), 1L,
                new FaultInjectingStore.Faults(1.0, 0, 0));

        assertThatThrownBy(() -> store.stat("s")).isInstanceOf(IOException.class);
        assertThatThrownBy(() -> store.get("g")).isInstanceOf(IOException.class);
        assertThatThrownBy(() -> store.list("l", null, 10)).isInstanceOf(IOException.class);

        assertThat(store.injected()).extracting(FaultInjectingStore.Injected::kind)
                .as("all three read verbs inject, and every one is recorded AS `unreachable` -- "
                        + "a relabelled or missing record is a run that cannot say what it saw")
                .containsExactly("unreachable", "unreachable", "unreachable");
        assertThat(store.injected()).extracting(FaultInjectingStore.Injected::key)
                .as("and each names WHICH object it refused, so a debugging session that trusts "
                        + "this record does not spend itself in the wrong chain")
                .containsExactly("s", "g", "l");
    }

    @Test
    void theInjectedRecordIsASNAPSHOTACallerCannotMutate() throws Exception {
        // ⚠️ `List.copyOf` was unconstrained: returning the live list left the
        // suite green, so a caller could edit the very evidence the sweep reports.
        FaultInjectingStore store = new FaultInjectingStore(new MemoryBinStore(), 1L,
                new FaultInjectingStore.Faults(1.0, 0, 0));
        assertThatThrownBy(() -> store.stat("s")).isInstanceOf(IOException.class);

        List<FaultInjectingStore.Injected> seen = store.injected();
        assertThatThrownBy(() -> seen.add(new FaultInjectingStore.Injected("forged", "x")))
                .as("the run's own record of what it faulted is not editable by its reader")
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void EVERYClassRecordsWHICHKeyItFaultedOnTheWritePathToo() throws Exception {
        // ⚠️ Round-5 review measured the write-path keys as unpinned: blanking the
        // key on `unreachable` in putIfAbsent, or on either `ambiguousPut` arm,
        // survived. The read verbs and duplicatePut were already covered, so this
        // closes the third class and the two write verbs.
        FaultInjectingStore unreachable = new FaultInjectingStore(new MemoryBinStore(), 1L,
                new FaultInjectingStore.Faults(1.0, 0, 0));
        assertThatThrownBy(() -> unreachable.putIfAbsent("wa", body("v")))
                .isInstanceOf(IOException.class);
        assertThat(unreachable.injected()).extracting(FaultInjectingStore.Injected::key)
                .as("the unreachable record names the key it refused").containsExactly("wa");

        FaultInjectingStore ambiguous = new FaultInjectingStore(new MemoryBinStore(), 1L,
                new FaultInjectingStore.Faults(0, 1.0, 0));
        assertThatThrownBy(() -> ambiguous.putIfAbsent("wb", body("v")))
                .isInstanceOf(IOException.class);
        var seeded = new MemoryBinStore();
        var version = seeded.putIfAbsent("wc", body("v")).orElseThrow();
        FaultInjectingStore matching = new FaultInjectingStore(seeded, 1L,
                new FaultInjectingStore.Faults(0, 1.0, 0));
        assertThatThrownBy(() -> matching.putIfMatch("wc", body("v2"), version))
                .isInstanceOf(IOException.class);
        assertThat(ambiguous.injected()).extracting(FaultInjectingStore.Injected::key)
                .as("and so does an ambiguous putIfAbsent").containsExactly("wb");
        assertThat(matching.injected()).extracting(FaultInjectingStore.Injected::key)
                .as("and an ambiguous putIfMatch -- the lease CAS, where knowing WHICH object "
                        + "was lost is the difference between debugging the right chain and the "
                        + "wrong one").containsExactly("wc");
    }


    @Test
    void anAmbiguousPutIfAbsentSTILLLOSESToAnOccupiedSlot()
            throws Exception {
        // ⚠️ EVERY OTHER AMBIGUOUS TEST USES A FREE KEY, where `put` and
        // `putIfAbsent` are indistinguishable -- so round-7 review measured that
        // degrading the injector's landing write to `delegate.put(...)` left all
        // 16 tests green. That mutant CLOBBERS the slot winner, manufacturing an
        // I1 violation inside the harness for M4.13's sweep to blame on the
        // protocol. It is verbatim the reasoning already applied to duplicatePut
        // ("THE SLOT MUST ALREADY BELONG TO SOMEBODY ELSE") and never carried
        // across to this arm.
        MemoryBinStore backing = new MemoryBinStore();
        backing.putIfAbsent("taken", body("A-was-here"));
        FaultInjectingStore store = new FaultInjectingStore(backing, 1L,
                new FaultInjectingStore.Faults(0, 1.0, 0));

        assertThatThrownBy(() -> store.putIfAbsent("taken", body("B-loses")))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("landed but the response was lost");

        assertThat(read(backing, "taken"))
                .as("the injected write is still CONDITIONAL -- it lost to the slot's owner, "
                        + "and an injector that overwrote here would fabricate the very I1 "
                        + "violation the sweep exists to detect")
                .isEqualTo("A-was-here");
    }

    @Test
    void anAmbiguousPutIfAbsentWRITESTHECALLERSExactBytesNotJustSomethingOfTheRightLength()
            throws Exception {
        // ⚠️ Round-7 review measured that writing `new byte[read(body).length]` --
        // the right LENGTH, all zeroes -- survived every test, because nothing read
        // back a payload written through putIfAbsent. A chain entry of the correct
        // size full of zeroes fails to decode, and the sweep would report a
        // corrupt chain the protocol never wrote.
        MemoryBinStore backing = new MemoryBinStore();
        FaultInjectingStore store = new FaultInjectingStore(backing, 1L,
                new FaultInjectingStore.Faults(0, 1.0, 0));

        assertThatThrownBy(() -> store.putIfAbsent("payload", body("exact-bytes")))
                .isInstanceOf(IOException.class);

        assertThat(read(backing, "payload"))
                .as("what landed is the caller's bytes, not a same-length placeholder")
                .isEqualTo("exact-bytes");
    }

    @Test
    void anAmbiguousPutIfMatchSTILLLOSESToASTALEVersionAndLandsNothing() throws Exception {
        // ⚠️ BOTH putIfMatch TESTS SEEDED A VERSION THAT STILL MATCHED, so the
        // CAS-would-fail case was unconstrained: round-7 review measured that
        // degrading the landing write to an unconditional `delegate.put(k, body)`
        // -- dropping the version entirely -- left all 16 green. That mutant IS
        // the zombie write M4.16 spent three rounds diagnosing as an I2
        // violation, manufactured by the harness rather than by the protocol.
        MemoryBinStore backing = new MemoryBinStore();
        var first = backing.putIfAbsent("cas", body("v1")).orElseThrow();
        // ⚠️ Move the object on, so `first` is now STALE.
        backing.putIfMatch("cas", body("v2"), first);

        FaultInjectingStore store = new FaultInjectingStore(backing, 1L,
                new FaultInjectingStore.Faults(0, 1.0, 0));
        assertThatThrownBy(() -> store.putIfMatch("cas", body("v3"), first))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("landed but the response was lost");

        assertThat(read(backing, "cas"))
                .as("the injected write is still a CAS -- it lost to the stale version and "
                        + "changed nothing, which is precisely what a fenced writer's write must "
                        + "do")
                .isEqualTo("v2");
        // ⚠️ AND THE RECORD IS HONEST ABOUT WHAT IT MEANS. `injected()` says an
        // ambiguousPut fault fired here; it does NOT claim a write landed, and on
        // this path none did. The javadoc on the branch is corrected to match --
        // "attempts the write, then reports failure" -- because a caller genuinely
        // cannot distinguish a lost response from a lost CAS, and that
        // indistinguishability is the whole fault being modelled.
        assertThat(store.injected()).extracting(FaultInjectingStore.Injected::kind)
                .containsExactly("ambiguousPut");
    }

}
