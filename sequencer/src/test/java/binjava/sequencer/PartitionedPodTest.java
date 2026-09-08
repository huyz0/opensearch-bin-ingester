// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import binjava.binstore.Body;
import binjava.binstore.backend.MemoryBinStore;
import java.io.ByteArrayInputStream;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * A partition cuts ONE pod off while the others keep working (M4.13e).
 *
 * <p>⚠️ WHY {@code unreachable} IS NOT THIS. The injector's {@code unreachable}
 * class is a global coin flip: it has no notion of WHICH pod is calling, so it
 * cannot cut one pod off the store while another stays connected. Every pod is
 * equally likely to be hit on any given call, which models a flaky store, not a
 * partition. The shape a lease fight actually takes — one leader isolated,
 * still believing it holds the lease, while a second pod takes over and starts
 * writing — is exactly the shape a global coin flip cannot produce, because it
 * never keeps one pod down for a stretch while leaving another up.
 *
 * <p>⚠️ AND IT IS THE SHAPE I2 AND I5 ARE ABOUT. A partitioned leader is the
 * canonical zombie: it cannot renew, it does not know it has been fenced, and
 * whatever it acknowledges in that window is what I5 exists to constrain. The
 * simulation recorded 0 zombie writes over 200 seeds precisely because nothing
 * could hold a pod down long enough to produce one.
 *
 * <p>⚠️ PARTITION IS NOT A PROBABILITY, and that is deliberate. The other four
 * classes are per-call rates; a partition is a STATE with a duration. Modelling
 * it as a rate would give a pod that is cut off for one call and back for the
 * next, which is the flaky store again under a different name.
 */
class PartitionedPodTest {

    private static final String PREFIX = "bins/cluster-a";

    private static Body body(String s) {
        byte[] b = s.getBytes();
        return new Body(b.length, () -> new ByteArrayInputStream(b));
    }

    private static FaultInjectingStore clean(MemoryBinStore backing) {
        return new FaultInjectingStore(backing, 1L, FaultInjectingStore.Faults.none());
    }

    @Test
    void aPARTITIONEDPodCannotReachTheStoreAtAll() throws Exception {
        MemoryBinStore backing = new MemoryBinStore();
        FaultInjectingStore store = clean(backing);

        store.actingAs("pod-a");
        store.partition("pod-a");

        // ⚠️ EVERY VERB, not just writes. A partition is a network fact: a pod
        // that cannot PUT cannot GET, LIST or STAT either. A model that cut
        // only writes would let an isolated leader keep reading the chain and
        // discover its own fencing, which is the one thing a real partition
        // guarantees it cannot do.
        assertThatThrownBy(() -> store.putIfAbsent("k", body("v")))
                .as("writes fail").hasMessageContaining("partition");
        assertThatThrownBy(() -> store.get("k"))
                .as("reads fail too").hasMessageContaining("partition");
        assertThatThrownBy(() -> store.list(PREFIX, null, 10))
                .as("and listing").hasMessageContaining("partition");
        assertThatThrownBy(() -> store.stat("k"))
                .as("and stat").hasMessageContaining("partition");
    }

    @Test
    void EVERYVerbIsGatedNotJustTheFourThatWereEasyToAssert() throws Exception {
        // ⚠️ NINE VERBS, NAMED ONE BY ONE. A first version of this test checked
        // four -- putIfAbsent, get, list, stat -- while its own comment claimed
        // "EVERY VERB". Review MEASURED the gap: deleting the guard from
        // `putIfMatch`, `put`, `delete`, `getRange` and `multipart` left all
        // 390 tests green. `putIfMatch` is the LEASE RENEW, so with that one
        // guard gone a partitioned leader keeps renewing, is never fenced, and
        // never becomes the zombie this fault class exists to produce -- the
        // sweep would report 0 zombie writes with every assertion green, which
        // is the silent result the class was written to fix.
        MemoryBinStore backing = new MemoryBinStore();
        var seeded = new MemoryBinStore();
        var version = seeded.putIfAbsent("m", body("v")).orElseThrow();
        FaultInjectingStore onSeeded = clean(seeded);
        FaultInjectingStore store = clean(backing);
        for (FaultInjectingStore s : List.of(store, onSeeded)) {
            s.actingAs("pod-a");
            s.partition("pod-a");
        }

        record Verb(String name, org.assertj.core.api.ThrowableAssert.ThrowingCallable call) {
        }
        List<Verb> verbs = List.of(
                new Verb("putIfAbsent", () -> store.putIfAbsent("k", body("v"))),
                new Verb("putIfMatch", () -> onSeeded.putIfMatch("m", body("v2"), version)),
                new Verb("put", () -> store.put("k", body("v"))),
                new Verb("get", () -> store.get("k")),
                new Verb("getRange", () -> store.getRange("k", 0, 1)),
                new Verb("stat", () -> store.stat("k")),
                new Verb("list", () -> store.list(PREFIX, null, 10)),
                new Verb("delete", () -> store.delete(List.of("k"))),
                new Verb("multipart", () -> store.multipart("k")));

        for (Verb v : verbs) {
            assertThatThrownBy(v.call())
                    .as("%s must be refused for a partitioned pod -- a partition is a "
                            + "network fact, not a write-path policy", v.name())
                    .hasMessageContaining("partition");
        }
        assertThat(verbs).as("and every verb the SPI declares is covered here")
                .hasSize(9);
    }

    @Test
    void theActorIsSTICKYAndNullIsHowACallerSaysItIsNotAPod() throws Exception {
        // ⚠️ THE FAIL-OPEN IS NOT AUTOMATIC, and review measured the gap: the
        // actor persists until it is changed, so a caller that never sets one
        // INHERITS whoever acted last. An invariant checker running after a
        // partitioned writer was refused with that writer's partition, and
        // would have reported a violation describing the harness rather than
        // the system. `anUNSETActorIsNeverPartitioned` could not see it,
        // because it exercises a virgin store -- a state that never recurs
        // once a driver starts acting as pods.
        MemoryBinStore backing = new MemoryBinStore();
        FaultInjectingStore store = clean(backing);
        store.actingAs("pod-a");
        store.partition("pod-a");
        assertThatThrownBy(() -> store.putIfAbsent("k", body("v")))
                .hasMessageContaining("partition");

        assertThatThrownBy(() -> store.list(PREFIX, null, 10))
                .as("a later caller that sets NO actor inherits pod-a -- this is the "
                        + "sticky behaviour, pinned so nobody is surprised by it")
                .hasMessageContaining("partition");

        store.actingAs(null);
        assertThat(store.putIfAbsent("k", body("v")))
                .as("and null is how a caller says 'I am not a pod' -- the only thing "
                        + "that actually makes the fail-open true")
                .isPresent();
    }

    @Test
    void anotherPodKeepsWorkingThroughTheSamePartition() throws Exception {
        MemoryBinStore backing = new MemoryBinStore();
        FaultInjectingStore store = clean(backing);
        store.partition("pod-a");

        store.actingAs("pod-b");
        assertThat(store.putIfAbsent("shared", body("b wrote this")))
                .as("pod-b is not partitioned and proceeds normally")
                .isPresent();

        store.actingAs("pod-a");
        assertThatThrownBy(() -> store.putIfAbsent("other", body("a cannot")))
                .as("while pod-a stays cut off")
                .hasMessageContaining("partition");

        // ⚠️ THE ASYMMETRY IS THE WHOLE POINT. Both pods share one store
        // object; what differs is who is calling. That is the notion the
        // injector lacked, and without it "partitioned leaders" could only be a
        // consequence of some other fault rather than a fault in its own right.
        assertThat(backing.stat("shared")).as("b's write really landed").isPresent();
        assertThat(backing.stat("other")).as("a's write really did not").isEmpty();
    }

    @Test
    void healingLetsThePodBackIn() throws Exception {
        MemoryBinStore backing = new MemoryBinStore();
        FaultInjectingStore store = clean(backing);
        store.actingAs("pod-a");
        store.partition("pod-a");
        assertThatThrownBy(() -> store.putIfAbsent("k", body("v")))
                .hasMessageContaining("partition");

        store.heal("pod-a");

        // ⚠️ A PARTITION THAT NEVER HEALS IS A CRASH, and the two have different
        // consequences: a crashed leader never comes back to discover it was
        // fenced, so the seal protocol's losing branch is never exercised from
        // that side. Healing is what turns an isolated pod into a ZOMBIE.
        assertThat(store.putIfAbsent("k", body("v")))
                .as("the pod rejoins and its writes land again").isPresent();
    }

    @Test
    void anUNSETActorIsNeverPartitioned() throws Exception {
        MemoryBinStore backing = new MemoryBinStore();
        FaultInjectingStore store = clean(backing);
        store.partition("pod-a");

        // ⚠️ FAIL OPEN HERE, DELIBERATELY, and this is the one place in this
        // file where that is right. Callers that are not a pod -- the invariant
        // checkers, the readers the sweep judges with -- must not be caught by
        // a partition aimed at a writer. A checker that could be partitioned
        // would report violations that describe the harness, not the system.
        assertThat(store.putIfAbsent("k", body("v")))
                .as("a caller with no pod identity is not subject to a partition")
                .isPresent();
    }

    @Test
    void theInjectedRecordNamesTheClassSoTheSweepCanSeeItFire() throws Exception {
        MemoryBinStore backing = new MemoryBinStore();
        FaultInjectingStore store = clean(backing);
        store.actingAs("pod-a");
        store.partition("pod-a");
        try {
            store.putIfAbsent("k", body("v"));
        } catch (Exception expected) {
            // the point is the record, below
        }
        assertThat(store.injected())
                .as("a partition is a fault class like any other and must be countable, "
                        + "or FaultClassEvidenceTest cannot tell it fired")
                .anySatisfy(i -> assertThat(i.kind()).isEqualTo("partition"));
    }
}
