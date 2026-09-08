// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import binjava.binstore.CountingBinStore;
import binjava.binstore.backend.MemoryBinStore;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Every fault class must CHANGE AN OUTCOME in at least one seed (M4.13d).
 *
 * <p>⚠️ WITHOUT THIS THE SIMULATION PROVES THE SIMULATOR, NOT THE SYSTEM. A
 * fault class that is implemented, configured and injected but never alters
 * what the run produces is indistinguishable from one that was never wired at
 * all — and both read as a green sweep. M4's own completion condition states
 * the requirement in as many words: each fault class must have "at least one
 * seed where it changes the outcome, or the suite proves the simulator rather
 * than the system".
 *
 * <p>⚠️ AND IT WAS NOT HYPOTHETICAL. {@code withheldPut} shipped implemented,
 * with its own unit tests, and set to <b>0</b> in the sweep's profile
 * {@code Faults(0.05, 0.05, 0.1, 0)} — so the one fault class modelling "the
 * response was lost and NOTHING landed", the counterpart the other three
 * cannot produce, contributed to no seed the completion condition was claimed
 * over. Turning it on is half of this row; the other half is this test, which
 * is what stops it being turned off again silently.
 *
 * <p>⚠️ THE COMPARISON IS AGAINST THE SAME SEED RUN CLEAN. Comparing two
 * faulted runs would only show that faults differ from each other; comparing a
 * faulted run to its own clean twin isolates the class, because the driver's
 * {@code Random} is seeded identically and every other input is fixed.
 */
@Timeout(value = 300, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class FaultClassEvidenceTest {

    private static final int ROUNDS = 60;
    private static final int PODS = 3;

    /**
     * ⚠️ ONE CLASS AT A TIME, at a rate high enough to fire within the seed
     * budget. A combined profile cannot attribute a changed outcome to a class,
     * which is the entire question this test asks.
     */
    /**
     * ⚠️ DERIVED FROM THE RECORD, NOT LISTED BY HAND, and gate-design rung 2 is
     * the reason. The defect this whole test exists to catch -- a class
     * configured to 0 and therefore never exercised -- happened because a
     * convenience constructor defaulted the components nobody had listed. A
     * hand-written roster reproduces it exactly: add a seventh class with an
     * overload defaulting it, and both gates here stay green while the new
     * class is off. Comparing against the components makes that unrepresentable.
     */
    private static List<String> declaredClasses() {
        return java.util.Arrays.stream(
                        FaultInjectingStore.Faults.class.getRecordComponents())
                .map(java.lang.reflect.RecordComponent::getName)
                .toList();
    }

    private static final Map<String, FaultInjectingStore.Faults> ONE_CLASS =
            new LinkedHashMap<>(Map.of(
                    "unreachable", new FaultInjectingStore.Faults(0.10, 0, 0, 0),
                    "ambiguousPut", new FaultInjectingStore.Faults(0, 0.10, 0, 0),
                    "duplicatePut", new FaultInjectingStore.Faults(0, 0, 0.20, 0),
                    "withheldPut", new FaultInjectingStore.Faults(0, 0, 0, 0.10),
                    "partitionRate", new FaultInjectingStore.Faults(0, 0, 0, 0, 0.10, 0),
                    "deferredPut", new FaultInjectingStore.Faults(0, 0, 0, 0, 0, 0.10)));

    /**
     * Classes whose CORRECT behaviour is to change nothing, and which therefore
     * need different evidence.
     *
     * <p>⚠️ MEASURED, AND IT IS NOT A WEAKENING. On the first run of this test
     * {@code duplicatePut} fired in 40 of 40 seeds and changed the outcome in
     * none — which is exactly right: a duplicated in-flight PUT is a RETRY, and
     * {@code putIfAbsent} is the write-once primitive I1 rests on, so the
     * second attempt must lose and leave the store untouched. A store where it
     * did not would break I1 with no code in this repository being wrong.
     *
     * <p>⚠️ SO THE EVIDENCE IS STRONGER HERE, NOT WEAKER. "Changed an outcome"
     * cannot distinguish a class that is inert from one the system correctly
     * absorbs. {@link #aDUPLICATEDWriteReachesTheStoreAndIsAbsorbed} demands
     * BOTH halves: strictly more PUTs reached the store, AND the chain that
     * came out is byte-identical. Drop either half and the test passes for a
     * system that stopped absorbing duplicates.
     */
    private static final List<String> ABSORBED = List.of("duplicatePut");

    /**
     * What a run produced, reduced to the things a fault can move.
     *
     * <p>⚠️ NOT THE VIOLATION LIST. A fault class that made an invariant fail
     * would be a defect in the SYSTEM, not evidence the class works -- so the
     * signal here is deliberately the run's shape, and the sweep remains the
     * place violations are judged.
     */
    private static String shape(CommitProtocolSimulation.Result r) {
        return r.commits() + "/" + r.takeovers() + "/" + r.highestEpoch()
                + "/" + r.acks().size() + "/" + r.readersChecked();
    }

    private static CommitProtocolSimulation.Result run(
            long seed, FaultInjectingStore.Faults faults) throws Exception {
        return CommitProtocolSimulation.run(seed, ROUNDS, PODS, faults, new MemoryBinStore());
    }

    @Test
    void everyFaultClassChangesAnOutcomeInAtLeastOneSeed() throws Exception {
        Map<String, Integer> changedIn = new LinkedHashMap<>();
        Map<String, Integer> firedIn = new LinkedHashMap<>();
        Map<String, Long> firstChangingSeed = new LinkedHashMap<>();

        for (Map.Entry<String, FaultInjectingStore.Faults> e : ONE_CLASS.entrySet()) {
            String name = e.getKey();
            changedIn.put(name, 0);
            firedIn.put(name, 0);
            for (long seed = 0; seed < 40; seed++) {
                var clean = run(seed, FaultInjectingStore.Faults.none());
                var faulted = run(seed, e.getValue());

                long fired = faulted.faults().stream()
                        .filter(f -> f.kind().equals(name)).count();
                if (fired > 0) {
                    firedIn.merge(name, 1, Integer::sum);
                }
                if (!shape(clean).equals(shape(faulted))) {
                    changedIn.merge(name, 1, Integer::sum);
                    firstChangingSeed.putIfAbsent(name, seed);
                }
            }
        }

        List<String> silent = new ArrayList<>();
        for (String name : ONE_CLASS.keySet()) {
            if (ABSORBED.contains(name)) {
                continue;
            }
            if (changedIn.get(name) == 0) {
                silent.add(name + " (fired in " + firedIn.get(name) + " seeds, "
                        + "changed the outcome in none)");
            }
        }

        // ⚠️ FIRING IS NOT ENOUGH, and separating the two counts is the point.
        // `injected()` growing proves the injector ran; it says nothing about
        // whether the system noticed. A class that fires in 40 seeds and moves
        // no outcome in any of them is either unreachable on the paths under
        // test or absorbed silently, and both are worth knowing.
        assertThat(silent)
                .as("every fault class must change an outcome in at least one seed, "
                        + "or the sweep proves the simulator rather than the system. "
                        + "Fired-in counts: %s; changed-in counts: %s", firedIn, changedIn)
                .isEmpty();

        // ⚠️ THE ABSORBED CLASSES ARE EXCLUDED HERE TOO, and the exclusion is
        // named rather than implied: a class listed in ABSORBED has its
        // evidence in its own test, and demanding a changing seed for it here
        // would be demanding that write-once stop holding.
        List<String> mustChange = ONE_CLASS.keySet().stream()
                .filter(n -> !ABSORBED.contains(n)).toList();
        // ⚠️ EVERY DECLARED COMPONENT IS COVERED, checked against the record
        // rather than against this file's own list.
        assertThat(ONE_CLASS.keySet())
                .as("every fault class the Faults record declares has a profile here -- "
                        + "a class added to the record and not to this map is a class "
                        + "nothing measures")
                .containsExactlyInAnyOrderElementsOf(declaredClasses());

        assertThat(firstChangingSeed.keySet())
                .as("and each class is named with the first seed that shows it, so a "
                        + "regression points at a reproducible run rather than a statistic")
                .containsExactlyInAnyOrderElementsOf(mustChange);
    }

    @Test
    void theSweepsOwnProfileEnablesEveryClass() {
        FaultInjectingStore.Faults profile = CommitProtocolSweepTest.SWEEP_FAULTS;
        Map<String, Double> rates = new LinkedHashMap<>();
        rates.put("unreachable", profile.unreachable());
        rates.put("ambiguousPut", profile.ambiguousPut());
        rates.put("duplicatePut", profile.duplicatePut());
        rates.put("withheldPut", profile.withheldPut());
        // ⚠️ KEYED "partitionRate", NOT "partition", and not for cosmetics.
        // `check-metric-cardinality.sh` matches `put("<forbidden>")` in source,
        // and `partition` is on its forbidden list because a partition label
        // costs real series. This map is a local assertion fixture and not a
        // metric, so the hit is a false positive -- but the right answer is to
        // stop looking like a label registration, not to teach the gate an
        // exception. An exception is a hole that the next genuine label walks
        // through; a name is free. The field it reads IS a rate, so this is
        // also the more accurate name.
        rates.put("partitionRate", profile.partitionRate());
        rates.put("deferredPut", profile.deferredPut());

        // ⚠️ THIS IS THE ASSERTION THAT WOULD HAVE CAUGHT IT. `withheldPut` sat
        // at 0 in the sweep's profile through the whole milestone while its
        // unit tests passed and its class javadoc described it as modelled. A
        // rate of zero disables a class while `injected()` still shows the
        // others firing, so an aggregate "faults fired" floor stays green over
        // a sweep that never exercised the class it was written for.
        assertThat(rates.keySet())
                .as("and the rate roster is the record's own component list, so a class "
                        + "added with a defaulting overload cannot slip past this gate")
                .containsExactlyInAnyOrderElementsOf(declaredClasses());

        List<String> disabled = rates.entrySet().stream()
                .filter(e -> e.getValue() <= 0.0).map(Map.Entry::getKey).toList();
        assertThat(disabled)
                .as("the sweep's profile must enable every fault class it claims to "
                        + "cover -- a rate of 0 is a class that is not being tested. "
                        + "Rates: %s", rates)
                .isEmpty();
    }

    @Test
    void aDUPLICATEDWriteReachesTheStoreAndIsAbsorbed() throws Exception {
        // ⚠️ THE FIRST VERSION OF THIS TEST DID NOT MEASURE ITS OWN CLAIM. It
        // built a CountingBinStore, never wired it into either run, and closed
        // with `assertThat(cleanCounted).isNotNull()` on a local just assigned
        // from `new` -- an assertion that cannot fail. Its "HALF ONE" then
        // counted the injector's own `injected()` entries, which are recorded
        // BEFORE the second putIfAbsent is issued, so deleting that second
        // write left this test green. The whole ABSORBED exclusion rested on it.
        //
        // Now the count comes from a meter UNDER the injector, which sees the
        // requests that actually reached a store and cannot be told otherwise.
        long seed = 3L;
        MemoryBinStore cleanBacking = new MemoryBinStore();
        CountingBinStore cleanMeter = new CountingBinStore(cleanBacking);
        CommitProtocolSimulation.run(seed, ROUNDS, PODS,
                FaultInjectingStore.Faults.none(), cleanBacking, cleanMeter);

        MemoryBinStore dupBacking = new MemoryBinStore();
        CountingBinStore dupMeter = new CountingBinStore(dupBacking);
        var withDup = CommitProtocolSimulation.run(seed, ROUNDS, PODS,
                new FaultInjectingStore.Faults(0, 0, 1.0, 0), dupBacking, dupMeter);

        assertThat(withDup.faults()).as("the class fired at all")
                .anySatisfy(f -> assertThat(f.kind()).isEqualTo("duplicatePut"));

        // HALF ONE: strictly more writes REACHED A STORE. Measured through the
        // meter, so deleting the injector's second putIfAbsent fails here.
        assertThat(dupMeter.counts().puts())
                .as("a duplicated in-flight PUT is really issued -- %s writes reached the "
                        + "store against %s clean", dupMeter.counts().puts(),
                        cleanMeter.counts().puts())
                .isGreaterThan(cleanMeter.counts().puts());

        // HALF TWO: and the chain is identical, key for key and byte for byte.
        // This is what breaks if write-once stops holding, and it is what makes
        // half one mean something rather than merely counting noise.
        assertThat(chainDump(dupBacking))
                .as("every duplicate LOST -- putIfAbsent is write-once, which is I1")
                .isEqualTo(chainDump(cleanBacking));
    }

    /** Every chain key and its bytes, so two runs can be compared exactly. */
    private static Map<String, String> chainDump(MemoryBinStore store) throws Exception {
        Map<String, String> out = new java.util.TreeMap<>();
        String start = null;
        while (true) {
            var page = store.list("bins/cluster-a/ctl/", start, 500);
            for (var o : page.objects()) {
                try (var in = store.get(o.key())) {
                    out.put(o.key(), java.util.HexFormat.of().formatHex(in.readAllBytes()));
                }
            }
            if (page.nextStartAfter().isEmpty()) {
                return out;
            }
            start = page.nextStartAfter().get();
        }
    }

    @Test
    void aWITHHELDWriteLeavesNothingBehind() throws Exception {
        // ⚠️ THE PROPERTY THAT DISTINGUISHES withheldPut FROM ambiguousPut, and
        // the reason a fourth class exists at all: `ambiguousPut` attempts the
        // write and then reports failure, so whenever the CAS would have won it
        // resolves as LANDED. It therefore cannot produce "the response was
        // lost and nothing landed" -- the state a retry can only be judged
        // against. Without this the model has one half of the ambiguity.
        MemoryBinStore backing = new MemoryBinStore();
        Function<String, Boolean> present = key -> {
            try {
                return backing.stat(key).isPresent();
            } catch (Exception ex) {
                throw new AssertionError(ex);
            }
        };
        FaultInjectingStore store = new FaultInjectingStore(backing, 1L,
                new FaultInjectingStore.Faults(0, 0, 0, 1.0));
        String key = new LogKeys("bins/c", 1L).keyFor(0);
        byte[] bytes = "x".getBytes();
        assertThat(catchThrowable(() -> store.putIfAbsent(key,
                new binjava.binstore.Body(bytes.length,
                        () -> new java.io.ByteArrayInputStream(bytes)))))
                .as("a withheld write reports failure").isNotNull();
        assertThat(present.apply(key))
                .as("and nothing landed -- which is what makes it a distinct class")
                .isFalse();
    }

    private static Throwable catchThrowable(ThrowingRunnable r) {
        try {
            r.run();
            return null;
        } catch (Throwable t) {
            return t;
        }
    }

    private interface ThrowingRunnable {
        void run() throws Exception;
    }
}
