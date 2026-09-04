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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The fault injector's RANDOMNESS properties, as distinct from what its faults do.
 *
 * <p>⚠️ SPLIT OUT OF {@code FaultInjectingStoreTest} at 548 lines against the
 * 500-line limit. code-structure.md rule 1: split it, never raise the limit.
 * The seam is real rather than arbitrary -- everything here is about WHICH
 * stream a class draws from and WHEN, and none of it asserts what a fault does
 * to a caller.
 *
 * <p>⚠️ FIVE PROPERTIES, AND EACH ONE WAS SHIPPED BROKEN FIRST, which is the
 * argument for a file of their own: one shared Random let a disabled class
 * re-align the others; `seed * 3 + k` gave phase-locked streams; drawing lazily
 * let a FIRING class shift the streams after it; the class-to-stream WIRING was
 * unconstrained even once the scrambler was pinned; and a deleted draw is
 * invisible to any toggle comparison because it shifts both arms alike. Every
 * one was caught by measurement in review rather than by a test, and criterion
 * 1's per-class attribution rests on all five.
 */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class FaultInjectingStoreStreamsTest {


    private record Fired(Map<String, List<Integer>> byKind) {
        List<Integer> of(String kind) {
            return byKind.getOrDefault(kind, List.of());
        }
    }

    @Test
    void theTHREEFaultStreamsAreINDEPENDENTCoinsAndNotOnePhaseLocked() {
        // ⚠️ THE DEFECT THIS PINS WAS SHIPPED TWICE and caught by measurement both
        // times, never by a test -- first as one shared Random, then as
        // `seed * 3 + k`, three CONSECUTIVE java.util.Random seeds whose
        // scrambler leaves the streams phase-locked. Round-3 review measured the
        // second: at p = 0.05 over 100,000 seeds the unreachable and ambiguousPut
        // classes fired TOGETHER on draw 1 in 4,778 runs against ~250 expected --
        // 19x -- and were mutually EXCLUSIVE on draws 2, 3, 20 and 50. Until this
        // test existed, reverting the fix left the whole suite green.
        // ⚠️ STATISTICAL, AND DETERMINISTIC: every draw comes from a seed, so this
        // is a fixed computation with a fixed answer, not a flaky sample.
        int seeds = 20_000;
        double p = 0.05;
        double expected = seeds * p * p;
        // ⚠️ ALL THREE PAIRS, not just (1,2). Round-4 review measured the gap:
        // aliasing stream 3 onto stream 1 left every test green, and that alias
        // is the worst of the three -- `unreachable` throws BEFORE the duplicate
        // branch is reached, so duplicatePut would fire exactly when it is
        // unobservable, defeating criterion 1 for that class by construction.
        int[][] pairs = {{1, 2}, {1, 3}, {2, 3}};
        for (int[] pair : pairs) {
            for (int draw : new int[] {1, 2, 3, 20}) {
                int both = 0;
                for (long seed = 0; seed < seeds; seed++) {
                    if (firesAt(seed, pair[0], draw, p) && firesAt(seed, pair[1], draw, p)) {
                        both++;
                    }
                }
                assertThat(both)
                        .as("streams %d and %d fire together %d times at draw %d, against %.0f "
                                + "expected for independent coins -- phase-locked streams "
                                + "overshoot by an order of magnitude here and go mutually "
                                + "exclusive elsewhere", pair[0], pair[1], both, draw, expected)
                        .isBetween((int) (expected / 3), (int) (expected * 3));
            }
        }
    }

    private static boolean firesAt(long seed, int stream, int draw, double p) {
        java.util.Random r = new java.util.Random(FaultInjectingStore.scramble(seed, stream));
        double d = 0;
        for (int i = 0; i < draw; i++) {
            d = r.nextDouble();
        }
        return d < p;
    }

    @Test
    void eachClassIsWIREDToItsOWNStreamAndNotAliasedOntoAnothers() throws Exception {
        // ⚠️ THE DECORRELATION TEST MEASURES `scramble`, NOT THE WIRING, and
        // round-5 review measured the gap that leaves: aliasing `duplicateDraws`
        // onto stream 1, or `ambiguousDraws` onto stream 3, survived every other
        // test in this file. ⚠️ ONE SHARED `Random` OBJECT is killed by
        // `everyClassDrawsOnEVERYFaultingCall...` rather than by this test, via
        // `countDraw`'s identity labelling -- round-6 review corrected an earlier
        // comment here that claimed the credit. A perfect scrambler wired
        // wrongly is the same defect as a bad scrambler.
        // ⚠️ WHY THE ALIAS MATTERS RATHER THAN BEING UNTIDY: `unreachable` throws
        // BEFORE the duplicate branch is reached, so if those two shared a stream,
        // `duplicatePut` would fire exactly when it is unobservable -- criterion
        // 1 defeated for that class by construction.
        // ⚠️ THROUGH THE STORE, one class enabled at a time, so what is compared
        // is where each class ACTUALLY faulted rather than what the scrambler
        // returns for a literal written in this test.
        List<Integer> unreachable = callsWhereOnly(0.9, 0, 0, "unreachable");
        List<Integer> ambiguous = callsWhereOnly(0, 0.9, 0, "ambiguousPut");
        List<Integer> duplicate = callsWhereOnly(0, 0, 0.9, "duplicatePut");

        assertThat(unreachable).as("every class must actually fire, or the comparison below "
                + "is between empty lists").isNotEmpty();
        assertThat(ambiguous).isNotEmpty();
        assertThat(duplicate).isNotEmpty();

        assertThat(unreachable).as("unreachable and ambiguousPut draw from DIFFERENT streams")
                .isNotEqualTo(ambiguous);
        assertThat(unreachable).as("unreachable and duplicatePut draw from DIFFERENT streams -- "
                + "the alias that would make duplicatePut unobservable").isNotEqualTo(duplicate);
        assertThat(ambiguous).as("ambiguousPut and duplicatePut draw from DIFFERENT streams")
                .isNotEqualTo(duplicate);
    }

    private static List<Integer> callsWhereOnly(double unreachable, double ambiguous,
            double duplicate, String kind) throws IOException {
        FaultInjectingStore store = new FaultInjectingStore(new MemoryBinStore(), 3L,
                new FaultInjectingStore.Faults(unreachable, ambiguous, duplicate));
        List<Integer> at = new ArrayList<>();
        int seen = 0;
        for (int call = 0; call < 60; call++) {
            try {
                store.putIfAbsent("w" + call, body("v"));
            } catch (IOException injected) {
                // recorded from injected() below
            }
            List<FaultInjectingStore.Injected> now = store.injected();
            for (int i = seen; i < now.size(); i++) {
                if (kind.equals(now.get(i).kind())) {
                    at.add(call);
                }
            }
            seen = now.size();
        }
        return at;
    }

    @Test
    void aFIRINGClassDoesNotSHIFTTheStreamsOfTheClassesAfterIt() throws Exception {
        // ⚠️ THE OTHER HALF OF THE INDEPENDENCE PROPERTY, and the half the
        // decorrelation test above CANNOT see: that one measures `scramble` as a
        // pure function, while this defect lives in the store's CONTROL FLOW.
        // Round-4 review measured three surviving mutations there -- reverting to
        // lazy draws, and deleting either of putIfMatch's two result-discarding
        // draws -- all with the whole suite green. Both reviewers named it, so it
        // is the third time this property has been shipped on a comment.
        // ⚠️ WHAT THIS DOES NOT COVER, because round-5 review caught an earlier
        // comment claiming otherwise: it drives `putIfAbsent` only, so it pins ONE
        // of the three alignment statements. `putIfMatch`'s two result-discarding
        // draws are unreachable by any toggle comparison -- a uniformly removed
        // draw shifts both arms alike -- and
        // `everyClassDrawsOnEVERYFaultingCallSoNoClassCanBeSKIPPEDSilently`
        // counts them instead.
        // ⚠️ DETERMINISTIC, NOT STATISTICAL: hold the seed, record the CALL
        // INDEXES at which ambiguousPut fires with `unreachable` off, then on, and
        // subtract the calls where `unreachable` actually fired -- those are
        // legitimately masked by its throw. Equal sets mean the draws stayed
        // aligned. Measured: identical on 200 of 200 seeds as written, and
        // different on 200 of 200 against the lazy-draw mutant.
        for (long seed = 0; seed < 25; seed++) {
            Fired quiet = firedIndexesWith(seed, 0.0);
            Fired loud = firedIndexesWith(seed, 0.05);
            for (String kind : new String[] {"ambiguousPut", "duplicatePut"}) {
                List<Integer> unmasked = new ArrayList<>(quiet.of(kind));
                unmasked.removeAll(loud.of("unreachable"));
                assertThat(loud.of(kind))
                        .as("seed %d, class %s: turning `unreachable` on must only MASK a later "
                                + "class where it actually fired, never MOVE it -- a class that "
                                + "shifts the streams after it makes criterion 1's per-class "
                                + "attribution unobtainable", seed, kind)
                        .isEqualTo(unmasked);
            }
        }
    }

    private static Fired firedIndexesWith(long seed, double unreachable) throws IOException {
        FaultInjectingStore store = new FaultInjectingStore(new MemoryBinStore(), seed,
                new FaultInjectingStore.Faults(unreachable, 0.05, 0.05));
        Map<String, List<Integer>> byKind = new java.util.TreeMap<>();
        int seen = 0;
        for (int call = 0; call < 120; call++) {
            try {
                store.putIfAbsent("k" + call, body("v"));
            } catch (IOException injected) {
                // recorded from injected() below; nothing is inferred here
            }
            List<FaultInjectingStore.Injected> now = store.injected();
            for (int k = seen; k < now.size(); k++) {
                byKind.computeIfAbsent(now.get(k).kind(), x -> new ArrayList<>()).add(call);
            }
            seen = now.size();
        }
        return new Fired(byKind);
    }

    @Test
    void everyClassDrawsOnEVERYFaultingCallSoNoClassCanBeSKIPPEDSilently() throws Exception {
        // ⚠️ THE ALIGNMENT INVARIANT AS A COUNT, which is the only way to pin it:
        // a draw removed UNIFORMLY shifts both arms of a toggle comparison alike
        // and cancels out, so no comparison can see it. The two statements it
        // protects in `putIfMatch` are result-discarding calls that read as dead
        // code, and M4.13 is scheduled to edit exactly them.
        // ⚠️ AN ASYMMETRIC MIX, and that is round-6's correction. With only
        // conditional writes the expected map was 4/4/4, so every count was equal
        // -- which meant swapping two labels in `countDraw`, or wiring a read verb
        // to the wrong class's stream, passed. Different totals per class make the
        // map name WHICH stream each verb consumed, not merely how many draws
        // happened.
        FaultInjectingStore quiet = new FaultInjectingStore(new MemoryBinStore(), 7L,
                FaultInjectingStore.Faults.none());
        quiet.putIfAbsent("a", body("v"));
        quiet.putIfAbsent("b", body("v"));
        var version = quiet.putIfAbsent("c", body("v"));
        quiet.putIfMatch("c", body("v2"), version.orElseThrow());
        // ⚠️ The READ verbs draw ONLY the unreachable stream, which is what makes
        // the totals differ -- and what a mis-wired read verb would destroy.
        quiet.stat("a");
        quiet.get("a").close();
        quiet.list("", null, 10);

        assertThat(quiet.drawCounts())
                .as("four conditional writes draw all three streams; the three read verbs draw "
                        + "ONLY unreachable -- so the totals differ per class, and a read verb "
                        + "pointed at another class's stream shows up here")
                .containsExactlyInAnyOrderEntriesOf(
                        Map.of("unreachable", 7, "ambiguousPut", 4, "duplicatePut", 4));

        // ⚠️ AND ONE putIfMatch THAT ACTUALLY FIRES, so the branch is TAKEN. The
        // count above runs fault-free, so it cannot see draw-versus-BRANCH order:
        // round-6 review measured that moving putIfMatch's two alignment draws
        // BELOW its ambiguous throw kept the counts at 4/4/4 and the whole suite
        // green, while moving the unreachable class's positions in 39 of 40 seeds.
        var seeded = new MemoryBinStore();
        var v2 = seeded.putIfAbsent("m", body("v")).orElseThrow();
        FaultInjectingStore firing = new FaultInjectingStore(seeded, 7L,
                new FaultInjectingStore.Faults(0, 1.0, 0));
        assertThatThrownBy(() -> firing.putIfMatch("m", body("v3"), v2))
                .isInstanceOf(IOException.class);
        assertThat(firing.drawCounts())
                .as("even when the ambiguous branch is TAKEN and throws, all three streams were "
                        + "already drawn -- the draws sit ABOVE the branch, not inside it")
                .containsExactlyInAnyOrderEntriesOf(
                        Map.of("unreachable", 1, "ambiguousPut", 1, "duplicatePut", 1));
    }


    @Test
    void theSameSeedInjectsTheSameFaultsSoAFailingRunIsAPermanentRegression()
            throws Exception {
        // ⚠️ The entire value of a seeded simulation. A failing seed must be a
        // named regression test, not a story about a flake -- so nothing here
        // may read a clock or a shared source of randomness.
        var faults = new FaultInjectingStore.Faults(0.3, 0.3, 0.3);
        assertThat(kindsFor(42L, faults))
                .as("the same seed replays exactly")
                .isEqualTo(kindsFor(42L, faults))
                .as("and it is not vacuous -- this seed really does inject something")
                .isNotEmpty();
        assertThat(kindsFor(43L, faults))
                .as("and a different seed explores a different interleaving")
                .isNotEqualTo(kindsFor(42L, faults));
    }

    private static java.util.List<String> kindsFor(long seed,
            FaultInjectingStore.Faults faults) throws IOException {
        FaultInjectingStore store =
                new FaultInjectingStore(new MemoryBinStore(), seed, faults);
        for (int i = 0; i < 40; i++) {
            try {
                store.putIfAbsent("k" + i, body("v"));
            } catch (IOException expected) {
                // an injected failure is the point
            }
        }
        return store.injected().stream().map(FaultInjectingStore.Injected::kind).toList();
    }
}
