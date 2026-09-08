// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import binjava.binstore.BinStore;
import binjava.binstore.Body;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

/**
 * A store that fails the way real object stores fail, deterministically per seed.
 *
 * <p>⚠️ THE AMBIGUOUS WRITE IS THE POINT. A conditional PUT whose response is
 * lost HAS LANDED, and the caller cannot tell that from one that never
 * happened — {@code BinStore}'s contract distinguishes a lost CAS race (an
 * empty {@code Optional}) from an unreachable store (an {@code IOException}),
 * but it cannot distinguish "unreachable AFTER the write committed". Every
 * ambiguity defect this module has recorded — M4.3d's self-fencing renew,
 * M4.3g's acquisition, M4.3h's single refresh attempt — lives in that gap, and
 * none of them can be reached by a store that only fails cleanly.
 *
 * <p>⚠️ SEEDED AND REPLAYABLE. The whole value of a simulation is that a failing
 * seed is a permanent, named regression rather than a story about a flake, so
 * nothing here reads a clock or a global source of randomness.
 *
 * <p>⚠️ IT RECORDS WHAT IT INJECTED, which is not bookkeeping. The spec names
 * "the simulation proves the simulator" — a fault-injecting store that never
 * injects the fault that matters — as a milestone risk, and the only defence is
 * being able to ASSERT that a given run actually saw a given fault. A seed that
 * silently injects nothing is a green test that proves the absence of testing.
 */
// ⚠️ WHERE THE METER SITS CHANGES WHAT IT MEASURES, and an earlier version of
// this note got the vocabulary backwards -- it said "outside" while its own
// snippet showed the opposite. This project's term of art comes from
// 07-pluggable-store-abstraction.md, which writes `CountingBinStore(Governing
// BinStore(...))` and calls that "counting OUTSIDE governing". So:
//
//   new FaultInjectingStore(new CountingBinStore(backing), faults)
//       -- meter BELOW the injector: counts what actually reached the store,
//          including the requests this class ADDS (the duplicate retry, the
//          ambiguous landing write). This is what a test asserting the injector's
//          own request cost wants.
//
//   new CountingBinStore(new FaultInjectingStore(backing, faults))
//       -- meter ABOVE the injector: counts what the CALLER issued, with injected
//          extras invisible. This is what M4.9's bounded-recovery budget wants,
//          because that budget is a claim about the sequencer's behaviour and not
//          about the harness's.
//
// ⚠️ NEITHER IS "correct" IN GENERAL -- pick by which question is being asked,
// and say which in the test. What is always wrong is asserting the injector's
// added requests through a meter that cannot see them.
public final class FaultInjectingStore implements BinStore {

    /** The pod whose calls follow, until the next call to this method. */
    void actingAs(String podId) {
        this.actor = podId;
    }

    /** Cut {@code podId} off from the store until {@link #heal} is called. */
    void partition(String podId) {
        partitioned.add(podId);
    }

    /** Let {@code podId} reach the store again -- which is what makes a zombie. */
    void heal(String podId) {
        partitioned.remove(podId);
    }

    /** Whether the pod currently acting is cut off. */
    private boolean isPartitioned() {
        String who = actor;
        return who != null && partitioned.contains(who);
    }

    /**
     * Refuses the call if the acting pod is partitioned.
     *
     * <p>⚠️ EVERY VERB, not only the writes. A partition is a network fact: a
     * pod that cannot PUT cannot GET, LIST or STAT either. Cutting only writes
     * would let an isolated leader keep reading the chain and discover its own
     * fencing -- the one thing a real partition guarantees it cannot do, and
     * the thing that makes a zombie a zombie.
     */
    private void refuseIfPartitioned() throws IOException {
        if (isPartitioned()) {
            injected.add(new Injected("partition", "pod:" + actor));
            throw new IOException("injected: partition -- pod " + actor
                    + " cannot reach the store");
        }
    }

    /** What may go wrong, as probabilities in [0,1]. */
    public record Faults(double unreachable, double ambiguousPut, double duplicatePut,
            double withheldPut) {

        /**
         * ⚠️ REJECTED, not clamped, and this is rung 1 of gate-design: the class
         * javadoc names "a seed that silently injects nothing is a green test
         * that proves the absence of testing" as the risk this file exists to
         * defend against, and a probability outside [0,1] is exactly that. A
         * negative one disables its class while `injected()` still shows the
         * others firing, so an aggregate "faults fired" assertion stays green
         * over a sweep that never exercised the class it was written for.
         */
        public Faults {
            check("unreachable", unreachable);
            check("ambiguousPut", ambiguousPut);
            check("duplicatePut", duplicatePut);
            check("withheldPut", withheldPut);
        }

        private static void check(String name, double p) {
            if (!(p >= 0.0 && p <= 1.0)) {
                throw new IllegalArgumentException(
                        name + " is a probability in [0,1], not " + p);
            }
        }

        public static Faults none() {
            return new Faults(0, 0, 0, 0);
        }
    }

    /** One injected fault, for asserting a run actually exercised something. */
    public record Injected(String kind, String key) {
    }

    /**
     * Who is calling, and who is cut off (M4.13e).
     *
     * <p>⚠️ THE STORE HAD NO NOTION OF WHICH POD WAS CALLING, and that is why
     * "partitioned leaders" could not be an injectable fault. {@code
     * unreachable} is a per-call coin flip applied to everybody equally, which
     * models a flaky store; a partition keeps ONE pod down for a stretch while
     * the others stay up, which is the shape a lease fight takes and the only
     * shape that produces a zombie -- an isolated leader that cannot renew,
     * does not know it has been fenced, and keeps acknowledging.
     *
     * <p>⚠️ A STATE, NOT A RATE. The other four classes are probabilities per
     * call. A partition expressed that way would cut a pod off for one call and
     * restore it for the next, which is the flaky store again under another
     * name. It is set and cleared explicitly, and the driver decides when.
     *
     * <p>⚠️ THE ACTOR IS STICKY, NOT SCOPED, AND THAT IS A TRAP WORTH NAMING.
     * It is a plain field on a SHARED store -- not thread-local, whatever a
     * previous draft of this sentence said -- and it persists until the next
     * call to {@link #actingAs}. So a caller that never sets one does NOT run
     * un-partitioned: it inherits whoever acted last. Review measured exactly
     * that -- after {@code actingAs("pod-a")} and a partition, an invariant
     * checker calling {@code list} was refused with pod-a's partition.
     *
     * <p>⚠️ THE CONSEQUENCE, AND THE DISCIPLINE IT DEMANDS. A checker that can
     * be cut off reports violations describing the HARNESS rather than the
     * system, which is worse than no checker. Two things keep that from
     * happening and both are needed: the checkers read the backing store
     * BELOW this decorator, and the driver calls {@code actingAs(null)} before
     * the judging phase. Passing null is how a caller says "I am not a pod",
     * and it is the only thing that makes the fail-open below true.
     *
     * <p>⚠️ SINGLE-THREADED DRIVING IS REQUIRED, as it already is for the
     * per-class draw streams. One shared actor field cannot describe two pods
     * acting at once: pod-b's {@code actingAs} landing between pod-a's and
     * pod-a's write would judge pod-a against pod-b's partition state, and no
     * assertion here could see it.
     */
    private final java.util.Set<String> partitioned =
            java.util.concurrent.ConcurrentHashMap.newKeySet();
    private volatile String actor;

    private final BinStore delegate;
    /**
     * ⚠️ ONE STREAM PER FAULT CLASS, drawn UNCONDITIONALLY, and both halves of
     * that matter for criterion 1. With a single shared {@code Random} and an
     * early return when {@code p == 0}, the number of draws per call depends on
     * which classes are enabled -- so toggling one class re-aligns every other
     * class's faults for the same seed. Criterion 1 wants "at least one seed
     * where THIS class changes the outcome", and the natural way to show it is
     * to hold the seed fixed and toggle one class; under a shared stream the
     * observed difference cannot be attributed to the class that was toggled.
     * ⚠️ Derived from the seed rather than passed in, so a run is still one
     * number and a failing seed is still reproducible by that number alone.
     */
    private final Random unreachableDraws;
    private final Random ambiguousDraws;
    private final Random duplicateDraws;
    private final Random withheldDraws;
    private final Faults faults;
    private final List<Injected> injected = new ArrayList<>();
    private final Map<String, Integer> drawCounts = new java.util.TreeMap<>();

    public FaultInjectingStore(BinStore delegate, long seed, Faults faults) {
        this.delegate = delegate;
        this.unreachableDraws = new Random(scramble(seed, 1));
        this.ambiguousDraws = new Random(scramble(seed, 2));
        this.duplicateDraws = new Random(scramble(seed, 3));
        this.withheldDraws = new Random(scramble(seed, 4));
        this.faults = faults;
    }

    /**
     * ⚠️ SCRAMBLED, NOT `seed * 3 + k`, and the difference was MEASURED rather
     * than assumed. Three consecutive integers handed to {@link Random} give
     * phase-locked LCG streams: over 100,000 seeds at p = 0.05, the unreachable
     * and ambiguousPut classes fired TOGETHER on draw 1 in 4,778 runs against
     * ~250 expected -- 19x -- and were mutually exclusive on draws 2, 3, 20 and
     * 50, firing together 0-1 times. Independent knobs is not the same property
     * as independent coins, and criterion 1 wants both: toggling a class must
     * not move another's faults, AND the classes must not fire in lockstep or
     * the "faults across the range" claim describes one coin wearing four hats.
     * ⚠️ THE MEASUREMENT ABOVE WAS TAKEN OVER THREE CLASSES, before M4.13c added
     * `withheldPut` at stream 4. It is left as it was rather than restated for
     * four, because it is a record of what was OBSERVED and re-running it is not
     * what this comment is for; what carries over is the design, not the number.
     * ⚠️ splitmix64's finalizer, chosen because it is a few lines, has no state
     * of its own, and a run is still identified by ONE number.
     */
    // ⚠️ Package-private so the decorrelation test can measure it directly. The
    // property is statistical, and driving it through the store would measure
    // the store's control flow as well as the streams.
    static long scramble(long seed, int stream) {
        long z = seed * 0x9E3779B97F4A7C15L + stream * 0xBF58476D1CE4E5B9L;
        z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
        z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
        return z ^ (z >>> 31);
    }

    /**
     * ⚠️ SINGLE-THREADED DRIVING IS REQUIRED and nothing enforces it. {@code
     * injected} is a plain {@code ArrayList}, {@code drawCounts} a plain {@code
     * TreeMap}, and the four streams are drawn in per-call order -- so "the same
     * seed replays exactly" holds only while one thread drives the store.
     * ⚠️ THIS IS A NOTE TO M4.20, whose stated criterion is "many logical pods":
     * interleave them LOGICALLY, one call at a time from one thread, rather than
     * one virtual thread per pod. Concurrency there would not corrupt the store,
     * it would corrupt REPRODUCIBILITY -- and a seed that no longer replays takes
     * the whole named-regression discipline with it.
     */
    /** Every fault this run injected, in order. */
    public List<Injected> injected() {
        return List.copyOf(injected);
    }

    /**
     * ⚠️ DRAWS EVEN WHEN {@code p == 0}, so a disabled class consumes its own
     * stream and no other class's fault positions move.
     */
    private boolean fires(Random draws, double p) {
        countDraw(draws);
        return draws.nextDouble() < p;
    }

    /**
     *
     * ⚠️ COUNTED, so "every class draws on every call" is CHECKABLE rather than
     * merely inspectable. Round-5 review showed why no comparison can do it: a
     * cross-configuration test holds the seed and toggles a probability, so a
     * draw DELETED uniformly shifts both arms alike and cancels out. Deleting
     * either of {@code putIfMatch}'s two result-discarding draws survived every
     * behavioural assertion in this file for exactly that reason -- and those two
     * statements read as dead code, one tidy-up away from making criterion 1's
     * per-class attribution unobtainable.
     *
     * <p>⚠️ WHAT THIS CANNOT DISTINGUISH, stated because round-6 review measured
     * it: swapping the {@code ambiguousPut} and
     * {@code duplicatePut} labels here is UNOBSERVABLE, because both streams are
     * drawn exactly once per conditional write and neither is drawn by a read
     * verb -- so their counts are equal in every possible workload. What a count
     * CAN see is the unreachable label (the read verbs make its total differ) and
     * any draw that goes missing or moves below a branch. Which stream each CLASS
     * actually consumes is pinned by
     * {@code eachClassIsWIREDToItsOWNStreamAndNotAliasedOntoAnothers} instead,
     * which observes fault POSITIONS rather than draw counts.
     *
     * <p>⚠️ AND IT IS FIXABLE, contrary to an earlier draft of this note that
     * said otherwise: the label only needs re-deriving because the streams are
     * bare {@code Random}s identified by reference. A stream that carries its own
     * name would remove the RE-DERIVATION, which is rung 2 -- derive it from a
     * source of truth -- and not rung 1: a construction site would still pair a
     * name with a scramble index by hand, so the bad state stays representable,
     * just harder to reach. Round-8 review corrected an earlier draft that
     * claimed rung 1. Not done here because it reshapes three fields
     * and every call site for a labelling defect with no behavioural consequence
     * (the two counts are equal in every workload), and this task has already
     * split once; recorded so the next hand knows the option exists.
     */
    private void countDraw(Random which) {
        String name = which == unreachableDraws ? "unreachable"
                : which == ambiguousDraws ? "ambiguousPut"
                : which == duplicateDraws ? "duplicatePut" : "withheldPut";
        drawCounts.merge(name, 1, Integer::sum);
    }

    /** How many times each class's stream has been drawn. */
    public Map<String, Integer> drawCounts() {
        return Map.copyOf(drawCounts);
    }

    @Override
    public Optional<binjava.binstore.Version> putIfAbsent(String key, Body body)
            throws IOException {
        // ⚠️ ALL FOUR DRAWN BEFORE ANY BRANCH, and the previous version got this
        // half right and half wrong. Removing the `p == 0` early return stopped a
        // DISABLED class from re-aligning the others; it did nothing about a
        // FIRING one, because the throw below skipped the two draws after it.
        // Round-3 review MEASURED the difference: toggling `unreachable` from 0
        // to 0.05 moved the other two classes' fault positions in 2000 of 2000
        // seeds -- on seed 1, `ambiguousPut` fired at [36,49,77,93,...] with it
        // off and [39,54,82,100,...] with it on, not one index in common. That is
        // the same aliasing the shared Random had, arriving through control flow
        // instead of through the stream, and it makes criterion 1's per-class
        // attribution unobtainable exactly as before.
        boolean unreachable = fires(unreachableDraws, faults.unreachable());
        boolean ambiguous = fires(ambiguousDraws, faults.ambiguousPut());
        boolean duplicate = fires(duplicateDraws, faults.duplicatePut());
        boolean withheld = fires(withheldDraws, faults.withheldPut());
        // ⚠️ AFTER THE DRAWS, NOT BEFORE, and review MEASURED why. A guard that
        // returns before the four unconditional draws makes a partitioned call
        // consume no stream, which re-aligns every other class's fault
        // positions: at seed 7 a pod that was never partitioned had its own
        // `unreachable` faults move from [3,9,18,19,21] to [3,10,13,14,23,24,26]
        // -- one index in common out of seven. That is the same aliasing this
        // file's round-3 note removed for the other classes, arriving through
        // control flow instead of through a shared Random, and it makes
        // criterion 1's per-class attribution unobtainable.
        refuseIfPartitioned();
        if (unreachable) {
            injected.add(new Injected("unreachable", key));
            throw new IOException("injected: the store was unreachable");
        }
        if (ambiguous) {
            // ⚠️ ATTEMPTS THE WRITE, THEN REPORTS FAILURE, and the wording matters:
            // on a lost CAS or an occupied slot NOTHING lands, and `injected()`
            // records that an ambiguousPut fault FIRED rather than that a write
            // succeeded. That indistinguishability is the fault being modelled.
            // The caller sees an IOException and
            // must not assume nothing happened. This is the shape that turns a
            // healthy leader into a self-fenced one when a retry re-races its
            // own successful write.
            byte[] bytes = read(body);
            delegate.putIfAbsent(key, new Body(bytes.length, () -> new ByteArrayInputStream(bytes)));
            injected.add(new Injected("ambiguousPut", key));
            throw new IOException("injected: the write landed but the response was lost");
        }
        if (withheld) {
            // ⚠️ THE OTHER HALF OF THE SAME AMBIGUITY, and it does NOT delegate.
            // `ambiguousPut` above attempts the write and then reports failure,
            // so whenever the CAS would have won it resolves as LANDED and the
            // model can never produce "the response was lost and NOTHING
            // landed" -- the shape `AmbiguousPutStore.Mode.LOST` names and the
            // injector could not make. To the caller the two are
            // indistinguishable, which is the point; what differs is the state
            // the store is left in, and a retry can only be judged against that.
            injected.add(new Injected("withheldPut", key));
            throw new IOException("injected: the response was lost and nothing landed");
        }
        byte[] bytes = read(body);
        Optional<binjava.binstore.Version> first = delegate.putIfAbsent(key,
                new Body(bytes.length, () -> new ByteArrayInputStream(bytes)));
        if (duplicate) {
            // ⚠️ A retried in-flight PUT. It must be harmless: putIfAbsent is
            // the write-once primitive I1 rests on, so the second attempt LOSES
            // and nothing changes. A store where it did not would break I1
            // without any code in this repository being wrong.
            injected.add(new Injected("duplicatePut", key));
            delegate.putIfAbsent(key, new Body(bytes.length, () -> new ByteArrayInputStream(bytes)));
        }
        return first;
    }

    private static byte[] read(Body body) throws IOException {
        try (var in = body.open().get()) {
            return in.readAllBytes();
        }
    }

    @Override public Optional<binjava.binstore.ObjectStat> stat(String k) throws IOException {
        refuseIfPartitioned();
        if (fires(unreachableDraws, faults.unreachable())) {
            injected.add(new Injected("unreachable", k));
            throw new IOException("injected: the store was unreachable");
        }
        return delegate.stat(k);
    }

    @Override public java.io.InputStream get(String k) throws IOException {
        refuseIfPartitioned();
        if (fires(unreachableDraws, faults.unreachable())) {
            injected.add(new Injected("unreachable", k));
            throw new IOException("injected: the store was unreachable");
        }
        return delegate.get(k);
    }

    @Override public java.io.InputStream getRange(String k, long a, long b) throws IOException {
        refuseIfPartitioned();
        return delegate.getRange(k, a, b);
    }

    @Override public binjava.binstore.Version put(String k, Body b) throws IOException {
        refuseIfPartitioned();
        return delegate.put(k, b);
    }

    @Override public Optional<binjava.binstore.Version> putIfMatch(String k, Body b,
            binjava.binstore.Version v) throws IOException {
        // ⚠️ ALL FOUR DRAWN, for the reason spelled out on putIfAbsent: a class
        // that fires must not shift the streams of the classes after it.
        // ⚠️ `unreachable` IS DRAWN AND DELIBERATELY NOT ACTED ON here. The lease
        // CAS has no clean-failure fault today -- a documented gap, owned by
        // M4.13e since M4.13 was split by mechanism -- and drawing it anyway
        // is what keeps the stream
        // aligned with `putIfAbsent`, so closing that gap later moves no other
        // class's positions.
        fires(unreachableDraws, faults.unreachable());
        boolean ambiguous = fires(ambiguousDraws, faults.ambiguousPut());
        fires(duplicateDraws, faults.duplicatePut());
        boolean withheld = fires(withheldDraws, faults.withheldPut());
        // ⚠️ AFTER THE DRAWS, as on putIfAbsent -- and this is the verb that
        // matters most. `putIfMatch` is the LEASE RENEW: with no guard here a
        // partitioned leader keeps renewing, is never fenced, and never
        // becomes the zombie this whole fault class exists to produce.
        refuseIfPartitioned();
        if (ambiguous) {
            byte[] bytes = read(b);
            delegate.putIfMatch(k, new Body(bytes.length,
                    () -> new ByteArrayInputStream(bytes)), v);
            injected.add(new Injected("ambiguousPut", k));
            throw new IOException("injected: the write landed but the response was lost");
        }
        if (withheld) {
            // ⚠️ THE VERSION DOES NOT MOVE, which is the whole reason the class
            // exists: it is what makes "the renew threw, nothing landed, and the
            // retry with the SAME version must succeed" reachable. Against the
            // LANDED arm that retry always loses, so a lease that treats every
            // ambiguous CAS failure as possible fencing is indistinguishable
            // from a correct one until this arm can fire.
            injected.add(new Injected("withheldPut", k));
            throw new IOException("injected: the response was lost and nothing landed");
        }
        return delegate.putIfMatch(k, b, v);
    }

    @Override public binjava.binstore.MultipartWriter multipart(String k) throws IOException {
        refuseIfPartitioned();
        return delegate.multipart(k);
    }

    @Override public binjava.binstore.ListPage list(String p, String a, int m)
            throws IOException {
        if (fires(unreachableDraws, faults.unreachable())) {
            injected.add(new Injected("unreachable", p));
            throw new IOException("injected: the store was unreachable");
        }
        refuseIfPartitioned();
        return delegate.list(p, a, m);
    }

    @Override public void delete(java.util.List<String> keys) throws IOException {
        refuseIfPartitioned();
        delegate.delete(keys);
    }

    @Override public binjava.binstore.Capabilities capabilities() {
        return delegate.capabilities();
    }

    @Override public void close() throws IOException {
        delegate.close();
    }
}
