// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import binjava.format.FetchMode;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.lang.reflect.RecordComponent;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * A consumer cannot ask for a fetch mode, because there is nothing to ask with
 * (M5.11, FR-6).
 *
 * <p>⚠️ ABSENCE NEEDS A TEST OR IT IS NOT A PROPERTY. This is the same lesson
 * {@code MembershipSeamTest} was written for: "the ingester chooses" is a
 * sentence in FR-6 until something fails when it stops being true. M5's SPEC
 * names the falsifier for this row in as many words -- "a policy that lets the
 * consumer choose" -- and names the failure mode it produces, "`direct`
 * becomes the default by accident". ADR-0004 priced that accident at
 * $3,732/month.
 *
 * <p>⚠️ AND IT IS A TEST DOING A GATE'S JOB, which is worth saying rather than
 * hiding. "No input to the fetch policy names a mode" is a predicate over
 * files in the tree, so non-negotiable 9 puts it at rung 3 and this is rung 6.
 * The edges are written here because a test can only reach the types it
 * imports: a NEW class elsewhere taking a caller-supplied {@code FetchMode}
 * is invisible to this file. {@code M0.108} already owns exactly this problem
 * for the membership seam and covers this one too.
 */
class FetchPolicySeamTest {

    /**
     * No public entry point into the policy ACCEPTS a {@link FetchMode}.
     *
     * <p>⚠️ THE DIRECTION IS THE WHOLE ASSERTION. {@code modeFor} RETURNS a
     * mode, which is its job; a mode arriving as an ARGUMENT is a consumer's
     * preference reaching the decision. An overload
     * {@code modeFor(SegmentDelivery, Capabilities, FetchMode preferred)} is
     * all it would take, and it would look like a courtesy.
     */
    @Test
    void noPublicENTRYPointIntoTheFetchPolicyTAKESAFetchMode() {
        List<String> offenders = new ArrayList<>();
        // ⚠️ NO DECLARING-CLASS FILTER. Round-2 review MEASURED that
        // `m.getDeclaringClass() == FetchPolicy.class` blinded this to the
        // whole INHERITANCE surface: a `BasePolicy` declaring
        // `modeFor(SegmentDelivery, Capabilities, FetchMode preferred)` with
        // `FetchPolicy extends BasePolicy` left the suite green while
        // `policy.modeFor(d, caps, DIRECT)` was publicly callable. The filter
        // bought nothing -- `getMethods()` is already public-only, and
        // `Object`'s methods take no `FetchMode`.
        for (Method m : FetchPolicy.class.getMethods()) {
            namesAMode("method " + m.getName(), m.getGenericParameterTypes(), offenders);
        }
        // ⚠️ CONSTRUCTORS TOO. Review MEASURED a second public constructor
        // `FetchPolicy(FetchPolicyConfig, FetchMode)` surviving a scan that
        // read only `getMethods()` -- a preference taken once at wiring time
        // and returned ahead of every rule, which is the same defect with a
        // longer fuse than an overload.
        for (var c : FetchPolicy.class.getConstructors()) {
            namesAMode("constructor", c.getGenericParameterTypes(), offenders);
        }
        assertThat(offenders)
                .as("a mode arriving as an ARGUMENT is the consumer choosing; FR-6 says the "
                        + "ingester does, and ADR-0004 priced the alternative at $3,732/month")
                .isEmpty();
    }

    /**
     * Does {@code type} mention {@link FetchMode} anywhere a caller could put
     * one?
     *
     * <p>⚠️ THREE SHAPES, AND ROUND-1 REVIEW MEASURED ALL THREE SURVIVING a
     * plain {@code FetchMode.class.isAssignableFrom(p.getType())}: a varargs
     * {@code FetchMode...}, whose parameter type is {@code FetchMode[]} and is
     * NOT assignable from {@code FetchMode}; an {@code Optional<FetchMode>},
     * which erases; and a {@code List<FetchMode>}, likewise. Each reads as a
     * courtesy at the call site and each is the consumer choosing.
     *
     * <p>⚠️ WHAT IT STILL CANNOT SEE, said rather than implied: a mode encoded
     * as a {@code String} or an {@code int}, and any NEW class elsewhere that
     * takes one. Both are why {@code M0.107}/{@code M0.108} exist -- a
     * predicate over files in the tree belongs in a script, and this is rung 6
     * standing in for rung 3.
     */
    private static void namesAMode(String where, java.lang.reflect.Type[] types,
            List<String> offenders) {
        for (java.lang.reflect.Type t : types) {
            if (mentionsFetchMode(t)) {
                offenders.add(where + " takes " + t.getTypeName());
            }
        }
    }

    private static boolean mentionsFetchMode(java.lang.reflect.Type t) {
        return mentionsFetchMode(t, new java.util.HashSet<>());
    }

    /**
     * ⚠️ IT RECURSES INTO A RECORD'S OWN COMPONENTS, because round-2 review
     * MEASURED two survivors that hid a mode one level down: a
     * {@code record Hint(FetchMode wanted)} as a fifth {@code SegmentDelivery}
     * component, and a {@code record ConsumerHints(Optional<FetchMode> wanted)}
     * accepted by a public overload. Neither component name hits the substring
     * list, and the outer type is not a {@code FetchMode}. ⚠️ M5.14 is one row
     * away and introduces exactly such a record, so this is the shape most
     * likely to arrive next.
     *
     * <p>⚠️ {@code seen} IS NOT DEFENSIVE PADDING: a record may reference its
     * own type through a nested collection, and without it this recurses until
     * the stack ends.
     */
    private static boolean mentionsFetchMode(java.lang.reflect.Type t, java.util.Set<Type> seen) {
        if (!seen.add(t)) {
            return false;
        }
        if (t instanceof Class<?> c) {
            if (FetchMode.class.isAssignableFrom(c)) {
                return true;
            }
            if (c.isArray()) {
                return mentionsFetchMode(c.getComponentType(), seen);
            }
            if (c.isRecord()) {
                for (RecordComponent rc : c.getRecordComponents()) {
                    if (mentionsFetchMode(rc.getGenericType(), seen)) {
                        return true;
                    }
                }
            }
            return false;
        }
        if (t instanceof java.lang.reflect.ParameterizedType pt) {
            for (java.lang.reflect.Type arg : pt.getActualTypeArguments()) {
                if (mentionsFetchMode(arg, seen)) {
                    return true;
                }
            }
            return mentionsFetchMode(pt.getRawType(), seen);
        }
        if (t instanceof java.lang.reflect.GenericArrayType ga) {
            return mentionsFetchMode(ga.getGenericComponentType(), seen);
        }
        return false;
    }

    /**
     * The decision's input record carries no mode and no preference.
     *
     * <p>⚠️ A RECORD COMPONENT IS THE CHEAPER WAY IN than a method overload,
     * because it needs no new signature and reads as data rather than as a
     * choice. {@code SegmentDelivery} is what a caller fills in, so a
     * {@code preferredMode} there would be honoured by any implementation that
     * simply read its own input.
     */
    @Test
    void theDeliveryRecordCarriesNOModeAndNOPreference() {
        List<String> offenders = new ArrayList<>();
        for (RecordComponent c : SegmentDelivery.class.getRecordComponents()) {
            String name = c.getName().toLowerCase(java.util.Locale.ROOT);
            // ⚠️ THE GENERIC TYPE, NOT THE RAW ONE, and the name checked
            // widely: review MEASURED that `String via` -- doc 04's own word
            // for the field -- matched neither the type test nor the two
            // substrings this once had.
            if (mentionsFetchMode(c.getGenericType())
                    || name.contains("mode") || name.contains("prefer")
                    || name.contains("via") || name.contains("request")) {
                offenders.add(c.getName() + ":" + c.getGenericType().getTypeName());
            }
        }
        assertThat(offenders)
                .as("the consumer fills this record in, so a mode or a preference on it is a "
                        + "request the ingester would grant by doing nothing")
                .isEmpty();
    }

    /**
     * The policy holds no store, no socket and no clock (non-negotiable 7).
     *
     * <p>⚠️ THE RULE HAS NO SCRIPT -- that is {@code M0.107}, opened by M5.8
     * for the membership seam -- so this asserts it for the one class where
     * breaking it would be easiest. A policy that read the segment's size from
     * the store, or the pod's pressure from a gauge behind a socket, would
     * need a fixture to test and would have moved the decision out of the
     * layer that can be tested at all.
     *
     * <p>⚠️ {@code Capabilities} IS NOT A STORE and is deliberately allowed: it
     * is a value record the pod already read at startup, which is why
     * {@code modeFor} can take it without reaching for anything.
     */
    @Test
    void thePolicyHoldsNOStoreNOSocketAndNOClock() {
        List<String> offenders = new ArrayList<>();
        // ⚠️ THE WHOLE OF `java.time`, NOT `java.time.Clock`. Review MEASURED
        // an `InstantSource` field READ INSIDE `modeFor` surviving a prefix
        // match on `java.time.Clock` -- a seam by another name.
        // ⚠️ AND THE CONFIG IS SCANNED TOO, because a store or a clock reached
        // THROUGH `FetchPolicyConfig` is held just as surely as one held here.
        for (var f : FetchPolicy.class.getDeclaredFields()) {
            offenders.addAll(ioOffenders("FetchPolicy." + f.getName(), f.getGenericType()));
        }
        for (var c : FetchPolicyConfig.class.getRecordComponents()) {
            offenders.addAll(
                    ioOffenders("FetchPolicyConfig." + c.getName(), c.getGenericType()));
        }
        // ⚠️ AND THE PARAMETERS, NOT ONLY THE FIELDS. Criterion 3 says the
        // policy TOUCHES no store; a scan of fields alone asserts only that it
        // HOLDS none. Round-2 review MEASURED
        // `modeFor(SegmentDelivery, Capabilities, BinStore store, String key)`
        // surviving -- a store handed in at the call site is the same I/O in
        // the same method, arriving by the one door the scan did not watch.
        for (Method m : FetchPolicy.class.getMethods()) {
            for (Type t : m.getGenericParameterTypes()) {
                offenders.addAll(ioOffenders("parameter of " + m.getName(), t));
            }
        }
        for (var c : FetchPolicy.class.getConstructors()) {
            for (Type t : c.getGenericParameterTypes()) {
                offenders.addAll(ioOffenders("constructor parameter", t));
            }
        }
        assertThat(offenders)
                .as("non-negotiable 7: if it needs I/O to test, it is in the wrong layer")
                .isEmpty();
    }

    /**
     * ⚠️ A FIELD SCAN CANNOT SEE A STATIC CALL, and saying so is better than a
     * green test implying otherwise: a bare {@code Instant.now()} inside
     * {@code modeFor}, or a {@code Supplier<Instant>} passed as an argument,
     * holds no field and is invisible here. {@code M0.107} owns making
     * non-negotiable 7 a script; this closes the shapes a field scan CAN see.
     */
    private static List<String> ioOffenders(String where, Type type) {
        // ⚠️ THE GENERIC TYPE, walked. Round-2 review MEASURED
        // `private final Supplier<Instant> now = Instant::now;` read inside
        // `modeFor` surviving a scan of the RAW type -- the identical
        // raw-versus-generic bug already fixed for the mode scan and missed
        // here, which is this session's recurring defect in one file.
        List<String> found = new ArrayList<>();
        collectIo(where, type, found, new java.util.HashSet<>());
        return found;
    }

    private static void collectIo(String where, Type t, List<String> found, java.util.Set<Type> seen) {
        if (!seen.add(t)) {
            return;
        }
        if (t instanceof Class<?> c) {
            String n = c.getName();
            if (n.contains("BinStore") || n.startsWith("java.time.")
                    || n.startsWith("java.net.") || n.startsWith("java.nio.channels")
                    || n.startsWith("java.io.") || n.contains("Executor")) {
                found.add(where + ":" + n);
            }
            return;
        }
        if (t instanceof java.lang.reflect.ParameterizedType pt) {
            collectIo(where, pt.getRawType(), found, seen);
            for (Type arg : pt.getActualTypeArguments()) {
                collectIo(where, arg, found, seen);
            }
        }
    }

    /**
     * The enum names exactly the three modes the contract document defines.
     *
     * <p>⚠️ THE CONTRACT IS THE RESEARCH DOCUMENT, NOT THIS ENUM.
     * {@code 04-discovery-and-tailing.md}'s "Do the bytes cost a fetch?" table
     * gives `inline`, `proxy` and `direct`; a fourth added here without going
     * back to that table would be a protocol change made in a Java file, which
     * is what `wire-format-change` exists to stop.
     */
    @Test
    void theEnumNamesEXACTLYTheThreeModesTheContractDefines() {
        assertThat(FetchMode.values())
                .containsExactly(FetchMode.INLINE, FetchMode.PROXY, FetchMode.DIRECT);
    }
}
